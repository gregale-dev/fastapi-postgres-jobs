"""A durable job queue in Postgres.

Workers claim jobs with FOR UPDATE SKIP LOCKED, so any number of processes
can drain the queue at once without taking the same job twice. A claimed job
holds a lease; if the process dies, the lease expires and a sweep puts the
job back in the queue. Failed jobs retry with exponential backoff.
"""

import hashlib
import time
from collections.abc import Callable
from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

LEASE_SECONDS = 300

Handler = Callable[[dict[str, Any]], Any]
HANDLERS: dict[str, Handler] = {}


def handler(kind: str) -> Callable[[Handler], Handler]:
    """Register a function as the handler for one job kind."""

    def register(fn: Handler) -> Handler:
        HANDLERS[kind] = fn
        return fn

    return register


# Example handlers. Replace them with your own work: sending email, calling
# an API, resizing an image. Keep each job well under the lease.


@handler("echo")
def echo(payload: dict[str, Any]) -> Any:
    return payload


@handler("checksum")
def checksum(payload: dict[str, Any]) -> Any:
    text = str(payload.get("text", ""))
    return {"sha256": hashlib.sha256(text.encode()).hexdigest(), "length": len(text)}


@handler("fail")
def fail(payload: dict[str, Any]) -> Any:
    raise RuntimeError(str(payload.get("message", "this job always fails")))


def enqueue(conn: Connection, kind: str, payload: dict[str, Any], max_attempts: int = 5) -> int:
    if kind not in HANDLERS:
        raise KeyError(kind)
    row = conn.execute(
        "INSERT INTO jobs (kind, payload, max_attempts) VALUES (%s, %s, %s) RETURNING id",
        (kind, Jsonb(payload), max_attempts),
    ).fetchone()
    return row[0]


def get(conn: Connection, job_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(
            "SELECT id, kind, status, attempts, max_attempts, result, last_error,"
            " created_at, updated_at FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()


def claim(conn: Connection) -> dict[str, Any] | None:
    """Take the next ready job and lease it, or return None."""
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(
            """
            UPDATE jobs
               SET status = 'running', attempts = attempts + 1,
                   locked_until = now() + make_interval(secs => %s), updated_at = now()
             WHERE id = (
                   SELECT id FROM jobs
                    WHERE status = 'queued' AND run_after <= now()
                    ORDER BY run_after, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1)
            RETURNING id, kind, payload, attempts, max_attempts
            """,
            (LEASE_SECONDS,),
        ).fetchone()


def finish(conn: Connection, job: dict[str, Any], result: Any) -> None:
    conn.execute(
        "UPDATE jobs SET status = 'done', result = %s, locked_until = NULL,"
        " last_error = NULL, updated_at = now() WHERE id = %s",
        (Jsonb(result), job["id"]),
    )


def retry_or_fail(conn: Connection, job: dict[str, Any], error: str) -> None:
    """Back off 10 s, 20 s, 40 s… up to an hour, then give up."""
    final = job["attempts"] >= job["max_attempts"]
    conn.execute(
        """
        UPDATE jobs
           SET status = %s, last_error = %s, locked_until = NULL, updated_at = now(),
               run_after = now() + make_interval(secs => least(3600, 5 * power(2, attempts)))
         WHERE id = %s
        """,
        ("failed" if final else "queued", error[:2000], job["id"]),
    )


def requeue_expired(conn: Connection) -> int:
    """Return jobs whose lease ran out (the process died) to the queue."""
    cur = conn.execute(
        """
        UPDATE jobs
           SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
               last_error = coalesce(last_error, 'lease expired'),
               locked_until = NULL, updated_at = now()
         WHERE status = 'running' AND locked_until < now()
        """
    )
    return cur.rowcount


def run_one(pool: ConnectionPool) -> bool:
    """Claim and run one job. Returns False when the queue has nothing ready."""
    with pool.connection() as conn:
        job = claim(conn)
    if job is None:
        return False
    try:
        result = HANDLERS[job["kind"]](job["payload"])
    except Exception as exc:  # noqa: BLE001 - any handler error retries the job
        with pool.connection() as conn:
            retry_or_fail(conn, job, f"{type(exc).__name__}: {exc}")
    else:
        with pool.connection() as conn:
            finish(conn, job, result)
    return True


def drain(pool: ConnectionPool, budget_seconds: float = 20.0) -> int:
    """Run ready jobs until the queue is empty or the time budget is spent."""
    deadline = time.monotonic() + budget_seconds
    done = 0
    while time.monotonic() < deadline and run_one(pool):
        done += 1
    return done
