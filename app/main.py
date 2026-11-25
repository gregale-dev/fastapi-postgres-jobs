"""FastAPI API with a Postgres job queue that costs nothing while idle.

POST /jobs stores a job and runs the queue right after the response is sent,
in the same instance. Nothing polls the database, so an idle app and an idle
database both stop. A rare cron (see README) sweeps jobs that a crash or a
redeploy left behind.
"""

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from app import db, jobs


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    db.close()


app = FastAPI(title="FastAPI + Postgres jobs", lifespan=lifespan)


class NewJob(BaseModel):
    kind: str = Field(examples=["checksum"])
    payload: dict[str, Any] = Field(default_factory=dict, examples=[{"text": "hello"}])
    max_attempts: int = Field(default=5, ge=1, le=20)


def drain_queue() -> None:
    jobs.drain(db.pool())


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    # Cheap and database-free, so health checks never wake the database.
    return {"ok": True}


@app.get("/")
def index() -> dict[str, Any]:
    return {"service": "fastapi-postgres-jobs", "job_kinds": sorted(jobs.HANDLERS)}


@app.post("/jobs", status_code=202)
def create_job(job: NewJob, background: BackgroundTasks) -> dict[str, Any]:
    if job.kind not in jobs.HANDLERS:
        raise HTTPException(422, f"unknown job kind {job.kind!r}; known: {sorted(jobs.HANDLERS)}")
    with db.pool().connection() as conn:
        job_id = jobs.enqueue(conn, job.kind, job.payload, job.max_attempts)
    background.add_task(drain_queue)
    return {"id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def read_job(job_id: int) -> dict[str, Any]:
    with db.pool().connection() as conn:
        row = jobs.get(conn, job_id)
    if row is None:
        raise HTTPException(404, "job not found")
    return row


@app.post("/internal/jobs/sweep")
def sweep(x_faas_invocation_source: str | None = Header(default=None)) -> dict[str, int]:
    """Called by a Gregale cron. Gregale strips X-Faas-* headers that visitors
    send, so the header proves the request came from your cron."""
    if x_faas_invocation_source != "cron" and os.environ.get("ALLOW_MANUAL_SWEEP") != "1":
        raise HTTPException(403, "only the cron may sweep")
    with db.pool().connection() as conn:
        requeued = jobs.requeue_expired(conn)
    return {"requeued": requeued, "ran": jobs.drain(db.pool())}
