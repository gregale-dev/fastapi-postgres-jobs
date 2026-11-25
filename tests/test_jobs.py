"""Queue and API tests. Tests that need Postgres run when TEST_DATABASE_URL
points at a database they may write to; the rest always run."""

import os

import psycopg
import pytest
from fastapi.testclient import TestClient

from app import db, jobs
from app.main import app

DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
needs_db = pytest.mark.skipif(not DATABASE_URL, reason="set TEST_DATABASE_URL to run")


@pytest.fixture
def conn(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with psycopg.connect(DATABASE_URL, autocommit=True) as c:
        c.execute(open("schema.sql").read())
        c.execute("TRUNCATE jobs RESTART IDENTITY")
        yield c
    db.close()


def test_health_does_not_need_the_database():
    assert TestClient(app).get("/healthz").json() == {"ok": True}


def test_unknown_kind_is_rejected_before_the_database():
    response = TestClient(app).post("/jobs", json={"kind": "nope"})
    assert response.status_code == 422


def test_sweep_needs_the_cron_header(monkeypatch):
    monkeypatch.delenv("ALLOW_MANUAL_SWEEP", raising=False)
    assert TestClient(app).post("/internal/jobs/sweep").status_code == 403


@needs_db
def test_job_runs_after_the_response(conn):
    client = TestClient(app)
    created = client.post("/jobs", json={"kind": "checksum", "payload": {"text": "hello"}})
    assert created.status_code == 202
    job = client.get(f"/jobs/{created.json()['id']}").json()
    assert job["status"] == "done"
    assert job["result"]["length"] == 5


@needs_db
def test_failing_job_retries_then_fails(conn):
    job_id = jobs.enqueue(conn, "fail", {"message": "boom"}, max_attempts=2)
    pool = db.pool()
    assert jobs.run_one(pool)
    assert jobs.get(conn, job_id)["status"] == "queued"
    conn.execute("UPDATE jobs SET run_after = now() WHERE id = %s", (job_id,))
    assert jobs.run_one(pool)
    row = jobs.get(conn, job_id)
    assert (row["status"], row["attempts"], row["last_error"]) == ("failed", 2, "RuntimeError: boom")


@needs_db
def test_two_workers_never_take_the_same_job(conn):
    for _ in range(2):
        jobs.enqueue(conn, "echo", {})
    with psycopg.connect(DATABASE_URL) as a, psycopg.connect(DATABASE_URL) as b:
        first, second = jobs.claim(a), jobs.claim(b)
    assert first["id"] != second["id"]


@needs_db
def test_sweep_requeues_an_expired_lease(conn, monkeypatch):
    job_id = jobs.enqueue(conn, "echo", {"n": 1})
    assert jobs.claim(conn)["id"] == job_id
    conn.execute("UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = %s", (job_id,))
    swept = TestClient(app).post("/internal/jobs/sweep", headers={"X-Faas-Invocation-Source": "cron"})
    assert swept.json() == {"requeued": 1, "ran": 1}
    assert jobs.get(conn, job_id)["status"] == "done"
