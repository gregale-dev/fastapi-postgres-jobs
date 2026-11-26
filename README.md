# FastAPI + Postgres background jobs that scale to zero

[![Deploy on Gregale](https://gregale.dev/deploy-badge.svg)](https://gregale.dev/deploy?repo=https://github.com/gregale-dev/fastapi-postgres-jobs)

A FastAPI API with a durable job queue in Postgres. There's no Redis and no
always-on worker:

- **Durable:** `POST /jobs` stores a job in Postgres, so it survives a crash
  or a redeploy.
- **Runs right away:** the same instance runs the queue right after the
  response is sent.
- **Concurrency-safe:** jobs are claimed with `FOR UPDATE SKIP LOCKED`, so
  any number of processes can drain the queue without taking the same job
  twice.
- **Retries:** a failed job retries with exponential backoff, from 10 seconds
  up to an hour, then stops at `failed`.
- **Leases:** a claimed job holds a 5-minute lease. If the process dies, a
  sweep puts the job back in the queue.
- **Idle means idle:** nothing polls the database. When no requests arrive,
  the app and the database both stop.

It runs anywhere that runs Uvicorn and Postgres. The `gregale.yaml` deploys
it to [Gregale](https://gregale.dev), which pauses the app about a second
after its last request and suspends the database when it's idle.

## Endpoints

| Method | Path | What it does |
|---|---|---|
| `POST` | `/jobs` | Queue a job: `{"kind": "checksum", "payload": {"text": "hello"}}`. Returns `202` with its `id`. |
| `GET` | `/jobs/{id}` | The job's `status` (`queued`, `running`, `done`, `failed`), `attempts`, `result` and `last_error`. |
| `POST` | `/internal/jobs/sweep` | Requeue expired leases and run ready jobs. Only a cron may call it. |
| `GET` | `/healthz` | Health check. Never touches the database. |

## Run it locally

You need Python 3.13 and a Postgres database:

```sh
docker run -d --name jobs-db -e POSTGRES_PASSWORD=dev -p 5432:5432 postgres:17
export DATABASE_URL=postgresql://postgres:dev@localhost:5432/postgres

python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
python migrate.py
uvicorn app.main:app --reload
```

Queue a job and read it back:

```sh
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"kind": "checksum", "payload": {"text": "hello"}}'
curl -s localhost:8000/jobs/1
```

Run the tests. The queue tests need a database they may empty:

```sh
TEST_DATABASE_URL=$DATABASE_URL pytest
```

## Add your own job

Register a function for a new kind in `app/jobs.py`:

```python
@handler("send_welcome_email")
def send_welcome_email(payload):
    send_email(to=payload["email"], template="welcome")
    return {"sent": True}
```

Its return value is stored as the job's `result`. An exception retries the
job. Keep each job well under the 5-minute lease (`LEASE_SECONDS`), and make
it safe to run twice: a job whose process dies mid-run runs again.

## Deploy to Gregale

```sh
gregale deploy --name fastapi-postgres-jobs
```

On the first deploy, `gregale.yaml`:

- creates a PostgreSQL 17 database and binds it as `DATABASE_URL`;
- gives only the release command a migration credential,
  `MIGRATION_DATABASE_URL`;
- runs `python migrate.py` before each new version takes traffic. If it fails,
  the previous version keeps serving.

Then add the sweep, which recovers jobs that a crash or redeploy interrupted:

```sh
gregale cron create --app fastapi-postgres-jobs --schedule "17 */6 * * *" --path /internal/jobs/sweep
```

Every six hours is enough, because jobs normally run as soon as they're
queued. Each sweep wakes the database, so a sweep every minute would keep it
running all month. The endpoint accepts only requests that carry
`X-Faas-Invocation-Source: cron`. Gregale removes that header from visitors'
requests, so only your cron can send it. Set `ALLOW_MANUAL_SWEEP=1` to call
the sweep yourself elsewhere.

Managed PostgreSQL and crons need the Hobby plan or higher. See
[Deploy a FastAPI app](https://gregale.dev/docs/deploy-fastapi),
[Managed PostgreSQL](https://gregale.dev/docs/postgres) and
[Crons](https://gregale.dev/docs/crons).

## Why there's no worker process

A separate worker that polls the queue, or holds a `LISTEN` connection, keeps
the database awake all the time, and it runs, and costs, even when nobody
uses the app. Here, a job runs in a background task right after the request
that queued it. The instance stays running while the task works, then pauses.

Two cases need more than this:

- A job that mostly waits on a timer, such as `time.sleep`, can be paused
  between requests. Set `lifecycle.pause: when_idle` in `gregale.yaml` if
  your jobs do that.
- Long batch work belongs in a [job](https://gregale.dev/docs/jobs), and a
  steady stream of heavy jobs in a dedicated worker. See
  [FastAPI background jobs with Postgres](https://gregale.dev/articles/fastapi-background-jobs-postgres)
  for when to choose which.

[What intermittent FastAPI traffic costs](https://gregale.dev/research/intermittent-fastapi-cost-2026-11-24)
measures the idle pause on real traffic.

## Using a coding agent

`AGENTS.md` tells Codex, Cursor, Claude Code and other agents how to deploy
and check this app with the `gregale` CLI. To let an agent manage your
Gregale account through MCP, see
[Use Gregale from coding agents](https://gregale.dev/docs/agents).

## License

MIT
