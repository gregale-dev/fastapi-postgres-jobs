-- Jobs survive restarts and redeploys because they live in Postgres.
CREATE TABLE IF NOT EXISTS jobs (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status       text        NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'running', 'done', 'failed')),
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    run_after    timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    result       jsonb,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (run_after, id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_leased ON jobs (locked_until) WHERE status = 'running';
