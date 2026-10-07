ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('queued', 'running', 'retrying', 'completed', 'failed'));

ALTER TABLE jobs
    -- Attempts allowed before the job is marked failed.
    ADD COLUMN max_attempts     integer     NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    -- When the ID was last pushed to the queue; finds queued jobs whose ID was lost.
    ADD COLUMN enqueued_at      timestamptz NOT NULL DEFAULT now(),
    -- Set while running; once passed, the worker is presumed dead.
    ADD COLUMN lease_expires_at timestamptz,
    -- Set while retrying; the job is queued again once this time has passed.
    ADD COLUMN run_after        timestamptz;
