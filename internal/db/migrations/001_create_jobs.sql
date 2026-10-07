CREATE TABLE jobs (
    id         text        PRIMARY KEY,
    type       text        NOT NULL,
    payload    jsonb,
    status     text        NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    attempts   integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    result     jsonb,
    error      text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
