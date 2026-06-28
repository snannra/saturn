CREATE TABLE IF NOT EXISTS pendingjobs (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id TEXT NOT NULL,
    scheduled_for TIMESTAMPTZ DEFAULT NOW(),
    job_data JSONB,
    status TEXT,
    claimed_by TEXT DEFAULT NULL,
    attempt_id TEXT DEFAULT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    lease_expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '30 seconds'
);