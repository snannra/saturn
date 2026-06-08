CREATE TABLE IF NOT EXISTS saturn_nodes (
    node_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ DEFAULT NULL
);