CREATE TABLE IF NOT EXISTS saved_connections (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL,
    config_json JSONB NOT NULL DEFAULT '{}',
    secret_ref  TEXT,
    created_by  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
