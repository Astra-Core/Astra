CREATE TABLE IF NOT EXISTS pipelines (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_kind TEXT NOT NULL,
    destination_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    active_spec_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_specs (
    id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    spec_version TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    spec_yaml TEXT NOT NULL,
    spec_json JSONB NOT NULL,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, version),
    UNIQUE (pipeline_id, content_hash)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    trigger_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_started_at
    ON pipeline_runs (pipeline_id, started_at DESC);

CREATE TABLE IF NOT EXISTS staged_artifacts (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    stream_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    bytes_written BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    content_type TEXT NOT NULL,
    content_encoding TEXT NOT NULL,
    schema_fingerprint TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_run_id, object_key),
    UNIQUE (pipeline_run_id, stream_name, partition_key, sequence)
);

CREATE INDEX IF NOT EXISTS idx_staged_artifacts_run_stream_sequence
    ON staged_artifacts (pipeline_run_id, stream_name, partition_key, sequence);

CREATE TABLE IF NOT EXISTS table_executions (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    stream_name TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_processed BIGINT NOT NULL DEFAULT 0,
    rows_total BIGINT,
    error_summary TEXT,
    checkpoint_next_sequence BIGINT,
    checkpoint_rows_staged BIGINT,
    checkpoint_last_chunk_key TEXT,
    checkpoint_completed BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(pipeline_run_id, stream_name)
);

CREATE INDEX IF NOT EXISTS idx_table_executions_run
    ON table_executions (pipeline_run_id);
