# Changelog

All notable changes to Astra are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Added
- `test-connection` CLI command — validates source or destination connectivity before running a pipeline. Runs `SELECT 1` against the configured Postgres database, reports round-trip latency, and verifies that every table in `capture.tables` exists. Usage: `astra test-connection <spec.yaml> [--target source|destination]`.
- `POST /api/v1/connections/test` API endpoint — accepts inline connection config (host, port, database, username, optional passwordRef/sslMode, optional tables list) and returns `{ "status": "ok", "latency_ms": N }` or `{ "status": "error", "message": "..." }`. Supports an optional `tables` array to verify table existence in `information_schema.tables`. Currently supports `"postgres"` kind only.
- `ConnectionTestResult` type in `astra-connectors` — structured result shared by the CLI and connector layer; carries status, latency, optional error message, and a list of missing tables.
- `PostgresSource::test_connection` method — tests the source connection and verifies all configured tables exist.
- `PostgresDestinationLoader::test_connection` method — tests the destination connection reachability.
- `ConnectionRepository` trait with Postgres and in-memory implementations — `list_connections`, `get_by_name`, `create`, `update`, `delete`. The Postgres impl is added to `PostgresPipelineRepository` and shares its connection pool; no additional pool is opened.
- `ConnectionService` — owns CRUD for saved connections and `resolve_spec`, which merges a saved connection's stored fields into a parsed `AstraSpec` before validation. Inline connection fields take precedence over the saved record; `secret_ref` is injected as `password: env:<SECRET_REF>`.
- `PipelineService::apply_spec` now calls `ConnectionService::resolve_spec` before `spec.validate()`, so specs using `connectionRef` resolve transparently at apply time.
- REST API for saved connections:
  - `GET /api/v1/connections` — list all saved connections
  - `POST /api/v1/connections` — create a connection (201)
  - `GET /api/v1/connections/:name` — get by name
  - `PUT /api/v1/connections/:name` — update
  - `DELETE /api/v1/connections/:name` — delete (204)
  - Connection `name` is validated against `^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$`; invalid names return 400.
- `saved_connections` table added to the Postgres metadata DB via a new `V2__saved_connections` refinery migration. Stores connection name, kind, non-sensitive config JSON, and an optional `secret_ref` — passwords are never persisted in the database.
- `connectionRef` field on `source` and `destination` in the `v1alpha1` YAML spec — allows referencing a named saved connection instead of inlining credentials. Mutually exclusive with the inline `connection` block; specifying both returns a new `AmbiguousConnection` validation error.
- `AstraError` enum in `astra-metadata` — structured error type with retryable/permanent classification and machine-readable `code()` field (`CONNECTION_FAILED`, `QUERY_FAILED`, `STAGING_FAILED`, `VALIDATION_ERROR`, `NOT_FOUND`, `INTERNAL_ERROR`).
- `AppError::Astra` variant in the control plane — `AstraError` values propagate directly to HTTP responses with correct status codes (404, 400, 502, 500).
- HTTP error responses now include `{ "error": "...", "code": "...", "retryable": bool }` instead of just `{ "error": "..." }`.
- Postgres connector `connect()` maps connection failures to `AstraError::ConnectionFailed { retryable: true }`.
- Postgres `discover_tables()` maps column/PK query failures to `AstraError::QueryFailed { retryable: false }` and missing tables to `AstraError::NotFound`.
- Structured logging via `tracing` throughout the CLI and control plane — replace `println!` with `tracing::info!`/`debug!` events carrying structured fields (pipeline name, table, row count, path).
- `ASTRA_LOG` environment variable for log-level control (defaults to `info`); supports standard `tracing` filter syntax (e.g. `ASTRA_LOG=astra=debug`).
- HTTP request tracing in the control plane via `tower-http` `TraceLayer` — each request emits method, path, status, and latency spans.
- Logs are written to **stderr**; user-facing data output (e.g. `discover-source` schema listing) remains on stdout.

---

## [0.1.0] - 2026-04-01

First public release of Astra. One complete vertical slice: Postgres snapshot → local/MinIO staging → Postgres raw destination, with a control plane API and web UI for visibility.

### Added

#### Pipeline spec
- `v1alpha1` YAML pipeline spec — parsing, validation, and structured error reporting
- `validate` CLI command — rejects malformed specs before any execution
- Spec stored as JSON in the control plane and shared across CLI, API, and UI

#### Postgres source
- `discover-source` — enumerates tables, column types, and primary keys
- Full snapshot with paginated chunking; configurable `chunkSize` per table
- Resumable execution — checkpoint ledger tracks processed chunks; interrupted snapshots resume from the last unfinished chunk
- `--no-resume` flag to force a full restart

#### Staging
- Local filesystem staging — rows written as JSONL.gz chunks
- MinIO/S3 staging — same chunk format and metadata schema via S3-compatible adapter
- Stable staging contract: `StageChunk` metadata (stream name, partition key, sequence, byte count, row count, schema fingerprint) consistent across backends

#### Postgres destination (raw loader)
- Loads staged JSONL.gz chunks into an `astra_raw` schema on the destination Postgres
- Table naming: `raw_<schema>_<table>` (e.g. `astra_raw.raw_public_smoke_users`)
- Idempotent chunk application — completed chunks tracked in `astra_raw._applied_chunks` and skipped on rerun

#### CLI commands
- `snapshot-to-local-staging` — snapshot Postgres tables to local JSONL.gz chunks
- `snapshot-to-minio-staging` — snapshot to MinIO/S3 chunks
- `load-local-staging-to-postgres` — load staged chunks into Postgres raw destination
- `execute-local-snapshot` — end-to-end: snapshot → local staging → Postgres load in one command

#### Control-plane API
- `GET /api/v1/pipelines` — list registered pipelines
- `POST /api/v1/specs/apply` — register or update a pipeline spec
- `GET /api/v1/pipelines/:name/runs` — list runs for a pipeline
- `GET /api/v1/pipelines/:name/latest-run` — latest run summary
- `GET /api/v1/pipelines/:name/run-history` — paginated run history
- `POST /api/v1/pipeline-runs` — create a run record
- `POST /api/v1/pipeline-runs/:id/status` — update run status and progress
- `GET /api/v1/pipeline-runs/:id/table-executions` — per-table execution status and row counts
- `POST /api/v1/pipeline-runs/:id/artifacts` — record a staged artifact

#### Web UI
- Pipeline inventory — name, source/destination kind, status, spec version
- Run history per pipeline — run ID, status, trigger mode, start time, duration
- Table-level drill-down per run — stream name, status, rows processed / rows total, error summary
- YAML studio — load, edit, and apply a pipeline spec via the API
- Onboarding wizard — 4-step guided pipeline creation flow
- Built with React 18 + TypeScript + Vite; served by the control-plane binary

#### Persistence
- In-memory (default) — suitable for local development; no config required
- Postgres-backed — set `ASTRA_DATABASE_URL`; pipeline, run, table execution, and artifact records are durable

#### Testing
- Unit and integration tests across all crates (`cargo test --workspace`)
- Postgres repository integration tests
- End-to-end snapshot smoke test (`python3 scripts/e2e_snapshot_smoke.py`) — seeds fixtures, runs `execute-local-snapshot`, verifies destination row counts, validates idempotent rerun

### Known limitations

See [`docs/v0.1-SCOPE.md`](docs/v0.1-SCOPE.md) for the full list. Key ones:

- **CDC not implemented** — returns an explicit error if attempted
- **Append-only destination writes** — no merge, upsert, or deduplication
- **Sequential table processing** — multi-table parallelism parsed but not enforced
- **Secrets via environment variables only** — `env:KEY` is the only supported `passwordRef` format
- **No structured observability** — execution output to stdout only
- **Single local worker** — no scheduler or distributed execution; pipelines triggered manually via CLI
