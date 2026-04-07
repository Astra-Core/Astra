---
id: api
title: REST API Reference
sidebar_position: 1
---

# Control Plane REST API

The control plane exposes a REST API at `/api/v1`. All request and response bodies are JSON.

## Start the control plane

```bash
# In-memory mode (no persistence across restarts)
cargo run -p astra-control-plane

# With Postgres persistence
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
  cargo run -p astra-control-plane
```

Default bind address: `127.0.0.1:8080`. Override with `ASTRA_CONTROL_PLANE_ADDR`.

---

## Health and meta

### `GET /`

Returns a welcome message. Used to verify the server is up.

### `GET /health`

Health check endpoint.

### `GET /ready`

Readiness check — returns 200 when the server is ready to handle requests.

### `GET /version`

Returns the server version string.

---

## Pipelines

### `GET /api/v1/pipelines`

List all registered pipelines.

**Response:**

```json
[
  {
    "name": "smoke-local-snapshot",
    "status": "active",
    "mode": "snapshot",
    "schedule": "manual",
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T10:00:00Z"
  }
]
```

### `GET /api/v1/pipelines/:pipeline_name`

Get the YAML spec for a single pipeline by name.

### `DELETE /api/v1/pipelines/:pipeline_name`

Delete a pipeline and its associated metadata.

### `POST /api/v1/pipelines/:pipeline_name/disable`

Disable a pipeline. Disabled pipelines will not be triggered by the scheduler.

### `POST /api/v1/pipelines/:pipeline_name/enable`

Re-enable a previously disabled pipeline.

### `POST /api/v1/pipelines/:pipeline_name/trigger`

Trigger a new run for a pipeline. The run is executed inline (embedded executor, spawned via `tokio::spawn`).

**Body:** Empty or `{}`

**Response:**

```json
{
  "run_id": "7b7b7b7b-...",
  "status": "started"
}
```

### `GET /api/v1/pipelines/:pipeline_name/runs`

List all runs for a specific pipeline.

### `GET /api/v1/pipelines/:pipeline_name/latest-run`

Get the most recent run for a pipeline.

### `GET /api/v1/pipelines/:pipeline_name/run-history`

Get the full run history for a pipeline, ordered by start time.

---

## Pipeline Runs

### `POST /api/v1/pipeline-runs`

Create a new pipeline run record (used internally by the executor to register a run before it starts).

### `POST /api/v1/pipeline-runs/:run_id/status`

Update the status of a pipeline run (e.g., mark as completed or failed).

### `POST /api/v1/pipeline-runs/:run_id/artifacts`

Record a staged artifact (chunk) for a run.

**Body:**

```json
{
  "pipeline_name": "smoke-local-snapshot",
  "stream_name": "public.smoke_users",
  "sequence": 1,
  "object_key": "pipelines/smoke-local-snapshot/streams/public.smoke_users/...",
  "row_count": 1000
}
```

### `GET /api/v1/pipeline-runs/:run_id/artifacts`

List all staged artifacts recorded for a run.

### `POST /api/v1/pipeline-runs/:run_id/table-executions`

Upsert a table-level execution record for a run. Tracks per-table status, row counts, and timing.

### `GET /api/v1/pipeline-runs/:run_id/table-executions`

List all table-level execution records for a run.

**Response:**

```json
[
  {
    "id": "...",
    "run_id": "...",
    "table_name": "public.smoke_users",
    "status": "completed",
    "rows_captured": 500,
    "chunks_staged": 1,
    "started_at": "...",
    "completed_at": "..."
  }
]
```

---

## Saved Connections

Saved connections store non-sensitive database credentials (host, port, database, username) and an optional `secret_ref` so that pipeline specs can reference them by name instead of inlining credentials. Passwords are never stored in the database.

### `GET /api/v1/connections`

List all saved connections.

**Response:**

```json
{
  "connections": [
    {
      "id": "a1b2c3d4-...",
      "name": "prod-postgres",
      "kind": "postgres",
      "host": "db.example.com",
      "port": 5432,
      "database": "app",
      "username": "replicator",
      "secret_ref": "env:POSTGRES_PASSWORD",
      "created_by": null,
      "created_at": "2026-04-07T10:00:00Z",
      "updated_at": "2026-04-07T10:00:00Z"
    }
  ]
}
```

### `POST /api/v1/connections`

Create a new saved connection. Returns `201 Created`.

**Body:**

```json
{
  "name": "prod-postgres",
  "kind": "postgres",
  "host": "db.example.com",
  "port": 5432,
  "database": "app",
  "username": "replicator",
  "secret_ref": "env:POSTGRES_PASSWORD"
}
```

`secret_ref` is optional. `name` must match `^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$`.

**Response:** the created connection record (same shape as the list item above).

### `GET /api/v1/connections/:name`

Get a single saved connection by name.

**Response:** the connection record, or `404` if not found.

### `PUT /api/v1/connections/:name`

Replace the fields of an existing saved connection. Same body shape as `POST`. Returns the updated record or `404` if not found.

### `DELETE /api/v1/connections/:name`

Delete a saved connection by name. Returns `204 No Content`, or `404` if not found.

> **Note:** Deleting a saved connection does not affect pipelines that were already applied — `connectionRef` is resolved and inlined at apply time, so existing pipeline specs are unaffected.

---

## Spec apply

### `POST /api/v1/specs/apply`

Parse, validate, and register a pipeline from a YAML spec string. Creates the pipeline if it doesn't exist, or updates it if a pipeline with the same name already exists.

**Body:**

```json
{
  "yaml": "version: v1alpha1\npipeline:\n  name: my-pipeline\n  ...",
  "created_by": "alice"
}
```

The `created_by` field is optional.

**Response (success):**

```json
{
  "pipeline_name": "my-pipeline",
  "created": true
}
```

**Response (validation error):**

```json
{
  "error": "validation failed",
  "details": ["pipeline.name is required", "source.kind must be one of: postgres"]
}
```

---

## Examples

### `GET /api/v1/examples/postgres-to-warehouse`

Returns a pre-built example YAML spec for a Postgres-to-warehouse pipeline. Used by the YAML Studio in the web UI to populate a starter template.

---

## Error responses

All errors follow this shape:

```json
{
  "error": "human-readable message",
  "code": "NOT_FOUND",
  "retryable": false
}
```

| HTTP status | `code` | Meaning |
|---|---|---|
| `400` | `BAD_REQUEST` / `VALIDATION_ERROR` | Validation error, bad request body, or invalid name format |
| `404` | `NOT_FOUND` | Resource not found |
| `502` | `CONNECTION_FAILED` / `QUERY_FAILED` | Upstream connector error |
| `500` | `INTERNAL_ERROR` | Internal server error |

`retryable: true` is set for transient errors (e.g., connection failures) where the client can safely retry.

---

## Web UI

The control plane also serves the React web UI at the root path (`/`). See [Web UI →](./web-ui.md).
