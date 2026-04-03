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

## Pipelines

### `GET /api/v1/pipelines`

List all registered pipelines.

**Response:**

```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "smoke-local-snapshot",
    "status": "active",
    "mode": "snapshot",
    "schedule": "manual",
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T10:00:00Z"
  }
]
```

### `GET /api/v1/pipelines/:id`

Get a single pipeline by ID.

### `POST /api/v1/pipelines`

Create a new pipeline (without applying a YAML spec). Prefer `/api/v1/specs/apply` for YAML-driven workflows.

**Body:**

```json
{
  "name": "my-pipeline",
  "mode": "snapshot",
  "schedule": "manual",
  "spec_json": { ... }
}
```

### `DELETE /api/v1/pipelines/:id`

Delete a pipeline and its associated metadata.

---

## Pipeline Runs

### `GET /api/v1/pipeline-runs`

List all pipeline runs across all pipelines.

**Response:**

```json
[
  {
    "id": "7b7b7b7b-7b7b-7b7b-7b7b-7b7b7b7b7b7b",
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440000",
    "pipeline_name": "smoke-local-snapshot",
    "status": "completed",
    "started_at": "2024-01-15T10:05:00Z",
    "completed_at": "2024-01-15T10:05:42Z"
  }
]
```

### `GET /api/v1/pipeline-runs/:id`

Get a single run by ID.

### `GET /api/v1/pipelines/:id/runs`

List all runs for a specific pipeline.

### `POST /api/v1/pipelines/:id/runs`

Trigger a new run for a pipeline. The run is executed inline (embedded executor).

**Body:** Empty or `{}`

**Response:**

```json
{
  "run_id": "7b7b7b7b-...",
  "status": "started"
}
```

### `GET /api/v1/pipeline-runs/:run_id/table-executions`

List all table-level execution records for a run. Each entry tracks the status of one table within the run.

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

## Spec apply

### `POST /api/v1/specs/apply`

Parse, validate, and register a pipeline from a YAML spec string. Creates the pipeline if it doesn't exist, or updates it if a pipeline with the same name already exists.

**Body:**

```json
{
  "spec": "version: v1alpha1\npipeline:\n  name: my-pipeline\n  ..."
}
```

**Response (success):**

```json
{
  "pipeline_id": "550e8400-...",
  "name": "my-pipeline",
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

## Error responses

All errors follow this shape:

```json
{
  "error": "human-readable message",
  "details": ["optional", "field-level", "errors"]
}
```

| HTTP status | Meaning |
|---|---|
| `400` | Validation error or bad request body |
| `404` | Resource not found |
| `409` | Conflict (e.g., duplicate pipeline name) |
| `500` | Internal server error |

---

## Web UI

The control plane also serves the React web UI at the root path (`/`). See [Web UI →](./web-ui.md).
