# Astra Metadata Schema Draft

This document defines the **v0.1 metadata model** for Astra’s control plane.

The goal is not academic beauty. The goal is to support:
- source + destination configuration
- pipeline spec versioning
- job scheduling/execution
- checkpointing and resume
- schema drift tracking
- enough auditability that operators are not blind

## Design principles

1. **Specs are versioned, runtime state is not embedded in specs**
2. **Job runs are immutable execution records**
3. **Checkpoints are granular enough to resume safely**
4. **Schema drift is tracked explicitly, not guessed from logs**
5. **Secrets are referenced, not stored inline in pipeline specs**

---

## Core entities

## 1. `sources`
Represents a reusable source definition.

### Purpose
Stores the source system identity and non-secret configuration that may be reused across pipelines.

### Suggested fields
- `id` (uuid)
- `name` (string, unique within workspace)
- `kind` (enum: `postgres`, `mysql`, `s3`, `stripe`, etc.)
- `config_json` (jsonb)
- `secret_refs_json` (jsonb)
- `created_at`
- `updated_at`
- `archived_at` (nullable)

### Notes
- `config_json` stores validated source configuration after YAML/API normalization.
- `secret_refs_json` stores references such as env vars, vault paths, or future secret-provider IDs.

---

## 2. `destinations`
Represents a reusable destination definition.

### Suggested fields
- `id` (uuid)
- `name` (string, unique within workspace)
- `kind` (enum: `snowflake`, `bigquery`, `s3`, `postgres`, etc.)
- `config_json` (jsonb)
- `secret_refs_json` (jsonb)
- `created_at`
- `updated_at`
- `archived_at` (nullable)

---

## 3. `pipelines`
Represents the durable pipeline identity.

### Purpose
A pipeline is the operator-facing object: “sync this source to this destination with these rules.”

### Suggested fields
- `id` (uuid)
- `name` (string, unique)
- `source_id` (fk -> `sources.id`)
- `destination_id` (fk -> `destinations.id`)
- `status` (enum: `draft`, `active`, `paused`, `failed`, `archived`)
- `active_spec_id` (nullable fk -> `pipeline_specs.id`)
- `last_run_at` (nullable)
- `last_success_at` (nullable)
- `created_at`
- `updated_at`
- `archived_at` (nullable)

### Notes
- A pipeline references its latest active spec version.
- Pausing a pipeline does not delete history or checkpoints.

---

## 4. `pipeline_specs`
Represents the canonical, versioned pipeline definition.

### Purpose
Preserves the exact applied config for reproducibility, audit, rollback, and UI/API consistency.

### Suggested fields
- `id` (uuid)
- `pipeline_id` (fk -> `pipelines.id`)
- `version` (integer, monotonically increasing per pipeline)
- `spec_version` (string, e.g. `v1alpha1`)
- `spec_yaml` (text)
- `spec_json` (jsonb)
- `content_hash` (string)
- `created_by` (nullable string/uuid depending on auth model)
- `created_at`
- `activated_at` (nullable)
- `superseded_at` (nullable)

### Notes
- UI and CLI must both produce the same normalized `spec_json` model.
- `content_hash` helps dedupe identical re-applies.

---

## 5. `jobs`
Represents scheduled or operator-triggered units of work.

### Purpose
Separates execution intent from execution attempts.

### Suggested fields
- `id` (uuid)
- `pipeline_id` (fk -> `pipelines.id`)
- `kind` (enum: `snapshot`, `cdc`, `backfill`, `validate`, `discover`, `apply`)
- `trigger_mode` (enum: `schedule`, `manual`, `system`)
- `status` (enum: `queued`, `running`, `succeeded`, `failed`, `cancelled`)
- `requested_at`
- `scheduled_for` (nullable)
- `started_at` (nullable)
- `finished_at` (nullable)
- `requested_by` (nullable)
- `priority` (nullable integer)
- `run_count` (integer)
- `last_error_summary` (nullable text)

### Notes
- `jobs` are mutable orchestration records.
- Detailed execution history belongs in `job_runs`.

---

## 6. `job_runs`
Represents immutable execution attempts.

### Purpose
Stores every run/attempt, including retries.

### Suggested fields
- `id` (uuid)
- `job_id` (fk -> `jobs.id`)
- `attempt` (integer)
- `worker_id` (nullable string)
- `phase` (enum: `planning`, `snapshot`, `cdc`, `stage_flush`, `sink_apply`, `finalize`)
- `status` (enum: `running`, `succeeded`, `failed`, `cancelled`)
- `started_at`
- `finished_at` (nullable)
- `stats_json` (jsonb)
- `error_code` (nullable string)
- `error_message` (nullable text)
- `logs_pointer` (nullable string)

### Notes
- `stats_json` may hold rows processed, bytes read, bytes staged, flush count, latency snapshots, etc.
- `logs_pointer` can later reference object storage, Loki, or another sink.

---

## 7. `checkpoints`
Represents resumable progress markers.

### Purpose
Supports safe resume for both snapshots and CDC.

### Suggested fields
- `id` (uuid)
- `pipeline_id` (fk -> `pipelines.id`)
- `job_run_id` (nullable fk -> `job_runs.id`)
- `stream_name` (string)
- `phase` (enum: `snapshot`, `cdc`, `sink_apply`)
- `cursor_json` (jsonb)
- `snapshot_chunk_key` (nullable string)
- `lsn_or_offset` (nullable string)
- `sink_commit_token` (nullable string)
- `recorded_at`

### Notes
- `cursor_json` should be flexible enough for API cursors, PK-range progress, timestamp bookmarks, or WAL/binlog metadata.
- `sink_commit_token` may later move into a separate apply ledger if destination semantics demand it.

### Current decision
For v0.1, keep snapshot and CDC checkpoints in the same table with a `phase` field.
That is simpler and good enough until usage proves otherwise.

---

## 8. `schema_events`
Tracks observed source schema changes.

### Suggested fields
- `id` (uuid)
- `pipeline_id` (fk -> `pipelines.id`)
- `stream_name` (string)
- `change_type` (enum: `additive`, `breaking`, `unknown`)
- `detected_at`
- `source_schema_json` (jsonb)
- `destination_schema_json` (nullable jsonb)
- `resolution_status` (enum: `detected`, `auto_applied`, `paused`, `ignored`, `resolved`)
- `notes` (nullable text)

### Notes
- This gives the UI and operators a first-class place to inspect drift instead of scraping logs.

---

## 9. `secret_refs`
Maps reusable secret references used across sources/destinations/pipelines.

### Suggested fields
- `id` (uuid)
- `name` (string, unique)
- `provider` (enum: `env`, `file`, `vault`, `aws_secrets_manager`, future)
- `reference` (string)
- `created_at`
- `updated_at`

### Notes
- v0.1 may not need a fully separate table if secret refs live comfortably in config JSON, but keeping the concept explicit in the model helps avoid inline secret sprawl.

---

## Relationships summary

- one `source` -> many `pipelines`
- one `destination` -> many `pipelines`
- one `pipeline` -> many `pipeline_specs`
- one `pipeline` -> many `jobs`
- one `job` -> many `job_runs`
- one `pipeline` -> many `checkpoints`
- one `pipeline` -> many `schema_events`

---

## Status enums

## Pipeline status
- `draft`
- `active`
- `paused`
- `failed`
- `archived`

## Job status
- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`

## Schema resolution status
- `detected`
- `auto_applied`
- `paused`
- `ignored`
- `resolved`

---

## Open questions

### 1. Should sink commit markers live in checkpoints or a separate apply ledger?
Current v0.1 answer: keep them in `checkpoints` as `sink_commit_token`.
If destination semantics become richer, split later.

### 2. Should sources/destinations always be reusable top-level objects?
Current v0.1 answer: yes, but pipeline specs should still embed enough normalized config to preserve exact historical behavior.

### 3. Should job scheduling live in `pipelines` or a separate schedules table?
Current v0.1 answer: schedule data can live in pipeline spec/config for now.
If scheduling becomes materially more complex, extract later.
