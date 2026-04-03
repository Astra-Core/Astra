---
id: crate-guide
title: Crate Guide
sidebar_position: 2
---

# Crate Guide

Astra is a Cargo workspace. This page describes each crate's responsibility and the key interfaces it exposes.

## Applications

### `apps/control-plane`

The main server binary. Responsibilities:

- Axum HTTP server (`/api/v1/...`)
- Serving the compiled web UI static assets
- `PipelineService` — all state mutations go through here
- `PipelineRepository` trait dispatch (Postgres or in-memory)
- Embedded pipeline executor (triggers runs from HTTP)
- Scheduler (cron-based run triggering)

Key types:
- `PipelineService` — orchestrates create/read/update for pipelines and runs
- `PostgresPipelineRepository` / `InMemoryPipelineRepository`
- API handlers in `src/http/`

### `apps/cli`

The `astra` CLI binary. Built with [Clap](https://docs.rs/clap).

Commands: `validate`, `apply`, `discover-source`, `snapshot-to-local-staging`, `snapshot-to-minio-staging`, `load-local-staging-to-postgres`, `execute-local-snapshot`.

### `apps/web`

React 18 + TypeScript + Vite frontend. Built separately and embedded as static assets in the control plane binary at compile time.

### `apps/worker`

Stub for a future distributed worker that executes pipeline runs on behalf of the control plane. Not implemented.

---

## Library crates

### `crates/astra-yaml`

YAML spec parsing and validation for the `v1alpha1` pipeline spec.

Key types:
- `AstraSpec` — top-level spec struct (derives `serde::Deserialize`)
- `Source`, `Destination`, `Runtime`, `Capture`, `Pipeline`
- `validate(spec: &AstraSpec) -> Result<(), Vec<String>>` — returns a list of validation error messages

Depended on by: `apps/control-plane`, `apps/cli`

### `crates/astra-metadata`

Shared enums and domain types used across the workspace:

- `PipelineStatus` — `Draft`, `Active`, `Disabled`, `Paused`, `Failed`, `Archived`
- `RunStatus` — `Started`, `Completed`, `Failed`
- `JobKind` — `Snapshot`, `Incremental`, `Cdc`
- `RunPhase` — `Capture`, `Load`, `Done`

Depended on by: all crates that deal with pipeline state.

### `crates/astra-runtime`

Staging backends and checkpoint logic. The core durability layer.

Key types:
- `StageChunkStore` async trait — `ensure_ready`, `write_chunk`, `read_chunk`
- `LocalStageChunkStore` — writes to the local filesystem; also exposes `list_chunks_for_pipeline()`
- `MinioStageChunkStore` — writes to MinIO or S3-compatible storage via the `object_store` crate
- `SnapshotCheckpointLedger` / `LocalCheckpointStore` — reads/writes per-pipeline checkpoint state as JSON
- `StageChunk` — chunk metadata struct (pipeline, stream, partition, sequence, row_count, object_key)

Staging object keys follow this convention:

```
pipelines/<pipeline_name>/streams/<stream_name>/partitions/<partition_key>/chunks/<sequence:020>.jsonl.gz
```

### `crates/astra-connectors`

Source and destination connector implementations.

Key types:
- `PostgresSource` — `from_spec()`, `discover()`, `snapshot_to_jsonl_gzip()`
- `PostgresDestinationLoader` — `from_spec()`, `load_local_stage_chunks()`

The Postgres source uses `to_jsonb()` for row serialization and system catalog queries for PK discovery. The Postgres destination creates and populates `astra_raw.<table>` tables with per-chunk idempotency tracking via `_applied_chunks`.

### `crates/astra-cdc`

CDC orchestration. Currently a stub — defines `CdcPhase`, `PostgresConfig`, `CdcConfig`, and `StreamProgress` types, but `status()` returns `"cdc skeleton defined"`. Returns an explicit "not implemented" error when triggered via the CLI.

Planned: Postgres logical replication via `pgoutput`, WAL position tracking, initial backfill + tail mode.

### `crates/astra-core`

Core domain value objects shared across the workspace. Small types that don't belong to any specific subsystem.

### `crates/astra-api`

HTTP API client types for the control-plane API. Used by CLI commands that communicate with the control plane.

### `crates/astra-secrets`

Credential management abstraction. Currently a stub — `status()` returns a string. The `env:` prefix for `passwordRef` is resolved directly in `crates/astra-connectors` without going through this crate. Vault and file-based secrets are planned.

### `crates/astra-observability`

Tracing and metrics stubs. Initializes `tracing-subscriber` for structured logging. Metrics export is planned but not implemented.

### `crates/astra-python-runtime`

Subprocess host for Python connector execution. Planned: reads a connector manifest, spawns a Python process, communicates via stdin/stdout JSON protocol.

### `crates/astra-saas-sdk`

SaaS connector SDK definitions: connector manifest format, stream/auth/pagination abstractions for community Python connectors.

## Dependency rules

To keep the modular monolith clean:

```
apps/control-plane  →  crates/*  (any)
apps/cli            →  crates/*  (any)
crates/*            →  crates/*  (DAG — no cycles)
apps/web            →  (no Rust deps — TypeScript only)
```

Specifically:
- `crates/astra-connectors` must NOT depend on `apps/control-plane`
- `crates/astra-runtime` must NOT depend on `crates/astra-connectors`
- `crates/astra-metadata` is a leaf — depended on by everything, depends on nothing internal
