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
- API handlers in `src/api/`

### `apps/cli`

The `astra` CLI binary. Built with [Clap](https://docs.rs/clap).

Commands: `validate`, `discover-source`, `snapshot-to-local-staging`, `execute-local-snapshot`, `run-pipeline`, `apply-spec`.

### `apps/web`

React 18 + TypeScript + Vite frontend. Built separately and embedded as static assets in the control plane binary at compile time.

### `apps/worker`

Stub for a future distributed worker that executes pipeline runs on behalf of the control plane. Not implemented.

---

## Library crates

### `crates/astra-yaml`

YAML spec parsing and validation for the `v1alpha1` pipeline spec.

Key types:
- `PipelineSpec` — top-level spec struct (derives `serde::Deserialize`)
- `SourceConfig`, `DestinationConfig`, `RuntimeConfig`
- `validate(spec: &PipelineSpec) -> Result<(), Vec<ValidationError>>`

Depended on by: `apps/control-plane`, `apps/cli`

### `crates/astra-metadata`

Shared enums and domain types used across the workspace:

- `PipelineStatus` — `Active`, `Paused`, `Error`
- `RunStatus` — `Started`, `Completed`, `Failed`
- `JobKind` — `Snapshot`, `Incremental`, `Cdc`
- `RunPhase` — `Capture`, `Load`, `Done`

Depended on by: all crates that deal with pipeline state.

### `crates/astra-runtime`

Staging backends and checkpoint logic. The core durability layer.

Key types:
- `StagingBackend` trait — `write_chunk`, `read_chunk`, `list_chunks`
- `LocalStagingBackend` — writes to the local filesystem
- `MinioStagingBackend` / `S3StagingBackend` — writes to object storage
- `CheckpointLedger` — reads/writes checkpoint state
- `StageChunk` — chunk metadata struct

### `crates/astra-connectors`

Source and destination connector implementations.

Key types:
- `PostgresSourceConnector` — `discover()`, `snapshot()`, `snapshot_incremental()`
- `PostgresDestinationConnector` — `load_chunks()`

The Postgres destination creates and populates `astra_raw.<table>` tables.

### `crates/astra-cdc`

CDC orchestration. Currently a stub that returns an explicit "not implemented" error when triggered.

Planned: Postgres logical replication via `pgoutput`, WAL position tracking, initial backfill + tail mode.

### `crates/astra-core`

Core domain value objects shared across the workspace. Small types that don't belong to any specific subsystem.

### `crates/astra-api`

HTTP API client types for the control-plane API. Used by the CLI commands that talk to the control plane (`run-pipeline`, `apply-spec`).

### `crates/astra-secrets`

Credential management abstraction. Currently supports `env:<VAR>` secret references. Vault and file-based secrets are planned.

Key types:
- `SecretResolver` trait
- `EnvSecretResolver` — resolves `env:VAR` to environment variable values

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
