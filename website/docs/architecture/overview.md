---
id: overview
title: Architecture Overview
sidebar_position: 1
---

# Architecture Overview

Astra is a **modular monolith**: a single control-plane binary with clean internal boundaries, rather than a fleet of microservices. This section explains the key design decisions and how the pieces fit together.

## High-level layout

```
┌─────────────────────────────────────────────────────────┐
│                   astra-control-plane                   │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐ │
│  │  Axum    │  │Scheduler │  │  PipelineService       │ │
│  │  HTTP    │  │          │  │  (only entry to state) │ │
│  │  API     │  │          │  └───────────┬───────────┘ │
│  └────┬─────┘  └─────┬────┘              │             │
│       │              │         ┌──────────▼──────────┐ │
│       └──────────────┘         │  PipelineRepository │ │
│                                │  (Postgres | InMem) │ │
│                                └─────────────────────┘ │
└─────────────────────────────────────────────────────────┘
         │                                │
    REST API                         Postgres
    Web UI                           (metadata)
         │
   ┌─────▼──────┐      ┌─────────────────┐
   │  astra CLI │ ───► │ astra-connectors │
   └────────────┘      │  (Postgres src) │
                       └────────┬────────┘
                                │
                       ┌────────▼────────┐
                       │  astra-runtime  │
                       │  (staging +     │
                       │   checkpoints)  │
                       └────────┬────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
              Local disk              MinIO / S3
              (JSONL.gz chunks)       (JSONL.gz chunks)
```

## Workspace structure

| Path | Purpose |
|---|---|
| `apps/control-plane` | Axum HTTP API + scheduler + metadata manager |
| `apps/cli` | Clap CLI — validate, discover, snapshot, load |
| `apps/web` | React 18 + TypeScript + Vite frontend |
| `apps/worker` | Distributed worker stub (future) |
| `crates/astra-yaml` | YAML spec parsing and validation (`v1alpha1`) |
| `crates/astra-metadata` | Shared enums/types: PipelineStatus, JobKind, RunPhase |
| `crates/astra-runtime` | Staging adapters (local, S3, MinIO) + checkpoint ledger |
| `crates/astra-connectors` | Postgres source (snapshot + discovery) + destination (raw load) |
| `crates/astra-cdc` | CDC orchestration (stub) |
| `crates/astra-core` | Core domain types |
| `crates/astra-api` | HTTP API client types |
| `crates/astra-secrets` | Credential management abstraction |
| `crates/astra-observability` | Tracing/metrics stubs |
| `crates/astra-python-runtime` | Python connector subprocess host (future) |
| `crates/astra-saas-sdk` | SaaS connector SDK (future) |

## Key design patterns

### Service/Repository separation

`PipelineService` is the only entry point to pipeline state. Controllers and handlers never touch the repository directly. This preserves a clear domain layer and makes the repository implementation swappable.

```
API handler → PipelineService → PipelineRepository (Postgres or InMemory)
```

### Trait-based storage backends

The `PipelineRepository` trait has two implementations:

- `PostgresPipelineRepository` — persistent, used in production
- `InMemoryPipelineRepository` — ephemeral, used in dev and tests

Switching between them requires only changing the `ASTRA_DATABASE_URL` environment variable.

### Spec-as-data

The YAML spec is parsed once by `astra-yaml`, serialized to JSON, and stored alongside the pipeline record. The UI, CLI, and API all operate on the same parsed representation. There is no parallel config system.

### Resumable workflows

Every snapshot run writes a checkpoint ledger entry per chunk. On restart, the ledger is read first and already-staged chunks are skipped. This makes every snapshot resumable at the chunk boundary.

## Request flow

1. Client sends `POST /api/v1/specs/apply` with a YAML string
2. Control plane parses and validates the spec via `astra-yaml`
3. `PipelineService` stores the pipeline record via `PipelineRepository`
4. Client triggers a run via `POST /api/v1/pipelines/:id/runs`
5. The embedded executor calls the Postgres connector
6. Connector paginates the source table in chunks
7. `astra-runtime` compresses each chunk to JSONL.gz and writes to staging
8. Checkpoint ledger is updated after each chunk
9. Load phase reads chunks from staging and inserts into `astra_raw`

## Further reading

- [ADR-0001: Why a modular monolith? →](./modular-monolith.md)
- [Data flow and staging →](./data-flow.md)
- [Staging contract spec →](./staging-contract.md)
