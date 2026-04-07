# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Astra

Astra is a self-hostable data replication platform (v0.1) — a Rust-first alternative to Airbyte/Fivetran for database CDC and bulk snapshot replication. Pipelines are defined in YAML (`v1alpha1` spec) and executed via CLI or control-plane API.

## Commands

### Rust (workspace root)
```bash
cargo build                    # build all crates
cargo test --workspace         # test all crates
cargo test -p <crate>          # test a single crate
cargo test <test_name>         # run a specific test by name
cargo clippy --workspace       # lint
cargo fmt --all                # format — always run before committing
```

### Control plane
```bash
cargo run -p astra-control-plane                          # in-memory mode
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra cargo run -p astra-control-plane
```

### CLI
```bash
cargo run -p astra -- validate examples/postgres-to-warehouse.astra.yaml
cargo run -p astra -- discover-source examples/postgres-to-warehouse.astra.yaml
cargo run -p astra -- snapshot-to-local-staging examples/postgres-to-warehouse.astra.yaml
cargo run -p astra -- execute-local-snapshot examples/postgres-to-warehouse.astra.yaml
```

### Web app (`apps/web`)
```bash
npm install && npm run dev     # Vite dev server at 127.0.0.1:4173
npm run build
npm run lint                   # TypeScript type check
```

### Local infrastructure
```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d   # starts Postgres + MinIO
```

### Python smoke test
```bash
python3 scripts/yaml_contract_smoke.py
```

## Architecture

**Modular monolith** (see `website/docs/architecture/modular-monolith.md`): single control-plane binary with clean internal boundaries, Postgres for metadata, S3/MinIO for durable staging.

### Workspace layout

| Path | Purpose |
|------|---------|
| `apps/control-plane` | Axum HTTP API + scheduler + metadata manager |
| `apps/cli` | Clap CLI — validate, discover, snapshot, load |
| `apps/web` | React 18 + TypeScript + Vite frontend |
| `crates/astra-yaml` | YAML spec parsing/validation (`v1alpha1`) |
| `crates/astra-metadata` | Shared enums/types (PipelineStatus, JobKind, RunPhase…) |
| `crates/astra-runtime` | Staging abstraction — local, S3, MinIO; chunked JSONL.gz |
| `crates/astra-connectors` | Postgres source (snapshot + discovery) + destination (raw load) |
| `crates/astra-cdc` | CDC orchestration (stub) |
| `deploy/docker-compose` | Local Postgres + MinIO stack |

### Request / data flow

1. **Spec ingestion** — YAML parsed by `astra-yaml`, validated, registered via `PipelineService`
2. **Repository layer** — `PipelineRepository` trait with two impls: `PostgresPipelineRepository` (persistent) and `InMemoryPipelineRepository` (dev fallback)
3. **Control plane REST API** — `/api/v1/pipelines`, `/api/v1/pipeline-runs`, `/api/v1/specs/apply`; web UI served from the same binary
4. **Snapshot execution** — CLI calls Postgres connector to paginate rows → `astra-runtime` stages JSONL.gz chunks → destination connector loads raw tables (`astra_raw` schema)
5. **Checkpointing** — `astra-runtime` persists a ledger of staged chunks; CLI `--no-resume` restarts from scratch

### Key design patterns

- **Service/Repository**: `PipelineService` is the only entry point to state; callers never touch the repo directly
- **Trait-based storage backends**: swap Postgres ↔ in-memory without changing service code
- **Spec-as-data**: YAML spec is parsed once, serialised to JSON, and shared across CLI, API, and UI
- **Staging contract** (`website/docs/architecture/staging-contract.md`): local/S3/MinIO share the same `StageChunk` metadata schema
- **Resumable workflows**: checkpoint ledger tracks processed chunks; snapshot can resume mid-run

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `ASTRA_CONTROL_PLANE_ADDR` | `127.0.0.1:8080` | Bind address |
| `ASTRA_DATABASE_URL` | _(none — in-memory)_ | Postgres metadata DB |
| `ASTRA_S3_ENDPOINT` | — | MinIO/S3 endpoint |
| `ASTRA_S3_REGION` | `us-east-1` | |
| `ASTRA_S3_ACCESS_KEY` / `ASTRA_S3_SECRET_KEY` | — | S3 credentials |
| `ASTRA_STAGING_LOCAL_ROOT` | — | Local staging dir |
| `ASTRA_CHECKPOINT_LOCAL_ROOT` | — | Checkpoint ledger dir |
| `POSTGRES_PASSWORD` | — | Source Postgres password |

See `.env.example` for a full reference.

### What is and isn't implemented (v0.1)

**Working**: YAML validation, Postgres schema discovery, local snapshot → JSONL.gz staging with resumption, MinIO staging, raw Postgres loading, control-plane API + web UI, in-memory and Postgres-backed persistence.

**Stubbed / not yet implemented**: CDC execution (returns explicit error), Python connector runtime, secrets management beyond `env:KEY`, observability, worker pool, schema evolution, multi-table parallelism.

## Pre-PR Checklist

Before raising a pull request, ensure the following:

### Documentation updates (required when functionality changes)

If your changes affect any of the following, update the relevant documentation **before** opening the PR:

- **New or changed CLI commands** → update `website/docs/cli/` and the `Commands` section of this file
- **New or changed API endpoints** → update `website/docs/control-plane/`
- **New or changed YAML spec fields** (`v1alpha1`) → update `website/docs/yaml-spec/`
- **New or changed connector behaviour** → update `website/docs/connectors/`
- **New or changed environment variables** → update the `Environment variables` table in this file and `.env.example`
- **New or changed architecture / data flow** → update `website/docs/architecture/`
- **Newly implemented stubs** → move items from the *Stubbed / not yet implemented* list above to the *Working* list
- **`CHANGELOG.md`** → add an entry describing the change under the appropriate version heading

> **Rule of thumb**: if a reviewer would need to understand your change to use or operate Astra correctly, the docs must be updated in the same PR.
