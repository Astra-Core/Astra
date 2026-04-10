---
id: intro
title: Introduction
sidebar_position: 1
---

# Astra

**Astra** is a self-hostable data replication platform — a Rust-first alternative to Airbyte and Fivetran for database CDC and bulk snapshot replication.

Pipelines are defined in YAML (`v1alpha1` spec) and executed via the CLI or the control-plane REST API. Everything ships as a single binary backed by Postgres and S3-compatible object storage.

## Why Astra?

| Problem with existing tools | How Astra addresses it |
|---|---|
| Managed services are expensive and opaque | Fully self-hostable; Apache 2.0 |
| Open-source alternatives are operationally heavy | Single binary, Docker Compose in one command |
| Slow on CDC and bulk replication hot paths | Rust-native core; no JVM, no orchestration overhead |
| UI and YAML config drift apart | One canonical spec shared by CLI, API, and web UI |
| Syncs can't survive failures mid-run | Resumable checkpoint ledger built into every pipeline |

## Core concepts

- **Pipeline** — A named replication job defined in YAML. Specifies source, destination, mode (snapshot / incremental / CDC), and schedule.
- **Run** — A single execution of a pipeline. Each run produces one or more staged chunks.
- **Staged chunk** — A compressed JSONL.gz file written to local disk or object storage. The atomic unit of durability.
- **Checkpoint** — A ledger entry tracking which chunks have been captured and applied. Enables resumption.
- **Connector** — A source or destination adapter. Today: Postgres source + Postgres destination. More coming.

## What's in v0.1

**Working**: YAML validation, Postgres schema discovery, full and incremental snapshot, local/MinIO/S3 staging (JSONL.gz), Postgres raw destination loader, resumable checkpoint ledger, 7 CLI commands, control-plane REST API (17+ endpoints), React web UI with pipeline dashboard, run history, and YAML Studio.

**Not yet implemented**: CDC execution (stub), Python connector runtime, Snowflake/BigQuery destinations, merge/upsert/replace write modes, schema evolution enforcement, distributed workers, observability.

## Quick navigation

- [Quickstart →](./getting-started/quickstart.md) — up and running in 15 minutes
- [YAML Spec →](./yaml-spec/overview.md) — full pipeline spec reference
- [CLI Reference →](./cli/reference.md) — all seven commands documented
- [API Reference →](./control-plane/api.md) — REST API for the control plane
- [Architecture →](./architecture/overview.md) — design decisions and data flow
