---
id: overview
title: Connector Overview
sidebar_position: 1
---

# Connectors

Connectors are the adapters that translate between a source or destination system and Astra's internal staging format. Astra uses a two-tier connector model.

## Tier A: Rust-native connectors

Core, high-volume connectors are written in Rust and compiled into the main binary. These run on the hot path and benefit directly from Astra's zero-overhead runtime.

**Implemented in v0.1:**
- [Postgres source →](./postgres.md) — full snapshot, incremental snapshot, schema discovery
- Postgres destination — raw loader to `astra_raw` schema

**Planned:**
- MySQL source
- Snowflake destination
- BigQuery destination

## Tier B: Python runtime connectors

Community and long-tail connectors will run as bounded subprocesses (Python 3). This isolates them from the hot path while still allowing the connector ecosystem to grow without requiring Rust expertise.

The Python runtime (`crates/astra-python-runtime`) is scaffolded but not yet implemented. Connector manifests and the subprocess protocol are defined in `crates/astra-saas-sdk`.

## How connectors are structured

The Postgres source is implemented as `PostgresSource` in `crates/astra-connectors`. Key methods:

- `from_spec(spec: &AstraSpec)` — constructs the connector from a parsed YAML spec
- `discover()` — queries the source database and returns the schema of all captured tables
- `snapshot_to_jsonl_gzip()` — paginates rows and writes compressed JSONL.gz chunks to staging

The Postgres destination is implemented as `PostgresDestinationLoader`. Key methods:

- `from_spec(spec: &AstraSpec)` — constructs the loader from a parsed YAML spec
- `load_local_stage_chunks()` — reads staged JSONL.gz chunks and bulk-inserts them into `astra_raw`

## Staging format

Between capture and load, data is stored as compressed JSONL:

- One JSON object per row
- Compressed with gzip
- Sequence number zero-padded to 20 digits
- Stored at the following path:

```
pipelines/<pipeline_name>/streams/<stream_name>/partitions/<partition_key>/chunks/<sequence:020>.jsonl.gz
```

**Example:**

```
pipelines/smoke-local-snapshot/streams/public.smoke_users/partitions/default/chunks/00000000000000000001.jsonl.gz
```

See [Staging Contract →](../architecture/staging-contract.md) for the full spec.
