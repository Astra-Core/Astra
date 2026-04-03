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

## Connector interface

Every source connector implements the `SourceConnector` trait:

```rust
// crates/astra-connectors
pub trait SourceConnector {
    async fn discover(&self, config: &SourceConfig) -> Result<Schema>;
    async fn snapshot(&self, config: &SourceConfig, stream: &StreamConfig)
        -> Result<impl Stream<Item = Result<RecordBatch>>>;
}
```

Every destination connector implements the `DestinationConnector` trait:

```rust
pub trait DestinationConnector {
    async fn load(&self, config: &DestinationConfig, chunks: impl Iterator<Item = StagedChunk>)
        -> Result<LoadResult>;
}
```

## Staging format

Between capture and load, data is stored as compressed JSONL:

- One JSON object per row
- Compressed with gzip
- Chunks named `<sequence>.jsonl.gz`
- Stored at `pipelines/<name>/streams/<stream>/partitions/<partition>/chunks/<seq>.jsonl.gz`

See [Staging Contract →](../architecture/staging-contract.md) for the full spec.
