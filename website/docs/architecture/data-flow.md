---
id: data-flow
title: Data Flow
sidebar_position: 3
---

# Data Flow

This page describes how data moves through Astra from source to destination.

## Two-phase execution

Every Astra pipeline run has two phases:

1. **Capture (snapshot-to-staging)** — read from the source and write chunks to staging
2. **Load (staging-to-destination)** — read chunks from staging and write to the destination

The two phases are decoupled by the staging layer. This means:

- A run can be interrupted after phase 1 and resumed later
- The destination load can be retried independently of the source query
- Different storage backends (local, MinIO, S3) can be swapped without changing capture or load logic

## Phase 1: Capture

```
Source Postgres
    │
    │  SELECT * FROM table ORDER BY pk LIMIT chunkSize OFFSET n
    │
    ▼
Postgres Connector (crates/astra-connectors)
    │
    │  Vec<Row> → serialize to JSONL
    │
    ▼
astra-runtime StageWriter
    │
    │  gzip compress → write chunk
    │
    ▼
Staging backend
    ├── Local: .astra/staging/<pipeline>/.../<seq>.jsonl.gz
    └── MinIO/S3: pipelines/<pipeline>/streams/<stream>/partitions/<p>/chunks/<seq>.jsonl.gz
    │
    ▼
Checkpoint ledger
    └── .astra/checkpoints/<pipeline>/<stream>.ledger
```

### Chunk lifecycle

1. Connector queries a page of rows from the source
2. Rows are serialized as newline-delimited JSON (one object per row)
3. The JSONL byte stream is compressed with gzip
4. The compressed chunk is written atomically to the staging backend
5. The chunk's metadata (`object_key`, `sequence`, `row_count`, `schema_fingerprint`) is appended to the checkpoint ledger
6. The next page is queried

If the process dies between steps 4 and 5, the orphaned chunk file is ignored on restart (the ledger is the source of truth).

## Phase 2: Load

```
Checkpoint ledger
    │
    │  list staged chunks
    │
    ▼
astra-runtime ChunkReader
    │
    │  decompress → parse JSONL
    │
    ▼
Postgres Destination Connector
    │
    │  COPY / INSERT INTO astra_raw.raw_<schema>_<table>
    │
    ▼
Destination Postgres
    │
    ▼
astra_raw._applied_chunks  ← record applied chunk key
```

### Idempotency

Before loading a chunk, the destination connector checks `astra_raw._applied_chunks`. If the chunk's `object_key` is already present, the chunk is skipped. This makes the load phase safe to re-run.

## Staging object key layout

```
pipelines/
  <pipeline-name>/
    streams/
      <stream-name>/
        partitions/
          <partition-id>/
            chunks/
              00000001.jsonl.gz
              00000002.jsonl.gz
              ...
```

For local staging, the path is prefixed with `$ASTRA_STAGING_LOCAL_ROOT/<bucket>/`.

## Incremental mode watermark

In incremental snapshot mode, the checkpoint ledger also records the last seen cursor value per stream:

```
ledger entry:
  stream: public.users
  last_cursor: 2024-01-15T10:05:42Z   ← updated_at value of last row in chunk
  staged_chunks: [00000001, 00000002, ...]
```

On the next run, the capture query becomes:
```sql
SELECT * FROM public.users
WHERE updated_at > '2024-01-15T10:05:42Z'
ORDER BY updated_at
LIMIT 50000 OFFSET 0
```

## Runtime metadata

While a run is in progress, the control plane tracks state in Postgres:

- `pipeline_runs` table — overall run status, start/end time
- `table_executions` table — per-table status within a run (rows captured, chunks staged, error message)

This metadata powers the web UI's run history and table drill-down views.
