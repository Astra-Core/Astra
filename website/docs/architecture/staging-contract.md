---
id: staging-contract
title: Staging Contract
sidebar_position: 4
---

# Staging Contract

The staging contract defines the interface between the capture phase and the load phase. All staging backends (local, MinIO, S3) implement the same contract.

## StageChunk metadata

Every staged chunk carries this metadata:

| Field | Type | Description |
|---|---|---|
| `pipeline` | `String` | Pipeline name |
| `stream` | `String` | Stream (table) name, e.g. `public.users` |
| `partition` | `String` | Partition identifier (currently `"default"`) |
| `sequence` | `u64` | Monotonically increasing chunk number within the stream |
| `row_count` | `u64` | Number of rows in this chunk |
| `schema_fingerprint` | `String` | Hash of the source schema at capture time |
| `object_key` | `String` | Full object key for the chunk file |

## Object key convention

```
pipelines/<pipeline>/streams/<stream>/partitions/<partition>/chunks/<sequence>.jsonl.gz
```

Example:

```
pipelines/smoke-local-snapshot/streams/public.smoke_users/partitions/default/chunks/00000001.jsonl.gz
```

### Local adapter

For local staging, the key is prefixed with the local root and bucket:

```
$ASTRA_STAGING_LOCAL_ROOT/<bucket>/<key>
```

Example:

```
.astra/staging/astra-smoke-staging/pipelines/smoke-local-snapshot/...
```

### MinIO / S3 adapter

The key is used as-is within the configured S3 bucket. The `ASTRA_S3_ENDPOINT`, `ASTRA_S3_REGION`, `ASTRA_S3_ACCESS_KEY`, and `ASTRA_S3_SECRET_KEY` variables control the connection.

## Chunk format

Each chunk file is a gzip-compressed JSONL file:

```
{"id":1,"name":"Alice","email":"alice@example.com","created_at":"2024-01-15T10:00:00Z"}
{"id":2,"name":"Bob","email":"bob@example.com","created_at":"2024-01-15T10:00:01Z"}
...
```

- One JSON object per line
- UTF-8 encoded
- Newline (`\n`) delimited
- gzip compressed (`.jsonl.gz`)
- All values follow the [Postgres type mapping](../connectors/postgres.md#supported-data-types)

## Checkpoint ledger

The checkpoint ledger tracks which chunks have been captured and (separately) which have been applied to the destination.

**Ledger file location:**

```
$ASTRA_CHECKPOINT_LOCAL_ROOT/<pipeline-name>/<stream-name>.ledger
```

**Ledger entry format:**

```json
{
  "object_key": "pipelines/smoke-local-snapshot/streams/public.smoke_users/...",
  "sequence": 1,
  "row_count": 1000,
  "cursor_value": "2024-01-15T10:05:42Z",
  "staged_at": "2024-01-15T10:05:43Z"
}
```

## Destination tracking

The Postgres destination records applied chunks in `astra_raw._applied_chunks`:

```sql
CREATE TABLE astra_raw._applied_chunks (
    object_key   TEXT PRIMARY KEY,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

This makes the load phase idempotent: re-running `execute-local-snapshot` skips already-applied chunks.

## Backend implementations

| Backend | Kind value | Implementation crate |
|---|---|---|
| Local filesystem | `local` | `crates/astra-runtime` |
| MinIO | `minio` | `crates/astra-runtime` |
| AWS S3 | `s3` | `crates/astra-runtime` |

All three implement the same `StagingBackend` trait. The pipeline spec's `destination.staging.kind` selects which implementation is used at runtime.
