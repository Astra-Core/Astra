---
id: reference
title: CLI Reference
sidebar_position: 1
---

# CLI Reference

The `astra` CLI is the primary interface for validating specs and executing pipeline runs locally. It is built with [Clap](https://docs.rs/clap) and lives in `apps/cli`.

## Build

```bash
cargo build -p astra
# or run directly:
cargo run -p astra -- <command> [options]
```

## Commands

### `validate`

Parse and validate a pipeline spec without connecting to any systems.

```bash
cargo run -p astra -- validate <spec-file>
```

**Example:**

```bash
cargo run -p astra -- validate examples/smoke-local-snapshot.astra.yaml
# valid Astra spec: smoke-local-snapshot  mode=snapshot  source=postgres  dest=postgres  tables=["public.smoke_users","public.smoke_orders"]
```

Exits with code 0 on success, non-zero on validation errors. All validation errors are printed to stderr.

---

### `apply`

Validate a pipeline spec and print the normalized representation. Intended to send the spec to the control-plane API — full API submission is not yet wired up in v0.1.

```bash
cargo run -p astra -- apply <spec-file>
```

**Example:**

```bash
cargo run -p astra -- apply my-pipeline.astra.yaml
```

---

### `discover-source`

Connect to the source database and print discovered table schemas.

```bash
cargo run -p astra -- discover-source <spec-file>
```

**Example:**

```bash
POSTGRES_PASSWORD=secret \
  cargo run -p astra -- discover-source my-pipeline.astra.yaml
```

Output includes table names, column names, data types, nullability, and primary key annotations. Useful for verifying connectivity and building the `capture.tables` list in your spec.

---

### `test-connection`

Test connectivity for the source or destination in an Astra YAML spec.

```bash
cargo run -p astra -- test-connection <spec-file> [--target source|destination]
```

**Options:**

| Flag | Description |
|---|---|
| `--target <TARGET>` | Which side to test: `source` or `destination` (default: `source`). |

**Example:**

```bash
POSTGRES_PASSWORD=secret \
  cargo run -p astra -- test-connection my-pipeline.astra.yaml --target source
# status: ok
# latency_ms: 4
```

**What it does:**

- Establishes a connection to the target database and runs `SELECT 1` to measure round-trip latency.
- For the `source` target, also verifies that every table in `capture.tables` exists in `information_schema.tables`.
- Exits with code 0 on success, non-zero if the connection fails or any configured table is missing.

**Failure output:**

```
status: error
message: failed to connect to Postgres source at db.example.com:5432: connection refused
```

---

### `snapshot-to-local-staging`

Run the capture phase: paginate through source tables and write compressed JSONL.gz chunks to a local directory.

```bash
cargo run -p astra -- snapshot-to-local-staging <spec-file> [options]
```

**Options:**

| Flag | Description |
|---|---|
| `--no-resume` | Ignore existing checkpoints and restart from scratch. |
| `--max-rows-per-table <N>` | Stop after capturing N rows per table (useful for testing). |
| `--staging-root <PATH>` | Override the local staging root directory. |
| `--checkpoint-root <PATH>` | Override the checkpoint ledger root directory. |
| `--chunk-size <N>` | Override the chunk size from the spec. |
| `--control-plane-url <URL>` | Control plane URL for recording run metadata. |

**Example:**

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- snapshot-to-local-staging examples/smoke-local-snapshot.astra.yaml
```

**What it does:**

1. Reads the pipeline spec
2. Opens a connection to the source Postgres database
3. Paginates each table in `chunkSize` batches
4. Serializes each batch as JSONL (one JSON object per row)
5. Compresses each batch with gzip
6. Writes chunks to the local staging location
7. Records each staged chunk in the checkpoint ledger

The checkpoint ledger lives at `.astra/checkpoints/<pipeline-name>/` by default. If a run is interrupted and restarted, already-staged chunks are skipped.

---

### `snapshot-to-minio-staging`

Run the capture phase and write compressed JSONL.gz chunks to a MinIO (or S3-compatible) bucket.

```bash
cargo run -p astra -- snapshot-to-minio-staging <spec-file> [options]
```

**Options:**

| Flag | Description |
|---|---|
| `--max-rows-per-table <N>` | Stop after capturing N rows per table. |
| `--endpoint <URL>` | MinIO/S3 endpoint URL (e.g. `http://localhost:9000`). |
| `--region <REGION>` | S3 region (default: `us-east-1`). |
| `--access-key <KEY>` | S3 access key. |
| `--secret-key <KEY>` | S3 secret key. |

**Example:**

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- snapshot-to-minio-staging examples/smoke-local-snapshot.astra.yaml \
    --endpoint http://localhost:9000 \
    --access-key astra \
    --secret-key astrastorage
```

---

### `load-local-staging-to-postgres`

Run the load phase: read staged JSONL.gz chunks from local storage and bulk-load them into the destination Postgres database.

```bash
cargo run -p astra -- load-local-staging-to-postgres <spec-file> [options]
```

**Options:**

| Flag | Description |
|---|---|
| `--staging-root <PATH>` | Override the local staging root directory. |
| `--control-plane-url <URL>` | Control plane URL for recording run metadata. |

**Example:**

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- load-local-staging-to-postgres examples/smoke-local-snapshot.astra.yaml
```

---

### `execute-local-snapshot`

Run the full snapshot pipeline end-to-end: capture to local staging, then load to destination. Equivalent to running `snapshot-to-local-staging` followed by `load-local-staging-to-postgres`.

```bash
cargo run -p astra -- execute-local-snapshot <spec-file> [options]
```

**Options:**

| Flag | Description |
|---|---|
| `--no-resume` | Ignore existing checkpoints and restart from scratch. |
| `--max-rows-per-table <N>` | Stop after capturing N rows per table. |
| `--staging-root <PATH>` | Override the local staging root directory. |
| `--checkpoint-root <PATH>` | Override the checkpoint ledger root directory. |
| `--chunk-size <N>` | Override the chunk size from the spec. |
| `--control-plane-url <URL>` | Control plane URL for recording run metadata. |

**Example:**

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml
```

**What it does:**

1. Reads the checkpoint ledger to find already-staged chunks (skips them unless `--no-resume`)
2. Paginates the source database and stages chunks to local storage
3. Decompresses each staged chunk
4. Parses JSONL rows
5. Bulk-inserts into `astra_raw.<table>` in the destination Postgres database

The destination tables are created automatically if they don't exist. Each row in `astra_raw` has:

| Column | Type | Description |
|---|---|---|
| `_sequence` | `BIGINT` | Chunk sequence number |
| `_loaded_at` | `TIMESTAMPTZ` | Timestamp when the row was loaded |
| `_data` | `JSONB` | The full source row as a JSON object |

---

## Environment variables

The CLI reads these variables at runtime:

| Variable | Used by | Description |
|---|---|---|
| `env:<VAR>` | YAML spec | Any variable referenced via `env:` in a spec's `passwordRef` field |
| `ASTRA_STAGING_LOCAL_ROOT` | staging | Root directory for local staging |
| `ASTRA_CHECKPOINT_LOCAL_ROOT` | checkpointing | Root directory for checkpoint ledgers |
| `ASTRA_S3_ENDPOINT` | MinIO/S3 staging | S3 endpoint URL |
| `ASTRA_S3_REGION` | MinIO/S3 staging | S3 region (default `us-east-1`) |
| `ASTRA_S3_ACCESS_KEY` | MinIO/S3 staging | S3 access key |
| `ASTRA_S3_SECRET_KEY` | MinIO/S3 staging | S3 secret key |

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Validation error or configuration problem |
| `2` | Runtime error (connection failure, IO error, etc.) |
