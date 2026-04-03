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
# Pipeline spec is valid.
```

Exits with code 0 on success, non-zero on validation errors. All validation errors are printed to stderr.

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

### `snapshot-to-local-staging`

Run the capture phase: paginate through source tables and write compressed JSONL.gz chunks to the configured staging location.

```bash
cargo run -p astra -- snapshot-to-local-staging <spec-file> [--no-resume]
```

**Options:**

| Flag | Description |
|---|---|
| `--no-resume` | Ignore existing checkpoints and restart from scratch. |

**Example:**

```bash
POSTGRES_PASSWORD=secret \
  cargo run -p astra -- snapshot-to-local-staging my-pipeline.astra.yaml
```

**What it does:**

1. Reads the pipeline spec
2. Opens a connection to the source Postgres database
3. Paginates each table in `chunkSize` batches
4. Serializes each batch as JSONL (one JSON object per row)
5. Compresses each batch with gzip
6. Writes chunks to the staging location (local or MinIO)
7. Records each staged chunk in the checkpoint ledger

The checkpoint ledger lives at `.astra/checkpoints/<pipeline-name>/` by default. If a run is interrupted and restarted, already-staged chunks are skipped.

---

### `execute-local-snapshot`

Run the load phase: read staged chunks and bulk-load them into the destination.

```bash
cargo run -p astra -- execute-local-snapshot <spec-file>
```

**Example:**

```bash
POSTGRES_PASSWORD=secret \
  cargo run -p astra -- execute-local-snapshot my-pipeline.astra.yaml
```

**What it does:**

1. Reads the checkpoint ledger to find all staged chunks
2. Decompresses each chunk
3. Parses JSONL rows
4. Bulk-inserts into `astra_raw.<table>` in the destination Postgres database

The destination tables are created automatically if they don't exist. Each row in `astra_raw` has:

| Column | Type | Description |
|---|---|---|
| `_object_key` | `TEXT` | Source staging object key |
| `_sequence` | `BIGINT` | Chunk sequence number |
| `_row_number` | `BIGINT` | Row position within the chunk |
| `_loaded_at` | `TIMESTAMPTZ` | Timestamp when the row was loaded |
| `_data` | `JSONB` | The full source row as a JSON object |

---

### `run-pipeline`

Trigger a pipeline run via the control-plane API (requires a running control plane).

```bash
cargo run -p astra -- run-pipeline <pipeline-name>
```

**Example:**

```bash
cargo run -p astra -- run-pipeline smoke-local-snapshot
```

---

### `apply-spec`

Register or update a pipeline spec via the control-plane API.

```bash
cargo run -p astra -- apply-spec <spec-file>
```

**Example:**

```bash
cargo run -p astra -- apply-spec my-pipeline.astra.yaml
```

Equivalent to `POST /api/v1/specs/apply`. The control plane must be running and reachable at `ASTRA_CONTROL_PLANE_ADDR` (default `127.0.0.1:8080`).

---

## Environment variables

The CLI reads these variables at runtime:

| Variable | Used by | Description |
|---|---|---|
| `env:<VAR>` | YAML spec | Any variable referenced via `env:` in a spec |
| `ASTRA_CONTROL_PLANE_ADDR` | `run-pipeline`, `apply-spec` | Address of the control plane (default `127.0.0.1:8080`) |
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
