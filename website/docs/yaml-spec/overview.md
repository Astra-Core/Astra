---
id: overview
title: YAML Spec Overview
sidebar_position: 1
---

# Pipeline YAML Spec (v1alpha1)

Astra pipelines are defined in a YAML file. The same spec is used by the CLI, the control-plane API (`/api/v1/specs/apply`), and the web UI's YAML studio. There is exactly one canonical format — no drift between interfaces.

## Top-level structure

```yaml
version: v1alpha1

pipeline:
  name: my-pipeline
  mode: snapshot          # snapshot | incremental | cdc
  schedule: manual        # manual | continuous | cron expression

source:
  kind: postgres
  connection: "<host>:<port>/<database>"
  credentials:
    user: "<username>"
    password: "env:<ENV_VAR>"
  capture:
    tables:
      - public.users
      - public.orders
    snapshot:
      mode: full            # full | incremental
      chunkSize: 50000

destination:
  kind: postgres            # postgres | snowflake | bigquery (future)
  connection: "<host>:<port>/<database>"
  credentials:
    user: "<username>"
    password: "env:<ENV_VAR>"
  staging:
    kind: local             # local | s3 | minio
    bucket: my-staging-bucket
    prefix: my-pipeline/
  write:
    mode: append            # append | merge (future)
    batchSize: 100000

runtime:
  parallelism:
    tables: 1               # parsed but not yet enforced in v0.1
  checkpointing:
    intervalSeconds: 30
```

## Fields

### `version`

Always `v1alpha1`. Future versions will introduce new spec versions with migration paths.

### `pipeline`

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Unique pipeline identifier. Used in staging paths and API endpoints. |
| `mode` | Yes | `snapshot` (full table copy), `incremental` (cursor-based), or `cdc` (log-based; not yet implemented). |
| `schedule` | Yes | `manual` (trigger only via CLI/API), `continuous` (run as soon as previous finishes), or a cron expression like `"0 * * * *"` (hourly). |

### `source`

| Field | Required | Description |
|---|---|---|
| `kind` | Yes | Source connector type. Currently only `postgres`. |
| `connection` | Yes | `host:port/database` format. |
| `credentials.user` | Yes | Database username. |
| `credentials.password` | Yes | Plain string or `env:<VAR_NAME>` to read from environment. |
| `capture.tables` | Yes | List of `schema.table` pairs to replicate. |
| `capture.snapshot.mode` | Yes (snapshot/incremental) | `full` — replicate all rows every run. `incremental` — only rows where `cursorField > lastCheckpoint`. |
| `capture.snapshot.cursorField` | Yes (incremental mode) | Column used as the watermark (e.g., `updated_at`). Must be monotonically increasing. |
| `capture.snapshot.chunkSize` | No | Rows per staged chunk. Default: `50000`. |

### `destination`

| Field | Required | Description |
|---|---|---|
| `kind` | Yes | Destination connector. Currently `postgres`. `snowflake` and `bigquery` are parsed but not implemented. |
| `connection` | Yes (postgres) | `host:port/database`. |
| `credentials` | Yes (postgres) | Same `user` / `password` as source. |
| `staging.kind` | Yes | `local`, `s3`, or `minio`. |
| `staging.bucket` | Yes | Bucket name (or local directory name). |
| `staging.prefix` | No | Key prefix for staged objects. |
| `write.mode` | Yes | `append` — inserts all rows. `merge` — upsert by primary key (not yet implemented). |
| `write.batchSize` | No | Rows per destination batch. Default: `100000`. |

### `runtime`

| Field | Description |
|---|---|
| `parallelism.tables` | Number of tables to process concurrently. Parsed but currently enforced as 1. |
| `checkpointing.intervalSeconds` | How often to flush the checkpoint ledger during a run. |

## Secret references

Passwords and credentials support two forms:

```yaml
password: "mysecretpassword"          # plain text (dev only)
password: "env:POSTGRES_PASSWORD"     # read from environment variable
```

Vault and file-based secrets are planned but not yet implemented.

## Validation rules

The CLI and control-plane API validate specs before storing or executing them:

- `name` must be non-empty and contain only alphanumeric characters, hyphens, and underscores
- `mode` must be one of the allowed values
- `schedule` must be `manual`, `continuous`, or a valid cron expression
- `source.kind` must be a known connector type
- `capture.tables` must be non-empty
- `incremental` mode requires `cursorField`
- `staging.kind` must be one of `local`, `s3`, `minio`
- `write.mode` must be `append` or `merge`

## Examples

See [YAML Examples →](./examples.md) for complete working pipeline specs.
