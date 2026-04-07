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
  description: "optional human-readable description"
  labels:
    env: production

source:
  kind: postgres
  connectionRef: my-prod-db    # reference a saved connection by name (alternative to inline connection)
  # connection:               # inline connection — omit when connectionRef is set
  #   host: localhost
  #   port: 5432
  #   database: mydb
  #   username: myuser
  #   passwordRef: "env:POSTGRES_PASSWORD"
  capture:
    tables:
      - public.users
      - public.orders
    snapshot:
      mode: full            # full | incremental
      chunkSize: 50000
      cursorField: updated_at   # required for incremental mode

destination:
  kind: postgres            # postgres | snowflake (parsed, not yet implemented)
  connection:               # required for postgres destinations
    host: localhost
    port: 5432
    database: warehouse
    username: loader
    passwordRef: "env:WAREHOUSE_PASSWORD"
    schema: astra_raw       # optional, defaults to astra_raw
    tablePrefix: raw_       # optional table name prefix
    applicationName: astra-loader  # optional Postgres application_name
    sslMode: prefer         # optional SSL mode
  staging:
    kind: local             # local | s3 | minio
    bucket: my-staging-bucket
    prefix: my-pipeline/
  write:
    mode: append            # append | upsert | merge | replace
    batchSize: 100000

runtime:
  parallelism:
    tables: 1               # parsed but not yet enforced in v0.1
  checkpointing:
    intervalSeconds: 30
  schemaEvolution:
    additiveChanges: auto-apply   # auto-apply | ignore | pause
    breakingChanges: pause        # pause | ignore
```

## Fields

### `version`

Always `v1alpha1`. Future versions will introduce new spec versions with migration paths.

### `pipeline`

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Unique pipeline identifier. Used in staging paths and API endpoints. Must be non-empty. |
| `mode` | Yes | `snapshot` (full table copy), `incremental` (cursor-based), or `cdc` (log-based; not yet implemented). |
| `schedule` | Yes | `manual` (trigger only via CLI/API), `continuous` (run as soon as previous finishes), or a cron expression like `"0 * * * *"` (hourly). |
| `description` | No | Human-readable description for display in the web UI. |
| `labels` | No | Arbitrary string key-value pairs for tagging. |

### `source`

| Field | Required | Description |
|---|---|---|
| `kind` | Yes | Source connector type. Currently only `postgres`. |
| `connectionRef` | No | Name of a saved connection (see [Saved Connections](#saved-connections)). Mutually exclusive with inline `connection`. |
| `connection.host` | Yes (if no `connectionRef`) | Database hostname. |
| `connection.port` | Yes (if no `connectionRef`) | Database port. |
| `connection.database` | Yes (if no `connectionRef`) | Database name. |
| `connection.username` | Yes (if no `connectionRef`) | Database username. |
| `connection.passwordRef` | Yes (if no `connectionRef`) | Password value. Use `"env:VAR_NAME"` to read from environment, or a plain string for dev. |
| `capture.tables` | Yes | List of `schema.table` pairs to replicate. |
| `capture.snapshot.mode` | Yes (snapshot/incremental) | `full` — replicate all rows every run. `incremental` — only rows where `cursorField > lastCheckpoint`. |
| `capture.snapshot.cursorField` | Yes (incremental mode) | Column used as the watermark (e.g., `updated_at`). Must be monotonically increasing. |
| `capture.snapshot.chunkSize` | No | Rows per staged chunk. Default: `50000`. |

CDC mode (`capture.cdc`) requires `slotName` and `publicationName` but is not yet executed — specifying it will produce a validation warning.

### `destination`

| Field | Required | Description |
|---|---|---|
| `kind` | Yes | Destination connector. Currently `postgres` (implemented). `snowflake` and `bigquery` are parsed but not implemented. |
| `connectionRef` | No | Name of a saved connection (see [Saved Connections](#saved-connections)). Mutually exclusive with inline `connection`. |
| `connection.host` | Yes (postgres, if no `connectionRef`) | Database hostname. |
| `connection.port` | Yes (postgres, if no `connectionRef`) | Database port. |
| `connection.database` | Yes (postgres, if no `connectionRef`) | Database name. |
| `connection.username` | Yes (postgres, if no `connectionRef`) | Database username. |
| `connection.passwordRef` | Yes (postgres, if no `connectionRef`) | Password — same `env:` syntax as source. |
| `connection.schema` | No | Target schema for raw tables. Defaults to `astra_raw`. |
| `connection.tablePrefix` | No | Prefix added to each raw table name. Defaults to `raw_`. |
| `connection.applicationName` | No | Postgres `application_name` for connection tracking. |
| `connection.sslMode` | No | Postgres SSL mode (e.g., `prefer`, `require`, `disable`). |
| `staging.kind` | Yes | `local`, `s3`, or `minio`. |
| `staging.bucket` | Yes | Bucket name (or local directory name). |
| `staging.prefix` | No | Key prefix for staged objects. |
| `write.mode` | Yes | `append` inserts all rows. `upsert`, `merge`, and `replace` are parsed but not yet implemented. |
| `write.batchSize` | No | Rows per destination batch. Default: `100000`. |

### `runtime`

| Field | Description |
|---|---|
| `parallelism.tables` | Number of tables to process concurrently. Parsed but currently enforced as 1. |
| `checkpointing.intervalSeconds` | How often to flush the checkpoint ledger during a run. |
| `schemaEvolution.additiveChanges` | What to do when new columns are added: `auto-apply`, `ignore`, or `pause`. Parsed but not yet enforced. |
| `schemaEvolution.breakingChanges` | What to do on breaking schema changes (column removed/renamed): `pause` or `ignore`. Parsed but not yet enforced. |

## Secret references

Passwords use `passwordRef` and support two forms:

```yaml
passwordRef: "mysecretpassword"        # plain text (dev only)
passwordRef: "env:POSTGRES_PASSWORD"   # read from environment variable
```

`file:` and `vault:` secret reference formats are recognized by the validator as valid syntax but are not yet resolved at runtime. Use `env:` for all credentials today.

## Validation rules

The CLI and control-plane API validate specs before storing or executing them:

- `name` must be non-empty
- `mode` must be one of the allowed values (`snapshot`, `incremental`, `cdc`)
- `schedule` must be `manual`, `continuous`, or a valid cron expression
- `source.kind` must be a known connector type
- `capture.tables` must be non-empty
- `incremental` mode requires `cursorField`
- `cdc` mode requires `slotName` and `publicationName` in `capture.cdc`
- `staging.kind` must be one of `local`, `s3`, `minio`
- `write.mode` must be one of `append`, `upsert`, `merge`, `replace`
- Postgres and Snowflake/BigQuery destinations require a `staging` block
- Specifying both `connectionRef` and an inline `connection` block on the same source or destination is an error (`AmbiguousConnection`)

## Saved Connections

Instead of inlining credentials in every spec file, you can reference a named saved connection:

```yaml
source:
  kind: postgres
  connectionRef: my-prod-db   # references saved_connections.name
  capture:
    tables: [public.users]
```

Saved connections store non-sensitive fields (`host`, `port`, `database`, `username`) and a single `secret_ref` (e.g. `env:POSTGRES_PASSWORD`) — passwords are never stored in the database. They are managed via the control-plane API (`/api/v1/connections`) and the web UI.

**Rules:**
- `connectionRef` and an inline `connection` block are mutually exclusive — specifying both is a validation error.
- The ref is resolved to a concrete connection at apply time by the control plane. The YAML crate only checks syntax; it does not verify that the named connection exists.

## Examples

See [YAML Examples →](./examples.md) for complete working pipeline specs.
