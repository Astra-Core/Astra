---
id: examples
title: Pipeline Examples
sidebar_position: 2
---

# Pipeline Examples

## Local smoke test (Postgres → Postgres)

The minimal example used by the e2e smoke test. Replicates two tables from a local Postgres instance back to the same instance using local file staging.

```yaml title="examples/smoke-local-snapshot.astra.yaml"
version: v1alpha1
pipeline:
  name: smoke-local-snapshot
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: astra
    username: astra
    passwordRef: env:ASTRA_SMOKE_PG_PASSWORD
  capture:
    tables:
      - public.smoke_users
      - public.smoke_orders
    snapshot:
      mode: full
      chunkSize: 1000
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: astra
    username: astra
    passwordRef: env:ASTRA_SMOKE_PG_PASSWORD
    schema: astra_raw
    tablePrefix: raw_
    applicationName: astra-smoke-loader
  staging:
    kind: local
    bucket: astra-smoke-staging
    prefix: smoke-local-snapshot/
  write:
    mode: append
    batchSize: 10000
runtime:
  parallelism:
    tables: 1
  checkpointing:
    intervalSeconds: 30
  schemaEvolution:
    additiveChanges: auto-apply
    breakingChanges: pause
```

**Run it:**

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- snapshot-to-local-staging examples/smoke-local-snapshot.astra.yaml

ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml
```

---

## Full snapshot (Postgres → Postgres, cross-instance)

Replicates two tables from an application Postgres instance to a separate warehouse instance using local file staging.

```yaml title="examples/postgres-to-postgres-raw.astra.yaml"
version: v1alpha1
pipeline:
  name: postgres-raw-local
  mode: snapshot
  schedule: manual
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
    passwordRef: env:POSTGRES_PASSWORD
  capture:
    tables:
      - public.users
      - public.orders
    snapshot:
      mode: full
      chunkSize: 50000
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
    passwordRef: env:WAREHOUSE_PASSWORD
    schema: astra_raw
    tablePrefix: raw_
    applicationName: astra-loader
  staging:
    kind: local
    bucket: astra-staging
    prefix: postgres-raw-local/
  write:
    mode: append
    batchSize: 100000
runtime:
  parallelism:
    tables: 2
  checkpointing:
    intervalSeconds: 30
  schemaEvolution:
    additiveChanges: auto-apply
    breakingChanges: pause
```

---

## Hourly incremental sync to data warehouse (Postgres → Snowflake)

Production-grade pattern: hourly cron, incremental snapshot using a cursor field, S3 staging, and merge write mode for idempotent upserts.

:::note
The Snowflake destination and `merge` write mode are parsed and validated by the spec parser, but are not yet executed in v0.1. This example shows the target spec shape for when these features are implemented.
:::

```yaml title="examples/postgres-to-warehouse.astra.yaml"
version: v1alpha1
pipeline:
  name: postgres-analytics
  mode: snapshot
  schedule: "0 * * * *"    # every hour
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: app
    username: app_user
    passwordRef: env:POSTGRES_PASSWORD
  capture:
    tables:
      - public.users
      - public.orders
    snapshot:
      mode: incremental
      cursorField: updated_at
      chunkSize: 50000
destination:
  kind: snowflake
  staging:
    kind: s3
    bucket: astra-staging
    prefix: postgres-analytics/
  write:
    mode: merge
    batchSize: 100000
runtime:
  parallelism:
    tables: 4
  checkpointing:
    intervalSeconds: 30
  schemaEvolution:
    additiveChanges: auto-apply
    breakingChanges: pause
```

---

## Applying a spec via the API

Instead of using the CLI, you can register a pipeline through the control plane:

```bash
curl -X POST http://127.0.0.1:8080/api/v1/specs/apply \
  -H "Content-Type: application/json" \
  -d '{
    "yaml": "version: v1alpha1\npipeline:\n  name: my-pipeline\n  ...",
    "created_by": "alice"
  }'
```

Or use the web UI's **YAML Studio** to paste, validate, and apply specs interactively.
