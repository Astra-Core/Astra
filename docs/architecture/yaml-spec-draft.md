# Astra YAML Spec Draft

This document defines the **canonical Astra pipeline specification**.

The same model must be used by:
- YAML files
- CLI validation/apply
- API payloads
- UI editing and export

If those drift apart, Astra turns into two broken products wearing one logo.

---

## Goals

The v1alpha1 spec must be able to describe:
- pipeline identity
- source config
- destination config
- capture behavior
- scheduling mode
- runtime parallelism and checkpointing
- schema evolution policy
- secret references

## Non-goals for v1alpha1
- arbitrary transformation graphs
- visual-builder-only features that cannot round-trip to YAML
- multi-pipeline bundles in one file
- full connector-specific exhaustiveness for every possible future connector

---

## Top-level shape

```yaml
version: v1alpha1
pipeline:
  name: postgres-analytics
  mode: cdc
  schedule: continuous
source:
  kind: postgres
  connection: {}
  capture: {}
destination:
  kind: snowflake
  staging: {}
  write: {}
runtime:
  parallelism: {}
  checkpointing: {}
  schemaEvolution: {}
```

---

## Top-level fields

## `version`
String. Required.

### Allowed values for now
- `v1alpha1`

This is the spec version, not the app version.

---

## `pipeline`
Required.

### Fields
- `name` (required, string)
- `mode` (required, enum)
- `schedule` (required, string or structured object in later versions)
- `description` (optional, string)
- `labels` (optional, map<string,string>)

### `mode`
Allowed values:
- `snapshot`
- `incremental`
- `cdc`

### `schedule`
Allowed v1alpha1 values:
- `continuous`
- `manual`
- cron string (for scheduled polling/sync workloads)

Notes:
- database CDC pipelines will usually use `continuous`
- SaaS/API pipelines may use cron-based schedules

---

## `source`
Required.

### Fields
- `kind` (required, string)
- `connection` (required, object)
- `capture` (required, object)

### `kind`
Initial target values:
- `postgres`
- `mysql`
- future: `stripe`, `hubspot`, etc.

### `connection`
Connector-specific object.
Must not contain raw secrets when a `*Ref` alternative exists.

#### Example for Postgres
```yaml
connection:
  host: localhost
  port: 5432
  database: app
  username: app_user
  passwordRef: env:POSTGRES_PASSWORD
  sslMode: prefer
```

### `capture`
Defines what to ingest and how.

#### Common fields
- `tables` or `streams` (required depending on source type)
- `snapshot` (optional)
- `cdc` (optional for CDC-capable connectors)
- `discovery` (optional later)

#### Snapshot block
```yaml
snapshot:
  mode: incremental
  chunkSize: 50000
```

Allowed snapshot modes:
- `full`
- `incremental`
- `none`

#### CDC block
```yaml
cdc:
  slotName: astra_slot
  publicationName: astra_publication
```

For CDC connectors, the `cdc` block defines connector-specific replication settings.

---

## `destination`
Required.

### Fields
- `kind` (required, string)
- `staging` (optional but strongly expected for warehouse/lake destinations)
- `write` (required, object)

### `kind`
Initial target values:
- `snowflake`
- `bigquery`
- future: `s3`, `postgres`, etc.

### `staging`
Defines the durable intermediate layer.

```yaml
staging:
  kind: s3
  bucket: astra-staging
  prefix: postgres-analytics/
```

### `write`
Defines apply behavior at the destination.

```yaml
write:
  mode: merge
  batchSize: 100000
```

Allowed write modes:
- `append`
- `upsert`
- `merge`
- `replace`

---

## `runtime`
Required.

### Fields
- `parallelism` (optional object)
- `checkpointing` (optional object)
- `schemaEvolution` (optional object)
- `retry` (optional in future versions)

### `parallelism`
```yaml
parallelism:
  tables: 4
```

For v1alpha1, this is intentionally small and understandable.
More knobs can be added later without turning YAML into industrial soup.

### `checkpointing`
```yaml
checkpointing:
  intervalSeconds: 30
```

### `schemaEvolution`
```yaml
schemaEvolution:
  additiveChanges: auto-apply
  breakingChanges: pause
```

Allowed values:
- `additiveChanges`: `auto-apply` | `ignore` | `pause`
- `breakingChanges`: `pause` | `ignore`

---

## Secret reference convention

Secret references should use explicit prefixes.

### Initial supported conventions
- `env:NAME`
- future: `file:/path/to/secret`
- future: `vault:path/to/secret`

Example:
```yaml
passwordRef: env:POSTGRES_PASSWORD
```

---

## Validation rules for v1alpha1

## Required
- `version`
- `pipeline.name`
- `pipeline.mode`
- `pipeline.schedule`
- `source.kind`
- `source.connection`
- `destination.kind`
- `destination.write`

## Semantic validation
- `version` must be `v1alpha1`
- `pipeline.name` must be non-empty
- `cdc` mode requires a CDC-capable connector
- `continuous` schedule is valid only for connectors that support continuous execution
- `chunkSize` must be > 0 if provided
- `batchSize` must be > 0 if provided
- secret references must use a recognized prefix

---

## Example: Postgres to Snowflake

```yaml
version: v1alpha1
pipeline:
  name: postgres-analytics
  mode: cdc
  schedule: continuous
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
      chunkSize: 50000
    cdc:
      slotName: astra_slot
      publicationName: astra_publication
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

## Evolution rules

For future versions:
- additive fields are preferred
- destructive meaning changes require a new spec version
- UI must preserve unknown fields where practical during round-trip edits
- connector-specific extensions should be namespaced or scoped carefully instead of infecting the whole schema
