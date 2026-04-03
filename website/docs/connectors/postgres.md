---
id: postgres
title: Postgres Connector
sidebar_position: 2
---

# Postgres Connector

The Postgres connector is Astra's first and most complete connector. It supports both source (capture) and destination (load) roles.

## Source: Postgres

### Connection

Specify the Postgres source in your pipeline YAML:

```yaml
source:
  kind: postgres
  connection: "localhost:5432/mydb"
  credentials:
    user: myuser
    password: "env:POSTGRES_PASSWORD"
```

The connection string format is `host:port/database`. IPv6 addresses and Unix sockets are not yet supported.

### Schema discovery

```bash
cargo run -p astra -- discover-source my-pipeline.astra.yaml
```

The connector issues `SELECT column_name, data_type, is_nullable FROM information_schema.columns` queries to enumerate tables and their schemas. Primary keys are discovered via `information_schema.table_constraints`.

### Snapshot modes

#### Full snapshot

Replicates all rows on every run. Good for small tables or when you need a complete refresh.

```yaml
capture:
  snapshot:
    mode: full
    chunkSize: 50000
```

The connector uses `SELECT * FROM <table> ORDER BY <primary_key> LIMIT <chunkSize> OFFSET <n>` pagination. Each chunk is committed to staging before advancing to the next.

#### Incremental snapshot

Replicates only rows where the cursor column is greater than the last committed watermark. Requires a monotonically increasing column (`updated_at`, `id`, etc.).

```yaml
capture:
  snapshot:
    mode: incremental
    cursorField: updated_at
    chunkSize: 50000
```

The watermark is stored in the checkpoint ledger. On the first run, all rows are captured. Subsequent runs use `WHERE updated_at > :last_watermark`.

:::caution
Incremental mode misses hard-deletes. Use CDC mode (not yet implemented) for full change capture.
:::

### Supported data types

The Postgres connector maps source types to JSON as follows:

| Postgres type | JSON representation |
|---|---|
| `INTEGER`, `BIGINT`, `SMALLINT` | JSON number |
| `FLOAT`, `DOUBLE PRECISION`, `NUMERIC` | JSON number |
| `TEXT`, `VARCHAR`, `CHAR`, `UUID` | JSON string |
| `BOOLEAN` | JSON boolean |
| `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `TIME` | ISO 8601 string |
| `JSONB`, `JSON` | Embedded JSON object |
| `ARRAY` | JSON array |
| `BYTEA` | Base64-encoded JSON string |
| `NULL` | JSON null |

---

## Destination: Postgres (raw loader)

The Postgres destination loads staged chunks into a `astra_raw` schema. This is an append-only raw layer — no deduplication or merge is performed in v0.1.

### Configuration

```yaml
destination:
  kind: postgres
  connection: "localhost:5432/warehouse"
  credentials:
    user: warehouse_user
    password: "env:WAREHOUSE_PASSWORD"
  staging:
    kind: local
    bucket: astra-staging
    prefix: my-pipeline/
  write:
    mode: append
    batchSize: 100000
```

### Raw table schema

For each source table `<schema>.<table>`, Astra creates `astra_raw.raw_<schema>_<table>`:

```sql
CREATE TABLE astra_raw.raw_public_users (
    _object_key   TEXT          NOT NULL,
    _sequence     BIGINT        NOT NULL,
    _row_number   BIGINT        NOT NULL,
    _loaded_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _data         JSONB         NOT NULL
);
```

- `_object_key` — the staging object key for the source chunk
- `_sequence` — chunk sequence number (monotonically increasing within a run)
- `_row_number` — row position within the chunk
- `_data` — the full source row as a JSON object

### Applied chunks tracking

To prevent double-loading, the destination tracks which chunks have been applied:

```sql
CREATE TABLE astra_raw._applied_chunks (
    object_key   TEXT PRIMARY KEY,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Re-running `execute-local-snapshot` skips already-applied chunks.

### CDC (planned)

Log-based CDC using Postgres logical replication (`pgoutput` plugin) is planned but not implemented in v0.1. The `crates/astra-cdc` crate is scaffolded and will return an explicit error if triggered.

### Permissions

The source user needs at minimum:

```sql
GRANT SELECT ON <schema>.<table> TO astra_user;
```

For CDC (future), the source user will also need `REPLICATION` privilege and a replication slot.

The destination user needs:

```sql
GRANT CREATE ON DATABASE <warehouse_db> TO astra_user;
-- or grant access to a pre-created astra_raw schema:
GRANT USAGE, CREATE ON SCHEMA astra_raw TO astra_user;
```
