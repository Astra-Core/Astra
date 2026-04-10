---
id: postgres
title: Postgres Connector
sidebar_position: 2
---

# Postgres Connector

The Postgres connector is Astra's first and most complete connector. It supports both source (capture) and destination (load) roles.

## Source: Postgres

### Connection

Specify the Postgres source in your pipeline YAML using the flat-map connection format:

```yaml
source:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    passwordRef: "env:POSTGRES_PASSWORD"
```

All fields are required. `passwordRef` accepts a plain string (dev only) or `env:VAR_NAME` to read from an environment variable.

### Schema discovery

```bash
cargo run -p astra -- discover-source my-pipeline.astra.yaml
```

The connector queries `information_schema.columns` to enumerate tables and their schemas. Primary keys are discovered via the Postgres system catalog (`pg_index`, `pg_class`, and `pg_attribute`), which is more reliable than `information_schema.table_constraints` for inherited tables and expression indexes.

### Snapshot modes

#### Full snapshot

Replicates all rows on every run. Good for small tables or when you need a complete refresh.

```yaml
capture:
  snapshot:
    mode: full
    chunkSize: 50000
```

The connector paginates using `SELECT to_jsonb(snapshot_row) AS record FROM (...) LIMIT <chunkSize> OFFSET <n>`. Each chunk is committed to staging before advancing to the next.

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

The Postgres connector serializes rows using `to_jsonb()`, which maps source types to JSON as follows:

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

The Postgres destination loads staged chunks into an `astra_raw` schema. This is an append-only raw layer — no deduplication or merge is performed in v0.1.

### Configuration

```yaml
destination:
  kind: postgres
  connection:
    host: localhost
    port: 5432
    database: warehouse
    username: warehouse_user
    passwordRef: "env:WAREHOUSE_PASSWORD"
    schema: astra_raw          # optional, defaults to astra_raw
    tablePrefix: raw_          # optional, defaults to raw_
    applicationName: astra-loader  # optional
    sslMode: prefer            # optional
  staging:
    kind: local
    bucket: astra-staging
    prefix: my-pipeline/
  write:
    mode: append
    batchSize: 100000
```

### Raw table schema

For each source table `<schema>.<table>`, Astra creates `<destination_schema>.<tablePrefix><schema>_<table>`. With defaults this becomes `astra_raw.raw_public_users`:

```sql
CREATE TABLE astra_raw.raw_public_users (
    _sequence     BIGINT        NOT NULL,
    _loaded_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _data         JSONB         NOT NULL
);
```

- `_sequence` — chunk sequence number (monotonically increasing within a run)
- `_loaded_at` — timestamp when the row was loaded
- `_data` — the full source row as a JSON object

### Applied chunks tracking

To prevent double-loading, the destination tracks which chunks have been applied in `astra_raw._applied_chunks`:

```sql
CREATE TABLE astra_raw._applied_chunks (
    pipeline_name  TEXT        NOT NULL,
    stream_name    TEXT        NOT NULL,
    sequence       BIGINT      NOT NULL,
    row_count      BIGINT,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, stream_name, sequence)
);
```

Each chunk load is wrapped in a transaction with `INSERT INTO _applied_chunks ... ON CONFLICT DO NOTHING`. Re-running `execute-local-snapshot` (or `load-local-staging-to-postgres`) skips already-applied chunks, making the load phase idempotent.

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
