---
id: quickstart
title: Quickstart
sidebar_position: 1
---

# Quickstart

Get Astra running and replicate your first table in about 15 minutes.

There are two ways to run a pipeline:

- **[Path A — CLI](#path-a-cli)**: run commands directly from the terminal. Good for scripting, CI, or if you just want to try things out without starting a server.
- **[Path B — Web UI](#path-b-web-ui)**: start the control plane, register your pipeline through the YAML Studio, and trigger runs from the dashboard. Good for an interactive workflow.

Both paths start with the same setup steps below.

---

## Prerequisites

- **Rust** — install via [rustup](https://rustup.rs) (`stable` toolchain)
- **Podman** or **Docker** with Compose support
- **Python 3.8+** — for smoke tests (optional)
- **Git**

---

## Shared setup

### 1. Clone and build

```bash
git clone https://github.com/suryachereddy/Astra.git
cd Astra
cargo build
```

The workspace builds all crates and binaries in one pass. First build takes a few minutes.

### 2. Start local infrastructure

Astra needs Postgres (metadata and source/destination) and MinIO (staging) locally. Both are in the provided Docker Compose file:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

This starts:
- **Postgres 16** at `localhost:5432` — user `astra`, password `astra`, database `astra`
- **MinIO** at `localhost:9000` — access key `astra`, secret key `astrastorage`
- MinIO console at `localhost:9001`

Verify both are up:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml ps
```

### 3. Seed test data

Create a table in the local Postgres database to replicate from:

```bash
PGPASSWORD=astra psql -h localhost -U astra -d astra -c "
  CREATE TABLE IF NOT EXISTS public.smoke_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
  );
  INSERT INTO public.smoke_users (name, email)
  SELECT 'user_' || i, 'user_' || i || '@example.com'
  FROM generate_series(1, 500) AS i;
"
```

---

## Path A: CLI

No server required. The CLI handles capture and load directly.

### 4. Validate the spec

The example pipeline replicates `public.smoke_users` from Postgres back to the same instance using local file staging:

```bash
cargo run -p astra -- validate examples/smoke-local-snapshot.astra.yaml
```

Expected output:
```
valid Astra spec: smoke-local-snapshot  mode=snapshot  source=postgres  dest=postgres  tables=[...]
```

Exits with code 0 on success. Validation errors are printed to stderr.

### 5. Discover the source schema

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- discover-source examples/smoke-local-snapshot.astra.yaml
```

Connects to the source database and prints each table's column names, types, nullability, and primary keys. Useful for building or verifying the `capture.tables` list in your spec.

### 6. Run the full snapshot (capture + load)

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
  cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml
```

This runs both phases in sequence:

1. **Capture** — paginates `smoke_users` in chunks of 1,000 rows, compresses each chunk to JSONL.gz, writes to local staging, and records progress in the checkpoint ledger.
2. **Load** — reads the staged chunks and bulk-inserts into `astra_raw.raw_public_smoke_users` in the destination database.

You'll see per-chunk progress logged to stdout. If the run is interrupted, re-running the same command resumes from the last checkpoint. Pass `--no-resume` to restart from scratch.

### 7. Verify the result

```bash
PGPASSWORD=astra psql -h localhost -U astra -d astra \
  -c "SELECT count(*) FROM astra_raw.raw_public_smoke_users;"
```

You should see 500 rows. Each row has a `_data` JSONB column containing the original source record and metadata columns `_sequence` and `_loaded_at`.

---

## Path B: Web UI

The web UI lets you register pipelines through a YAML editor, trigger runs with one click, and inspect run history and per-table results.

### 4. Start the control plane

```bash
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
  cargo run -p astra-control-plane
```

The control plane binds to `127.0.0.1:8080` by default. It serves both the REST API and the embedded React web UI.

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) — you should see the pipeline dashboard (empty for now).

### 5. Register a pipeline via YAML Studio

1. Click **YAML Studio** in the navigation.
2. Click **Load example** — this pre-fills the editor with the `postgres-to-warehouse` starter spec from the API.
3. Replace the contents with the smoke test spec below (or paste your own):

```yaml
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

4. Click **Apply** — this calls `POST /api/v1/specs/apply`, validates the spec, and registers the pipeline. You should see a success confirmation.

### 6. View the pipeline in the dashboard

Navigate back to the **Overview** page. Your `smoke-local-snapshot` pipeline now appears in the list with status `active`.

From the pipeline card you can:
- **Enable / Disable** — disable stops the scheduler from triggering runs automatically.
- **Trigger** — manually kick off a run immediately (see next step).
- **Delete** — removes the pipeline and its metadata.

### 7. Trigger a run

Click **Trigger** on the `smoke-local-snapshot` pipeline card. The control plane spawns the pipeline executor inline and the run begins.

:::note
The executor needs the `ASTRA_SMOKE_PG_PASSWORD` environment variable to be set in the shell where the control plane is running — it reads secrets from the environment at execution time. Make sure you started the control plane with that variable exported:

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
  cargo run -p astra-control-plane
```
:::

### 8. Inspect run history and table results

Click the pipeline name to open its detail page. You will see:

**Run History** — a chronological list of runs showing:
- Run ID and trigger time
- Duration
- Status (`started` / `completed` / `failed`)

Click any run to drill down into **Table Executions**:
- Table name
- Rows captured and chunks staged
- Per-table status and start/end timestamps

This is especially useful for multi-table pipelines where one table might fail while others succeed.

### 9. Verify the result

```bash
PGPASSWORD=astra psql -h localhost -U astra -d astra \
  -c "SELECT count(*) FROM astra_raw.raw_public_smoke_users;"
```

You should see 500 rows.

---

## Optional: run smoke tests

```bash
python3 scripts/yaml_contract_smoke.py
```

---

## What's next?

- [Understand the YAML spec →](../yaml-spec/overview.md)
- [Explore all CLI commands →](../cli/reference.md)
- [Browse the REST API →](../control-plane/api.md)
- [Learn the architecture →](../architecture/overview.md)
