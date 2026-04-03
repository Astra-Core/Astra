---
id: quickstart
title: Quickstart
sidebar_position: 1
---

# Quickstart

Get Astra running and replicate your first table in about 15 minutes.

## Prerequisites

- **Rust** — install via [rustup](https://rustup.rs) (`stable` toolchain)
- **Podman** or **Docker** with Compose support
- **Python 3.8+** — for smoke tests (optional)
- **Git**

## 1. Clone and build

```bash
git clone https://github.com/suryachereddy/Astra.git
cd Astra
cargo build
```

The workspace builds all crates and binaries in one pass. First build takes a few minutes.

## 2. Start local infrastructure

Astra needs Postgres (metadata) and MinIO (staging) locally. Both are in the provided Docker Compose file:

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

## 3. Seed test data

Create tables in the local Postgres database to replicate from:

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

## 4. Validate your pipeline spec

The example pipeline replicates `public.smoke_users` from Postgres back to the same Postgres instance (useful for local testing):

```bash
cargo run -p astra -- validate examples/smoke-local-snapshot.astra.yaml
```

Expected output: `Pipeline spec is valid.`

## 5. Discover the source schema

```bash
cargo run -p astra -- discover-source examples/smoke-local-snapshot.astra.yaml
```

This connects to the source database and prints the discovered tables with their column types.

## 6. Run the snapshot

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
cargo run -p astra -- snapshot-to-local-staging examples/smoke-local-snapshot.astra.yaml
```

Astra paginates through `smoke_users` in chunks of 1,000 rows, compresses each chunk to JSONL.gz, and writes them to the local staging directory. You'll see progress logged to stdout.

## 7. Load to the destination

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml
```

This reads the staged chunks and bulk-loads them into the `astra_raw` schema of the destination database. Check the result:

```bash
PGPASSWORD=astra psql -h localhost -U astra -d astra -c "
  SELECT count(*) FROM astra_raw.raw_public_smoke_users;
"
```

You should see 500 rows.

## 8. Start the control plane (optional)

The control plane exposes a REST API and serves the web UI:

```bash
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
cargo run -p astra-control-plane
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) to see the web UI.

## 9. Run smoke tests (optional)

```bash
python3 scripts/yaml_contract_smoke.py
```

## What's next?

- [Understand the YAML spec →](../yaml-spec/overview.md)
- [Explore all CLI commands →](../cli/reference.md)
- [Browse the REST API →](../control-plane/api.md)
- [Learn the architecture →](../architecture/overview.md)
