# Astra

Astra is a self-hostable data replication platform — a Rust-first alternative to Airbyte/Fivetran for database CDC and bulk snapshot replication. Pipelines are defined in YAML and executed via CLI or control-plane API.

## Quickstart

### Prerequisites

- [Rust](https://rustup.rs) (stable)
- [Node.js](https://nodejs.org) 20+
- [Podman](https://podman.io) + `podman-compose` (or Docker Compose)
- Python 3.9+ (for smoke tests)

### 1. Start the local stack

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

This starts Postgres (`:5432`) and MinIO (`:9000` API, `:9001` console).

### 2. Build

```bash
cargo build
cd apps/web && npm install && npm run build && cd ../..
```

### 3. Start the control plane

```bash
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
  cargo run -p astra-control-plane
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) for the web UI.

### 4. Seed fixture data

```sql
-- connect: psql postgres://astra:astra@localhost:5432/astra
CREATE TABLE IF NOT EXISTS public.smoke_users (
  id SERIAL PRIMARY KEY, name TEXT, email TEXT
);
CREATE TABLE IF NOT EXISTS public.smoke_orders (
  id SERIAL PRIMARY KEY, user_id INT, amount NUMERIC
);
INSERT INTO public.smoke_users (name, email)
  SELECT 'user_' || i, 'user_' || i || '@example.com'
  FROM generate_series(1, 20) AS i;
INSERT INTO public.smoke_orders (user_id, amount)
  SELECT (random() * 19 + 1)::int, (random() * 1000)::numeric(10,2)
  FROM generate_series(1, 50);
```

### 5. Run an end-to-end snapshot

```bash
export ASTRA_SMOKE_PG_PASSWORD=astra
export ASTRA_STAGING_LOCAL_ROOT=.astra/staging
export ASTRA_CHECKPOINT_LOCAL_ROOT=.astra/checkpoints

cargo run -p astra -- execute-local-snapshot \
  examples/smoke-local-snapshot.astra.yaml \
  --control-plane-url http://127.0.0.1:8080
```

### 6. Verify

```bash
psql postgres://astra:astra@localhost:5432/astra \
  -c "SELECT COUNT(*) FROM astra_raw.raw_public_smoke_users;"
# expect: 20

psql postgres://astra:astra@localhost:5432/astra \
  -c "SELECT COUNT(*) FROM astra_raw.raw_public_smoke_orders;"
# expect: 50
```

Check run history in the UI or via API:

```bash
curl http://127.0.0.1:8080/api/v1/pipelines
curl http://127.0.0.1:8080/api/v1/pipelines/smoke-local-snapshot/run-history
```

---

## CLI commands

| Command | What it does |
|---------|-------------|
| `validate` | Parse and validate a YAML spec |
| `discover-source` | Enumerate Postgres tables, columns, and primary keys |
| `snapshot-to-local-staging` | Snapshot Postgres tables to local JSONL.gz chunks |
| `snapshot-to-minio-staging` | Snapshot Postgres tables to MinIO/S3 chunks |
| `load-local-staging-to-postgres` | Load locally staged chunks into Postgres raw destination |
| `execute-local-snapshot` | End-to-end: snapshot → local staging → Postgres load |

## Control plane API

All endpoints under `/api/v1/`. Key ones:

| Endpoint | Purpose |
|----------|---------|
| `GET /pipelines` | List registered pipelines |
| `POST /specs/apply` | Register or update a pipeline spec |
| `GET /pipelines/:name/run-history` | Paginated run history |
| `GET /pipeline-runs/:id/table-executions` | Per-table status and row counts |

## Architecture

Rust-first modular monolith. Single control-plane binary, Postgres for metadata, S3/MinIO for durable staging.

```
apps/control-plane   Axum HTTP API + scheduler + metadata
apps/cli             Clap CLI
apps/web             React 18 + TypeScript + Vite
crates/astra-yaml    YAML spec parsing/validation (v1alpha1)
crates/astra-runtime Staging — local, S3, MinIO; chunked JSONL.gz
crates/astra-connectors  Postgres source + destination
```

See [`docs/v0.1-SCOPE.md`](docs/v0.1-SCOPE.md) for what is and isn't implemented, and [`docs/architecture/rfc-0001-v1-architecture.md`](docs/architecture/rfc-0001-v1-architecture.md) for the full architecture RFC.

## Development

```bash
cargo test --workspace          # run all tests
cargo clippy --workspace        # lint
cd apps/web && npm run dev      # Vite dev server at 127.0.0.1:4173
python3 scripts/e2e_snapshot_smoke.py  # end-to-end smoke test
```

Copy `.env.example` to `.env` for a full reference of environment variables.
