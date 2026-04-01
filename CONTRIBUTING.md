# Contributing to Astra

## Local bootstrap

### Prerequisites
- Rust toolchain (stable): `rustup` recommended
- Node.js 18+ and npm (for web app)
- Podman or Docker with Compose (for local Postgres + MinIO stack)

### Clone and build

```bash
git clone https://github.com/Astra-Core/Astra.git
cd Astra
cargo build
```

### Start local infrastructure

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

Default local ports:
- Postgres: `5432` (user: `astra`, password: `astra`, db: `astra`)
- MinIO S3 API: `9000`
- MinIO console: `9001`

Stop with:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml down
```

### Run tests

```bash
cargo test --workspace
```

### Build and run the web app

```bash
cd apps/web
npm install
npm run build
cd ../..
```

The built frontend is served by the control plane at `http://127.0.0.1:8080`.

### Run the control plane

Without Postgres (in-memory mode):

```bash
cargo run -p astra-control-plane
```

With Postgres persistence:

```bash
export ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra
cargo run -p astra-control-plane
```

## What works vs what is stubbed

### Working flows (verified)
- `cargo run -p astra -- validate examples/postgres-to-warehouse.astra.yaml` — validates a YAML spec
- `cargo run -p astra -- apply examples/postgres-to-warehouse.astra.yaml` — apply stub (prints confirmation, does not yet persist to control plane from CLI)
- `cargo run -p astra -- discover-source examples/postgres-to-warehouse.astra.yaml` — discovers Postgres source schema (requires a reachable Postgres with the configured tables)
- `cargo run -p astra -- snapshot-to-local-staging examples/postgres-to-warehouse.astra.yaml --max-rows-per-table 1000` — snapshots Postgres tables to local filesystem staging (requires reachable Postgres, set `POSTGRES_PASSWORD`)
- `cargo run -p astra -- snapshot-to-minio-staging examples/postgres-to-postgres-raw.astra.yaml` — snapshots to MinIO staging (requires reachable Postgres and MinIO)
- `cargo run -p astra -- load-local-staging-to-postgres examples/postgres-to-postgres-raw.astra.yaml` — loads staged chunks into a raw Postgres destination (requires staged data and reachable destination Postgres)
- `cargo run -p astra -- execute-local-snapshot examples/postgres-to-postgres-raw.astra.yaml` — end-to-end local snapshot: stages rows to local filesystem, then loads into the Postgres destination in one command (requires reachable source and destination Postgres, set `POSTGRES_PASSWORD`)
- Control plane pipeline list/apply API at `http://127.0.0.1:8080`
- Web UI pipeline inventory and YAML editor at `http://127.0.0.1:8080`
- YAML contract smoke test: `python3 scripts/yaml_contract_smoke.py`

### Partial / requires infrastructure
- `discover-source` and all snapshot/load commands require a reachable Postgres instance
- MinIO staging requires a running MinIO instance
- Postgres-backed control-plane persistence requires `ASTRA_DATABASE_URL`

### Not yet implemented
- CDC execution (explicitly rejected with a clear error message)
- Python connector runtime
- Observability / structured diagnostics
- Multi-table parallelism
- Schema evolution handling

## Local snapshot quickstart

This is the copy-paste path to move rows from a Postgres source to a Postgres destination in a single command.

> **Note:** `execute-local-snapshot` requires `destination.kind: postgres`. Use `examples/smoke-local-snapshot.astra.yaml` for this quickstart — `examples/postgres-to-warehouse.astra.yaml` targets Snowflake and will be rejected.

### 1. Start local infrastructure

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

### 2. Seed fixture tables

```bash
psql postgres://astra:astra@localhost:5432/astra <<'SQL'
CREATE TABLE IF NOT EXISTS public.smoke_users (
  id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL
);
CREATE TABLE IF NOT EXISTS public.smoke_orders (
  id serial PRIMARY KEY, user_id int NOT NULL,
  product text NOT NULL, amount_cents int NOT NULL
);
INSERT INTO public.smoke_users (name, email)
  SELECT 'User ' || i, 'user' || i || '@smoke.test'
  FROM generate_series(1, 20) i
  ON CONFLICT DO NOTHING;
INSERT INTO public.smoke_orders (user_id, product, amount_cents)
  SELECT (i % 20) + 1, 'Product ' || i, i * 100
  FROM generate_series(1, 50) i
  ON CONFLICT DO NOTHING;
SQL
```

### 3. Run the end-to-end snapshot

```bash
ASTRA_SMOKE_PG_PASSWORD=astra \
cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml
```

Expected output:
```
local snapshot execution: smoke-local-snapshot
stage 1/2: snapshot -> local staging
  ...staged chunks for smoke_users and smoke_orders...
stage 2/2: local staging -> postgres destination
  ...loaded rows into astra_raw...
done.
```

### 4. Verify destination row counts

```bash
psql postgres://astra:astra@localhost:5432/astra \
  -c "SELECT COUNT(*) FROM astra_raw.raw_public_smoke_users;"
# Expected: 20

psql postgres://astra:astra@localhost:5432/astra \
  -c "SELECT COUNT(*) FROM astra_raw.raw_public_smoke_orders;"
# Expected: 50
```

### 5. Optional: surface runs in the web UI

To record the run in the control plane and see per-table progress in the web UI:

```bash
# Terminal 1 — control plane
cargo run -p astra-control-plane

# Terminal 2 — apply the spec (one-time)
curl -s -X POST http://127.0.0.1:8080/api/v1/specs/apply \
  -H 'content-type: application/json' \
  -d "$(jq -Rs '{yaml: ., created_by: "cli"}' < examples/smoke-local-snapshot.astra.yaml)"

# Terminal 2 — run with control plane reporting
ASTRA_SMOKE_PG_PASSWORD=astra \
cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml \
  --control-plane-url http://127.0.0.1:8080
```

Open `http://127.0.0.1:8080`, go to **Job status**, click **View runs** on `smoke-local-snapshot`, then **Tables** to see per-table row counts and status.

### 6. Tear down

```bash
podman compose -f deploy/docker-compose/docker-compose.yml down
rm -rf .astra/staging .astra/checkpoints
```

Alternatively, the full automated smoke test (seeds, executes, verifies idempotency, and cleans up) can be run directly:

```bash
python3 scripts/e2e_snapshot_smoke.py
```

## Definition of done
A change is done when:
- acceptance criteria are satisfied
- docs are updated where the change affects user or contributor behavior
- obvious follow-up work is captured in issues instead of silently ignored
- local validation steps were run where possible

## Current expectations
- prefer small PRs over heroic garbage dumps
- keep architecture changes documented in `docs/architecture/` or `docs/decisions/`
- if something is a stub, say it is a stub
- do not pretend incomplete code is production-ready
