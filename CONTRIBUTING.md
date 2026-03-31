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
- Run history / execution status in the web UI
- Python connector runtime
- Observability / structured diagnostics

## Local snapshot quickstart

This is the copy-paste path to validate the current local vertical slice.

### 1. Start local infrastructure

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

### 2. Seed fixture data (if needed)

Connect to the local Postgres and create test tables:

```bash
psql postgres://astra:astra@localhost:5432/astra -c "
CREATE TABLE IF NOT EXISTS public.orders (
  id SERIAL PRIMARY KEY,
  customer TEXT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  created_at TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT now()
);
INSERT INTO public.orders (customer, amount) VALUES ('alice', 99.99), ('bob', 149.50), ('carol', 75.00);
INSERT INTO public.users (name, email) VALUES ('alice', 'alice@example.com'), ('bob', 'bob@example.com');
"
```

### 3. Validate the spec

```bash
cargo run -p astra -- validate examples/postgres-to-warehouse.astra.yaml
```

Expected output includes: `valid Astra spec: postgres-analytics`

### 4. Discover the source

```bash
export POSTGRES_PASSWORD=astra
cargo run -p astra -- discover-source examples/postgres-to-warehouse.astra.yaml
```

Expected output includes discovered tables and a snapshot skeleton.

### 5. Snapshot to local staging

```bash
export POSTGRES_PASSWORD=astra
export ASTRA_STAGING_LOCAL_ROOT=.astra/staging
cargo run -p astra -- snapshot-to-local-staging examples/postgres-to-warehouse.astra.yaml --max-rows-per-table 1000
```

Expected output includes staged chunk paths under `.astra/staging/`.

### 6. Tear down

```bash
podman compose -f deploy/docker-compose/docker-compose.yml down
rm -rf .astra/staging
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
