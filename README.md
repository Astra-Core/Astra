# Astra

Astra is an open-source data replication platform aiming to be the easy, fast alternative to Airbyte and the self-hostable answer to Fivetran.

## Product goals

- **Blazing fast pipelines** for database CDC and bulk replication
- **Stupidly easy onboarding** through both UI flows and declarative YAML
- **Easy self-hosting** with a sane Podman Compose path
- **Cloud-ready architecture** without forcing cloud complexity onto local installs

## v0.1 priorities

Astra v0.1 focuses on one real vertical slice:
- configure a source and destination
- run an initial snapshot/backfill
- continue with incremental sync/CDC where supported
- view job history and status in the UI
- manage the same config via YAML

## Proposed architecture

- Rust-first modular monolith for the control plane
- Postgres for metadata/state/checkpoints
- Object storage for durable staging/replay
- High-performance DB CDC lane
- Python runtime for long-tail SaaS/community connectors

See the docs folder for the real details instead of README cosplay.

## Docs

- Product brief: `docs/product/brief.md`
- Architecture RFC: `docs/architecture/rfc-0001-v1-architecture.md`
- YAML spec draft: `docs/architecture/yaml-spec-draft.md`
- Staging contract draft: `docs/architecture/staging-contract.md`
- Example pipeline config: `examples/postgres-to-warehouse.astra.yaml`
- Kanban + epics + issue seed: `docs/product/kanban-and-issue-seed.md`
- v0.1 roadmap: `docs/product/v0.1-roadmap.md`
- ADR: `docs/decisions/adr-0001-modular-monolith.md`

## Local development stack

Astra includes a minimal local dependency stack at `deploy/docker-compose/docker-compose.yml`.

Start it with:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

Stop it with:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml down
```

Default local ports:
- Postgres metadata/state: `5432`
- MinIO S3 API: `9000`
- MinIO console: `9001`

For local-first staging without a live object store, the runtime crate now includes a filesystem-backed adapter that preserves the same bucket/object-key layout used by MinIO/S3. See `docs/architecture/staging-contract.md` and `.env.example` for the current local staging knobs.

## Suggested repo layout

```text
apps/
  control-plane/
  web/
  worker/
  cli/
crates/
  astra-core/
  astra-runtime/
  astra-metadata/
  astra-connectors/
  astra-cdc/
  astra-saas-sdk/
  astra-python-runtime/
  astra-yaml/
  astra-api/
  astra-observability/
  astra-secrets/
connectors/
  rust/
  python/
docs/
deploy/
```

## Immediate next steps

1. Finalize the YAML spec and metadata model
2. Build the control plane skeleton
3. Implement the first Postgres CDC path
4. Add object-storage staging and one destination loader
5. Add UI onboarding and job history

## Running the current web app foundation

The React + TypeScript web app foundation lives in `apps/web`.

For backend-only work, run the control plane from the repo root:

```bash
cargo run -p astra-control-plane
```

For frontend development, run the control plane in one terminal, then in `apps/web/` run:

```bash
npm install
npm run dev
```

Open <http://127.0.0.1:4173> for the Vite dev server.

For a self-hosted frontend build served by the Rust control plane, in `apps/web/` run:

```bash
npm install
npm run build
```

Then open <http://127.0.0.1:8080> after starting `cargo run -p astra-control-plane`.

If `ASTRA_DATABASE_URL` points at a reachable Postgres instance, the control plane now persists applied pipeline specs and pipeline summaries there. If not, it falls back to in-memory storage so local hacking still works instead of faceplanting.

For the self-hosted Postgres source skeleton, start the local stack with Podman Compose, export the source password if your YAML uses `passwordRef: env:POSTGRES_PASSWORD`, then run:

```bash
export POSTGRES_PASSWORD=astra
cargo run -p astra -- discover-source examples/postgres-to-warehouse.astra.yaml
```

That currently does the minimum useful thing: parse and validate the Postgres source config, inspect table schemas from a reachable Postgres instance, and emit a snapshot-oriented SQL plan for the captured tables.

There is also a first honest execution slice for local/self-hosted development:

```bash
export POSTGRES_PASSWORD=...
export ASTRA_STAGING_LOCAL_ROOT=.astra/staging
cargo run -p astra -- snapshot-to-local-staging examples/postgres-to-warehouse.astra.yaml --max-rows-per-table 1000
```

That flow reuses the Postgres connector for discovery plus snapshot reads, converts rows to JSONL.gz in-process, and writes one staged chunk per captured table via the filesystem-backed staging adapter. It's intentionally narrow: no sink apply yet, no resume/checkpoint loop yet, and no fake cloud dependencies.

The shell is intentionally lightweight and the React + TypeScript follow-up is tracked in issue #26.

## Status

Repo scaffold and planning docs have been initialized. The next real move is implementation, not more slideware.
