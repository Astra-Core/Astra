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

## Running the current web shell

The temporary onboarding/job-status shell lives in `apps/web` and is served by the control-plane app.

```bash
cargo run -p astra-control-plane
```

Then open <http://127.0.0.1:8080>.

If `ASTRA_DATABASE_URL` points at a reachable Postgres instance, the control plane now persists applied pipeline specs and pipeline summaries there. If not, it falls back to in-memory storage so local hacking still works instead of faceplanting.

The shell is intentionally lightweight and the React + TypeScript follow-up is tracked in issue #26.

## Status

Repo scaffold and planning docs have been initialized. The next real move is implementation, not more slideware.
