# Live Demo Checklist (v0.1)

Verify end-to-end flow: bootstrap → spec → run → UI/API verification.

## 1. Bootstrap Local Stack

- [ ] Clone repo: `git clone https://github.com/Astra-core/Astra`
- [ ] `cd Astra`
- [ ] Start deps: `podman compose -f deploy/docker-compose/docker-compose.yml up -d`
- [ ] Verify services: Postgres(5432), MinIO(9000/9001)
- [ ] Set env: `export POSTGRES_PASSWORD=astra` (etc. from `.env.example`)

## 2. Quickstart Snapshot Flow (CLI)

- [ ] Seed fixture tables (see CONTRIBUTING.md quickstart for SQL)
- [ ] `ASTRA_SMOKE_PG_PASSWORD=astra cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml --control-plane-url http://127.0.0.1:8080`
- [ ] Verify destination row counts: `psql postgres://astra:astra@localhost:5432/astra -c "SELECT COUNT(*) FROM astra_raw.raw_public_smoke_users;"` (expect 20)
- [ ] Verify destination row counts: `psql postgres://astra:astra@localhost:5432/astra -c "SELECT COUNT(*) FROM astra_raw.raw_public_smoke_orders;"` (expect 50)

## 3. Control Plane + UI

- [ ] `cargo run -p astra-control-plane`
- [ ] Open http://127.0.0.1:8080 (built UI) or http://127.0.0.1:4173 (dev)
- [ ] Apply pipeline via UI or API: POST `/pipelines` with YAML
- [ ] View pipeline status/history in UI
- [ ] Trigger run, monitor progress

## 4. API Verification

- [ ] `curl http://127.0.0.1:8080/api/v1/pipelines` (list pipelines)
- [ ] `curl http://127.0.0.1:8080/api/v1/pipelines/smoke-local-snapshot/run-history` (run history)
- [ ] `curl http://127.0.0.1:8080/api/v1/pipeline-runs/<run-id>/table-executions` (per-table status and row counts)

## Known Limitations (v0.1)

- CDC not executable — returns an explicit error if attempted
- No worker orchestration — pipelines are triggered manually via CLI
- Append-only destination writes — no merge or upsert semantics
- `execute-local-snapshot` requires `destination.kind: postgres`

See [docs/v0.1-SCOPE.md](./v0.1-SCOPE.md) for the full limitations list.