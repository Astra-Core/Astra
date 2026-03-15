# Live Demo Checklist (v0.1)

Verify end-to-end flow: bootstrap → spec → run → UI/API verification.

## 1. Bootstrap Local Stack

- [ ] Clone repo: `git clone https://github.com/Astra-core/Astra`
- [ ] `cd Astra`
- [ ] Start deps: `podman compose -f deploy/docker-compose/docker-compose.yml up -d`
- [ ] Verify services: Postgres(5432), MinIO(9000/9001)
- [ ] Set env: `export POSTGRES_PASSWORD=astra` (etc. from `.env.example`)

## 2. Quickstart Snapshot Flow (CLI)

- [ ] `cargo run -p astra -- snapshot-to-local-staging examples/postgres-to-warehouse.astra.yaml --max-rows-per-table 1000`
- [ ] Verify staging: `ls -la .astra/staging/` (JSONL.gz chunks)
- [ ] `cargo run -p astra -- load-local-staging-to-postgres examples/postgres-to-postgres-raw.astra.yaml`
- [ ] Verify loaded: Query raw tables in warehouse Postgres (`astra_raw` schema)

## 3. Control Plane + UI

- [ ] `cargo run -p astra-control-plane`
- [ ] Open http://127.0.0.1:8080 (built UI) or http://127.0.0.1:4173 (dev)
- [ ] Apply pipeline via UI or API: POST `/pipelines` with YAML
- [ ] View pipeline status/history in UI
- [ ] Trigger run, monitor progress

## 4. API Verification

- [ ] `curl http://127.0.0.1:8080/api/pipelines` (list)
- [ ] `curl http://127.0.0.1:8080/api/pipelines/{id}/runs` (history)
- [ ] Latest run shows snapshot progress/stages

## Known Limitations (v0.1)

- CDC not executable (source skeleton only)
- No worker orchestration (manual CLI runs)
- Local staging only (MinIO adapter available but unproven)
- UI is foundation (job history WIP)