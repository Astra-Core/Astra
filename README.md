# Astra

Astra is a self-hostable data replication platform — a Rust-first alternative to Airbyte/Fivetran for database CDC and bulk snapshot replication. Pipelines are defined in YAML and executed via CLI or control-plane API.

**Documentation: [astra-core.github.io](https://astra-core.github.io)**

---

## Quickstart

```bash
# 1. Start local infrastructure (Postgres + MinIO)
podman compose -f deploy/docker-compose/docker-compose.yml up -d

# 2. Build
cargo build

# 3. Validate and run a snapshot
export ASTRA_SMOKE_PG_PASSWORD=astra
cargo run -p astra -- validate examples/smoke-local-snapshot.astra.yaml
cargo run -p astra -- snapshot-to-local-staging examples/smoke-local-snapshot.astra.yaml
cargo run -p astra -- execute-local-snapshot examples/smoke-local-snapshot.astra.yaml

# 4. Start the control plane + web UI
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
  cargo run -p astra-control-plane
# → http://127.0.0.1:8080
```

Full step-by-step guide: [astra-core.github.io/docs/getting-started/quickstart](https://astra-core.github.io/docs/getting-started/quickstart)

---

## Documentation

| Topic | Link |
|---|---|
| Quickstart | [Getting started in 15 minutes](https://astra-core.github.io/docs/getting-started/quickstart) |
| YAML Spec | [v1alpha1 reference](https://astra-core.github.io/docs/yaml-spec/overview) |
| CLI Reference | [All commands](https://astra-core.github.io/docs/cli/reference) |
| REST API | [Control plane API](https://astra-core.github.io/docs/control-plane/api) |
| Architecture | [Design overview](https://astra-core.github.io/docs/architecture/overview) |
| Self-Hosting | [Docker Compose + env vars](https://astra-core.github.io/docs/self-hosting/docker-compose) |
| Contributing | [Developer guide](https://astra-core.github.io/docs/development/contributing) |
| Roadmap | [v0.1 + post-v0.1 plans](https://astra-core.github.io/docs/roadmap) |

---

## Development

```bash
cargo test --workspace          # run all tests
cargo clippy --workspace        # lint
cargo fmt --all                 # format (required before committing)
cd apps/web && npm run dev      # Vite dev server at 127.0.0.1:4173
python3 scripts/e2e_snapshot_smoke.py  # end-to-end smoke test
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [developer guide](https://astra-core.github.io/docs/development/contributing) for full setup instructions.

## License

Apache 2.0 — see [LICENSE](LICENSE).
