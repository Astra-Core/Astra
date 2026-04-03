---
id: roadmap
title: Roadmap
sidebar_position: 10
---

# Roadmap

## v0.1 — What shipped

v0.1 is the foundation release. It proves the architecture and delivers a working end-to-end snapshot pipeline.

### Shipped

| Feature | Status |
|---|---|
| YAML pipeline spec (v1alpha1) | ✅ |
| Postgres schema discovery | ✅ |
| Full snapshot (paginated) | ✅ |
| Incremental snapshot (cursor watermark) | ✅ |
| Local filesystem staging (JSONL.gz) | ✅ |
| MinIO / S3 staging | ✅ |
| Resumable checkpoint ledger | ✅ |
| Postgres raw destination loader | ✅ |
| 6 CLI commands | ✅ |
| Control-plane REST API (10 endpoints) | ✅ |
| React web UI — pipeline inventory | ✅ |
| React web UI — run history + table drill-down | ✅ |
| YAML Studio (inline editor + apply) | ✅ |
| In-memory pipeline repository (dev mode) | ✅ |
| Postgres-backed pipeline repository | ✅ |
| Embedded pipeline executor (run from UI) | ✅ |
| Unit + integration tests | ✅ |
| E2E smoke test | ✅ |
| GitHub Actions CI | ✅ |

### Known limitations

- CDC execution is stubbed — returns an explicit error
- `execute-local-snapshot` only supports Postgres destinations
- Write mode is append-only (no merge/upsert)
- Table parallelism is parsed but not enforced (always 1)
- No schema evolution enforcement
- Secrets: `env:VAR` only (no Vault, no file)
- Single local worker (no distributed execution)
- No Python connector runtime
- No structured observability / metrics export
- UI onboarding wizard is a placeholder

---

## Post-v0.1 roadmap

### Near-term

**Postgres CDC**
- Log-based change capture via `pgoutput` logical replication plugin
- Initial backfill + tail mode
- WAL position tracking and checkpoint semantics
- Requires `REPLICATION` privilege on the source

**Merge / upsert write mode**
- Destination writes that deduplicate by primary key
- Required for CDC correctness (deletes, updates)
- Postgres destination: `INSERT ... ON CONFLICT DO UPDATE`

**Additional destinations**
- Snowflake — COPY INTO via S3 staging
- BigQuery — load jobs via GCS staging

### Medium-term

**Python connector runtime**
- Subprocess host (`crates/astra-python-runtime`)
- JSON protocol over stdin/stdout
- Connector manifest spec (`crates/astra-saas-sdk`)
- First community connectors: Stripe, GitHub, HubSpot

**Distributed workers**
- `apps/worker` — distributed worker binary
- Worker registration and heartbeat
- Control plane dispatches runs to available workers
- Required for multi-tenant and high-throughput deployments

**Schema evolution**
- Detect schema changes between runs
- Configurable policies: `additive_changes: allow`, `breaking_changes: error`
- Schema fingerprint comparison per chunk

### Longer-term

**Secrets management**
- Vault integration
- File-based secrets
- Kubernetes Secrets support

**Observability**
- Structured metrics export (Prometheus)
- OpenTelemetry traces for runs
- Alert hooks

**MySQL source**
- Snapshot + CDC via binlog replication

**UI enhancements**
- Onboarding wizard (guided pipeline creation)
- Connection testing from the UI
- Real-time run progress streaming
- Pipeline scheduling editor
