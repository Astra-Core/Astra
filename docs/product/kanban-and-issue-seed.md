# Astra Kanban, Epics, and Issue Seed

## Kanban columns
- Inbox
- Ready
- In Progress
- Blocked
- In Review
- In QA
- Done

## Board rules
- Nothing enters **In Progress** without acceptance criteria.
- Anything blocked for more than 24 hours gets surfaced.
- “Done” means merged, tested, docs updated where relevant, and deployed to preview/staging if applicable.
- Keep WIP low. Half-finished work is just organized disappointment.

## Recommended labels
- arch
- backend
- cdc
- connector
- destination
- ui
- yaml
- infra
- docs
- perf
- v0.1
- epic

## Initial epics
1. Repository foundation
2. Metadata + control plane
3. YAML spec + validation
4. DB CDC runtime
5. Destination loaders
6. UI onboarding flow
7. Python connector runtime
8. Observability + ops
9. v0.1 docs + release readiness

## Initial issue seed

### Repository foundation
- Initialize Rust workspace and crate structure
- Add formatting, linting, and CI workflows
- Add Docker Compose for Postgres + MinIO + local services
- Define environment/config loading strategy
- Rewrite root README for actual onboarding
- Add contribution guide and PR template

### Metadata + control plane
- Define metadata schema for sources, destinations, jobs, checkpoints, and run history
- Build control-plane service skeleton
- Add health, readiness, and version endpoints
- Add job scheduler skeleton
- Add secrets abstraction and local dev provider

### YAML spec + validation
- Define v1 YAML schema
- Implement YAML parser and validator crate
- Add CLI `validate` and `apply` commands
- Store spec versions and renderable summaries in metadata

### DB CDC runtime
- Implement Postgres source connector skeleton
- Support initial snapshot for selected tables
- Add incremental snapshot chunking
- Add WAL-based CDC tailing
- Add checkpoint persistence and resume
- Add partitioned loading pipeline

### Destination loaders
- Implement object-storage staging contract
- Implement first warehouse destination loader
- Support bulk load + merge/upsert path
- Record sink commit markers and retry semantics

### UI onboarding flow
- Build sources/destinations/pipeline onboarding wizard
- Add YAML preview/export from the UI
- Add job history/status view
- Add schema drift and error surfacing

### Python connector runtime
- Define connector manifest/protocol
- Build subprocess launcher and resource limits
- Ship one simple SaaS connector as proof

### Observability + ops
- Structured logging across services/crates
- Metrics and tracing hooks
- Failure classification and surfaced diagnostics
- Local runbook and staging deployment path

### Docs + release readiness
- Product brief
- Architecture RFCs
- ADRs for stack decisions
- Demo data/demo script
- v0.1 release checklist
