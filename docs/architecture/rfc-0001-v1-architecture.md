# RFC-0001: Astra v1 Architecture

## Status
Proposed

## Summary

Astra v1 will be built as a **Rust-first modular monolith** with:
- Postgres as metadata/state storage
- object storage as durable staging/replay
- a high-performance database CDC lane
- a pragmatic Python runtime for long-tail SaaS/community connectors
- UI and YAML sharing one canonical configuration model

## Context

Astra aims to be easier to install than Airbyte while being materially faster on CDC and bulk replication workloads. This requires avoiding orchestration-heavy designs and minimizing per-sync startup, serialization, and coordination overhead.

## Decision

### 1. Control plane
Astra will start as a single primary service with clean internal boundaries:
- API
- scheduler
- metadata/state manager
- config validation
- orchestration/runtime control
- secrets abstraction
- observability

This is a modular monolith, not a bag of mud. Internal interfaces should make future extraction possible.

### 2. Runtime lanes
Astra will explicitly split runtime concerns into two major lanes.

#### Lane A: Database CDC lane
Optimized for:
- Postgres
- MySQL
- later high-value relational sources

Key capabilities:
- log-based CDC
- initial snapshot/backfill
- incremental snapshot chunking
- resumable checkpoints
- partitioned parallelism
- destination-native bulk load + merge/upsert

#### Lane B: SaaS/API lane
Optimized for:
- incremental API syncs
- rate-limited sources
- long-tail connector breadth

Key capabilities:
- cursor/bookmark state
- pagination/auth/rate-limit abstractions
- per-stream failure isolation
- easier connector authoring

### 3. Connector model
Astra will use a two-tier connector strategy.

#### Tier A: Rust-native connectors
Use for core/high-volume connectors where Astra’s performance reputation is made or lost.

#### Tier B: Python runtime connectors
Use for community and long-tail connectors. These run in bounded subprocesses instead of contaminating the hot path.

### 4. Storage strategy
#### Metadata/state
- Postgres

#### Durable staging/replay
- S3 / GCS / R2 / MinIO compatible object storage

Object storage is the default durability layer for replay, recovery, and decoupling capture from destination apply.

### 5. Canonical configuration model
Astra will define one canonical configuration/spec model.

That model will be:
- authored via YAML
- validated via CLI/API
- edited through the UI
- versioned in metadata

This prevents a split-brain product where the UI and declarative setup paths drift apart.

### 6. Deployment model
For v1, Astra must support:
- Docker Compose self-hosting
- single-node local development
- future Kubernetes support without making Kubernetes a prerequisite

## Consequences

### Positive
- simpler installation and operations
- lower runtime overhead than Airbyte-like designs
- clear differentiation around CDC performance and YAML-driven onboarding
- future path toward remote workers without a rewrite

### Negative
- more discipline required inside the modular monolith
- Rust-first core raises the bar for some contributors
- Python subprocess model adds some runtime complexity for non-core connectors

## Explicit anti-goals
Astra v1 will not:
- require Kafka/Redpanda/NATS as core infrastructure
- optimize for dozens of connectors before the hot path is excellent
- become a transformation engine competitor to dbt/Flink
- split into many services before scale justifies it

## Follow-up RFCs needed
- YAML spec schema and versioning
- metadata schema and state model
- Postgres CDC lifecycle and checkpoint semantics
- destination bulk loading contract
- Python connector runtime manifest/protocol
