---
id: modular-monolith
title: "ADR-0001: Modular Monolith"
sidebar_position: 2
---

# ADR-0001: Modular Monolith

**Status:** Accepted

## Context

Astra aims to be easier to install than Airbyte while being materially faster on CDC and bulk replication workloads. The first architectural question was: single service or microservices?

Airbyte's platform-as-microservices design requires a full Kubernetes cluster and a non-trivial ops burden just to get started. Simpler tools like `pglogical` or `pg_dump` pipelines are operationally lightweight but lack a control plane, web UI, and extensible connector model.

Astra needs to hit a middle ground: easy to self-host on a single node while still being production-ready.

## Decision

Astra v1 is a **modular monolith** — a single control-plane binary with clean internal boundaries. The boundaries are:

- **API** — HTTP handlers (Axum)
- **Scheduler** — trigger and manage pipeline runs
- **Metadata / state manager** — pipeline and run storage via `PipelineService`
- **Config validation** — `astra-yaml` crate
- **Orchestration / runtime control** — embedded executor
- **Secrets abstraction** — `astra-secrets` crate
- **Observability** — `astra-observability` crate (tracing hooks)
- **Worker transport** — connector dispatch (local today, remote workers future)

These are **not** separate deployable services. They are separate Rust modules with defined interfaces. The internal boundaries must be preserved so future extraction remains possible.

## Consequences

### Positive

- **Simpler installation** — one binary, one `docker compose up`
- **Lower runtime overhead** — no inter-process serialization on the hot path
- **Easier local development** — single process, single set of logs
- **Clearer CDC differentiation** — Rust-native performance isn't diluted by polyglot orchestration

### Negative

- **More discipline required** — a monolith can become a "big ball of mud" if internal boundaries erode. Code review must enforce them.
- **Rust-first raises the contribution bar** — some contributors may be more comfortable with Go or Python; the core stays in Rust.
- **Python subprocess complexity** — Tier B connectors (future) will run as subprocesses, adding some runtime coordination overhead.

## Guardrails

To prevent the monolith from becoming a ball of mud:

1. No crate should import another crate's `internal` module — only public API
2. All state changes go through `PipelineService`, never directly to the repository
3. Connectors live in `crates/astra-connectors` and must not depend on `apps/control-plane`
4. The staging contract (`StageChunk` schema) is defined in `crates/astra-runtime` and is the only coupling point between capture and load

## When to revisit

Extract services when:

- Remote workers are needed (multi-node execution)
- Multi-tenant SaaS pressure requires per-tenant isolation
- Python connector isolation exceeds what a subprocess boundary can provide

The internal boundaries are designed to make this extraction incremental, not a rewrite.
