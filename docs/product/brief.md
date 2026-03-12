# Astra Product Brief

## One-line pitch

Astra is an open-source, high-performance data replication platform: the self-hostable alternative to Fivetran and a leaner, faster alternative to Airbyte.

## Problem

Current ELT/replication tools usually fail in one of two ways:

1. **Managed-first tools** like Fivetran are convenient but expensive and closed.
2. **Open-source tools** often become operationally heavy, connector-fragile, or slower than they should be.

Teams want something that:
- installs quickly
- runs on-prem or in cloud
- handles serious CDC and bulk sync workloads
- is usable by both UI-first users and Git/YAML-driven teams

## Product thesis

Astra should win by being:
- operationally lighter than Airbyte
- easier to self-host than heavyweight orchestration stacks
- faster on the hot path through a lean runtime and destination-native bulk loading
- declarative enough for GitOps and enterprise onboarding

## Target users

### Primary
- engineering teams moving data from OLTP systems into warehouses/lakes
- startups and mid-market teams that want Fivetran-like convenience without Fivetran pricing/control limits
- self-hosting/on-prem teams that cannot ship data through a SaaS-only control plane

### Secondary
- data platform teams that want a hackable replication core
- open-source teams who want YAML-first deployment patterns

## v0.1 goals

Astra v0.1 must prove one serious vertical slice:
- source configuration through UI and YAML
- initial snapshot/backfill
- continuous incremental sync/CDC for at least one database source
- one production-relevant destination
- job history, status, retries, and resume semantics
- self-host install via Docker Compose

## Non-goals for v0.1

- dozens of connectors
- full enterprise RBAC matrix
- transformation engine that tries to replace dbt/Flink
- complex stream-processing backbone as default install path
- giant marketplace/plugin platform
- multi-cluster managed cloud control plane

## Early source/destination focus

### Sources
- Postgres CDC
- MySQL CDC or one high-demand SaaS connector

### Destinations
- Snowflake or BigQuery
- object storage staging as the durable replay layer

## Differentiators

1. **Fast CDC + bulk replication** instead of protocol-heavy generic syncs
2. **YAML as a first-class source of truth**
3. **UI and YAML sharing the same canonical model**
4. **Simple self-host story** without distributed-systems cosplay
5. **Hybrid connector strategy**: fast Rust core, pragmatic Python long tail

## Success metrics for early versions

- first install to successful sync in under 15 minutes
- first meaningful Postgres -> warehouse sync substantially faster than an equivalent Airbyte setup
- successful resume after interruption without manual cleanup
- a contributor can author a simple SaaS connector without fighting the platform for days
