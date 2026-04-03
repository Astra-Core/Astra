---
id: environment-variables
title: Environment Variables
sidebar_position: 2
---

# Environment Variables

Astra is configured via environment variables. See `.env.example` in the repository root for a copy-pasteable template.

## Control plane

| Variable | Default | Description |
|---|---|---|
| `ASTRA_CONTROL_PLANE_ADDR` | `127.0.0.1:8080` | Bind address and port for the HTTP server |
| `ASTRA_DATABASE_URL` | _(none)_ | Postgres connection string. If unset, the control plane starts in **in-memory mode** (data not persisted). Format: `postgres://user:password@host:port/database` |

## Object storage (S3 / MinIO)

| Variable | Default | Description |
|---|---|---|
| `ASTRA_S3_ENDPOINT` | _(none)_ | Custom S3 endpoint URL. Set this for MinIO (`http://localhost:9000`) or non-AWS S3-compatible services. |
| `ASTRA_S3_REGION` | `us-east-1` | S3 region. Use any value for MinIO. |
| `ASTRA_S3_ACCESS_KEY` | _(none)_ | S3 / MinIO access key ID |
| `ASTRA_S3_SECRET_KEY` | _(none)_ | S3 / MinIO secret access key |

## Local staging and checkpointing

| Variable | Default | Description |
|---|---|---|
| `ASTRA_STAGING_LOCAL_ROOT` | `.astra/staging` | Root directory for local staging chunks (used when `staging.kind: local`) |
| `ASTRA_CHECKPOINT_LOCAL_ROOT` | `.astra/checkpoints` | Root directory for checkpoint ledger files |

## Source and destination credentials

Credentials referenced in pipeline specs via `env:<VAR_NAME>` are resolved from the process environment at runtime. For example:

```yaml
credentials:
  password: "env:POSTGRES_PASSWORD"
```

Requires `POSTGRES_PASSWORD` to be set in the environment before running `cargo run -p astra`.

## Example `.env` file

```bash
# Control plane
ASTRA_CONTROL_PLANE_ADDR=127.0.0.1:8080
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra

# MinIO (local development)
ASTRA_S3_ENDPOINT=http://localhost:9000
ASTRA_S3_REGION=us-east-1
ASTRA_S3_ACCESS_KEY=astra
ASTRA_S3_SECRET_KEY=astrastorage

# Local staging
ASTRA_STAGING_LOCAL_ROOT=.astra/staging
ASTRA_CHECKPOINT_LOCAL_ROOT=.astra/checkpoints

# Source database credential (referenced by pipeline specs)
POSTGRES_PASSWORD=astra
```

To load this file:

```bash
export $(grep -v '^#' .env | xargs)
```

Or use a tool like [direnv](https://direnv.net/).
