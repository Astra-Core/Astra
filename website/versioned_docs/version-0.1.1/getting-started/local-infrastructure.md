---
id: local-infrastructure
title: Local Infrastructure
sidebar_position: 2
---

# Local Infrastructure

Astra uses Postgres for pipeline metadata and S3-compatible object storage for staging. The provided Docker Compose stack runs both locally.

## Docker Compose stack

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

### Services

| Service | Container | Ports | Default credentials |
|---|---|---|---|
| Postgres 16 | `astra-postgres` | `5432` | user: `astra`, pw: `astra`, db: `astra` |
| MinIO | `astra-minio` | API: `9000`, Console: `9001` | key: `astra`, secret: `astrastorage` |

Both services use named Docker volumes (`postgres-data`, `minio-data`) so data persists across restarts.

### Stop and reset

```bash
# Stop services (data persists)
podman compose -f deploy/docker-compose/docker-compose.yml down

# Destroy data volumes (full reset)
podman compose -f deploy/docker-compose/docker-compose.yml down -v
```

## Postgres

Postgres stores all pipeline metadata: pipeline definitions, run records, table execution state, and spec history.

### Connect

```bash
PGPASSWORD=astra psql -h localhost -U astra -d astra
```

### Schema

Astra automatically creates its metadata tables on startup when `ASTRA_DATABASE_URL` is set. The tables live in the default `public` schema.

Replicated data is written to the `astra_raw` schema with one table per replicated stream: `raw_<source_schema>_<table_name>`.

## MinIO

MinIO provides an S3-compatible API for staged chunks. Each chunk is a compressed JSONL.gz file stored at:

```
pipelines/<pipeline-name>/streams/<stream>/partitions/<partition>/chunks/<sequence>.jsonl.gz
```

### Access the console

Open [http://localhost:9001](http://localhost:9001) and log in with `astra` / `astrastorage`.

### Use with the CLI

Set these environment variables to point Astra at your MinIO instance:

```bash
export ASTRA_S3_ENDPOINT=http://localhost:9000
export ASTRA_S3_REGION=us-east-1
export ASTRA_S3_ACCESS_KEY=astra
export ASTRA_S3_SECRET_KEY=astrastorage
```

Then reference MinIO staging in your pipeline YAML:

```yaml
destination:
  staging:
    kind: minio
    bucket: my-staging-bucket
    prefix: my-pipeline/
```

## Local file staging

For development, Astra also supports staging to the local filesystem — no MinIO needed:

```yaml
destination:
  staging:
    kind: local
    bucket: astra-staging
    prefix: my-pipeline/
```

Set `ASTRA_STAGING_LOCAL_ROOT` to control where files are written (defaults to `.astra/staging`).

## In-memory mode (no Postgres)

The control plane can run without Postgres, using an in-memory store. Data does not survive restarts. Useful for quick experiments:

```bash
cargo run -p astra-control-plane
# No ASTRA_DATABASE_URL — starts in-memory mode
```

## Environment variable reference

See [Environment Variables →](../self-hosting/environment-variables.md) for the full list.
