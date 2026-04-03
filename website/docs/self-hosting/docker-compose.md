---
id: docker-compose
title: Docker Compose Deployment
sidebar_position: 1
---

# Docker Compose Deployment

The simplest way to run Astra's dependencies locally is with the provided Docker Compose file.

## Prerequisites

- [Podman](https://podman.io) (recommended) or [Docker](https://docker.com) with Compose v2

## Start the stack

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

This starts:

- **Postgres 16** on port `5432`
- **MinIO** on ports `9000` (API) and `9001` (web console)

## Services

### Postgres

| Setting | Value |
|---|---|
| Image | `postgres:16` |
| Container name | `astra-postgres` |
| Port | `5432` |
| User | `astra` |
| Password | `astra` |
| Database | `astra` |
| Volume | `postgres-data` |

Connect with psql:

```bash
PGPASSWORD=astra psql -h localhost -U astra -d astra
```

### MinIO

| Setting | Value |
|---|---|
| Image | `minio/minio` |
| Container name | `astra-minio` |
| API port | `9000` |
| Console port | `9001` |
| Access key | `astra` |
| Secret key | `astrastorage` |
| Volume | `minio-data` |

Access the console at [http://localhost:9001](http://localhost:9001).

## Common operations

```bash
# Check status
podman compose -f deploy/docker-compose/docker-compose.yml ps

# View logs
podman compose -f deploy/docker-compose/docker-compose.yml logs -f

# Stop (data persists)
podman compose -f deploy/docker-compose/docker-compose.yml down

# Full reset (destroy all data)
podman compose -f deploy/docker-compose/docker-compose.yml down -v
```

## Start the control plane

After the infrastructure is up:

```bash
ASTRA_DATABASE_URL=postgres://astra:astra@localhost:5432/astra \
ASTRA_S3_ENDPOINT=http://localhost:9000 \
ASTRA_S3_ACCESS_KEY=astra \
ASTRA_S3_SECRET_KEY=astrastorage \
cargo run -p astra-control-plane
```

The control plane will:

1. Connect to Postgres and create metadata tables
2. Start the Axum HTTP server at `127.0.0.1:8080`
3. Serve the web UI at `http://127.0.0.1:8080`

## Production deployment

For production, the Docker Compose stack is intended as a template. You would typically:

- Use a managed Postgres service (RDS, Cloud SQL, etc.)
- Use AWS S3 or GCS instead of MinIO
- Run the control plane binary behind a reverse proxy (nginx, Caddy)
- Set `ASTRA_CONTROL_PLANE_ADDR=0.0.0.0:8080` to bind on all interfaces
- Use proper secrets management instead of environment variables

Kubernetes support is on the roadmap but is not a prerequisite for v0.1.
