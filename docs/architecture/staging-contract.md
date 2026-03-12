# Astra Staging Contract Draft

This document defines the v0.1 staging contract between capture and destination apply.

## Why this exists
Astra needs a durable layer between source ingestion and destination writes so it can:
- replay failed work
- resume safely after interruption
- decouple capture speed from sink speed
- support object-storage-backed operation in both cloud and self-hosted installs

## Default approach
For v0.1, Astra uses object storage as the default durable staging layer.

Primary targets:
- MinIO for local/self-hosted development
- S3-compatible storage for production-style deployment
- future: GCS/R2/Azure Blob via adapter layers

## Stage chunk model
A stage chunk represents a flushed batch of records ready for sink apply.

### Required metadata
- pipeline name
- stream/table name
- partition key
- sequence number
- bucket
- object key
- bytes written
- row count
- content type
- content encoding/compression
- schema fingerprint or version marker where useful
- creation timestamp

The runtime crate now models this as a `StageChunk` plus a `StageChunkRequest` payload that can be written by any adapter implementing `StageChunkStore`.

## Object key convention
Suggested v0.1 pattern:

`pipelines/<pipeline>/streams/<stream>/partitions/<partition>/chunks/<sequence>.jsonl.gz`

Example:

`pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000042.jsonl.gz`

This is boring on purpose. Boring keys are easier to debug at 2 AM.

If a staging prefix is configured, it is prepended once and normalized so local filesystem paths and MinIO/S3 object keys line up instead of becoming slash soup.

## Write flow
1. source capture reads records/change events
2. runtime buffers a batch locally
3. runtime flushes a compressed chunk to staging
4. destination apply reads chunk metadata and writes to the sink
5. checkpoint is recorded only after sink commit succeeds

## Checkpoint semantics
Checkpoint data should record:
- upstream progress marker (snapshot chunk or WAL/binlog offset)
- staged chunk identity
- sink commit token where available

This allows:
- re-reading from staging when sink apply fails
- avoiding duplicate commit confusion where sink semantics support idempotency

## Local adapter expectations
The first local adapter is intentionally simple:
- target a local filesystem root, but preserve the same bucket/object-key contract used by MinIO/S3
- support bucket existence checks by creating the bucket root on first use
- write predictable object keys
- return metadata required by checkpointing
- read staged chunks back for retry/replay tests
- avoid premature abstraction theater

The implemented `LocalStageChunkStore` writes chunks to:

`<root>/<bucket>/<prefix>/pipelines/<pipeline>/streams/<stream>/partitions/<partition>/chunks/<sequence>.jsonl.gz`

That means local Podman-based development can stage chunks without requiring a running object-store client SDK, while still matching the same key layout a future MinIO-backed adapter will use.

## MinIO / S3-compatible adapter expectations
The next adapter keeps the exact same `StageChunkStore` contract and swaps the backing store from the filesystem to a bucket reachable over an S3-compatible API.

Current v0.1 behavior:
- uses an explicit endpoint URL, access key, and secret key
- forces path-style addressing so local MinIO behaves predictably
- ensures the bucket exists before writing the first chunk
- writes chunk payload bytes with the same object key convention as local staging
- reads chunk bytes back for replay/retry paths
- keeps the contract local-first instead of pretending cloud-only defaults are acceptable

The runtime crate now includes `MinioStageChunkStore`, and the CLI exposes `snapshot-to-minio-staging` so the same snapshot flow can write directly into a local Podman-managed MinIO instance.

## First destination loader: local Postgres raw tables
Issue #37 starts with the least delusional destination apply path:
- read staged `JSONL.gz` chunks from the local staging adapter
- connect to a self-hosted/local Postgres destination
- create a raw schema (default `astra_raw`) if it does not exist
- create one raw table per captured stream, with names derived from the stream (`public.orders` -> `raw_public_orders` by default)
- insert each JSON document into a `_data jsonb` column plus loader metadata columns
- track applied chunk object keys in `astra_raw._applied_chunks` so re-running the loader skips already applied chunks instead of duplicating them forever

Current raw table shape:
- `_object_key text`
- `_sequence bigint`
- `_row_number bigint`
- `_loaded_at timestamptz default now()`
- `_data jsonb`

This is intentionally narrow. It is not pretending to be merge/upsert semantics yet. It gives Astra a real, self-hostable destination leg that can be run locally under Podman without inventing warehouse-specific nonsense too early.

## Podman-based local development
The repo ships a MinIO service in the local Podman Compose stack.

Bring it up with:

```bash
podman compose -f deploy/docker-compose/docker-compose.yml up -d
```

With the default local credentials from `.env.example`, the MinIO-backed staging path can be exercised with:

```bash
export POSTGRES_PASSWORD=astra
export ASTRA_S3_ENDPOINT=http://127.0.0.1:9000
export ASTRA_S3_REGION=us-east-1
export ASTRA_S3_ACCESS_KEY=astra
export ASTRA_S3_SECRET_KEY=astrastorage
cargo run -p astra -- snapshot-to-minio-staging examples/postgres-to-warehouse.astra.yaml --max-rows-per-table 1000
```

If MinIO is not running, the filesystem adapter is still the cheapest dev/test loop.

For the Postgres raw loader, a local/self-hosted setup can skip MinIO entirely and stage to the filesystem:
1. run a source Postgres and destination Postgres (or separate DBs on one instance)
2. snapshot to `.astra/staging`
3. load the staged chunks into the destination raw schema

## Open questions
- whether v0.1 should standardize on JSONL.gz or allow Parquet early
- whether a separate apply ledger is cleaner than embedding sink commit markers in checkpoints
- when to introduce per-destination staging format negotiation
- when it becomes worth splitting staging backends into a dedicated storage crate instead of keeping the first two adapters in `astra-runtime`
- when the Postgres raw loader should graduate from row-by-row inserts to `COPY`-based bulk loading
- how soon to add normalized/table-shaped destination writers on top of the raw landing zone
