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
- object key
- bytes written
- row count
- content encoding/compression
- schema fingerprint or version marker where useful

## Object key convention
Suggested v0.1 pattern:

`pipelines/<pipeline>/streams/<stream>/partitions/<partition>/chunks/<sequence>.jsonl.gz`

Example:

`pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000042.jsonl.gz`

This is boring on purpose. Boring keys are easier to debug at 2 AM.

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
The first local adapter should:
- target MinIO/S3-compatible storage
- support bucket existence checks
- write predictable object keys
- return metadata required by checkpointing
- avoid premature abstraction theater

## Open questions
- whether v0.1 should standardize on JSONL.gz or allow Parquet early
- whether a separate apply ledger is cleaner than embedding sink commit markers in checkpoints
- when to introduce per-destination staging format negotiation
