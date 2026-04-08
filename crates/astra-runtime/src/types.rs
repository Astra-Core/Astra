use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::utils::{build_chunk_key, normalize_prefix, required_env, unix_time_ms};

// ── Staging format constants ──────────────────────────────────────────────────

pub const STAGING_CONTENT_TYPE_JSONL: &str = "application/x-ndjson";
pub const STAGING_CONTENT_ENCODING_GZIP: &str = "gzip";
pub const DEFAULT_AWS_REGION: &str = "us-east-1";

// ── Core enumerations ─────────────────────────────────────────────────────────

/// Which storage backend a staging config targets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StagingKind {
    S3,
    Minio,
    Local,
}

// ── Staging configuration types ───────────────────────────────────────────────

/// Storage-backend–agnostic staging parameters (bucket name + optional prefix).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingConfig {
    pub kind: StagingKind,
    pub bucket: String,
    pub prefix: String,
}

impl StagingConfig {
    /// Returns the prefix with surrounding whitespace and `/` characters stripped.
    pub fn normalized_prefix(&self) -> String {
        normalize_prefix(&self.prefix)
    }

    /// Builds the full object key for a chunk, prepending the normalised prefix
    /// when one is set.
    pub fn chunk_key(
        &self,
        pipeline_name: &str,
        stream_name: &str,
        partition_key: &str,
        sequence: u64,
    ) -> String {
        let base_key = build_chunk_key(pipeline_name, stream_name, partition_key, sequence);
        let prefix = self.normalized_prefix();
        if prefix.is_empty() {
            base_key
        } else {
            format!("{prefix}/{base_key}")
        }
    }
}

/// Configuration for the local-filesystem staging backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalStagingConfig {
    pub root_dir: PathBuf,
    pub storage: StagingConfig,
}

/// Configuration for the MinIO / S3-compatible staging backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MinioStagingConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub storage: StagingConfig,
}

impl MinioStagingConfig {
    /// Constructs a [`MinioStagingConfig`] from environment variables.
    ///
    /// Prefers AWS-standard variable names (`AWS_*`) and falls back to the
    /// Astra-specific `ASTRA_S3_*` equivalents.
    pub fn from_env(storage: StagingConfig) -> Result<Self> {
        Ok(Self {
            endpoint: required_env("ASTRA_S3_ENDPOINT")?,
            region: std::env::var("AWS_REGION")
                .ok()
                .or_else(|| std::env::var("ASTRA_S3_REGION").ok())
                .unwrap_or_else(|| DEFAULT_AWS_REGION.to_string()),
            access_key: std::env::var("AWS_ACCESS_KEY_ID")
                .ok()
                .or_else(|| std::env::var("ASTRA_S3_ACCESS_KEY").ok())
                .context("missing ASTRA_S3_ACCESS_KEY or AWS_ACCESS_KEY_ID")?,
            secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .ok()
                .or_else(|| std::env::var("ASTRA_S3_SECRET_KEY").ok())
                .context("missing ASTRA_S3_SECRET_KEY or AWS_SECRET_ACCESS_KEY")?,
            storage,
        })
    }
}

// ── Chunk data types ──────────────────────────────────────────────────────────

/// Metadata record for a chunk that has been written to a staging backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageChunk {
    pub pipeline_name: String,
    pub stream_name: String,
    pub partition_key: String,
    pub sequence: u64,
    pub bucket: String,
    pub object_key: String,
    pub bytes_written: u64,
    pub row_count: u64,
    pub content_type: String,
    pub content_encoding: String,
    pub schema_fingerprint: Option<String>,
    pub created_at_unix_ms: u64,
}

/// The raw bytes and metadata for a chunk that is about to be staged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageChunkPayload {
    pub row_count: u64,
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub content_encoding: String,
    pub schema_fingerprint: Option<String>,
}

impl StageChunkPayload {
    /// Convenience constructor for the standard JSONL + gzip format.
    pub fn jsonl_gzip(row_count: u64, bytes: Vec<u8>) -> Self {
        Self {
            row_count,
            bytes,
            content_type: STAGING_CONTENT_TYPE_JSONL.to_string(),
            content_encoding: STAGING_CONTENT_ENCODING_GZIP.to_string(),
            schema_fingerprint: None,
        }
    }
}

/// A request to write a single chunk to the staging backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageChunkRequest {
    pub pipeline_name: String,
    pub stream_name: String,
    pub partition_key: String,
    pub sequence: u64,
    pub payload: StageChunkPayload,
}

impl StageChunkRequest {
    /// Derives the [`StageChunk`] metadata record that will be persisted once
    /// this request is successfully written to `storage`.
    pub fn to_chunk(&self, storage: &StagingConfig) -> StageChunk {
        StageChunk {
            pipeline_name: self.pipeline_name.clone(),
            stream_name: self.stream_name.clone(),
            partition_key: self.partition_key.clone(),
            sequence: self.sequence,
            bucket: storage.bucket.clone(),
            object_key: storage.chunk_key(
                &self.pipeline_name,
                &self.stream_name,
                &self.partition_key,
                self.sequence,
            ),
            bytes_written: self.payload.bytes.len() as u64,
            row_count: self.payload.row_count,
            content_type: self.payload.content_type.clone(),
            content_encoding: self.payload.content_encoding.clone(),
            schema_fingerprint: self.payload.schema_fingerprint.clone(),
            created_at_unix_ms: unix_time_ms(),
        }
    }
}

// ── Sink commit ───────────────────────────────────────────────────────────────

/// Acknowledgement from a destination connector that a batch has been committed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkCommit {
    pub destination_kind: String,
    pub commit_token: String,
    pub rows_written: u64,
}
