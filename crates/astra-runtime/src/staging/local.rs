use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
};

use crate::{
    staging::StageChunkStore,
    types::{
        LocalStagingConfig, StageChunk, StageChunkRequest, STAGING_CONTENT_ENCODING_GZIP,
        STAGING_CONTENT_TYPE_JSONL,
    },
    utils::{ensure_chunk_belongs_to_bucket, parse_sequence},
};

/// Stages chunks as plain files under a local directory tree.
///
/// Layout: `<root_dir>/<bucket>/<prefix>/pipelines/<pipeline>/streams/<stream>/partitions/<partition>/chunks/<seq>.jsonl.gz`
#[derive(Debug, Clone)]
pub struct LocalStageChunkStore {
    config: LocalStagingConfig,
}

impl LocalStageChunkStore {
    pub fn new(config: LocalStagingConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &LocalStagingConfig {
        &self.config
    }

    /// Returns the directory that acts as the bucket root for this store.
    pub fn bucket_root(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.storage.bucket)
    }

    /// Translates an object key into an absolute filesystem path by splitting
    /// the key on `/` and joining each segment onto the bucket root.
    pub fn resolve_path(&self, object_key: &str) -> PathBuf {
        object_key
            .split('/')
            .filter(|segment| !segment.is_empty())
            .fold(self.bucket_root(), |path, segment| path.join(segment))
    }

    /// Scans the local staging tree and returns all chunks that belong to
    /// `pipeline_name`, sorted by stream name then sequence number.
    pub fn list_chunks_for_pipeline(&self, pipeline_name: &str) -> Result<Vec<StageChunk>> {
        let mut chunks = Vec::new();
        let prefix = self.config.storage.normalized_prefix();
        let streams_dir = if prefix.is_empty() {
            self.bucket_root()
        } else {
            self.bucket_root().join(&prefix)
        }
        .join("pipelines")
        .join(pipeline_name)
        .join("streams");

        if !streams_dir.exists() {
            return Ok(chunks);
        }

        let stream_dirs = fs::read_dir(&streams_dir).with_context(|| {
            format!(
                "failed to list staged streams under {}",
                streams_dir.display()
            )
        })?;

        for entry in stream_dirs {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let stream_name = entry.file_name().to_string_lossy().to_string();
            let chunks_dir = entry
                .path()
                .join("partitions")
                .join("default")
                .join("chunks");

            if !chunks_dir.exists() {
                continue;
            }

            for chunk_entry in fs::read_dir(&chunks_dir).with_context(|| {
                format!(
                    "failed to list staged chunks under {}",
                    chunks_dir.display()
                )
            })? {
                let chunk_entry = chunk_entry?;
                if !chunk_entry.file_type()?.is_file() {
                    continue;
                }
                let file_name = chunk_entry.file_name().to_string_lossy().to_string();
                if !file_name.ends_with(".jsonl.gz") {
                    continue;
                }
                let sequence = parse_sequence(&file_name)?;
                let object_key =
                    self.config
                        .storage
                        .chunk_key(pipeline_name, &stream_name, "default", sequence);
                let bytes_written = chunk_entry.metadata()?.len();
                chunks.push(StageChunk {
                    pipeline_name: pipeline_name.to_string(),
                    stream_name: stream_name.clone(),
                    partition_key: "default".to_string(),
                    sequence,
                    bucket: self.config.storage.bucket.clone(),
                    object_key,
                    bytes_written,
                    row_count: 0,
                    content_type: STAGING_CONTENT_TYPE_JSONL.to_string(),
                    content_encoding: STAGING_CONTENT_ENCODING_GZIP.to_string(),
                    schema_fingerprint: None,
                    created_at_unix_ms: 0,
                });
            }
        }

        chunks.sort_by(|a, b| {
            a.stream_name
                .cmp(&b.stream_name)
                .then(a.sequence.cmp(&b.sequence))
        });
        Ok(chunks)
    }
}

#[async_trait]
impl StageChunkStore for LocalStageChunkStore {
    async fn ensure_ready(&self) -> Result<()> {
        fs::create_dir_all(self.bucket_root()).with_context(|| {
            format!(
                "failed to create local staging bucket root at {}",
                self.bucket_root().display()
            )
        })
    }

    async fn write_chunk(&self, request: StageChunkRequest) -> Result<StageChunk> {
        self.ensure_ready().await?;
        let chunk = request.to_chunk(&self.config.storage);
        let path = self.resolve_path(&chunk.object_key);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create local staging directory for {}",
                    parent.display()
                )
            })?;
        }

        let mut file = fs::File::create(&path)
            .with_context(|| format!("failed to create staged chunk at {}", path.display()))?;
        file.write_all(&request.payload.bytes)
            .with_context(|| format!("failed to write staged chunk at {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to flush staged chunk at {}", path.display()))?;

        Ok(chunk)
    }

    async fn read_chunk(&self, chunk: &StageChunk) -> Result<Vec<u8>> {
        ensure_chunk_belongs_to_bucket(&self.config.storage.bucket, chunk)?;
        let path = self.resolve_path(&chunk.object_key);
        let mut file = fs::File::open(&path)
            .with_context(|| format!("failed to open staged chunk at {}", path.display()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read staged chunk at {}", path.display()))?;
        Ok(bytes)
    }
}
