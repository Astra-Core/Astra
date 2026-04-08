pub mod local;
pub mod minio;

pub use local::LocalStageChunkStore;
pub use minio::MinioStageChunkStore;

use anyhow::Result;
use async_trait::async_trait;

use crate::types::{StageChunk, StageChunkRequest};

/// Abstraction over all staging backends (local filesystem, MinIO, S3 …).
///
/// Implementations write raw bytes to durable storage and return the metadata
/// record ([`StageChunk`]) that callers can use to retrieve them later.
#[async_trait]
pub trait StageChunkStore: Send + Sync {
    /// Ensures the backing store is ready to accept writes (creates buckets /
    /// directories as needed).
    async fn ensure_ready(&self) -> Result<()>;

    /// Writes a chunk to the backing store and returns its metadata.
    async fn write_chunk(&self, request: StageChunkRequest) -> Result<StageChunk>;

    /// Reads the raw bytes of a previously staged chunk.
    async fn read_chunk(&self, chunk: &StageChunk) -> Result<Vec<u8>>;
}
