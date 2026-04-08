use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as ObjectPath,
    ObjectStoreExt,
};

use crate::{
    staging::StageChunkStore,
    types::{MinioStagingConfig, StageChunk, StageChunkRequest},
    utils::ensure_chunk_belongs_to_bucket,
};

// ── Additional I/O methods on MinioStagingConfig ──────────────────────────────

impl MinioStagingConfig {
    /// Builds a configured [`AmazonS3`] object-store client for `bucket`.
    pub(crate) fn object_store(&self, bucket: &str) -> Result<AmazonS3> {
        AmazonS3Builder::new()
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_region(&self.region)
            .with_bucket_name(bucket)
            .with_endpoint(&self.endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()
            .context("failed to build S3 object store client")
    }

    /// Creates `bucket` in MinIO via a plain HTTP PUT, tolerating a `409
    /// Conflict` response (bucket already exists).
    pub(crate) async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        let url = format!("{}/{}", self.endpoint.trim_end_matches('/'), bucket);
        let resp = reqwest::Client::new()
            .put(&url)
            .header(
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
            .send()
            .await
            .with_context(|| format!("failed to reach MinIO at {url}"))?;

        // 200 = created, 409 = already exists — both are acceptable.
        if resp.status().is_success() || resp.status() == 409 {
            Ok(())
        } else {
            Err(anyhow!(
                "unexpected status creating bucket {bucket}: {}",
                resp.status()
            ))
        }
    }
}

// ── MinioStageChunkStore ──────────────────────────────────────────────────────

/// Stages chunks in a MinIO (or any S3-compatible) object store.
#[derive(Debug, Clone)]
pub struct MinioStageChunkStore {
    config: MinioStagingConfig,
}

impl MinioStageChunkStore {
    pub fn new(config: MinioStagingConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &MinioStagingConfig {
        &self.config
    }
}

#[async_trait]
impl StageChunkStore for MinioStageChunkStore {
    async fn ensure_ready(&self) -> Result<()> {
        self.config.ensure_bucket(&self.config.storage.bucket).await
    }

    async fn write_chunk(&self, request: StageChunkRequest) -> Result<StageChunk> {
        self.ensure_ready().await?;
        let chunk = request.to_chunk(&self.config.storage);
        let store = self.config.object_store(&chunk.bucket)?;
        let path = ObjectPath::from(chunk.object_key.as_str());
        store
            .put(&path, Bytes::from(request.payload.bytes).into())
            .await
            .with_context(|| {
                format!(
                    "failed to write staged chunk to s3://{}/{} via {}",
                    chunk.bucket, chunk.object_key, self.config.endpoint
                )
            })?;
        Ok(chunk)
    }

    async fn read_chunk(&self, chunk: &StageChunk) -> Result<Vec<u8>> {
        ensure_chunk_belongs_to_bucket(&self.config.storage.bucket, chunk)?;
        let store = self.config.object_store(&chunk.bucket)?;
        let path = ObjectPath::from(chunk.object_key.as_str());
        let result = store.get(&path).await.with_context(|| {
            format!(
                "failed to read staged chunk from s3://{}/{} via {}",
                chunk.bucket, chunk.object_key, self.config.endpoint
            )
        })?;
        result
            .bytes()
            .await
            .context("failed to collect staged object body")
            .map(|b| b.to_vec())
    }
}
