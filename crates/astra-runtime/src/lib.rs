use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_config::{self, BehaviorVersion};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder as S3ConfigBuilder, Region},
    primitives::ByteStream,
    Client,
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

pub const CRATE_NAME: &str = "astra-runtime";
pub const STAGING_CONTENT_TYPE_JSONL: &str = "application/x-ndjson";
pub const STAGING_CONTENT_ENCODING_GZIP: &str = "gzip";
pub const DEFAULT_AWS_REGION: &str = "us-east-1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StagingKind {
    S3,
    Minio,
    Local,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingConfig {
    pub kind: StagingKind,
    pub bucket: String,
    pub prefix: String,
}

impl StagingConfig {
    pub fn normalized_prefix(&self) -> String {
        normalize_prefix(&self.prefix)
    }

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalStagingConfig {
    pub root_dir: PathBuf,
    pub storage: StagingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MinioStagingConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub storage: StagingConfig,
}

impl MinioStagingConfig {
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

    async fn s3_client(&self) -> Result<Client> {
        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                self.access_key.clone(),
                self.secret_key.clone(),
                None,
                None,
                "astra-minio-stage-store",
            ))
            .region(Region::new(self.region.clone()))
            .load()
            .await;

        let s3_config = S3ConfigBuilder::from(&shared_config)
            .endpoint_url(self.endpoint.clone())
            .force_path_style(true)
            .build();

        Ok(Client::from_conf(s3_config))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageChunkPayload {
    pub row_count: u64,
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub content_encoding: String,
    pub schema_fingerprint: Option<String>,
}

impl StageChunkPayload {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageChunkRequest {
    pub pipeline_name: String,
    pub stream_name: String,
    pub partition_key: String,
    pub sequence: u64,
    pub payload: StageChunkPayload,
}

impl StageChunkRequest {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkCommit {
    pub destination_kind: String,
    pub commit_token: String,
    pub rows_written: u64,
}

#[async_trait]
pub trait StageChunkStore {
    async fn ensure_ready(&self) -> Result<()>;
    async fn write_chunk(&self, request: StageChunkRequest) -> Result<StageChunk>;
    async fn read_chunk(&self, chunk: &StageChunk) -> Result<Vec<u8>>;
}

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

    pub fn bucket_root(&self) -> PathBuf {
        self.config.root_dir.join(&self.config.storage.bucket)
    }

    pub fn resolve_path(&self, object_key: &str) -> PathBuf {
        object_key
            .split('/')
            .filter(|segment| !segment.is_empty())
            .fold(self.bucket_root(), |path, segment| path.join(segment))
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
        let client = self.config.s3_client().await?;
        let bucket = &self.config.storage.bucket;

        match client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(_) => {
                client
                    .create_bucket()
                    .bucket(bucket)
                    .send()
                    .await
                    .with_context(|| format!("failed to create staging bucket {bucket}"))?;
                Ok(())
            }
        }
    }

    async fn write_chunk(&self, request: StageChunkRequest) -> Result<StageChunk> {
        self.ensure_ready().await?;
        let client = self.config.s3_client().await?;
        let chunk = request.to_chunk(&self.config.storage);

        client
            .put_object()
            .bucket(&chunk.bucket)
            .key(&chunk.object_key)
            .content_type(chunk.content_type.clone())
            .content_encoding(chunk.content_encoding.clone())
            .body(ByteStream::from(request.payload.bytes))
            .send()
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
        let client = self.config.s3_client().await?;
        let output = client
            .get_object()
            .bucket(&chunk.bucket)
            .key(&chunk.object_key)
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to read staged chunk from s3://{}/{} via {}",
                    chunk.bucket, chunk.object_key, self.config.endpoint
                )
            })?;

        let aggregated = output
            .body
            .collect()
            .await
            .context("failed to collect staged object body")?;
        Ok(aggregated.into_bytes().to_vec())
    }
}

pub fn build_chunk_key(
    pipeline_name: &str,
    stream_name: &str,
    partition_key: &str,
    sequence: u64,
) -> String {
    format!(
        "pipelines/{pipeline_name}/streams/{stream_name}/partitions/{partition_key}/chunks/{sequence:020}.jsonl.gz"
    )
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim().trim_matches('/').to_string()
}

fn ensure_chunk_belongs_to_bucket(expected_bucket: &str, chunk: &StageChunk) -> Result<()> {
    if chunk.bucket == expected_bucket {
        Ok(())
    } else {
        Err(anyhow!(
            "chunk bucket mismatch: expected {}, got {}",
            expected_bucket,
            chunk.bucket
        ))
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing {name}"))
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn status() -> &'static str {
    "runtime staging contract implemented"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let unique = format!("astra-runtime-{name}-{}", unix_time_ms());
        std::env::temp_dir().join(unique)
    }

    #[test]
    fn builds_predictable_chunk_key() {
        assert_eq!(
            build_chunk_key("postgres-analytics", "public.orders", "default", 42),
            "pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000042.jsonl.gz"
        );
    }

    #[test]
    fn prefixes_chunk_keys_without_double_slashes() {
        let config = StagingConfig {
            kind: StagingKind::Local,
            bucket: "astra-staging".to_string(),
            prefix: "/postgres-analytics//".to_string(),
        };

        assert_eq!(
            config.chunk_key("postgres-analytics", "public.orders", "default", 7),
            "postgres-analytics/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000007.jsonl.gz"
        );
    }

    #[tokio::test]
    async fn local_store_writes_and_reads_chunks() {
        let root_dir = temp_root("write-read");
        let store = LocalStageChunkStore::new(LocalStagingConfig {
            root_dir: root_dir.clone(),
            storage: StagingConfig {
                kind: StagingKind::Local,
                bucket: "astra-staging".to_string(),
                prefix: "dev".to_string(),
            },
        });

        let request = StageChunkRequest {
            pipeline_name: "postgres-analytics".to_string(),
            stream_name: "public.orders".to_string(),
            partition_key: "default".to_string(),
            sequence: 42,
            payload: StageChunkPayload::jsonl_gzip(2, b"pretend-gzip-jsonl".to_vec()),
        };

        let chunk = store.write_chunk(request).await.expect("chunk writes");
        assert_eq!(chunk.bucket, "astra-staging");
        assert_eq!(chunk.row_count, 2);
        assert_eq!(chunk.bytes_written, 18);
        assert_eq!(
            chunk.object_key,
            "dev/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000042.jsonl.gz"
        );

        let resolved = store.resolve_path(&chunk.object_key);
        assert!(
            resolved.exists(),
            "expected staged file at {}",
            resolved.display()
        );

        let bytes = store.read_chunk(&chunk).await.expect("chunk reads");
        assert_eq!(bytes, b"pretend-gzip-jsonl");

        fs::remove_dir_all(root_dir).ok();
    }

    #[tokio::test]
    async fn local_store_rejects_bucket_mismatch() {
        let root_dir = temp_root("bucket-mismatch");
        let store = LocalStageChunkStore::new(LocalStagingConfig {
            root_dir: root_dir.clone(),
            storage: StagingConfig {
                kind: StagingKind::Local,
                bucket: "astra-staging".to_string(),
                prefix: String::new(),
            },
        });

        store.ensure_ready().await.expect("store initializes");
        let chunk = StageChunk {
            pipeline_name: "postgres-analytics".to_string(),
            stream_name: "public.orders".to_string(),
            partition_key: "default".to_string(),
            sequence: 1,
            bucket: "wrong-bucket".to_string(),
            object_key: build_chunk_key("postgres-analytics", "public.orders", "default", 1),
            bytes_written: 0,
            row_count: 0,
            content_type: STAGING_CONTENT_TYPE_JSONL.to_string(),
            content_encoding: STAGING_CONTENT_ENCODING_GZIP.to_string(),
            schema_fingerprint: None,
            created_at_unix_ms: unix_time_ms(),
        };

        let error = store
            .read_chunk(&chunk)
            .await
            .expect_err("bucket mismatch should fail");
        assert!(error.to_string().contains("chunk bucket mismatch"));

        fs::remove_dir_all(root_dir).ok();
    }

    #[test]
    fn minio_config_reads_local_first_env() {
        let storage = StagingConfig {
            kind: StagingKind::Minio,
            bucket: "astra-staging".to_string(),
            prefix: "dev".to_string(),
        };
        std::env::set_var("ASTRA_S3_ENDPOINT", "http://127.0.0.1:9000");
        std::env::set_var("ASTRA_S3_ACCESS_KEY", "astra");
        std::env::set_var("ASTRA_S3_SECRET_KEY", "astrastorage");
        std::env::remove_var("ASTRA_S3_REGION");
        std::env::remove_var("AWS_REGION");

        let config = MinioStagingConfig::from_env(storage).expect("env config loads");
        assert_eq!(config.endpoint, "http://127.0.0.1:9000");
        assert_eq!(config.region, DEFAULT_AWS_REGION);
        assert_eq!(config.access_key, "astra");
        assert_eq!(config.secret_key, "astrastorage");
    }
}
