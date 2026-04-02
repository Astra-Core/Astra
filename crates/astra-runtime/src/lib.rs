use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{aws::{AmazonS3, AmazonS3Builder}, path::Path as ObjectPath, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
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

    fn object_store(&self, bucket: &str) -> Result<AmazonS3> {
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

    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        let url = format!("{}/{}", self.endpoint.trim_end_matches('/'), bucket);
        let resp = reqwest::Client::new()
            .put(&url)
            .header("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .send()
            .await
            .with_context(|| format!("failed to reach MinIO at {url}"))?;
        // 200 = created, 409 = already exists — both are fine
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

    pub fn list_chunks_for_pipeline(&self, pipeline_name: &str) -> Result<Vec<StageChunk>> {
        let mut chunks = Vec::new();
        let prefix = self.config.storage.normalized_prefix();
        let base = if prefix.is_empty() {
            self.bucket_root()
        } else {
            self.bucket_root().join(&prefix)
        }
        .join("pipelines")
        .join(pipeline_name)
        .join("streams");

        if !base.exists() {
            return Ok(chunks);
        }

        let stream_dirs = fs::read_dir(&base)
            .with_context(|| format!("failed to list staged streams under {}", base.display()))?;
        for stream_dir in stream_dirs {
            let stream_dir = stream_dir?;
            if !stream_dir.file_type()?.is_dir() {
                continue;
            }
            let stream_name = stream_dir.file_name().to_string_lossy().to_string();
            let chunks_dir = stream_dir
                .path()
                .join("partitions")
                .join("default")
                .join("chunks");
            if !chunks_dir.exists() {
                continue;
            }
            for entry in fs::read_dir(&chunks_dir).with_context(|| {
                format!(
                    "failed to list staged chunks under {}",
                    chunks_dir.display()
                )
            })? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let file_name = entry.file_name().to_string_lossy().to_string();
                if !file_name.ends_with(".jsonl.gz") {
                    continue;
                }
                let sequence = parse_sequence(&file_name)?;
                let object_key =
                    self.config
                        .storage
                        .chunk_key(pipeline_name, &stream_name, "default", sequence);
                let bytes_written = entry.metadata()?.len();
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
        let result = store
            .get(&path)
            .await
            .with_context(|| {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnapshotCheckpointLedger {
    pub pipeline_name: String,
    pub updated_at_unix_ms: u64,
    #[serde(default)]
    pub tables: BTreeMap<String, SnapshotTableCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnapshotTableCheckpoint {
    pub next_sequence: u64,
    pub rows_staged: u64,
    pub last_chunk_key: Option<String>,
    pub completed: bool,
    pub updated_at_unix_ms: u64,
}

#[derive(Debug, Clone)]
pub struct LocalCheckpointStore {
    root_dir: PathBuf,
}

impl LocalCheckpointStore {
    pub fn new(root_dir: PathBuf) -> Self {
        Self { root_dir }
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn ensure_ready(&self) -> Result<()> {
        fs::create_dir_all(&self.root_dir).with_context(|| {
            format!(
                "failed to create local checkpoint root at {}",
                self.root_dir.display()
            )
        })
    }

    pub fn ledger_path(&self, pipeline_name: &str) -> PathBuf {
        self.root_dir.join(format!(
            "{}.snapshot-checkpoints.json",
            sanitize_name(pipeline_name)
        ))
    }

    pub fn load(&self, pipeline_name: &str) -> Result<SnapshotCheckpointLedger> {
        self.ensure_ready()?;
        let path = self.ledger_path(pipeline_name);
        if !path.exists() {
            return Ok(SnapshotCheckpointLedger {
                pipeline_name: pipeline_name.to_string(),
                updated_at_unix_ms: unix_time_ms(),
                tables: BTreeMap::new(),
            });
        }

        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read checkpoint ledger at {}", path.display()))?;
        let mut ledger: SnapshotCheckpointLedger = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse checkpoint ledger at {}", path.display()))?;
        if ledger.pipeline_name.is_empty() {
            ledger.pipeline_name = pipeline_name.to_string();
        }
        Ok(ledger)
    }

    pub fn save(&self, ledger: &SnapshotCheckpointLedger) -> Result<()> {
        self.ensure_ready()?;
        let path = self.ledger_path(&ledger.pipeline_name);
        let payload =
            serde_json::to_vec_pretty(ledger).context("failed to encode checkpoint ledger")?;
        let mut file = fs::File::create(&path)
            .with_context(|| format!("failed to create checkpoint ledger at {}", path.display()))?;
        file.write_all(&payload)
            .with_context(|| format!("failed to write checkpoint ledger at {}", path.display()))?;
        file.write_all(b"\n").with_context(|| {
            format!("failed to finalize checkpoint ledger at {}", path.display())
        })?;
        file.sync_all()
            .with_context(|| format!("failed to flush checkpoint ledger at {}", path.display()))
    }

    pub fn record_chunk_staged(
        &self,
        pipeline_name: &str,
        table_name: &str,
        sequence: u64,
        row_count: u64,
        chunk_key: &str,
    ) -> Result<SnapshotCheckpointLedger> {
        let mut ledger = self.load(pipeline_name)?;
        let now = unix_time_ms();
        let checkpoint = ledger.tables.entry(table_name.to_string()).or_default();
        checkpoint.next_sequence = sequence + 1;
        checkpoint.rows_staged = checkpoint.rows_staged.saturating_add(row_count);
        checkpoint.last_chunk_key = Some(chunk_key.to_string());
        checkpoint.completed = false;
        checkpoint.updated_at_unix_ms = now;
        ledger.updated_at_unix_ms = now;
        self.save(&ledger)?;
        Ok(ledger)
    }

    pub fn mark_table_complete(
        &self,
        pipeline_name: &str,
        table_name: &str,
    ) -> Result<SnapshotCheckpointLedger> {
        let mut ledger = self.load(pipeline_name)?;
        let now = unix_time_ms();
        let checkpoint = ledger.tables.entry(table_name.to_string()).or_default();
        checkpoint.completed = true;
        checkpoint.updated_at_unix_ms = now;
        ledger.updated_at_unix_ms = now;
        self.save(&ledger)?;
        Ok(ledger)
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

fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '-',
        })
        .collect()
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

fn parse_sequence(file_name: &str) -> Result<u64> {
    let trimmed = file_name.trim_end_matches(".jsonl.gz");
    trimmed
        .parse::<u64>()
        .with_context(|| format!("invalid staged chunk sequence in file name {file_name}"))
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
    async fn local_store_lists_chunks_for_pipeline() {
        let root_dir = temp_root("list");
        let store = LocalStageChunkStore::new(LocalStagingConfig {
            root_dir: root_dir.clone(),
            storage: StagingConfig {
                kind: StagingKind::Local,
                bucket: "astra-staging".to_string(),
                prefix: "dev".to_string(),
            },
        });

        store
            .write_chunk(StageChunkRequest {
                pipeline_name: "postgres-analytics".to_string(),
                stream_name: "public.orders".to_string(),
                partition_key: "default".to_string(),
                sequence: 2,
                payload: StageChunkPayload::jsonl_gzip(1, vec![1, 2, 3]),
            })
            .await
            .unwrap();
        store
            .write_chunk(StageChunkRequest {
                pipeline_name: "postgres-analytics".to_string(),
                stream_name: "public.users".to_string(),
                partition_key: "default".to_string(),
                sequence: 1,
                payload: StageChunkPayload::jsonl_gzip(1, vec![4, 5, 6]),
            })
            .await
            .unwrap();

        let chunks = store
            .list_chunks_for_pipeline("postgres-analytics")
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].stream_name, "public.orders");
        assert_eq!(chunks[0].sequence, 2);
        assert_eq!(chunks[1].stream_name, "public.users");

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

    #[test]
    fn checkpoint_store_persists_resume_state() {
        let root_dir = temp_root("checkpoint-store");
        let store = LocalCheckpointStore::new(root_dir.clone());

        store
            .record_chunk_staged(
                "postgres-analytics",
                "public.orders",
                0,
                500,
                "dev/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000000.jsonl.gz",
            )
            .expect("checkpoint writes");
        store
            .mark_table_complete("postgres-analytics", "public.orders")
            .expect("completion writes");

        let ledger = store.load("postgres-analytics").expect("ledger loads");
        let checkpoint = ledger.tables.get("public.orders").expect("table exists");
        assert_eq!(checkpoint.next_sequence, 1);
        assert_eq!(checkpoint.rows_staged, 500);
        assert!(checkpoint.completed);
        assert_eq!(
            checkpoint.last_chunk_key.as_deref(),
            Some("dev/pipelines/postgres-analytics/streams/public.orders/partitions/default/chunks/00000000000000000000.jsonl.gz")
        );

        fs::remove_dir_all(root_dir).ok();
    }
}
