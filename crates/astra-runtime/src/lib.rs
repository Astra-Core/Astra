use serde::{Deserialize, Serialize};

pub const CRATE_NAME: &str = "astra-runtime";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StagingKind {
    S3,
    Minio,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageChunk {
    pub pipeline_name: String,
    pub stream_name: String,
    pub partition_key: String,
    pub sequence: u64,
    pub object_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagingConfig {
    pub kind: StagingKind,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkCommit {
    pub destination_kind: String,
    pub commit_token: String,
    pub rows_written: u64,
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

pub fn status() -> &'static str {
    "runtime staging contract defined"
}
