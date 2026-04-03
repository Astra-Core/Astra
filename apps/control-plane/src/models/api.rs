use crate::repositories::{
    PipelineRecord, PipelineRunRecord, StagedArtifactRecord, TableExecutionRecord,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct PipelineSummaryResponse {
    pub name: String,
    pub source_kind: String,
    pub destination_kind: String,
    pub status: String,
    pub spec_version: i32,
}

impl From<PipelineRecord> for PipelineSummaryResponse {
    fn from(record: PipelineRecord) -> Self {
        Self {
            name: record.name,
            source_kind: record.source_kind,
            destination_kind: record.destination_kind,
            status: record.status.to_string(),
            spec_version: record.spec_version,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PipelinesResponse {
    pub pipelines: Vec<PipelineSummaryResponse>,
}

#[derive(Debug, Deserialize)]
pub struct ApplySpecRequest {
    pub yaml: String,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApplySpecResponse {
    pub pipeline_name: String,
    pub spec_version: i32,
    pub content_hash: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct CreatePipelineRunRequest {
    pub pipeline_name: String,
    pub trigger_mode: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct PipelineRunResponse {
    pub id: Uuid,
    pub pipeline_name: String,
    pub trigger_mode: String,
    pub status: String,
    pub worker_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub stats_json: Option<serde_json::Value>,
}

impl From<PipelineRunRecord> for PipelineRunResponse {
    fn from(record: PipelineRunRecord) -> Self {
        Self {
            id: record.id,
            pipeline_name: record.pipeline_name,
            trigger_mode: record.trigger_mode,
            status: record.status,
            worker_id: record.worker_id,
            started_at: record.started_at,
            finished_at: record.finished_at,
            created_at: record.created_at,
            updated_at: record.updated_at,
            stats_json: record.stats_json,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PipelineRunsResponse {
    pub runs: Vec<PipelineRunResponse>,
}

#[derive(Debug, Deserialize)]
pub struct RecordStagedArtifactRequest {
    pub stream_name: String,
    pub partition_key: String,
    pub sequence: i64,
    pub bucket: String,
    pub object_key: String,
    pub bytes_written: i64,
    pub row_count: i64,
    pub content_type: String,
    pub content_encoding: String,
    #[serde(default)]
    pub schema_fingerprint: Option<String>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct StagedArtifactResponse {
    pub id: Uuid,
    pub pipeline_run_id: Uuid,
    pub stream_name: String,
    pub partition_key: String,
    pub sequence: i64,
    pub bucket: String,
    pub object_key: String,
    pub bytes_written: i64,
    pub row_count: i64,
    pub content_type: String,
    pub content_encoding: String,
    pub schema_fingerprint: Option<String>,
    pub metadata_json: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl From<StagedArtifactRecord> for StagedArtifactResponse {
    fn from(record: StagedArtifactRecord) -> Self {
        Self {
            id: record.id,
            pipeline_run_id: record.pipeline_run_id,
            stream_name: record.stream_name,
            partition_key: record.partition_key,
            sequence: record.sequence,
            bucket: record.bucket,
            object_key: record.object_key,
            bytes_written: record.bytes_written,
            row_count: record.row_count,
            content_type: record.content_type,
            content_encoding: record.content_encoding,
            schema_fingerprint: record.schema_fingerprint,
            metadata_json: record.metadata_json,
            created_at: record.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StagedArtifactsResponse {
    pub artifacts: Vec<StagedArtifactResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Progress {
    pub current: u32,
    pub total: u32,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePipelineRunStatusRequest {
    pub status: String,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub progress: Option<Progress>,
    #[serde(default)]
    pub stats_json: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct TableExecutionResponse {
    pub id: Uuid,
    pub stream_name: String,
    pub status: String,
    pub rows_processed: i64,
    pub rows_total: Option<i64>,
    pub error_summary: Option<String>,
    pub checkpoint_next_sequence: Option<i64>,
    pub checkpoint_rows_staged: Option<i64>,
    pub checkpoint_last_chunk_key: Option<String>,
    pub checkpoint_completed: bool,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl From<TableExecutionRecord> for TableExecutionResponse {
    fn from(record: TableExecutionRecord) -> Self {
        Self {
            id: record.id,
            stream_name: record.stream_name,
            status: record.status,
            rows_processed: record.rows_processed,
            rows_total: record.rows_total,
            error_summary: record.error_summary,
            checkpoint_next_sequence: record.checkpoint_next_sequence,
            checkpoint_rows_staged: record.checkpoint_rows_staged,
            checkpoint_last_chunk_key: record.checkpoint_last_chunk_key,
            checkpoint_completed: record.checkpoint_completed,
            started_at: record.started_at,
            finished_at: record.finished_at,
            updated_at: record.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TableExecutionsResponse {
    pub tables: Vec<TableExecutionResponse>,
}

#[derive(Debug, Serialize)]
pub struct TriggerPipelineResponse {
    pub run_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct UpsertTableExecutionRequest {
    pub stream_name: String,
    pub status: String,
    pub rows_processed: i64,
    pub rows_total: Option<i64>,
    pub error_summary: Option<String>,
    #[serde(default)]
    pub checkpoint_next_sequence: Option<i64>,
    #[serde(default)]
    pub checkpoint_rows_staged: Option<i64>,
    #[serde(default)]
    pub checkpoint_last_chunk_key: Option<String>,
    #[serde(default)]
    pub checkpoint_completed: Option<bool>,
}
