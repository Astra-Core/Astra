use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PipelineRecord {
    pub name: String,
    pub source_kind: String,
    pub destination_kind: String,
    pub status: String,
    pub spec_version: i32,
}

#[derive(Debug, Clone)]
pub struct AppliedPipelineRecord {
    pub pipeline: PipelineRecord,
    pub content_hash: String,
}

#[derive(Debug, Clone)]
pub struct CreatePipelineRunRecord {
    pub pipeline_name: String,
    pub trigger_mode: String,
    pub status: String,
    pub worker_id: Option<String>,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PipelineRunRecord {
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

#[derive(Debug, Clone)]
pub struct RecordStagedArtifactRecord {
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
    pub metadata_json: Value,
}

#[derive(Debug, Clone)]
pub struct StagedArtifactRecord {
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
    pub metadata_json: Value,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait PipelineRepository: Send + Sync {
    async fn get_pipeline_yaml(&self, pipeline_name: &str) -> anyhow::Result<Option<String>>;
    async fn update_pipeline_run_status(
        &self,
        run_id: Uuid,
        status: String,
        stats_json: serde_json::Value,
    ) -> anyhow::Result<PipelineRunRecord>;
    async fn list_pipelines(&self) -> anyhow::Result<Vec<PipelineRecord>>;
    async fn apply_spec(
        &self,
        spec: astra_yaml::AstraSpec,
        raw_yaml: String,
        created_by: Option<String>,
    ) -> anyhow::Result<AppliedPipelineRecord>;
    async fn create_pipeline_run(
        &self,
        run: CreatePipelineRunRecord,
    ) -> anyhow::Result<PipelineRunRecord>;
    async fn list_pipeline_runs(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Vec<PipelineRunRecord>>;
    async fn get_latest_run(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Option<PipelineRunRecord>>;
    async fn get_run_history(
        &self,
        pipeline_name: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<PipelineRunRecord>>;
    async fn record_staged_artifact(
        &self,
        artifact: RecordStagedArtifactRecord,
    ) -> anyhow::Result<StagedArtifactRecord>;
    async fn list_staged_artifacts(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<StagedArtifactRecord>>;
}
