use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const CRATE_NAME: &str = "astra-metadata";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStatus {
    Draft,
    Active,
    Disabled,
    Paused,
    Failed,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Snapshot,
    Cdc,
    Backfill,
    Validate,
    Discover,
    Apply,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TriggerMode {
    Schedule,
    Manual,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Planning,
    Snapshot,
    Cdc,
    StageFlush,
    SinkApply,
    Finalize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointPhase {
    Snapshot,
    Cdc,
    SinkApply,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeType {
    Additive,
    Breaking,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaResolutionStatus {
    Detected,
    AutoApplied,
    Paused,
    Ignored,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SecretProvider {
    Env,
    File,
    Vault,
    AwsSecretsManager,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub config_json: Value,
    pub secret_refs_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Destination {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub config_json: Value,
    pub secret_refs_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: Uuid,
    pub name: String,
    pub source_id: Uuid,
    pub destination_id: Uuid,
    pub status: PipelineStatus,
    pub active_spec_id: Option<Uuid>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub version: i32,
    pub spec_version: String,
    pub spec_yaml: String,
    pub spec_json: Value,
    pub content_hash: String,
    pub created_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
    pub superseded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub kind: JobKind,
    pub trigger_mode: TriggerMode,
    pub status: JobStatus,
    pub requested_at: DateTime<Utc>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub requested_by: Option<String>,
    pub priority: Option<i32>,
    pub run_count: i32,
    pub last_error_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt: i32,
    pub worker_id: Option<String>,
    pub phase: RunPhase,
    pub status: JobStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stats_json: Value,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub logs_pointer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub job_run_id: Option<Uuid>,
    pub stream_name: String,
    pub phase: CheckpointPhase,
    pub cursor_json: Value,
    pub snapshot_chunk_key: Option<String>,
    pub lsn_or_offset: Option<String>,
    pub sink_commit_token: Option<String>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvent {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub stream_name: String,
    pub change_type: SchemaChangeType,
    pub detected_at: DateTime<Utc>,
    pub source_schema_json: Value,
    pub destination_schema_json: Option<Value>,
    pub resolution_status: SchemaResolutionStatus,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretRef {
    pub id: Uuid,
    pub name: String,
    pub provider: SecretProvider,
    pub reference: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub fn status() -> &'static str {
    "metadata models defined"
}
