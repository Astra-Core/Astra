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
    Paused,
    Failed,
    Archived,
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

pub fn status() -> &'static str {
    "metadata models defined"
}
