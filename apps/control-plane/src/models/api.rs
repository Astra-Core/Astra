use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct PipelineSummaryResponse {
    pub name: String,
    pub source_kind: String,
    pub destination_kind: String,
    pub status: String,
    pub spec_version: i32,
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
