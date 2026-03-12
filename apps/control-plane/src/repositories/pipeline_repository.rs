use async_trait::async_trait;

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

#[async_trait]
pub trait PipelineRepository: Send + Sync {
    async fn list_pipelines(&self) -> anyhow::Result<Vec<PipelineRecord>>;
    async fn apply_spec(
        &self,
        spec: astra_yaml::AstraSpec,
        raw_yaml: String,
        created_by: Option<String>,
    ) -> anyhow::Result<AppliedPipelineRecord>;
}
