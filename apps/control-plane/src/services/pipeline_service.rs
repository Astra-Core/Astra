use crate::{
    models::api::{ApplySpecResponse, PipelineSummaryResponse},
    repositories::PipelineRepository,
};
use std::sync::Arc;

pub struct PipelineService {
    repository: Arc<dyn PipelineRepository>,
}

impl PipelineService {
    pub fn new(repository: Arc<dyn PipelineRepository>) -> Self {
        Self { repository }
    }

    pub async fn list_pipelines(&self) -> anyhow::Result<Vec<PipelineSummaryResponse>> {
        let pipelines = self.repository.list_pipelines().await?;
        Ok(pipelines
            .into_iter()
            .map(|p| PipelineSummaryResponse {
                name: p.name,
                source_kind: p.source_kind,
                destination_kind: p.destination_kind,
                status: p.status,
                spec_version: p.spec_version,
            })
            .collect())
    }

    pub async fn apply_spec(
        &self,
        yaml: String,
        created_by: Option<String>,
    ) -> anyhow::Result<ApplySpecResponse> {
        let spec = astra_yaml::AstraSpec::parse_yaml(&yaml)?;
        spec.validate()?;
        let applied = self.repository.apply_spec(spec, yaml, created_by).await?;
        Ok(ApplySpecResponse {
            pipeline_name: applied.pipeline.name,
            spec_version: applied.pipeline.spec_version,
            content_hash: applied.content_hash,
            message: "pipeline spec applied in memory".to_string(),
        })
    }
}
