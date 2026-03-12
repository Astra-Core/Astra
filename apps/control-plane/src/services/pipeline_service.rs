use crate::{
    models::api::{
        ApplySpecResponse, PipelineRunResponse, PipelineSummaryResponse, StagedArtifactResponse,
    },
    repositories::{CreatePipelineRunRecord, PipelineRepository, RecordStagedArtifactRecord},
};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

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
            message: "pipeline spec applied".to_string(),
        })
    }

    pub async fn create_pipeline_run(
        &self,
        pipeline_name: String,
        trigger_mode: String,
        status: Option<String>,
        worker_id: Option<String>,
        started_at: Option<chrono::DateTime<Utc>>,
    ) -> anyhow::Result<PipelineRunResponse> {
        let run = self
            .repository
            .create_pipeline_run(CreatePipelineRunRecord {
                pipeline_name,
                trigger_mode,
                status: status.unwrap_or_else(|| "running".to_string()),
                worker_id,
                started_at: started_at.unwrap_or_else(Utc::now),
            })
            .await?;
        Ok(PipelineRunResponse {
            id: run.id,
            pipeline_name: run.pipeline_name,
            trigger_mode: run.trigger_mode,
            status: run.status,
            worker_id: run.worker_id,
            started_at: run.started_at,
            finished_at: run.finished_at,
            created_at: run.created_at,
            updated_at: run.updated_at,
        })
    }

    pub async fn list_pipeline_runs(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Vec<PipelineRunResponse>> {
        let runs = self.repository.list_pipeline_runs(pipeline_name).await?;
        Ok(runs
            .into_iter()
            .map(|run| PipelineRunResponse {
                id: run.id,
                pipeline_name: run.pipeline_name,
                trigger_mode: run.trigger_mode,
                status: run.status,
                worker_id: run.worker_id,
                started_at: run.started_at,
                finished_at: run.finished_at,
                created_at: run.created_at,
                updated_at: run.updated_at,
            })
            .collect())
    }

    pub async fn record_staged_artifact(
        &self,
        pipeline_run_id: Uuid,
        stream_name: String,
        partition_key: String,
        sequence: i64,
        bucket: String,
        object_key: String,
        bytes_written: i64,
        row_count: i64,
        content_type: String,
        content_encoding: String,
        schema_fingerprint: Option<String>,
        metadata_json: Option<serde_json::Value>,
    ) -> anyhow::Result<StagedArtifactResponse> {
        let artifact = self
            .repository
            .record_staged_artifact(RecordStagedArtifactRecord {
                pipeline_run_id,
                stream_name,
                partition_key,
                sequence,
                bucket,
                object_key,
                bytes_written,
                row_count,
                content_type,
                content_encoding,
                schema_fingerprint,
                metadata_json: metadata_json.unwrap_or_else(|| serde_json::json!({})),
            })
            .await?;
        Ok(StagedArtifactResponse {
            id: artifact.id,
            pipeline_run_id: artifact.pipeline_run_id,
            stream_name: artifact.stream_name,
            partition_key: artifact.partition_key,
            sequence: artifact.sequence,
            bucket: artifact.bucket,
            object_key: artifact.object_key,
            bytes_written: artifact.bytes_written,
            row_count: artifact.row_count,
            content_type: artifact.content_type,
            content_encoding: artifact.content_encoding,
            schema_fingerprint: artifact.schema_fingerprint,
            metadata_json: artifact.metadata_json,
            created_at: artifact.created_at,
        })
    }

    pub async fn list_staged_artifacts(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<StagedArtifactResponse>> {
        let artifacts = self
            .repository
            .list_staged_artifacts(pipeline_run_id)
            .await?;
        Ok(artifacts
            .into_iter()
            .map(|artifact| StagedArtifactResponse {
                id: artifact.id,
                pipeline_run_id: artifact.pipeline_run_id,
                stream_name: artifact.stream_name,
                partition_key: artifact.partition_key,
                sequence: artifact.sequence,
                bucket: artifact.bucket,
                object_key: artifact.object_key,
                bytes_written: artifact.bytes_written,
                row_count: artifact.row_count,
                content_type: artifact.content_type,
                content_encoding: artifact.content_encoding,
                schema_fingerprint: artifact.schema_fingerprint,
                metadata_json: artifact.metadata_json,
                created_at: artifact.created_at,
            })
            .collect())
    }
}
