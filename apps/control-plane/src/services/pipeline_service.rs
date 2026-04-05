use crate::{
    error::BadRequestError,
    models::api::{
        ApplySpecResponse, PipelineRunResponse, PipelineSummaryResponse, Progress,
        StagedArtifactResponse, TableExecutionResponse, UpsertTableExecutionRequest,
    },
    repositories::{
        ApplySpecRecord, CreatePipelineRunRecord, PipelineRepository, RecordStagedArtifactRecord,
        UpsertTableExecutionRecord,
    },
};
use astra_metadata::PipelineStatus;
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
        Ok(pipelines.into_iter().map(Into::into).collect())
    }

    pub async fn apply_spec(
        &self,
        yaml: String,
        created_by: Option<String>,
    ) -> anyhow::Result<ApplySpecResponse> {
        if yaml.trim().is_empty() {
            anyhow::bail!(BadRequestError("yaml must not be empty".to_string()));
        }
        let spec = astra_yaml::AstraSpec::parse_yaml(&yaml)?;
        spec.validate()?;
        if spec.requests_cdc() {
            anyhow::bail!(
                "spec apply does not execute CDC yet. Astra's Postgres source is currently limited to schema discovery and snapshot staging; remove pipeline.mode=cdc and source.capture.cdc before applying this spec."
            );
        }
        let spec_json = serde_json::to_value(&spec)
            .map_err(|e| anyhow::anyhow!("failed to serialize spec to JSON: {}", e))?;
        let record = ApplySpecRecord {
            name: spec.pipeline.name.clone(),
            source_kind: spec.source.kind.clone(),
            destination_kind: spec.destination.kind.clone(),
            mode: serde_json::to_value(&spec.pipeline.mode)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| format!("{:?}", spec.pipeline.mode).to_lowercase()),
            spec_version: spec.version.clone(),
            spec_json,
            raw_yaml: yaml,
            created_by,
        };
        let applied = self.repository.apply_spec(record).await?;
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
        if pipeline_name.trim().is_empty() {
            anyhow::bail!(BadRequestError(
                "pipeline_name must not be empty".to_string()
            ));
        }
        if trigger_mode.trim().is_empty() {
            anyhow::bail!(BadRequestError(
                "trigger_mode must not be empty".to_string()
            ));
        }
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
        Ok(run.into())
    }

    pub async fn list_pipeline_runs(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Vec<PipelineRunResponse>> {
        let runs = self.repository.list_pipeline_runs(pipeline_name).await?;
        Ok(runs.into_iter().map(Into::into).collect())
    }

    pub async fn get_latest_run(
        &self,
        pipeline_name: &str,
    ) -> anyhow::Result<Option<PipelineRunResponse>> {
        let run = self.repository.get_latest_run(pipeline_name).await?;
        Ok(run.map(Into::into))
    }

    pub async fn get_run_history(
        &self,
        pipeline_name: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<PipelineRunResponse>> {
        let runs = self
            .repository
            .get_run_history(pipeline_name, limit)
            .await?;
        Ok(runs.into_iter().map(Into::into).collect())
    }

    pub async fn record_staged_artifact(
        &self,
        mut record: RecordStagedArtifactRecord,
    ) -> anyhow::Result<StagedArtifactResponse> {
        if record.stream_name.trim().is_empty() {
            anyhow::bail!(BadRequestError("stream_name must not be empty".to_string()));
        }
        if record.object_key.trim().is_empty() {
            anyhow::bail!(BadRequestError("object_key must not be empty".to_string()));
        }
        if record.metadata_json.is_null() {
            record.metadata_json = serde_json::json!({});
        }
        let artifact = self.repository.record_staged_artifact(record).await?;
        Ok(artifact.into())
    }

    pub async fn list_staged_artifacts(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<StagedArtifactResponse>> {
        let artifacts = self
            .repository
            .list_staged_artifacts(pipeline_run_id)
            .await?;
        Ok(artifacts.into_iter().map(Into::into).collect())
    }

    pub async fn list_table_executions(
        &self,
        pipeline_run_id: Uuid,
    ) -> anyhow::Result<Vec<TableExecutionResponse>> {
        let tables = self
            .repository
            .list_table_executions(pipeline_run_id)
            .await?;
        Ok(tables.into_iter().map(Into::into).collect())
    }

    pub async fn upsert_table_execution(
        &self,
        pipeline_run_id: Uuid,
        request: UpsertTableExecutionRequest,
    ) -> anyhow::Result<TableExecutionResponse> {
        if request.stream_name.trim().is_empty() {
            anyhow::bail!(BadRequestError("stream_name must not be empty".to_string()));
        }
        let record = UpsertTableExecutionRecord {
            pipeline_run_id,
            stream_name: request.stream_name,
            status: request.status,
            rows_processed: request.rows_processed,
            rows_total: request.rows_total,
            error_summary: request.error_summary,
            checkpoint_next_sequence: request.checkpoint_next_sequence,
            checkpoint_rows_staged: request.checkpoint_rows_staged,
            checkpoint_last_chunk_key: request.checkpoint_last_chunk_key,
            checkpoint_completed: request.checkpoint_completed,
        };
        let table = self.repository.upsert_table_execution(record).await?;
        Ok(table.into())
    }

    pub async fn get_pipeline_yaml(&self, pipeline_name: &str) -> anyhow::Result<Option<String>> {
        self.repository.get_pipeline_yaml(pipeline_name).await
    }

    pub async fn delete_pipeline(&self, pipeline_name: &str) -> anyhow::Result<()> {
        self.repository.delete_pipeline(pipeline_name).await
    }

    pub async fn update_pipeline_status(
        &self,
        pipeline_name: &str,
        status: PipelineStatus,
    ) -> anyhow::Result<PipelineSummaryResponse> {
        let record = self
            .repository
            .update_pipeline_status(pipeline_name, status)
            .await?;
        Ok(record.into())
    }

    pub async fn update_pipeline_run_status(
        &self,
        run_id: Uuid,
        status: String,
        phase: Option<String>,
        progress: Option<Progress>,
        stats_json: Option<serde_json::Value>,
    ) -> anyhow::Result<PipelineRunResponse> {
        let mut merged_stats = stats_json.unwrap_or_else(|| serde_json::json!({}));
        if let Some(p) = phase {
            merged_stats["phase"] = serde_json::Value::String(p);
        }
        if let Some(prog) = progress {
            merged_stats["progress"] =
                serde_json::json!({"current": prog.current, "total": prog.total});
        }
        let run = self
            .repository
            .update_pipeline_run_status(run_id, status, merged_stats)
            .await?;
        Ok(run.into())
    }
}
