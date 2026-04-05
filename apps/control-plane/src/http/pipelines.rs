use crate::{
    error::{AppError, NotFoundError},
    models::api::{
        ApplySpecRequest, CreatePipelineRunRequest, PipelineRunsResponse, PipelinesResponse,
        RecordStagedArtifactRequest, StagedArtifactsResponse, TableExecutionResponse,
        TableExecutionsResponse, TriggerPipelineResponse, UpsertTableExecutionRequest,
    },
    repositories::RecordStagedArtifactRecord,
    state::AppState,
};
use astra_metadata::PipelineStatus;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use uuid::Uuid;

pub async fn list_pipelines(
    State(state): State<AppState>,
) -> Result<Json<PipelinesResponse>, AppError> {
    let pipelines = state.pipeline_service.list_pipelines().await?;
    Ok(Json(PipelinesResponse { pipelines }))
}

pub async fn apply_spec(
    State(state): State<AppState>,
    Json(request): Json<ApplySpecRequest>,
) -> Result<Json<crate::models::api::ApplySpecResponse>, AppError> {
    let response = state
        .pipeline_service
        .apply_spec(request.yaml, request.created_by)
        .await?;
    Ok(Json(response))
}

pub async fn create_pipeline_run(
    State(state): State<AppState>,
    Json(request): Json<CreatePipelineRunRequest>,
) -> Result<Json<crate::models::api::PipelineRunResponse>, AppError> {
    let response = state
        .pipeline_service
        .create_pipeline_run(
            request.pipeline_name,
            request.trigger_mode,
            request.status,
            request.worker_id,
            request.started_at,
        )
        .await?;
    Ok(Json(response))
}

pub async fn list_pipeline_runs(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<PipelineRunsResponse>, AppError> {
    let runs = state
        .pipeline_service
        .list_pipeline_runs(&pipeline_name)
        .await?;
    Ok(Json(PipelineRunsResponse { runs }))
}

pub async fn get_latest_run(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<Option<crate::models::api::PipelineRunResponse>>, AppError> {
    let run = state
        .pipeline_service
        .get_latest_run(&pipeline_name)
        .await?;
    Ok(Json(run))
}

pub async fn get_pipeline_yaml(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<Option<String>>, AppError> {
    let yaml = state
        .pipeline_service
        .get_pipeline_yaml(&pipeline_name)
        .await?;
    Ok(Json(yaml))
}

pub async fn get_run_history(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<PipelineRunsResponse>, AppError> {
    // Default limit of 10 runs if not specified
    let runs = state
        .pipeline_service
        .get_run_history(&pipeline_name, 10)
        .await?;
    Ok(Json(PipelineRunsResponse { runs }))
}

pub async fn record_staged_artifact(
    State(state): State<AppState>,
    Path(pipeline_run_id): Path<Uuid>,
    Json(request): Json<RecordStagedArtifactRequest>,
) -> Result<Json<crate::models::api::StagedArtifactResponse>, AppError> {
    let response = state
        .pipeline_service
        .record_staged_artifact(RecordStagedArtifactRecord {
            pipeline_run_id,
            stream_name: request.stream_name,
            partition_key: request.partition_key,
            sequence: request.sequence,
            bucket: request.bucket,
            object_key: request.object_key,
            bytes_written: request.bytes_written,
            row_count: request.row_count,
            content_type: request.content_type,
            content_encoding: request.content_encoding,
            schema_fingerprint: request.schema_fingerprint,
            metadata_json: request
                .metadata_json
                .unwrap_or_else(|| serde_json::json!({})),
        })
        .await?;
    Ok(Json(response))
}

pub async fn update_pipeline_run_status(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Json(request): Json<crate::models::api::UpdatePipelineRunStatusRequest>,
) -> Result<Json<crate::models::api::PipelineRunResponse>, AppError> {
    let updated_run = state
        .pipeline_service
        .update_pipeline_run_status(
            run_id,
            request.status.clone(),
            request.phase.clone(),
            request.progress.clone(),
            request.stats_json.clone(),
        )
        .await?;
    Ok(Json(updated_run))
}

pub async fn list_staged_artifacts(
    State(state): State<AppState>,
    Path(pipeline_run_id): Path<Uuid>,
) -> Result<Json<StagedArtifactsResponse>, AppError> {
    let artifacts = state
        .pipeline_service
        .list_staged_artifacts(pipeline_run_id)
        .await?;
    Ok(Json(StagedArtifactsResponse { artifacts }))
}

pub async fn list_table_executions(
    State(state): State<AppState>,
    Path(pipeline_run_id): Path<Uuid>,
) -> Result<Json<TableExecutionsResponse>, AppError> {
    let tables = state
        .pipeline_service
        .list_table_executions(pipeline_run_id)
        .await?;
    Ok(Json(TableExecutionsResponse { tables }))
}

pub async fn upsert_table_execution(
    State(state): State<AppState>,
    Path(pipeline_run_id): Path<Uuid>,
    Json(request): Json<UpsertTableExecutionRequest>,
) -> Result<Json<TableExecutionResponse>, AppError> {
    let response = state
        .pipeline_service
        .upsert_table_execution(pipeline_run_id, request)
        .await?;
    Ok(Json(response))
}

pub async fn delete_pipeline(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<StatusCode, AppError> {
    state
        .pipeline_service
        .delete_pipeline(&pipeline_name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn disable_pipeline(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<crate::models::api::PipelineSummaryResponse>, AppError> {
    let pipeline = state
        .pipeline_service
        .update_pipeline_status(&pipeline_name, PipelineStatus::Disabled)
        .await?;
    Ok(Json(pipeline))
}

pub async fn enable_pipeline(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<crate::models::api::PipelineSummaryResponse>, AppError> {
    let pipeline = state
        .pipeline_service
        .update_pipeline_status(&pipeline_name, PipelineStatus::Active)
        .await?;
    Ok(Json(pipeline))
}

pub async fn trigger_pipeline(
    State(state): State<AppState>,
    Path(pipeline_name): Path<String>,
) -> Result<Json<TriggerPipelineResponse>, AppError> {
    let yaml = state
        .pipeline_service
        .get_pipeline_yaml(&pipeline_name)
        .await?
        .ok_or_else(|| AppError::NotFound(NotFoundError(pipeline_name.clone())))?;

    let run = state
        .pipeline_service
        .create_pipeline_run(
            pipeline_name,
            "ui-trigger".to_string(),
            Some("running".to_string()),
            None,
            None,
        )
        .await?;

    let run_id = run.id;
    let service = Arc::clone(&state.pipeline_service);
    tokio::spawn(async move {
        crate::executor::execute_pipeline(service, run_id, yaml).await;
    });

    Ok(Json(TriggerPipelineResponse { run_id }))
}
