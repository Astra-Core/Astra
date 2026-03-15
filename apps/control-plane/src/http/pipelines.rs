use crate::{
    error::AppError,
    models::api::{
        ApplySpecRequest, CreatePipelineRunRequest, PipelineRunsResponse, PipelinesResponse,
        RecordStagedArtifactRequest, StagedArtifactsResponse,
    },
    state::AppState,
};
use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::Value;
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
    if request.yaml.trim().is_empty() {
        return Err(AppError::BadRequest("yaml must not be empty".to_string()));
    }
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
    if request.pipeline_name.trim().is_empty() {
        return Err(AppError::BadRequest(
            "pipeline_name must not be empty".to_string(),
        ));
    }
    if request.trigger_mode.trim().is_empty() {
        return Err(AppError::BadRequest(
            "trigger_mode must not be empty".to_string(),
        ));
    }

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
    if request.stream_name.trim().is_empty() {
        return Err(AppError::BadRequest(
            "stream_name must not be empty".to_string(),
        ));
    }
    if request.object_key.trim().is_empty() {
        return Err(AppError::BadRequest(
            "object_key must not be empty".to_string(),
        ));
    }

    let response = state
        .pipeline_service
        .record_staged_artifact(
            pipeline_run_id,
            request.stream_name,
            request.partition_key,
            request.sequence,
            request.bucket,
            request.object_key,
            request.bytes_written,
            request.row_count,
            request.content_type,
            request.content_encoding,
            request.schema_fingerprint,
            request.metadata_json,
        )
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
