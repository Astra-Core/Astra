use crate::{
    error::AppError,
    models::api::{ApplySpecRequest, PipelinesResponse},
    state::AppState,
};
use axum::{extract::State, Json};

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
