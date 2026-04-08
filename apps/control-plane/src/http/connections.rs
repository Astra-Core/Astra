use crate::{
    error::{AppError, NotFoundError},
    models::api::{
        ConnectionTestRequest, ConnectionTestResponse, SavedConnectionRequest,
        SavedConnectionResponse, SavedConnectionsResponse,
    },
    repositories::CreateSavedConnectionInput,
    state::AppState,
};
use astra_connectors::{test_postgres_connection, PostgresConnectionConfig};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

/// Validates that `name` matches `^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$`.
fn validate_name(name: &str) -> Result<(), AppError> {
    let len = name.len();
    let is_alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let valid = (2..=63).contains(&len)
        && name.chars().next().map(is_alnum).unwrap_or(false)
        && name.chars().last().map(is_alnum).unwrap_or(false)
        && name.chars().all(|c| is_alnum(c) || c == '-');
    if !valid {
        return Err(AppError::BadRequest(format!(
            "connection name '{}' is invalid: must match ^[a-z0-9][a-z0-9-]{{0,61}}[a-z0-9]$",
            name
        )));
    }
    Ok(())
}

fn build_input(req: SavedConnectionRequest) -> CreateSavedConnectionInput {
    let config_json = serde_json::json!({
        "host": req.host,
        "port": req.port,
        "database": req.database,
        "username": req.username,
    });
    CreateSavedConnectionInput {
        name: req.name,
        kind: req.kind,
        config_json,
        secret_ref: req.secret_ref,
        created_by: None,
    }
}

pub async fn list_connections(
    State(state): State<AppState>,
) -> Result<Json<SavedConnectionsResponse>, AppError> {
    let records = state.connection_service.list_connections().await?;
    let connections = records
        .into_iter()
        .map(SavedConnectionResponse::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Json(SavedConnectionsResponse { connections }))
}

pub async fn create_connection(
    State(state): State<AppState>,
    Json(request): Json<SavedConnectionRequest>,
) -> Result<(StatusCode, Json<SavedConnectionResponse>), AppError> {
    validate_name(&request.name)?;
    let input = build_input(request);
    let record = state.connection_service.create_connection(input).await?;
    let response = SavedConnectionResponse::try_from(record)?;
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn get_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<SavedConnectionResponse>, AppError> {
    let record = state
        .connection_service
        .get_connection(&name)
        .await?
        .ok_or_else(|| NotFoundError(name.clone()))?;
    let response = SavedConnectionResponse::try_from(record)?;
    Ok(Json(response))
}

pub async fn update_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<SavedConnectionRequest>,
) -> Result<Json<SavedConnectionResponse>, AppError> {
    validate_name(&request.name)?;
    let input = build_input(request);
    let record = state
        .connection_service
        .update_connection(&name, input)
        .await?;
    let response = SavedConnectionResponse::try_from(record)?;
    Ok(Json(response))
}

pub async fn delete_connection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    state.connection_service.delete_connection(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn test_connection(
    Json(req): Json<ConnectionTestRequest>,
) -> Result<Json<ConnectionTestResponse>, AppError> {
    if req.kind != "postgres" {
        return Err(AppError::BadRequest(format!(
            "connection kind '{}' is not supported; only 'postgres' is currently supported",
            req.kind
        )));
    }

    let config = PostgresConnectionConfig {
        host: req.host,
        port: req.port,
        database: req.database,
        username: req.username,
        password_ref: req.password_ref,
        ssl_mode: req.ssl_mode,
        application_name: None,
    };

    let result = test_postgres_connection(&config, &req.tables).await;

    Ok(Json(ConnectionTestResponse {
        status: result.status,
        latency_ms: result.latency_ms,
        message: result.message,
        missing_tables: result.missing_tables,
    }))
}
