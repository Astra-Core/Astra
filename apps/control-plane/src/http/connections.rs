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
    state
        .connection_service
        .get_connection(&name)
        .await?
        .ok_or_else(|| NotFoundError(name.clone()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        authz::AstraEnforcer,
        repositories::{InMemoryConnectionRepository, InMemoryPipelineRepository},
        services::{ConnectionService, PipelineService},
        state::AppState,
    };
    use axum::{
        body::Body,
        http::{self, Request},
        routing::get,
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn test_app() -> Router {
        let conn_repo = Arc::new(InMemoryConnectionRepository::default());
        let connection_service = Arc::new(ConnectionService::new(conn_repo));
        let pipeline_repo = Arc::new(InMemoryPipelineRepository::default());
        let pipeline_service = Arc::new(PipelineService::new(
            pipeline_repo,
            connection_service.clone(),
        ));
        let enforcer = Arc::new(AstraEnforcer::new_memory().await.unwrap());
        let state = AppState::new(pipeline_service, connection_service, None, enforcer);

        Router::new()
            .route(
                "/api/v1/connections",
                get(list_connections).post(create_connection),
            )
            .route(
                "/api/v1/connections/:name",
                get(get_connection)
                    .put(update_connection)
                    .delete(delete_connection),
            )
            .with_state(state)
    }

    fn connection_body(name: &str) -> Body {
        Body::from(
            serde_json::to_vec(&serde_json::json!({
                "name": name,
                "kind": "postgres",
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "username": "testuser"
            }))
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn list_connections_returns_empty_list() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::GET)
                    .uri("/api/v1/connections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["connections"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn create_connection_returns_201() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/connections")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(connection_body("my-pg"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "my-pg");
        assert_eq!(json["kind"], "postgres");
        assert_eq!(json["host"], "localhost");
        assert_eq!(json["port"], 5432);
        assert!(!json["id"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_connection_invalid_name_returns_400() {
        let app = test_app().await;
        let bad_body = Body::from(
            serde_json::to_vec(&serde_json::json!({
                "name": "INVALID_NAME",
                "kind": "postgres",
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "username": "testuser"
            }))
            .unwrap(),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/connections")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(bad_body)
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_connection_returns_200() {
        let app = test_app().await;

        // Create first
        app.clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/connections")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(connection_body("get-test"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then get it
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::GET)
                    .uri("/api/v1/connections/get-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "get-test");
    }

    #[tokio::test]
    async fn get_connection_not_found_returns_404() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::GET)
                    .uri("/api/v1/connections/no-such-conn")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_connection_returns_200() {
        let app = test_app().await;

        // Create first
        app.clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/connections")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(connection_body("upd-test"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Update it
        let updated_body = Body::from(
            serde_json::to_vec(&serde_json::json!({
                "name": "upd-test",
                "kind": "postgres",
                "host": "newhost",
                "port": 5433,
                "database": "newdb",
                "username": "newuser"
            }))
            .unwrap(),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::PUT)
                    .uri("/api/v1/connections/upd-test")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(updated_body)
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["host"], "newhost");
        assert_eq!(json["port"], 5433);
    }

    #[tokio::test]
    async fn delete_connection_returns_204() {
        let app = test_app().await;

        // Create first
        app.clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::POST)
                    .uri("/api/v1/connections")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(connection_body("del-test"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete it
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/api/v1/connections/del-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn delete_nonexistent_connection_returns_404() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri("/api/v1/connections/ghost")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
