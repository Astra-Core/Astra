use crate::{
    error::AppError,
    models::api::{CreateUserRequest, RolesResponse, SetRolesRequest, UserResponse, UsersResponse},
    state::AppState,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use uuid::Uuid;

fn require_auth(
    state: &AppState,
) -> Result<&std::sync::Arc<crate::services::AuthService>, AppError> {
    state.auth_service.as_ref().ok_or_else(|| {
        AppError::BadRequest(
            "authentication is disabled; set ASTRA_JWT_SECRET to enable".to_string(),
        )
    })
}

async fn user_with_roles(
    state: &AppState,
    user: crate::repositories::UserRecord,
) -> anyhow::Result<UserResponse> {
    let roles = state
        .enforcer
        .get_roles_for_user(&user.id.to_string())
        .await;
    Ok(UserResponse {
        id: user.id,
        email: user.email,
        auth_source: user.auth_source,
        roles,
    })
}

/// `GET /api/v1/users`
pub async fn list_users(State(state): State<AppState>) -> Result<Json<UsersResponse>, AppError> {
    let auth = require_auth(&state)?;
    let records = auth.list_users().await?;

    let mut users = Vec::with_capacity(records.len());
    for record in records {
        users.push(user_with_roles(&state, record).await?);
    }

    Ok(Json(UsersResponse { users }))
}

/// `POST /api/v1/users`
pub async fn create_user(
    State(state): State<AppState>,
    Json(req): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserResponse>), AppError> {
    let auth = require_auth(&state)?;
    let record = auth.create_user(&req.email, &req.password).await?;

    // Assign initial roles.
    for role in &req.roles {
        state
            .enforcer
            .add_role_for_user(&record.id.to_string(), role)
            .await?;
    }

    let response = user_with_roles(&state, record).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

/// `DELETE /api/v1/users/:id`
pub async fn delete_user(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let auth = require_auth(&state)?;
    auth.delete_user(id)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/v1/users/:id/roles`
pub async fn get_roles(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<RolesResponse>, AppError> {
    // Verify the user exists.
    require_auth(&state)?;
    let roles = state.enforcer.get_roles_for_user(&id.to_string()).await;
    Ok(Json(RolesResponse { roles }))
}

/// `PUT /api/v1/users/:id/roles`
pub async fn set_roles(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(req): Json<SetRolesRequest>,
) -> Result<Json<RolesResponse>, AppError> {
    require_auth(&state)?;
    state
        .enforcer
        .set_roles_for_user(&id.to_string(), req.roles)
        .await?;
    let roles = state.enforcer.get_roles_for_user(&id.to_string()).await;
    Ok(Json(RolesResponse { roles }))
}
