use crate::{
    error::AppError,
    models::api::{LdapGroupMappingRequest, LdapGroupMappingResponse, LdapGroupMappingsResponse},
    repositories::LdapGroupMapping,
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

fn mapping_response(m: LdapGroupMapping) -> LdapGroupMappingResponse {
    LdapGroupMappingResponse {
        id: m.id,
        ldap_group: m.ldap_group,
        astra_role: m.astra_role,
        created_at: m.created_at.to_rfc3339(),
    }
}

/// `GET /api/v1/ldap/group-mappings`
pub async fn list_mappings(
    State(state): State<AppState>,
) -> Result<Json<LdapGroupMappingsResponse>, AppError> {
    let auth = require_auth(&state)?;
    let records = auth.list_ldap_group_mappings().await?;
    Ok(Json(LdapGroupMappingsResponse {
        mappings: records.into_iter().map(mapping_response).collect(),
    }))
}

/// `POST /api/v1/ldap/group-mappings`
pub async fn add_mapping(
    State(state): State<AppState>,
    Json(req): Json<LdapGroupMappingRequest>,
) -> Result<(StatusCode, Json<LdapGroupMappingResponse>), AppError> {
    let auth = require_auth(&state)?;
    let record = auth
        .add_ldap_group_mapping(&req.ldap_group, &req.astra_role)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(mapping_response(record))))
}

/// `DELETE /api/v1/ldap/group-mappings/:id`
pub async fn delete_mapping(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let auth = require_auth(&state)?;
    auth.delete_ldap_group_mapping(id)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}
