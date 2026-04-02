use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
#[error("pipeline '{0}' not found")]
pub struct NotFoundError(pub String);

#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        if let Some(validation) = value.downcast_ref::<astra_yaml::ValidationError>() {
            return Self::BadRequest(validation.to_string());
        }

        if let Some(not_found) = value.downcast_ref::<NotFoundError>() {
            return Self::NotFound(not_found.to_string());
        }

        Self::Internal(value.to_string())
    }
}

impl From<astra_yaml::ValidationError> for AppError {
    fn from(value: astra_yaml::ValidationError) -> Self {
        Self::BadRequest(value.to_string())
    }
}
