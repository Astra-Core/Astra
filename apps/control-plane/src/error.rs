use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
#[error("pipeline '{0}' not found")]
pub struct NotFoundError(pub String);

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    NotFound(#[from] NotFoundError),
    #[error(transparent)]
    Validation(#[from] astra_yaml::ValidationError),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound(e) => (StatusCode::NOT_FOUND, e.to_string()),
            Self::Validation(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
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
            return Self::NotFound(NotFoundError(not_found.0.clone()));
        }

        Self::Internal(value.to_string())
    }
}
