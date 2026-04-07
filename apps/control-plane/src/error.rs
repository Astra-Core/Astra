use astra_metadata::AstraError;
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
#[error("{0}")]
pub struct BadRequestError(pub String);

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
    /// Typed Astra error surfaced from the service or connector layer.
    #[error(transparent)]
    Astra(#[from] AstraError),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    code: String,
    retryable: bool,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::NotFound(e) => (
                StatusCode::NOT_FOUND,
                ErrorBody {
                    error: e.to_string(),
                    code: "NOT_FOUND".to_string(),
                    retryable: false,
                },
            ),
            Self::Validation(e) => (
                StatusCode::BAD_REQUEST,
                ErrorBody {
                    error: e.to_string(),
                    code: "VALIDATION_ERROR".to_string(),
                    retryable: false,
                },
            ),
            Self::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorBody {
                    error: msg,
                    code: "BAD_REQUEST".to_string(),
                    retryable: false,
                },
            ),
            Self::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorBody {
                    error: msg,
                    code: "INTERNAL_ERROR".to_string(),
                    retryable: false,
                },
            ),
            Self::Astra(astra_err) => {
                let status = match &astra_err {
                    AstraError::NotFound(_) => StatusCode::NOT_FOUND,
                    AstraError::ValidationError(_) => StatusCode::BAD_REQUEST,
                    AstraError::ConnectionFailed { .. }
                    | AstraError::QueryFailed { .. }
                    | AstraError::StagingFailed { .. } => StatusCode::BAD_GATEWAY,
                    AstraError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
                };
                let retryable = astra_err.is_retryable();
                let code = astra_err.code().to_string();
                let error = astra_err.to_string();
                (
                    status,
                    ErrorBody {
                        error,
                        code,
                        retryable,
                    },
                )
            }
        };
        (status, Json(body)).into_response()
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

        if let Some(bad_request) = value.downcast_ref::<BadRequestError>() {
            return Self::BadRequest(bad_request.0.clone());
        }

        Self::Internal(value.to_string())
    }
}
