use thiserror::Error;

/// Top-level structured error type for Astra operations.
///
/// Carries enough information for callers to decide whether to retry and for
/// the control plane to return a machine-readable error response.
#[derive(Debug, Error)]
pub enum AstraError {
    /// A connection to an external system (database, object store) could not
    /// be established. Typically retryable — the host may be temporarily
    /// unavailable.
    #[error("connection failed: {message}")]
    ConnectionFailed {
        message: String,
        retryable: bool,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// A query or read operation against an external system failed. SQL syntax
    /// errors and missing tables are permanent; lock timeouts are retryable.
    #[error("query failed: {message}")]
    QueryFailed {
        message: String,
        retryable: bool,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Writing or reading from the staging layer failed.
    #[error("staging failed: {message}")]
    StagingFailed {
        message: String,
        retryable: bool,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Invalid user input or spec configuration. Always permanent.
    #[error("validation error: {0}")]
    ValidationError(String),

    /// A referenced resource (pipeline, run, artifact) does not exist. Permanent.
    #[error("not found: {0}")]
    NotFound(String),

    /// An unexpected internal error. Should be investigated; not retried.
    #[error("internal error: {0}")]
    Internal(String),
}

impl AstraError {
    /// Returns `true` if the operation that produced this error may succeed on retry.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::ConnectionFailed { retryable, .. } => *retryable,
            Self::QueryFailed { retryable, .. } => *retryable,
            Self::StagingFailed { retryable, .. } => *retryable,
            Self::ValidationError(_) | Self::NotFound(_) | Self::Internal(_) => false,
        }
    }

    /// Returns the machine-readable error code for this variant.
    pub fn code(&self) -> &'static str {
        match self {
            Self::ConnectionFailed { .. } => "CONNECTION_FAILED",
            Self::QueryFailed { .. } => "QUERY_FAILED",
            Self::StagingFailed { .. } => "STAGING_FAILED",
            Self::ValidationError(_) => "VALIDATION_ERROR",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Internal(_) => "INTERNAL_ERROR",
        }
    }

    /// Convenience constructor for a retryable connection failure.
    pub fn connection_failed_retryable(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::ConnectionFailed {
            message: message.into(),
            retryable: true,
            source: Some(Box::new(source)),
        }
    }

    /// Convenience constructor for a permanent connection failure (e.g. bad credentials).
    pub fn connection_failed_permanent(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::ConnectionFailed {
            message: message.into(),
            retryable: false,
            source: Some(Box::new(source)),
        }
    }

    /// Convenience constructor for a permanent query failure (syntax error, missing table).
    pub fn query_failed_permanent(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::QueryFailed {
            message: message.into(),
            retryable: false,
            source: Some(Box::new(source)),
        }
    }

    /// Convenience constructor for a retryable query failure (lock timeout, serialization failure).
    pub fn query_failed_retryable(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::QueryFailed {
            message: message.into(),
            retryable: true,
            source: Some(Box::new(source)),
        }
    }
}
