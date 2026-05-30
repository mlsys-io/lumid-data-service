//! Error type with axum `IntoResponse`. Status mapping mirrors the Python
//! services (400 bad input, 404 not found, 422 validation, 403 ACL, 503
//! upstream/infra, 500 otherwise).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("validation failed")]
    Validation(serde_json::Value),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
    #[error("internal error")]
    Internal(#[from] anyhow::Error),
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Postgres errors → 500 (with the message logged, not leaked verbatim).
impl From<tokio_postgres::Error> for ApiError {
    fn from(e: tokio_postgres::Error) -> Self {
        ApiError::Internal(anyhow::anyhow!(e))
    }
}

impl From<deadpool_postgres::PoolError> for ApiError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        ApiError::Unavailable(format!("db pool: {e}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = match &self {
            ApiError::Validation(v) => json!({"detail": v}),
            ApiError::Internal(e) => {
                tracing::error!("internal error: {e:#}");
                json!({"detail": "internal server error"})
            }
            other => json!({"detail": other.to_string()}),
        };
        (status, Json(body)).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
