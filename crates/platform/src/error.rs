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
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("rate limited")]
    RateLimited { retry_after_s: u64, limit: String },
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
    /// Full cause chain, for SERVER-SIDE LOGS only.
    ///
    /// `Display` on `Internal` is deliberately opaque — it renders "internal
    /// error" so an HTTP response never leaks internals to a client. The cost is
    /// that a log line written as `{e}` says exactly nothing, which is how
    /// `read layer disabled: internal error` cost two production deploys and two
    /// rollbacks to learn nothing from. Log sites should use this instead; it
    /// renders the anyhow chain (`{:#}`) so the ROOT cause is named.
    ///
    /// Never put this in a response body.
    pub fn log_detail(&self) -> String {
        match self {
            ApiError::Internal(e) => format!("{e:#}"),
            other => other.to_string(),
        }
    }

    /// Shallow clone for cache single-flight (`moka::try_get_with` hands back
    /// `Arc<ApiError>`; the caller needs an owned `ApiError` to return). The
    /// `Internal` source chain isn't `Clone`, so it's flattened to its message.
    pub fn clone_lite(&self) -> ApiError {
        match self {
            ApiError::BadRequest(s) => ApiError::BadRequest(s.clone()),
            ApiError::Unauthorized(s) => ApiError::Unauthorized(s.clone()),
            ApiError::RateLimited { retry_after_s, limit } => ApiError::RateLimited {
                retry_after_s: *retry_after_s,
                limit: limit.clone(),
            },
            ApiError::NotFound(s) => ApiError::NotFound(s.clone()),
            ApiError::Validation(v) => ApiError::Validation(v.clone()),
            ApiError::Forbidden(s) => ApiError::Forbidden(s.clone()),
            ApiError::Unavailable(s) => ApiError::Unavailable(s.clone()),
            ApiError::Internal(e) => ApiError::Internal(anyhow::anyhow!("{e:#}")),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
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
        // Rate-limit responses carry Retry-After + X-RateLimit-Limit, mirroring
        // the Python _rate_limit_handler.
        if let ApiError::RateLimited { retry_after_s, limit } = &self {
            let body = json!({"detail": format!("Rate limit exceeded: {limit}")});
            let mut resp = (status, Json(body)).into_response();
            let h = resp.headers_mut();
            h.insert("Retry-After", retry_after_s.to_string().parse().unwrap());
            h.insert("X-RateLimit-Limit", limit.parse().unwrap());
            return resp;
        }
        let body = match &self {
            ApiError::Validation(v) => json!({"detail": v}),
            ApiError::Internal(e) => {
                // A CORRELATION ID ON BOTH SIDES.
                //
                // The body stays deliberately opaque — an internal error must
                // not leak a query plan, a DSN or a path to the caller. But
                // "internal server error" with nothing else meant a reporter
                // could describe a two-day outage precisely and still leave the
                // operator grepping by timestamp across two clusters to find
                // the matching line. Both users who reported the 2026-08-30/31
                // `/retrieve` outage asked for exactly this.
                //
                // The id is generated here, logged beside the real cause, and
                // returned in the body and the `x-request-id` header, so a bug
                // report can quote one token that finds the server-side error.
                let rid = uuid::Uuid::new_v4().to_string();
                tracing::error!(request_id = %rid, "internal error: {e:#}");
                json!({"detail": "internal server error", "request_id": rid})
            }
            other => json!({"detail": other.to_string()}),
        };
        let rid = body.get("request_id").and_then(|v| v.as_str()).map(str::to_owned);
        let mut resp = (status, Json(body)).into_response();
        // Mirror the id into a header so proxies and clients that discard the
        // body on 5xx still surface something traceable.
        if let Some(rid) = rid {
            if let Ok(v) = rid.parse() {
                resp.headers_mut().insert("x-request-id", v);
            }
        }
        resp
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
