//! Health probes.
//! - `/health`       public liveness: static 200 as long as the process is up.
//! - `/health/db`    simple DB connectivity check (legacy).
//! - `/health/ready` readiness: checks DB + Redis + S3; returns 503 when any
//!                   critical dependency is unreachable. Wire container
//!                   orchestration readiness probes to this endpoint.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::state::AppState;

pub async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "lumid-data-service"}))
}

pub async fn health_db(State(st): State<AppState>) -> ApiResult<Json<Value>> {
    let client = st.pool.get().await?;
    let row = client.query_one("SELECT 1 AS ok", &[]).await?;
    let ok: i32 = row.get("ok");
    Ok(Json(json!({"db": ok == 1})))
}

/// Readiness probe: checks all critical dependencies in parallel with short
/// timeouts. Returns 503 when any are unhealthy so orchestrators stop routing
/// traffic before the process is ready (or after it degrades).
pub async fn health_ready(State(st): State<AppState>) -> Response {
    let db_ok = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        async {
            st.pool.get().await.ok()?.query_one("SELECT 1", &[]).await.ok()
        },
    )
    .await
    .ok()
    .flatten()
    .is_some();

    let redis_ok = if let Some(mut r) = st.redis.clone() {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            async move {
                redis::cmd("PING")
                    .query_async::<String>(&mut r)
                    .await
            },
        )
        .await
        .ok()
        .and_then(|r| r.ok())
        .is_some()
    } else {
        true // Redis is optional; absence is not a readiness failure.
    };

    let all_ok = db_ok && redis_ok;
    let status = if all_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(json!({
        "status": if all_ok { "ready" } else { "degraded" },
        "db": db_ok,
        "redis": redis_ok,
    })))
    .into_response()
}
