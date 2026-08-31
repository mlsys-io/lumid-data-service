//! Health probes.
//! - `/health`       public liveness: static 200 as long as the process is up.
//! - `/health/db`    simple DB connectivity check (legacy).
//! - `/health/ready` readiness: checks DB + Redis + the OBJECT STORE; returns
//!                   503 when any critical dependency is unreachable. Wire
//!                   container orchestration readiness probes to this endpoint.
//!
//! `/health` stays a static liveness probe on purpose. Two users asked for it
//! to report degraded during the 2026-08-30/31 outage, and the right answer to
//! that ask is the readiness probe below, not a failing liveness one: a
//! liveness probe that goes red gets the container RESTARTED, which would have
//! turned a broken result-write into a crash-loop without fixing anything.

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

    // PROBE THE THING THAT ACTUALLY BROKE.
    //
    // The docstring above used to claim this checked S3; it never did. During
    // the 2026-08-30/31 outage EVERY `/retrieve` failed at the object-store
    // write — `LocalFileSystem::put_opts` returning NotImplemented — while
    // Postgres and Redis were both perfectly healthy. So readiness would have
    // reported "ready" throughout, exactly like `/health` did, and the reports
    // that said "health is green while every query 500s" would have been just
    // as true of this endpoint.
    //
    // A read probe would not have caught it either: the failure is on WRITE.
    // This performs the same write, through the same helper, with the same
    // non-empty attribute set — so readiness exercises the real failure mode
    // rather than a cheaper proxy for it. One fixed key, overwritten each
    // probe, so it cannot accumulate objects.
    let store_ok = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let path = object_store::path::Path::from("health/ready-probe");
        let attrs = object_store::Attributes::from_iter([(
            object_store::Attribute::ContentType,
            "text/plain".to_string(),
        )]);
        crate::objstore::put_with_optional_attrs(
            &st.blob_store,
            &path,
            bytes::Bytes::from_static(b"ok"),
            attrs,
        )
        .await
        .is_ok()
    })
    .await
    .unwrap_or(false);

    let all_ok = db_ok && redis_ok && store_ok;
    let status = if all_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(json!({
        "status": if all_ok { "ready" } else { "degraded" },
        "db": db_ok,
        "redis": redis_ok,
        "object_store": store_ok,
    })))
    .into_response()
}
