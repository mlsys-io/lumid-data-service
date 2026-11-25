//! `/jobs/v1/*` — the HTTP face of [`super::service`]. Mounted (inside the
//! auth gate) only when `LUMID_JOBS_ENABLED`.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::service::{self, EventIn, GrantIn, JResult, JobSpec, JobsError, SourceIn};
use crate::auth::Identity;
use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/jobs/v1/sources", get(list_sources).post(create_source))
        .route("/jobs/v1/sources/shared-with-me", get(shared_with_me))
        .route("/jobs/v1/sources/:id", delete(delete_source))
        .route("/jobs/v1/sources/:id/grants", post(share))
        .route(
            "/jobs/v1/sources/:id/grants/:grantee/:access",
            delete(unshare),
        )
        .route("/jobs/v1/plan", post(plan))
        .route("/jobs/v1/jobs", get(list_jobs).post(submit))
        .route("/jobs/v1/jobs/:id", get(get_job))
        .route("/jobs/v1/jobs/:id/cancel", post(cancel))
        .route(
            "/jobs/v1/jobs/:id/events",
            get(list_events).post(post_event),
        )
        .route("/jobs/v1/jobs/:id/token", post(issue_token))
        .route("/jobs/v1/admin/route-health", post(route_health))
        .route("/jobs/v1/admin/routes", get(list_routes))
}

fn parse_id(s: &str) -> JResult<Uuid> {
    Uuid::parse_str(s)
        .map_err(|_| JobsError::new(StatusCode::NOT_FOUND, "job_not_found", "no such job"))
}

#[derive(Deserialize)]
struct SourcesQ {
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    q: Option<String>,
}

async fn list_sources(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Query(p): Query<SourcesQ>,
) -> JResult<Json<Value>> {
    Ok(Json(
        service::list_sources(&st, &id, p.scope.as_deref().unwrap_or("all"), p.q).await?,
    ))
}

async fn shared_with_me(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
) -> JResult<Json<Value>> {
    Ok(Json(service::list_sources(&st, &id, "shared", None).await?))
}

async fn create_source(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Json(body): Json<SourceIn>,
) -> JResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::CREATED,
        Json(service::create_source(&st, &id, body).await?),
    ))
}

async fn delete_source(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(sid): Path<String>,
) -> JResult<Json<Value>> {
    Ok(Json(service::delete_source(&st, &id, &sid).await?))
}

async fn share(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(sid): Path<String>,
    Json(body): Json<GrantIn>,
) -> JResult<Json<Value>> {
    Ok(Json(service::share(&st, &id, &sid, body).await?))
}

async fn unshare(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path((sid, grantee, access)): Path<(String, String, String)>,
) -> JResult<Json<Value>> {
    Ok(Json(
        service::unshare(&st, &id, &sid, &grantee, &access).await?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanIn {
    spec: JobSpec,
}

async fn plan(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Json(body): Json<PlanIn>,
) -> JResult<Json<Value>> {
    Ok(Json(service::plan(&st, &id, body.spec).await?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitIn {
    #[serde(default)]
    spec: Option<JobSpec>,
    #[serde(default)]
    plan_id: Option<Uuid>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

async fn submit(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    headers: HeaderMap,
    Json(body): Json<SubmitIn>,
) -> JResult<(StatusCode, Json<Value>)> {
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or(body.idempotency_key);
    let (code, v) = service::submit(&st, &id, body.spec, body.plan_id, key).await?;
    Ok((code, Json(v)))
}

#[derive(Deserialize)]
struct JobsQ {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    app: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

async fn list_jobs(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Query(p): Query<JobsQ>,
) -> JResult<Json<Value>> {
    Ok(Json(
        service::list_jobs(&st, &id, p.status, p.app, p.limit.unwrap_or(50)).await?,
    ))
}

async fn get_job(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(jid): Path<String>,
) -> JResult<Json<Value>> {
    Ok(Json(service::get_job(&st, &id, parse_id(&jid)?).await?))
}

async fn cancel(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(jid): Path<String>,
) -> JResult<Json<Value>> {
    Ok(Json(service::cancel(&st, &id, parse_id(&jid)?).await?))
}

async fn list_events(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(jid): Path<String>,
) -> JResult<Json<Value>> {
    Ok(Json(service::list_events(&st, &id, parse_id(&jid)?).await?))
}

async fn post_event(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(jid): Path<String>,
    Json(body): Json<EventIn>,
) -> JResult<Json<Value>> {
    Ok(Json(
        service::post_event(&st, &id, parse_id(&jid)?, body).await?,
    ))
}

async fn issue_token(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Path(jid): Path<String>,
) -> JResult<Json<Value>> {
    Ok(Json(service::issue_token(&st, &id, parse_id(&jid)?).await?))
}

#[derive(Deserialize)]
struct RoutesQ {
    #[serde(default)]
    from_site: Option<String>,
}

async fn list_routes(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Query(p): Query<RoutesQ>,
) -> JResult<Json<Value>> {
    Ok(Json(service::list_routes(&st, &id, p.from_site).await?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthIn {
    route_id: String,
    health: String,
    #[serde(default)]
    latency_ms: Option<i32>,
}

async fn route_health(
    State(st): State<AppState>,
    Extension(id): Extension<Identity>,
    Json(b): Json<HealthIn>,
) -> JResult<Json<Value>> {
    Ok(Json(
        service::route_health(&st, &id, &b.route_id, &b.health, b.latency_ms).await?,
    ))
}

/// OpenAPI path items for `/jobs/v1/*` (merged into `/openapi.json`).
pub fn openapi_paths() -> Value {
    let op = |summary: &str| json!({"summary": summary, "tags": ["jobs"], "responses": {"200": {"description": "OK"}}});
    json!({
        "/jobs/v1/sources": {
            "get": op("Sources you may use (scope=mine|shared|public|all, q=search)"),
            "post": op("Register a source you own ({name, ref, visibility?, max_bytes?})")
        },
        "/jobs/v1/sources/shared-with-me": {"get": op("Sources others shared with you")},
        "/jobs/v1/sources/{id}": {"delete": op("Delete a source you own")},
        "/jobs/v1/sources/{id}/grants": {"post": op("Share a source ({grantee: sub, access: read|write, expires_at?})")},
        "/jobs/v1/sources/{id}/grants/{grantee}/{access}": {"delete": op("Withdraw a share (running jobs lose access within 15 s)")},
        "/jobs/v1/plan": {"post": op("Dry run: resolve sources and return the networking plan ({spec})")},
        "/jobs/v1/jobs": {
            "get": op("Your jobs (status=, app=, limit=)"),
            "post": op("Submit a job ({spec} or {plan_id}; Idempotency-Key header)")
        },
        "/jobs/v1/jobs/{id}": {"get": op("A job, its plan, and per-source bytes")},
        "/jobs/v1/jobs/{id}/cancel": {"post": op("Cancel a job (revokes its tokens)")},
        "/jobs/v1/jobs/{id}/events": {
            "get": op("A job's events"),
            "post": op("Report progress (runtime service key or the job's own token)")
        },
        "/jobs/v1/jobs/{id}/token": {"post": op("Issue a job token (runtime service key or the job itself)")},
        "/jobs/v1/admin/route-health": {"post": op("Prober health report ({route_id, health: up|degraded|down, latency_ms?})")},
        "/jobs/v1/admin/routes": {"get": op("Routes a prober should probe (from_site=; a prober key sees only its own site)")}
    })
}
