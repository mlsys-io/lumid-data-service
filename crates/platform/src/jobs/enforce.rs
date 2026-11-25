//! Job-token enforcement: a job may touch only the sources it declared.
//!
//! Layered after `auth::gate` (the `Identity` is resolved) and before
//! everything else. A no-op for every caller whose role is not `job`, so
//! humans, agents with their own PATs, and local keys see no change.
//!
//! Default-deny: [`scope::classify`] names the few routes a job may use at all,
//! and each is checked against the job's live scope (its declared sources that
//! its owner may still use — re-checked every `token::SCOPE_TTL`). Bytes read
//! or written are added to `jobs.job_sources.actual_bytes` for the declared
//! ref they were attributed to, so the planner can learn real sizes.

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::{header, Method};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::Value;
use uuid::Uuid;

use super::access::Access;
use super::scope::{self, check_sql, Need, Scope};
use super::token;
use crate::auth::Identity;
use crate::error::ApiError;
use crate::retrieve::plan::RetrievalOp;
use crate::state::AppState;

/// Largest `/retrieve` request body a job may send (the plan, not the data).
const MAX_RETRIEVE_BODY: usize = 1024 * 1024;
/// Largest `/retrieve` response inspected for `size_bytes` (it is a small
/// JSON envelope; the data itself is a materialized blob).
const MAX_RETRIEVE_RESPONSE: usize = 1024 * 1024;

fn refuse(code: &str, detail: impl Into<String>) -> ApiError {
    ApiError::Forbidden(format!("{code}: {}", detail.into()))
}

pub async fn enforce(
    State(st): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let is_job = req
        .extensions()
        .get::<Identity>()
        .is_some_and(|id| id.role == "job");
    if !is_job {
        return Ok(next.run(req).await);
    }
    // The gate already resolved this token; this is a cache hit.
    let token = crate::auth::extract_token(req.headers())
        .ok_or_else(|| ApiError::Unauthorized("job token missing".into()))?;
    let ctx = token::resolve(
        &st.pool,
        &st.settings.hub_id,
        &st.settings.jobs_instance,
        &token,
    )
    .await
    .map_err(|e| ApiError::Unavailable(format!("job scope lookup failed: {e}")))?
    .ok_or_else(|| ApiError::Unauthorized("invalid, expired, or revoked job token".into()))?;

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    match scope::classify(method.as_str(), &path, &st.settings.retrieval_prefix) {
        Need::Allow => Ok(next.run(req).await),
        Need::OwnJob(id) => {
            if id == ctx.job_id.to_string() {
                Ok(next.run(req).await)
            } else {
                Err(refuse(
                    "not_this_job",
                    "a job token may only act on its own job",
                ))
            }
        }
        Need::Deny(code) => Err(refuse(
            code,
            format!("{method} {path} is not available to job tokens"),
        )),
        Need::BlobKey(key) => {
            let r = ctx
                .scope
                .covers_key(&key)
                .ok_or_else(|| refuse("source_not_in_job_scope", format!("blob key '{key}'")))?
                .raw
                .clone();
            let resp = next.run(req).await;
            Ok(count_response(&st, ctx.job_id, &r, resp))
        }
        Need::MeBlob(sha) => {
            let r = ctx
                .scope
                .covers_sha(&sha)
                .ok_or_else(|| refuse("source_not_in_job_scope", format!("blob {sha}")))?
                .raw
                .clone();
            let resp = next.run(req).await;
            Ok(count_response(&st, ctx.job_id, &r, resp))
        }
        Need::Ingest { schema, table } => {
            let r = ctx
                .scope
                .covers_table(&schema, &table, Access::Write)
                .ok_or_else(|| {
                    refuse(
                        "sink_not_in_job_scope",
                        format!(
                            "{}://{schema}.{table} is not a declared write target",
                            ctx.scope.instance
                        ),
                    )
                })?
                .raw
                .clone();
            let in_bytes = req
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let resp = next.run(req).await;
            if resp.status().is_success() {
                record_bytes(&st, ctx.job_id, &r, Access::Write, in_bytes);
            }
            Ok(resp)
        }
        Need::Retrieve => retrieve(&st, ctx.job_id, &ctx.scope, req, next).await,
    }
}

/// `/retrieve`: buffer the (small) plan, check every op against the scope,
/// then run it and attribute the materialized size.
async fn retrieve(
    st: &AppState,
    job_id: Uuid,
    scope: &Scope,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    debug_assert_eq!(req.method(), Method::POST);
    let (parts, body) = req.into_parts();
    let bytes = to_bytes(body, MAX_RETRIEVE_BODY)
        .await
        .map_err(|_| ApiError::BadRequest("retrieve body too large for a job token".into()))?;
    let json: Value = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;
    let (plan, _) = crate::handlers::retrieve::parse_request(&json)?;

    let mut used: Vec<String> = Vec::new();
    let rp = st.settings.retrieval_prefix.trim_matches('/');
    for op in &plan.plan {
        match op {
            RetrievalOp::Sql(sql) => {
                for r in check_sql(scope, &sql.query)
                    .map_err(|e| refuse("source_not_in_job_scope", e))?
                {
                    if !used.contains(&r) {
                        used.push(r);
                    }
                }
            }
            RetrievalOp::StorageGet(sg) => {
                let key = sg.key.trim_start_matches('/');
                let own_output = !rp.is_empty() && key.starts_with(&format!("{rp}/"));
                if !own_output {
                    let r = scope.covers_key(key).ok_or_else(|| {
                        refuse("source_not_in_job_scope", format!("storage key '{key}'"))
                    })?;
                    if !used.contains(&r.raw) {
                        used.push(r.raw.clone());
                    }
                }
            }
        }
    }

    let req = Request::from_parts(parts, Body::from(bytes));
    let resp = next.run(req).await;
    if !resp.status().is_success() || used.is_empty() {
        return Ok(resp);
    }
    // Attribute the materialized size to the first declared ref the plan
    // read (a multi-source plan is not split; the total is what moved).
    let (parts, body) = resp.into_parts();
    let body = match to_bytes(body, MAX_RETRIEVE_RESPONSE).await {
        Ok(b) => b,
        Err(_) => {
            return Err(ApiError::Internal(anyhow::anyhow!(
                "retrieve response too large"
            )))
        }
    };
    if let Some(n) = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("size_bytes").and_then(Value::as_i64))
    {
        record_bytes(st, job_id, &used[0], Access::Read, n);
    }
    Ok(Response::from_parts(parts, Body::from(body)))
}

/// Count the bytes a successful read response ACTUALLY streams to the job
/// (blob bodies are streamed without a Content-Length), and record them once
/// the body is finished or dropped — so an aborted download counts only what
/// was sent.
fn count_response(st: &AppState, job_id: Uuid, raw_ref: &str, resp: Response) -> Response {
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    if !resp.status().is_success() {
        return resp;
    }
    struct Tally {
        st: AppState,
        job_id: Uuid,
        raw_ref: String,
        n: AtomicI64,
    }
    impl Drop for Tally {
        fn drop(&mut self) {
            record_bytes(
                &self.st,
                self.job_id,
                &self.raw_ref,
                Access::Read,
                self.n.load(Ordering::Relaxed),
            );
        }
    }
    let tally = Arc::new(Tally {
        st: st.clone(),
        job_id,
        raw_ref: raw_ref.to_string(),
        n: AtomicI64::new(0),
    });
    let (parts, body) = resp.into_parts();
    let stream = body.into_data_stream().inspect(move |chunk| {
        if let Ok(bytes) = chunk {
            tally.n.fetch_add(bytes.len() as i64, Ordering::Relaxed);
        }
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

/// Fire-and-forget `actual_bytes += n` for one declared ref.
fn record_bytes(st: &AppState, job_id: Uuid, raw_ref: &str, access: Access, n: i64) {
    if n <= 0 {
        return;
    }
    let pool = st.pool.clone();
    let raw_ref = raw_ref.to_string();
    tokio::spawn(async move {
        let res = async {
            let c = pool.get().await?;
            c.execute(
                "UPDATE jobs.job_sources
                    SET actual_bytes = actual_bytes + $4, updated_at = now()
                  WHERE job_id = $1 AND ref = $2 AND access = $3",
                &[&job_id, &raw_ref, &access.as_str(), &n],
            )
            .await?;
            anyhow::Ok(())
        }
        .await;
        if let Err(e) = res {
            tracing::warn!(%job_id, "jobs: actual_bytes update failed: {e}");
        }
    });
}
