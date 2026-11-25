//! The jobs service: everything the HTTP API and the MCP tools do, as plain
//! functions of `(state, caller, input)`. Both surfaces call these, so a human
//! in the UI, an agent over MCP, and a script over HTTP get identical rules
//! and identical errors.
//!
//! Errors are `JobsError {status, code, message, details}` — a stable machine
//! code an agent can branch on, plus a sentence a person can read.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::access::{self, Access};
use super::inputs;
use super::planner::{self, PlanOutcome, Policy, RuntimeSel, Size, SourceNeed};
use super::refs::{self, SourceRef};
use super::seed;
use super::token;
use crate::auth::Identity;
use crate::state::AppState;

// ───────────────────────────────────────────────────────────────── errors

#[derive(Debug)]
pub struct JobsError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub details: Value,
}

impl JobsError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details: Value::Null,
        }
    }
    pub fn with(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
    pub fn body(&self) -> Value {
        json!({"error": {"code": self.code, "message": self.message, "details": self.details}})
    }
}

impl std::fmt::Display for JobsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.body().to_string())
    }
}

impl IntoResponse for JobsError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}

impl From<anyhow::Error> for JobsError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("jobs: {e:#}");
        JobsError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error",
        )
    }
}
impl From<tokio_postgres::Error> for JobsError {
    fn from(e: tokio_postgres::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}
impl From<deadpool_postgres::PoolError> for JobsError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        anyhow::Error::from(e).into()
    }
}

pub type JResult<T> = Result<T, JobsError>;

fn bad(code: &'static str, msg: impl Into<String>) -> JobsError {
    JobsError::new(StatusCode::BAD_REQUEST, code, msg)
}
fn forbidden(code: &'static str, msg: impl Into<String>) -> JobsError {
    JobsError::new(StatusCode::FORBIDDEN, code, msg)
}
fn not_found(code: &'static str, msg: impl Into<String>) -> JobsError {
    JobsError::new(StatusCode::NOT_FOUND, code, msg)
}

/// Only people and agents with their own credentials use the jobs API; a job
/// token never submits, plans, or shares.
fn require_principal(id: &Identity) -> JResult<()> {
    if id.role == "job" {
        return Err(forbidden(
            "job_tokens_cannot_manage_jobs",
            "a job token cannot plan, submit, or share",
        ));
    }
    Ok(())
}

fn is_slug(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

// ───────────────────────────────────────────────────────────────── specs

/// The one thing a caller submits.
#[derive(Clone, Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    #[serde(default)]
    pub name: Option<String>,
    /// The submitting app (required — the history never recorded it).
    pub app: String,
    /// `flowmesh` | `lumilake` | `sandbox` | `auto`.
    pub runtime: String,
    #[serde(default)]
    pub sources: Vec<SourceDecl>,
    #[serde(default)]
    pub sinks: Vec<SourceDecl>,
    /// The runtime-native body (FlowMesh task, Lumilake request, sandbox spec).
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub parent_job_id: Option<Uuid>,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDecl {
    #[serde(rename = "ref")]
    pub r: String,
    #[serde(default)]
    pub max_bytes: Option<i64>,
    /// Free-form time window (e.g. `now-24h..now`), recorded for the runtime.
    #[serde(default)]
    pub window: Option<String>,
}

const MAX_DECLS: usize = 64;
const RUNTIMES: &[&str] = &["flowmesh", "lumilake", "sandbox"];

fn validate_spec(spec: &JobSpec) -> JResult<RuntimeSel> {
    if !is_slug(&spec.app, 64) {
        return Err(bad("invalid_spec", "app must be [A-Za-z0-9._-], 1..=64"));
    }
    if spec.sources.len() + spec.sinks.len() > MAX_DECLS {
        return Err(bad(
            "invalid_spec",
            format!("at most {MAX_DECLS} sources + sinks"),
        ));
    }
    if spec.sources.is_empty() && spec.sinks.is_empty() {
        return Err(bad("invalid_spec", "declare at least one source or sink"));
    }
    if spec
        .sources
        .iter()
        .chain(&spec.sinks)
        .any(|d| d.max_bytes.is_some_and(|m| m <= 0))
    {
        return Err(bad("invalid_spec", "max_bytes must be positive"));
    }
    match spec.runtime.as_str() {
        "auto" => Ok(RuntimeSel::auto()),
        r if RUNTIMES.contains(&r) => Ok(RuntimeSel::Named(r.to_string())),
        other => Err(bad(
            "invalid_spec",
            format!("runtime '{other}' (expected flowmesh, lumilake, sandbox, or auto)"),
        )),
    }
}

// ────────────────────────────────────────────────────────── resolution

/// A declared ref, resolved: what it actually points at, which registered
/// source authorizes it, where the data lives, and how big it may be.
#[derive(Clone, Debug)]
pub struct Resolved {
    pub declared: String,
    pub underlying: String,
    pub parsed: SourceRef,
    pub access: Access,
    /// The registered source that AUTHORIZES the read (a user's alias, an
    /// estate source, an implicit blob source) — what enforcement re-checks.
    pub source_id: Option<String>,
    /// The data store the bytes actually come from — what routes and
    /// locations are keyed by (a `src://` alias over a lumid-data table is
    /// routed like the lumid-data instance).
    pub store_id: Option<String>,
    pub site: Option<String>,
    pub size: Size,
    pub window: Option<String>,
}

/// The data store a (non-alias) data-plane ref reads from.
fn store_of(st: &AppState, r: &SourceRef) -> Option<String> {
    match r {
        SourceRef::Table { instance, .. } => Some(instance.clone()),
        SourceRef::Blob { hub, .. } | SourceRef::BlobPrefix { hub, .. }
            if *hub == st.settings.hub_id =>
        {
            Some(st.settings.jobs_instance.clone())
        }
        _ => None,
    }
}

/// Instances this hub is home to.
fn home_instances(st: &AppState) -> Vec<String> {
    seed::parse_estate(&st.settings.jobs_estate).unwrap_or_default()
}

async fn resolve_decl(
    st: &AppState,
    caller: &Identity,
    d: &SourceDecl,
    access: Access,
) -> JResult<Resolved> {
    let parsed = refs::parse(&d.r).map_err(|e| bad("invalid_ref", e.0))?;
    let hub = st.settings.hub_id.as_str();
    let deny = |reason: &str| {
        forbidden("source_not_entitled", format!("{}: {reason}", d.r))
            .with(json!({"ref": d.r, "access": access.as_str(), "reason": reason}))
    };

    // A src:// alias resolves to the ref it was registered for.
    let (source_id, underlying_raw, underlying) = match &parsed {
        SourceRef::SourceId { .. } | SourceRef::SourceName { .. } => {
            let c = st.pool.get().await?;
            let row = match &parsed {
                SourceRef::SourceId { source_id } => {
                    c.query_opt(
                        "SELECT source_id, definition FROM jobs.sources
                          WHERE source_id = $1 AND deleted_at IS NULL",
                        &[source_id],
                    )
                    .await?
                }
                SourceRef::SourceName { owner, name } => {
                    let owner = if owner == "me" {
                        caller.sub.as_str()
                    } else {
                        owner.as_str()
                    };
                    c.query_opt(
                        "SELECT source_id, definition FROM jobs.sources
                          WHERE owner_sub = $1 AND name = $2 AND deleted_at IS NULL
                          ORDER BY (home_hub = $3) DESC LIMIT 1",
                        &[&owner, name, &hub],
                    )
                    .await?
                }
                _ => unreachable!(),
            };
            let Some(row) = row else {
                return Err(not_found(
                    "source_not_found",
                    format!("{} does not exist", d.r),
                ));
            };
            let sid: String = row.get("source_id");
            let def: Value = row.get("definition");
            let inner = def
                .get("ref")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let inner_parsed = refs::parse(&inner)
                .map_err(|_| bad("invalid_source", format!("{sid} has no usable ref")))?;
            let dec = access::can(&st.pool, &caller.sub, &sid, access).await?;
            if !dec.allow {
                return Err(deny(dec.reason));
            }
            (Some(sid), inner, inner_parsed)
        }
        // Models and external APIs are not data-plane: nothing to authorize.
        SourceRef::Model { .. } | SourceRef::External { .. } => {
            if access == Access::Write {
                return Err(bad("invalid_sink", format!("{} cannot be written to", d.r)));
            }
            (None, d.r.clone(), parsed.clone())
        }
        // An estate table: authorized by the estate source (public, read-only).
        SourceRef::Table { instance, .. } => {
            let dec = access::can(&st.pool, &caller.sub, instance, access).await?;
            if !dec.allow {
                return Err(deny(if access == Access::Write {
                    "write into estate tables through a registered src:// sink"
                } else {
                    dec.reason
                }));
            }
            (Some(instance.clone()), d.r.clone(), parsed.clone())
        }
        SourceRef::BlobPrefix { hub: h, .. } | SourceRef::Blob { hub: h, .. } if h != hub => {
            return Err(deny(
                "blobs on another hub must be registered src:// sources there",
            ));
        }
        SourceRef::BlobPrefix { .. } => {
            // The shared blob namespace of this hub's instance (tenants/ and
            // retrievals/ are refused by the ref grammar).
            if access == Access::Write {
                return Err(deny(
                    "write into a blob prefix through a registered src:// sink",
                ));
            }
            let inst = st.settings.jobs_instance.clone();
            let dec = access::can(&st.pool, &caller.sub, &inst, access).await?;
            if !dec.allow {
                return Err(deny(dec.reason));
            }
            (Some(inst), d.r.clone(), parsed.clone())
        }
        SourceRef::Blob { sha256, .. } => {
            if access == Access::Write {
                return Err(bad(
                    "invalid_sink",
                    "a content-addressed blob cannot be written",
                ));
            }
            let c = st.pool.get().await?;
            // The caller's own private upload → an implicit private source.
            let own: bool = c
                .query_opt(
                    "SELECT 1 FROM raw.tenant_blobs WHERE owner_sub = $1 AND blob_sha256 = $2",
                    &[&caller.sub, sha256],
                )
                .await
                .map(|r| r.is_some())
                .unwrap_or(false);
            if own {
                let sid = implicit_blob_source(st, &caller.sub, sha256, &d.r).await?;
                (Some(sid), d.r.clone(), parsed.clone())
            } else {
                let public: bool = c
                    .query_opt("SELECT 1 FROM raw.blobs WHERE blob_sha256 = $1", &[sha256])
                    .await
                    .map(|r| r.is_some())
                    .unwrap_or(false);
                if !public {
                    return Err(deny("no such blob, or it is someone else's private upload"));
                }
                (
                    Some(st.settings.jobs_instance.clone()),
                    d.r.clone(),
                    parsed.clone(),
                )
            }
        }
    };

    let store_id = store_of(st, &underlying);
    let site = match store_id.as_deref().or(source_id.as_deref()) {
        Some(id) => inputs::source_site(&st.pool, id)
            .await?
            .or_else(|| Some(st.settings.hub_site.clone())),
        None => None,
    };
    let size = inputs::estimate(
        &st.pool,
        &st.settings.jobs_instance,
        &underlying,
        d.max_bytes,
    )
    .await?;
    Ok(Resolved {
        declared: d.r.clone(),
        underlying: underlying_raw,
        parsed: underlying,
        access,
        source_id,
        store_id,
        site,
        size,
        window: d.window.clone(),
    })
}

/// `tb_<sha16>_<owner8>`: one private, owner-only source per (owner, blob).
async fn implicit_blob_source(st: &AppState, owner: &str, sha: &str, r: &str) -> JResult<String> {
    use sha2::{Digest, Sha256};
    let oh = hex::encode(&Sha256::digest(owner.as_bytes())[..4]);
    let sid = format!("tb_{}_{oh}", &sha[..16]);
    let c = st.pool.get().await?;
    c.execute(
        "INSERT INTO jobs.sources (source_id, home_hub, owner_sub, name, kind, definition, site, visibility)
         VALUES ($1, $2, $3, $1, 'blob_set', jsonb_build_object('ref', $4::text), $5, 'private')
         ON CONFLICT (source_id) DO NOTHING",
        &[&sid, &st.settings.hub_id, &owner, &r, &st.settings.hub_site],
    )
    .await?;
    Ok(sid)
}

async fn resolve_all(st: &AppState, caller: &Identity, spec: &JobSpec) -> JResult<Vec<Resolved>> {
    let mut out = Vec::new();
    for d in &spec.sources {
        out.push(resolve_decl(st, caller, d, Access::Read).await?);
    }
    for d in &spec.sinks {
        out.push(resolve_decl(st, caller, d, Access::Write).await?);
    }
    Ok(out)
}

fn policy() -> Policy {
    let mut p = Policy::default();
    if let Some(n) = crate::config::env_var("JOBS_REMOTE_MAX_BYTES").and_then(|v| v.parse().ok()) {
        p.remote_max_bytes = n;
    }
    p
}

async fn plan_for(
    st: &AppState,
    resolved: &[Resolved],
    runtime: &RuntimeSel,
) -> JResult<PlanOutcome> {
    let needs: Vec<SourceNeed> = resolved
        .iter()
        .map(|r| SourceNeed {
            raw_ref: r.underlying.clone(),
            parsed: r.parsed.clone(),
            access: r.access,
            // The planner routes by data store; authorization stays on
            // `source_id` (written to job_sources, re-checked by enforcement).
            source_id: r.store_id.clone().or_else(|| r.source_id.clone()),
            site: r.site.clone(),
            size: r.size,
        })
        .collect();
    let sites = inputs::parse_sites(&st.settings.jobs_sites_json)
        .map_err(|e| JobsError::new(StatusCode::SERVICE_UNAVAILABLE, "sites_misconfigured", e))?;
    let routes = inputs::load_routes(&st.pool).await?;
    Ok(planner::plan(&needs, &sites, &routes, &policy(), runtime))
}

// ───────────────────────────────────────────────────────────── plan/submit

/// Dry run: resolve, authorize, and plan, without creating a job.
pub async fn plan(st: &AppState, caller: &Identity, spec: JobSpec) -> JResult<Value> {
    require_principal(caller)?;
    let runtime = validate_spec(&spec)?;
    let resolved = resolve_all(st, caller, &spec).await?;
    let outcome = plan_for(st, &resolved, &runtime).await?;
    let plan_json = serde_json::to_value(&outcome).map_err(anyhow::Error::from)?;
    let c = st.pool.get().await?;
    let plan_id: Uuid = c
        .query_one(
            "INSERT INTO jobs.plans (owner_sub, spec, plan, expires_at)
             VALUES ($1, $2, $3, now() + interval '15 minutes') RETURNING plan_id",
            &[
                &caller.sub,
                &serde_json::to_value(&spec).map_err(anyhow::Error::from)?,
                &plan_json,
            ],
        )
        .await?
        .get(0);
    Ok(json!({"plan_id": plan_id, "plan": plan_json, "sources": resolved_view(&resolved)}))
}

fn resolved_view(rs: &[Resolved]) -> Value {
    Value::Array(
        rs.iter()
            .map(|r| {
                json!({"declared": r.declared, "ref": r.underlying, "access": r.access.as_str(),
                       "source_id": r.source_id, "site": r.site, "size": r.size, "window": r.window})
            })
            .collect(),
    )
}

fn max_concurrent() -> i64 {
    crate::config::env_var("JOBS_MAX_CONCURRENT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}
fn max_planned_bytes() -> i64 {
    crate::config::env_var("JOBS_MAX_PLANNED_BYTES")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50 * 1024 * 1024 * 1024)
}

/// Submit a job: from a spec, or from a plan_id the caller made earlier.
/// Re-resolves and re-plans either way (a plan is advice, not a grant).
pub async fn submit(
    st: &AppState,
    caller: &Identity,
    spec: Option<JobSpec>,
    plan_id: Option<Uuid>,
    idempotency_key: Option<String>,
) -> JResult<(StatusCode, Value)> {
    require_principal(caller)?;
    if let Some(k) = &idempotency_key {
        if k.is_empty() || k.len() > 128 {
            return Err(bad("invalid_idempotency_key", "1..=128 characters"));
        }
        if let Some(existing) = job_by_key(st, &caller.sub, k).await? {
            return Ok((StatusCode::OK, existing));
        }
    }
    let spec = match (spec, plan_id) {
        (Some(s), None) => s,
        (None, Some(pid)) => {
            let c = st.pool.get().await?;
            let row = c
                .query_opt(
                    "SELECT spec FROM jobs.plans WHERE plan_id = $1 AND owner_sub = $2 AND expires_at > now()",
                    &[&pid, &caller.sub],
                )
                .await?
                .ok_or_else(|| not_found("plan_not_found", "no such plan, or it expired (plans last 15 minutes)"))?;
            serde_json::from_value(row.get::<_, Value>("spec"))
                .map_err(|e| bad("invalid_spec", e.to_string()))?
        }
        _ => {
            return Err(bad(
                "invalid_request",
                "send exactly one of spec or plan_id",
            ))
        }
    };
    let runtime = validate_spec(&spec)?;
    if let Some(parent) = spec.parent_job_id {
        let c = st.pool.get().await?;
        let ok = c
            .query_opt(
                "SELECT 1 FROM jobs.jobs WHERE job_id = $1 AND owner_sub = $2",
                &[&parent, &caller.sub],
            )
            .await?
            .is_some();
        if !ok {
            return Err(not_found("parent_not_found", "parent job not found"));
        }
    }
    let resolved = resolve_all(st, caller, &spec).await?;
    let outcome = plan_for(st, &resolved, &runtime).await?;
    let spec_json = serde_json::to_value(&spec).map_err(anyhow::Error::from)?;
    let plan_json = serde_json::to_value(&outcome).map_err(anyhow::Error::from)?;

    let mut c = st.pool.get().await?;
    let tx = c.transaction().await?;
    // Serialize one owner's submits so the quota check can't be raced.
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtext($1))",
        &[&format!("jobs:{}", caller.sub)],
    )
    .await?;
    if let Some(k) = &idempotency_key {
        if let Some(existing) = job_by_key_tx(&tx, &caller.sub, k).await? {
            tx.commit().await?;
            return Ok((StatusCode::OK, existing));
        }
    }
    let (status, site, rt, est, http) = match &outcome {
        PlanOutcome::Placed(p) => (
            "planned",
            Some(p.site.clone()),
            p.runtime.clone(),
            p.local_bytes + p.remote_bytes,
            StatusCode::CREATED,
        ),
        PlanOutcome::Refused { .. } => (
            "refused",
            None,
            match &runtime {
                RuntimeSel::Named(r) => r.clone(),
                RuntimeSel::Any(v) => v[0].clone(),
            },
            0,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    };
    if status == "planned" {
        let active: i64 = tx
            .query_one(
                "SELECT count(*) FROM jobs.jobs
                  WHERE owner_sub = $1 AND status IN ('planned','submitted','running')",
                &[&caller.sub],
            )
            .await?
            .get(0);
        if active >= max_concurrent() {
            return Err(JobsError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "quota_exceeded",
                format!(
                    "{active} active jobs (limit {}); wait or cancel one",
                    max_concurrent()
                ),
            ));
        }
        if est > max_planned_bytes() {
            return Err(JobsError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "quota_exceeded",
                format!("plan moves {est} bytes (limit {})", max_planned_bytes()),
            ));
        }
    }
    let job_id: Uuid = tx
        .query_one(
            "INSERT INTO jobs.jobs
               (home_hub, owner_sub, app, runtime, site, parent_job_id, status, spec, plan,
                plan_id, idempotency_key, est_bytes, ended_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, CASE WHEN $7 = 'refused' THEN now() END)
             RETURNING job_id",
            &[
                &st.settings.hub_id,
                &caller.sub,
                &spec.app,
                &rt,
                &site,
                &spec.parent_job_id,
                &status,
                &spec_json,
                &plan_json,
                &plan_id,
                &idempotency_key,
                &est,
            ],
        )
        .await?
        .get(0);
    if let PlanOutcome::Placed(p) = &outcome {
        for (r, ps) in resolved.iter().zip(&p.sources) {
            if !r.parsed.is_data_plane() {
                continue;
            }
            tx.execute(
                "INSERT INTO jobs.job_sources
                   (job_id, ref, source_id, access, site, route_id, route_kind, url, est_bytes, home_hub)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                 ON CONFLICT (job_id, ref, access) DO NOTHING",
                &[
                    &job_id,
                    &r.underlying,
                    &r.source_id,
                    &r.access.as_str(),
                    &r.site,
                    &ps.route_id,
                    &ps.via,
                    &ps.url,
                    &match r.size {
                        Size::Exact(n) | Size::Declared(n) | Size::UpperBound(n) => Some(n),
                        Size::Unknown => None,
                    },
                    &st.settings.hub_id,
                ],
            )
            .await?;
        }
    }
    tx.execute(
        "INSERT INTO jobs.job_events (job_id, kind, status, payload, home_hub)
         VALUES ($1, 'submitted', $2, $3, $4)",
        &[
            &job_id,
            &status,
            &json!({"by": caller.sub}),
            &st.settings.hub_id,
        ],
    )
    .await?;
    tx.commit().await?;
    if status == "planned"
        && site
            .as_deref()
            .is_some_and(|s| super::dispatch::dispatches(st, &rt, s))
    {
        // Hand it to its runtime in the background; the reconciler keeps the
        // status current from there. Without a dispatcher for this runtime and
        // site the job stays `planned` for an external adapter to claim.
        let st2 = st.clone();
        tokio::spawn(async move { super::dispatch::dispatch(&st2, job_id).await });
    }
    let body = get_job_unchecked(st, job_id).await?;
    Ok((http, body))
}

async fn job_by_key(st: &AppState, owner: &str, key: &str) -> JResult<Option<Value>> {
    let c = st.pool.get().await?;
    let id: Option<Uuid> = c
        .query_opt(
            "SELECT job_id FROM jobs.jobs WHERE owner_sub = $1 AND idempotency_key = $2",
            &[&owner, &key],
        )
        .await?
        .map(|r| r.get(0));
    drop(c);
    match id {
        Some(id) => Ok(Some(get_job_unchecked(st, id).await?)),
        None => Ok(None),
    }
}

async fn job_by_key_tx(
    tx: &tokio_postgres::Transaction<'_>,
    owner: &str,
    key: &str,
) -> JResult<Option<Value>> {
    let row = tx
        .query_opt(
            "SELECT job_id, status, site, runtime, plan FROM jobs.jobs WHERE owner_sub = $1 AND idempotency_key = $2",
            &[&owner, &key],
        )
        .await?;
    Ok(row.map(|r| {
        json!({"job_id": r.get::<_, Uuid>("job_id"), "status": r.get::<_, String>("status"),
               "site": r.get::<_, Option<String>>("site"), "runtime": r.get::<_, String>("runtime"),
               "plan": r.get::<_, Option<Value>>("plan")})
    }))
}

// ───────────────────────────────────────────────────────────── job reads

const JOB_COLS: &str =
    "job_id, home_hub, owner_sub, app, runtime, runtime_ref, site, parent_job_id,
    status, spec, plan, est_bytes, error, created_at, started_at, ended_at, updated_at";

fn job_json(r: &tokio_postgres::Row) -> Value {
    json!({
        "job_id": r.get::<_, Uuid>("job_id"),
        "hub": r.get::<_, String>("home_hub"),
        "owner": r.get::<_, String>("owner_sub"),
        "app": r.get::<_, String>("app"),
        "runtime": r.get::<_, String>("runtime"),
        "runtime_ref": r.get::<_, Option<String>>("runtime_ref"),
        "site": r.get::<_, Option<String>>("site"),
        "parent_job_id": r.get::<_, Option<Uuid>>("parent_job_id"),
        "status": r.get::<_, String>("status"),
        "spec": r.get::<_, Value>("spec"),
        "plan": r.get::<_, Option<Value>>("plan"),
        "est_bytes": r.get::<_, Option<i64>>("est_bytes"),
        "error": r.get::<_, Option<Value>>("error"),
        "created_at": r.get::<_, DateTime<Utc>>("created_at"),
        "started_at": r.get::<_, Option<DateTime<Utc>>>("started_at"),
        "ended_at": r.get::<_, Option<DateTime<Utc>>>("ended_at"),
        "updated_at": r.get::<_, DateTime<Utc>>("updated_at"),
    })
}

/// A job as the hub sees it, without an access check (dispatcher/reconciler).
pub(crate) async fn job_snapshot(st: &AppState, job_id: Uuid) -> JResult<Value> {
    get_job_unchecked(st, job_id).await
}

async fn get_job_unchecked(st: &AppState, job_id: Uuid) -> JResult<Value> {
    let c = st.pool.get().await?;
    let row = c
        .query_opt(
            &format!("SELECT {JOB_COLS} FROM jobs.jobs WHERE job_id = $1"),
            &[&job_id],
        )
        .await?
        .ok_or_else(|| not_found("job_not_found", "no such job"))?;
    let mut j = job_json(&row);
    let srcs = c
        .query(
            "SELECT ref, source_id, access, site, route_kind, url, est_bytes, actual_bytes
               FROM jobs.job_sources WHERE job_id = $1 ORDER BY ref, access",
            &[&job_id],
        )
        .await?;
    j["sources"] = Value::Array(
        srcs.iter()
            .map(|r| {
                json!({"ref": r.get::<_, String>("ref"), "source_id": r.get::<_, Option<String>>("source_id"),
                       "access": r.get::<_, String>("access"), "site": r.get::<_, Option<String>>("site"),
                       "via": r.get::<_, Option<String>>("route_kind"), "url": r.get::<_, Option<String>>("url"),
                       "est_bytes": r.get::<_, Option<i64>>("est_bytes"),
                       "actual_bytes": r.get::<_, i64>("actual_bytes")})
            })
            .collect(),
    );
    Ok(j)
}

/// A job, if the caller owns it (or is privileged). Someone else's job is
/// indistinguishable from a missing one.
pub async fn get_job(st: &AppState, caller: &Identity, job_id: Uuid) -> JResult<Value> {
    let j = get_job_unchecked(st, job_id).await?;
    if may_see_job(caller, &j, job_id) {
        Ok(j)
    } else {
        Err(not_found("job_not_found", "no such job"))
    }
}

/// The owner (as a person/agent, not as some other job of theirs), the job
/// itself, the runtime it was dispatched to, or a super_admin. Not "any local
/// key": local keys are service credentials, not operators.
fn may_see_job(caller: &Identity, j: &Value, job_id: Uuid) -> bool {
    let owner = j["owner"].as_str() == Some(caller.sub.as_str());
    (owner && caller.role != "job")
        || is_this_job(caller, job_id)
        || is_runtime_for(caller, j["runtime"].as_str().unwrap_or(""))
        || caller.role == "super_admin"
}

pub async fn list_jobs(
    st: &AppState,
    caller: &Identity,
    status: Option<String>,
    app: Option<String>,
    limit: i64,
) -> JResult<Value> {
    require_principal(caller)?;
    let c = st.pool.get().await?;
    let rows = c
        .query(
            &format!(
                "SELECT {JOB_COLS} FROM jobs.jobs
                  WHERE owner_sub = $1 AND deleted_at IS NULL
                    AND ($2::text IS NULL OR status = $2) AND ($3::text IS NULL OR app = $3)
                  ORDER BY created_at DESC LIMIT $4"
            ),
            &[&caller.sub, &status, &app, &limit.clamp(1, 500)],
        )
        .await?;
    Ok(
        json!({"jobs": rows.iter().map(job_json).collect::<Vec<_>>(), "hubs": [{"id": st.settings.hub_id, "ok": true}]}),
    )
}

const TERMINAL: &[&str] = &["succeeded", "failed", "cancelled", "refused"];

pub async fn cancel(st: &AppState, caller: &Identity, job_id: Uuid) -> JResult<Value> {
    require_principal(caller)?;
    let j = get_job(st, caller, job_id).await?;
    let status = j["status"].as_str().unwrap_or("");
    if TERMINAL.contains(&status) {
        return Err(JobsError::new(
            StatusCode::CONFLICT,
            "job_already_ended",
            format!("job is {status}"),
        ));
    }
    transition(st, job_id, "cancelled", None, json!({"by": caller.sub})).await?;
    let st2 = st.clone();
    let snapshot = j.clone();
    tokio::spawn(async move { super::dispatch::cancel_at_runtime(&st2, &snapshot).await });
    get_job_unchecked(st, job_id).await
}

/// Move a job to `status` (recording an event), stamping started/ended and
/// revoking its tokens when it ends.
pub(crate) async fn transition(
    st: &AppState,
    job_id: Uuid,
    status: &str,
    runtime_ref: Option<&str>,
    payload: Value,
) -> JResult<()> {
    let c = st.pool.get().await?;
    c.execute(
        "UPDATE jobs.jobs SET status = $2, updated_at = now(),
                runtime_ref = COALESCE($3, runtime_ref),
                started_at = CASE WHEN $2 = 'running' AND started_at IS NULL THEN now() ELSE started_at END,
                ended_at = CASE WHEN $2 IN ('succeeded','failed','cancelled') THEN now() ELSE ended_at END
          WHERE job_id = $1",
        &[&job_id, &status, &runtime_ref],
    )
    .await?;
    c.execute(
        "INSERT INTO jobs.job_events (job_id, kind, status, payload, home_hub) VALUES ($1, 'status', $2, $3, $4)",
        &[&job_id, &status, &payload, &st.settings.hub_id],
    )
    .await?;
    if TERMINAL.contains(&status) {
        token::revoke_job(&st.pool, job_id).await?;
    }
    Ok(())
}

/// Allowed lifecycle moves.
pub(crate) fn can_move(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            "planned",
            "submitted" | "running" | "succeeded" | "failed" | "cancelled"
        ) | (
            "submitted",
            "running" | "succeeded" | "failed" | "cancelled"
        ) | ("running", "succeeded" | "failed" | "cancelled")
    )
}

/// Is `caller` the runtime this job was dispatched to? Runtime service keys
/// are local keys labelled `runtime:<name>` (sub `local:runtime:<name>`).
fn is_runtime_for(caller: &Identity, runtime: &str) -> bool {
    caller.role == "local" && caller.sub == format!("local:runtime:{runtime}")
}

fn is_this_job(caller: &Identity, job_id: Uuid) -> bool {
    caller.role == "job" && caller.scopes.iter().any(|s| *s == format!("job:{job_id}"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventIn {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub runtime_ref: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub metrics: Option<Value>,
}

/// A runtime (or the job itself) reports progress.
pub async fn post_event(
    st: &AppState,
    caller: &Identity,
    job_id: Uuid,
    ev: EventIn,
) -> JResult<Value> {
    let j = get_job_unchecked(st, job_id).await?;
    let runtime = j["runtime"].as_str().unwrap_or("");
    if !(is_this_job(caller, job_id)
        || is_runtime_for(caller, runtime)
        || caller.role == "super_admin")
    {
        return Err(not_found("job_not_found", "no such job"));
    }
    if ev.message.as_ref().is_some_and(|m| m.len() > 4096) {
        return Err(bad("invalid_event", "message longer than 4096"));
    }
    let payload = json!({"by": caller.sub, "message": ev.message, "metrics": ev.metrics, "runtime_ref": ev.runtime_ref});
    match ev.status.as_deref() {
        Some(to) => {
            let from = j["status"].as_str().unwrap_or("");
            if !can_move(from, to) {
                return Err(JobsError::new(
                    StatusCode::CONFLICT,
                    "invalid_transition",
                    format!("{from} -> {to} is not allowed"),
                ));
            }
            transition(st, job_id, to, ev.runtime_ref.as_deref(), payload).await?;
        }
        None => {
            let c = st.pool.get().await?;
            c.execute(
                "INSERT INTO jobs.job_events (job_id, kind, payload, home_hub) VALUES ($1, 'log', $2, $3)",
                &[&job_id, &payload, &st.settings.hub_id],
            )
            .await?;
        }
    }
    get_job_unchecked(st, job_id).await
}

pub async fn list_events(st: &AppState, caller: &Identity, job_id: Uuid) -> JResult<Value> {
    get_job(st, caller, job_id).await?;
    let c = st.pool.get().await?;
    let rows = c
        .query(
            "SELECT id, at, kind, status, payload FROM jobs.job_events WHERE job_id = $1 ORDER BY id LIMIT 1000",
            &[&job_id],
        )
        .await?;
    Ok(json!({"events": rows.iter().map(|r| json!({
        "id": r.get::<_, i64>("id"), "at": r.get::<_, DateTime<Utc>>("at"), "kind": r.get::<_, String>("kind"),
        "status": r.get::<_, Option<String>>("status"), "payload": r.get::<_, Value>("payload")})).collect::<Vec<_>>()}))
}

/// Issue a job token — to the runtime the job was dispatched to (with its own
/// service key), or to the job itself (renewal). Never to a person: a token
/// a human could copy out would outlive the job's purpose.
/// Issue a job token. Callers: the job itself (refresh), or the runtime the
/// job belongs to, which must say WHICH user it authenticated (`for_sub`).
///
/// A runtime takes `lumid_job.job_id` from a submit request, and the submitter
/// is not trusted to name only their own jobs: without this check, anyone able
/// to submit to a runtime could quote another user's job id and have the
/// runtime fetch that job's token — their workload reading the victim's data.
/// Each runtime could compare owners itself, but one that forgets fails open;
/// here it fails closed, for every runtime at once.
pub async fn issue_token(
    st: &AppState,
    caller: &Identity,
    job_id: Uuid,
    for_sub: Option<String>,
) -> JResult<Value> {
    let j = get_job_unchecked(st, job_id).await?;
    let runtime = j["runtime"].as_str().unwrap_or("");
    let as_runtime = is_runtime_for(caller, runtime);
    if !(as_runtime || is_this_job(caller, job_id)) {
        return Err(not_found("job_not_found", "no such job"));
    }
    if as_runtime {
        let Some(sub) = for_sub.filter(|s| !s.is_empty()) else {
            return Err(bad(
                "for_sub_required",
                "a runtime must pass for_sub: the lum.id sub of the user whose request carried this job id",
            ));
        };
        if j["owner"].as_str() != Some(sub.as_str()) {
            return Err(forbidden(
                "not_the_owner",
                "this job does not belong to the user the runtime authenticated",
            ));
        }
    }
    let status = j["status"].as_str().unwrap_or("");
    if TERMINAL.contains(&status) {
        return Err(JobsError::new(
            StatusCode::CONFLICT,
            "job_already_ended",
            format!("job is {status}"),
        ));
    }
    let (tok, exp) = token::mint(&st.pool, job_id, token::MAX_TTL).await?;
    Ok(json!({"token": tok, "expires_at": exp, "job_id": job_id}))
}

// ───────────────────────────────────────────────────────────── sources

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIn {
    pub name: String,
    #[serde(rename = "ref")]
    pub r: String,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub max_bytes: Option<i64>,
    #[serde(default)]
    pub description: Option<String>,
}

const SOURCE_COLS: &str =
    "source_id, home_hub, owner_sub, name, kind, definition, site, visibility, size_bytes_est, created_at, updated_at";

fn source_json(r: &tokio_postgres::Row) -> Value {
    json!({
        "source_id": r.get::<_, String>("source_id"), "hub": r.get::<_, String>("home_hub"),
        "owner": r.get::<_, String>("owner_sub"), "name": r.get::<_, String>("name"),
        "kind": r.get::<_, String>("kind"), "definition": r.get::<_, Value>("definition"),
        "site": r.get::<_, Option<String>>("site"), "visibility": r.get::<_, String>("visibility"),
        "size_bytes_est": r.get::<_, Option<i64>>("size_bytes_est"),
        "created_at": r.get::<_, DateTime<Utc>>("created_at"), "updated_at": r.get::<_, DateTime<Utc>>("updated_at"),
    })
}

/// Register a source the caller owns. It must point at data on THIS hub, and
/// the caller must be able to read that data themselves — you can only share
/// what you can see.
pub async fn create_source(st: &AppState, caller: &Identity, s: SourceIn) -> JResult<Value> {
    require_principal(caller)?;
    if !is_slug(&s.name, 96) {
        return Err(bad("invalid_source", "name must be [A-Za-z0-9._-], 1..=96"));
    }
    let vis = s.visibility.unwrap_or_else(|| "private".into());
    if !matches!(vis.as_str(), "private" | "shared" | "public") {
        return Err(bad(
            "invalid_source",
            "visibility is private, shared, or public",
        ));
    }
    let parsed = refs::parse(&s.r).map_err(|e| bad("invalid_ref", e.0))?;
    let kind = match &parsed {
        SourceRef::Table { instance, .. } => {
            if !home_instances(st).contains(instance) {
                return Err(bad(
                    "source_not_on_this_hub",
                    format!("{instance} is not served by hub {}", st.settings.hub_id),
                ));
            }
            "table"
        }
        SourceRef::Blob { hub, .. } | SourceRef::BlobPrefix { hub, .. } => {
            if *hub != st.settings.hub_id {
                return Err(bad(
                    "source_not_on_this_hub",
                    format!("register it on hub {hub}"),
                ));
            }
            "blob_set"
        }
        SourceRef::Model { .. } => "model",
        SourceRef::External { .. } => "external",
        SourceRef::SourceId { .. } | SourceRef::SourceName { .. } => {
            return Err(bad(
                "invalid_source",
                "a source cannot point at another source",
            ))
        }
    };
    if parsed.is_data_plane() {
        // You can only register (and so share) what you can read yourself.
        resolve_decl(
            st,
            caller,
            &SourceDecl {
                r: s.r.clone(),
                max_bytes: None,
                window: None,
            },
            Access::Read,
        )
        .await?;
    }
    let sid = format!("src_{}", Uuid::new_v4().simple());
    let c = st.pool.get().await?;
    let def = json!({"ref": s.r, "max_bytes": s.max_bytes, "description": s.description});
    let row = c
        .query_opt(
            &format!(
                "INSERT INTO jobs.sources (source_id, home_hub, owner_sub, name, kind, definition, site, visibility)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT (home_hub, owner_sub, name) DO NOTHING RETURNING {SOURCE_COLS}"
            ),
            &[&sid, &st.settings.hub_id, &caller.sub, &s.name, &kind, &def, &st.settings.hub_site, &vis],
        )
        .await?
        .ok_or_else(|| JobsError::new(StatusCode::CONFLICT, "source_exists", format!("you already have a source named {}", s.name)))?;
    Ok(source_json(&row))
}

async fn owned_source(
    st: &AppState,
    caller: &Identity,
    source_id: &str,
) -> JResult<tokio_postgres::Row> {
    let c = st.pool.get().await?;
    c.query_opt(
        &format!(
            "SELECT {SOURCE_COLS} FROM jobs.sources WHERE source_id = $1 AND deleted_at IS NULL"
        ),
        &[&source_id],
    )
    .await?
    .filter(|r| {
        r.get::<_, String>("owner_sub") == caller.sub
            && r.get::<_, String>("home_hub") == st.settings.hub_id
    })
    .ok_or_else(|| not_found("source_not_found", "no such source of yours on this hub"))
}

pub async fn delete_source(st: &AppState, caller: &Identity, source_id: &str) -> JResult<Value> {
    require_principal(caller)?;
    owned_source(st, caller, source_id).await?;
    let c = st.pool.get().await?;
    c.execute(
        "UPDATE jobs.sources SET deleted_at = now(), updated_at = now() WHERE source_id = $1",
        &[&source_id],
    )
    .await?;
    token::invalidate_all();
    Ok(json!({"source_id": source_id, "deleted": true}))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantIn {
    /// The grantee's lum.id `sub`.
    pub grantee: String,
    #[serde(default = "read_access")]
    pub access: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}
fn read_access() -> String {
    "read".into()
}

pub async fn share(
    st: &AppState,
    caller: &Identity,
    source_id: &str,
    g: GrantIn,
) -> JResult<Value> {
    require_principal(caller)?;
    let row = owned_source(st, caller, source_id).await?;
    let access =
        Access::parse(&g.access).ok_or_else(|| bad("invalid_grant", "access is read or write"))?;
    if !is_slug(&g.grantee, 128) || g.grantee == caller.sub {
        return Err(bad(
            "invalid_grant",
            "grantee must be another user's lum.id sub",
        ));
    }
    if g.expires_at.is_some_and(|t| t <= Utc::now()) {
        return Err(bad("invalid_grant", "expires_at is in the past"));
    }
    let c = st.pool.get().await?;
    c.execute(
        "INSERT INTO jobs.source_grants (source_id, grantee_sub, access, granted_by, home_hub, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (source_id, grantee_sub, access) DO UPDATE
           SET revoked_at = NULL, expires_at = EXCLUDED.expires_at, granted_by = EXCLUDED.granted_by,
               updated_at = now()",
        &[&source_id, &g.grantee, &access.as_str(), &caller.sub, &st.settings.hub_id, &g.expires_at],
    )
    .await?;
    // A private source that is now shared says so.
    if row.get::<_, String>("visibility") == "private" {
        c.execute(
            "UPDATE jobs.sources SET visibility = 'shared', updated_at = now() WHERE source_id = $1",
            &[&source_id],
        )
        .await?;
    }
    Ok(
        json!({"source_id": source_id, "grantee": g.grantee, "access": access.as_str(), "expires_at": g.expires_at}),
    )
}

pub async fn unshare(
    st: &AppState,
    caller: &Identity,
    source_id: &str,
    grantee: &str,
    access: &str,
) -> JResult<Value> {
    require_principal(caller)?;
    owned_source(st, caller, source_id).await?;
    let c = st.pool.get().await?;
    let n = c
        .execute(
            "UPDATE jobs.source_grants SET revoked_at = now(), updated_at = now()
              WHERE source_id = $1 AND grantee_sub = $2 AND access = $3 AND revoked_at IS NULL",
            &[&source_id, &grantee, &access],
        )
        .await?;
    // This process re-checks at once; other replicas within token::SCOPE_TTL.
    token::invalidate_all();
    Ok(json!({"source_id": source_id, "grantee": grantee, "access": access, "revoked": n == 1}))
}

/// Sources the caller may use: `mine`, `shared` (with me), `public`, or all.
pub async fn list_sources(
    st: &AppState,
    caller: &Identity,
    scope: &str,
    q: Option<String>,
) -> JResult<Value> {
    require_principal(caller)?;
    let where_scope = match scope {
        "mine" => "s.owner_sub = $1",
        "shared" => {
            "EXISTS (SELECT 1 FROM jobs.source_grants g WHERE g.source_id = s.source_id
                       AND g.grantee_sub = $1 AND g.revoked_at IS NULL
                       AND (g.expires_at IS NULL OR g.expires_at > now()))"
        }
        "public" => "s.visibility = 'public'",
        "all" | "" => {
            "(s.owner_sub = $1 OR s.visibility = 'public' OR EXISTS (
                       SELECT 1 FROM jobs.source_grants g WHERE g.source_id = s.source_id
                       AND g.grantee_sub = $1 AND g.revoked_at IS NULL
                       AND (g.expires_at IS NULL OR g.expires_at > now())))"
        }
        other => {
            return Err(bad(
                "invalid_scope",
                format!("scope '{other}' (mine, shared, public, all)"),
            ))
        }
    };
    let pattern = q.map(|q| {
        format!(
            "%{}%",
            q.replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        )
    });
    let c = st.pool.get().await?;
    let rows = c
        .query(
            &format!(
                "SELECT {} FROM jobs.sources s WHERE s.deleted_at IS NULL AND {where_scope}
                   AND ($2::text IS NULL OR s.name ILIKE $2 OR s.definition->>'ref' ILIKE $2)
                 ORDER BY s.home_hub, s.owner_sub = 'system' DESC, s.name LIMIT 500",
                SOURCE_COLS
                    .split(", ")
                    .map(|c| format!("s.{}", c.trim()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            &[&caller.sub, &pattern],
        )
        .await?;
    Ok(
        json!({"sources": rows.iter().map(source_json).collect::<Vec<_>>(), "hubs": [{"id": st.settings.hub_id, "ok": true}]}),
    )
}

/// A prober reports route health (local keys labelled `prober:<site>`).
fn is_prober_or_super(caller: &Identity) -> bool {
    (caller.role == "local" && caller.sub.starts_with("local:prober:"))
        || caller.role == "super_admin"
}

/// The routes a prober should probe. A `prober:<site>` key sees only its own
/// site's routes (asking for another site is refused, not silently widened);
/// super_admin may ask for any site or all.
pub async fn list_routes(
    st: &AppState,
    caller: &Identity,
    from_site: Option<String>,
) -> JResult<Value> {
    if !is_prober_or_super(caller) {
        return Err(forbidden("not_a_prober", "route lists are for prober keys"));
    }
    let own = caller.sub.strip_prefix("local:prober:").map(str::to_string);
    let site = match (&own, from_site) {
        (Some(o), Some(f)) if *o != f => {
            return Err(forbidden(
                "not_your_site",
                format!("this prober key is for site {o}"),
            ))
        }
        (Some(o), _) => Some(o.clone()),
        (None, f) => f,
    };
    let routes = inputs::list_routes(&st.pool, &st.settings.hub_id, site.as_deref())
        .await
        .map_err(|e| {
            JobsError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "routes_unavailable",
                e.to_string(),
            )
        })?;
    Ok(json!({"hub": st.settings.hub_id, "from_site": site, "routes": routes}))
}

pub async fn route_health(
    st: &AppState,
    caller: &Identity,
    route_id: &str,
    health: &str,
    latency_ms: Option<i32>,
) -> JResult<Value> {
    if !is_prober_or_super(caller) {
        return Err(forbidden(
            "not_a_prober",
            "route health is reported by prober keys",
        ));
    }
    let ok =
        inputs::record_route_health(&st.pool, &st.settings.hub_id, route_id, health, latency_ms)
            .await
            .map_err(|e| bad("invalid_health", e.to_string()))?;
    if !ok {
        return Err(not_found(
            "route_not_found",
            "no such route owned by this hub",
        ));
    }
    Ok(json!({"route_id": route_id, "health": health, "latency_ms": latency_ms}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(sub: &str, role: &str, scopes: &[&str]) -> Identity {
        Identity {
            sub: sub.into(),
            role: role.into(),
            email: None,
            active: true,
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn lifecycle_moves() {
        assert!(can_move("planned", "submitted"));
        assert!(can_move("submitted", "running"));
        assert!(can_move("running", "succeeded"));
        assert!(!can_move("succeeded", "running"));
        assert!(!can_move("refused", "running"));
        assert!(!can_move("running", "planned"));
        assert!(!can_move("cancelled", "succeeded"));
    }

    #[test]
    fn who_may_act_on_a_job() {
        let j = Uuid::new_v4();
        assert!(is_runtime_for(
            &ident("local:runtime:flowmesh", "local", &[]),
            "flowmesh"
        ));
        assert!(!is_runtime_for(
            &ident("local:runtime:flowmesh", "local", &[]),
            "lumilake"
        ));
        assert!(!is_runtime_for(
            &ident("local:runtime:flowmesh", "user", &[]),
            "flowmesh"
        ));
        assert!(is_this_job(&ident("bob", "job", &[&format!("job:{j}")]), j));
        assert!(!is_this_job(
            &ident("bob", "job", &[&format!("job:{}", Uuid::new_v4())]),
            j
        ));
        assert!(!is_this_job(
            &ident("bob", "user", &[&format!("job:{j}")]),
            j
        ));
        assert!(require_principal(&ident("bob", "job", &[])).is_err());
        assert!(require_principal(&ident("bob", "user", &[])).is_ok());
    }

    #[test]
    fn job_visibility() {
        let id = Uuid::new_v4();
        let j = json!({"owner": "bob", "runtime": "lumilake"});
        assert!(may_see_job(&ident("bob", "user", &[]), &j, id));
        assert!(!may_see_job(&ident("alice", "user", &[]), &j, id));
        assert!(!may_see_job(&ident("alice", "admin", &[]), &j, id));
        assert!(may_see_job(&ident("root", "super_admin", &[]), &j, id));
        assert!(may_see_job(
            &ident("local:runtime:lumilake", "local", &[]),
            &j,
            id
        ));
        assert!(!may_see_job(
            &ident("local:runtime:flowmesh", "local", &[]),
            &j,
            id
        ));
        assert!(!may_see_job(&ident("local:e2e", "local", &[]), &j, id));
        // A service account sees the jobs it submitted itself.
        let mine = json!({"owner": "local:e2e", "runtime": "lumilake"});
        assert!(may_see_job(&ident("local:e2e", "local", &[]), &mine, id));
        // Bob's OTHER job can't read this one.
        assert!(!may_see_job(
            &ident("bob", "job", &[&format!("job:{}", Uuid::new_v4())]),
            &j,
            id
        ));
        assert!(may_see_job(
            &ident("bob", "job", &[&format!("job:{id}")]),
            &j,
            id
        ));
    }

    #[test]
    fn spec_validation() {
        let mut s: JobSpec = serde_json::from_value(json!({
            "app": "lqt", "runtime": "auto", "sources": [{"ref": "lumid-data://a.b"}]
        }))
        .unwrap();
        assert_eq!(validate_spec(&s).unwrap(), RuntimeSel::auto());
        s.runtime = "k8s".into();
        assert_eq!(validate_spec(&s).unwrap_err().code, "invalid_spec");
        s.runtime = "lumilake".into();
        s.app = "bad app".into();
        assert!(validate_spec(&s).is_err());
        s.app = "lqt".into();
        s.sources.clear();
        assert!(validate_spec(&s).is_err());
        assert!(serde_json::from_value::<JobSpec>(
            json!({"app": "x", "runtime": "auto", "surprise": 1})
        )
        .is_err());
    }

    #[test]
    fn errors_carry_code_and_message() {
        let e = forbidden("source_not_entitled", "nope").with(json!({"ref": "x"}));
        let b = e.body();
        assert_eq!(b["error"]["code"], "source_not_entitled");
        assert_eq!(b["error"]["details"]["ref"], "x");
        assert_eq!(e.status, StatusCode::FORBIDDEN);
    }
}
