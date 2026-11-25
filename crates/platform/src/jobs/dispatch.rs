//! Dispatch planned jobs to their runtime, and keep their status current.
//!
//! The hub submits **as the job's owner**: a 15-minute lum.id bridge token is
//! minted for the owner's `sub`, so the runtime records the real user (not an
//! operator credential) and applies that user's own permissions. Only
//! Lumilake is wired so far; FlowMesh and sandbox follow the same shape.
//!
//! The submit body is the job's `payload` (the runtime's native request) plus a
//! `lumid_job` binding, with which Lumilake fetches the job-scoped data token
//! itself (`POST /jobs/v1/jobs/{id}/token` with its runtime key) — the token
//! never travels in this request.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::service;
use crate::state::AppState;

/// `LUMID_JOBS_RUNTIMES_JSON`: `{"lumilake": {"<site>": "<base url>", …}}`.
pub type RuntimeUrls = HashMap<String, HashMap<String, String>>;

pub fn parse_runtimes(raw: &str) -> Result<RuntimeUrls, String> {
    if raw.trim().is_empty() {
        return Ok(HashMap::new());
    }
    let v: RuntimeUrls =
        serde_json::from_str(raw).map_err(|e| format!("LUMID_JOBS_RUNTIMES_JSON: {e}"))?;
    for (rt, sites) in &v {
        if !matches!(rt.as_str(), "flowmesh" | "lumilake" | "sandbox") {
            return Err(format!("LUMID_JOBS_RUNTIMES_JSON: unknown runtime '{rt}'"));
        }
        for (site, url) in sites {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(format!(
                    "LUMID_JOBS_RUNTIMES_JSON: {rt}@{site} url must be http(s)"
                ));
            }
        }
    }
    Ok(v)
}

fn base_url(st: &AppState, runtime: &str, site: &str) -> Result<String, String> {
    let urls = parse_runtimes(&st.settings.jobs_runtimes_json)?;
    urls.get(runtime)
        .and_then(|s| s.get(site))
        .map(|u| u.trim_end_matches('/').to_string())
        .ok_or_else(|| format!("no {runtime} endpoint configured for site {site}"))
}

#[derive(Deserialize)]
struct MintData {
    token: String,
}
#[derive(Deserialize)]
struct MintResp {
    data: MintData,
}

/// Scope that lets a principal bind a submission to a lum.id/data job. No
/// user can mint it on a PAT (identity does not grant it), so a runtime that
/// requires it knows the binding came from a hub.
pub const JOB_BIND_SCOPE: &str = "lumid-job:bind";

/// Scopes on the owner token the hub dispatches with: the runtime's own
/// submit/read scopes plus the bind scope. Nothing broader.
pub fn owner_scopes(runtime: &str) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = match runtime {
        "lumilake" => vec![
            "lumilake:jobs:write",
            "lumilake:jobs:read",
            "lumilake:jobs:cancel",
        ],
        "flowmesh" => vec![
            "flowmesh:workflows:write",
            "flowmesh:workflows:read",
            "flowmesh:tasks:read",
            "flowmesh:results:read",
        ],
        _ => vec![],
    };
    v.push(JOB_BIND_SCOPE);
    v
}

/// A short-lived lum.id token for `sub`, so the runtime sees the real user.
/// Its audience is the runtime, and its scopes are `owner_scopes(runtime)`.
async fn mint_for_owner(st: &AppState, sub: &str, runtime: &str) -> Result<String, String> {
    let audience = runtime;
    if sub.starts_with("local:") {
        return Err("jobs owned by a service key cannot be dispatched as a user".into());
    }
    let secret = st.settings.bridge_secret.as_str();
    if secret.is_empty() {
        return Err("LUMID_BRIDGE_SECRET is not configured on this hub".into());
    }
    let resp = st
        .http
        .post(format!(
            "{}/api/v1/internal/mint-user-token",
            st.settings.bridge_url.trim_end_matches('/')
        ))
        .header("X-Bridge-Secret", secret)
        .json(&json!({"sub": sub, "audience": audience, "scopes": owner_scopes(runtime), "ttl_seconds": 900}))
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("lum.id unreachable while minting: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "lum.id refused to mint for the owner (HTTP {})",
            resp.status()
        ));
    }
    resp.json::<MintResp>()
        .await
        .map(|m| m.data.token)
        .map_err(|e| format!("lum.id mint response: {e}"))
}

/// Lumilake base URL -> (supports `lumid_job`, checked at). Positive answers
/// are reused for 10 min, negative for 1 min (so a release is picked up fast).
static CAPABILITY: Mutex<Option<HashMap<String, (bool, Instant)>>> = Mutex::new(None);

/// Does the runtime at `base` understand `lumid_job`? Answered from its
/// published OpenAPI schema. Without it, a runtime's request model may
/// silently IGNORE the binding and the job would read with a server-wide
/// token — unscoped, while the hub believed otherwise — so the hub refuses.
async fn runtime_supports_jobs(st: &AppState, base: &str, token: &str) -> Result<bool, String> {
    if let Some((ok, at)) = CAPABILITY
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .get(base)
        .copied()
    {
        let ttl = if ok {
            Duration::from_secs(600)
        } else {
            Duration::from_secs(60)
        };
        if at.elapsed() < ttl {
            return Ok(ok);
        }
    }
    let resp = st
        .http
        .get(format!("{base}/openapi.json"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| format!("{base} unreachable (schema check): {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "{base} schema check failed (HTTP {})",
            resp.status()
        ));
    }
    let body = resp
        .text()
        .await
        .map_err(|e| format!("{base} schema: {e}"))?;
    let ok = schema_supports_jobs(&body);
    CAPABILITY
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .insert(base.to_string(), (ok, Instant::now()));
    Ok(ok)
}

/// Whether a Lumilake OpenAPI document shows `lumid_job` support: the
/// `LumidJobBinding` schema, or a Lumilake release that shipped it (>= 0.1.11).
/// The version matters because Lumilake's submit route publishes a hand-written
/// request schema that omits the field, so 0.1.11 accepts `lumid_job` while its
/// schema never mentions it (found when switching job tokens on, 2026-11-25).
fn schema_supports_jobs(body: &str) -> bool {
    if body.contains("LumidJobBinding") {
        return true;
    }
    let version = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v["info"]["version"].as_str().map(str::to_string))
        .unwrap_or_default();
    let nums: Vec<u64> = version
        .trim_start_matches('v')
        .split(|c: char| c == '.' || c == '-' || c == '+')
        .take(3)
        .map(|p| p.parse().unwrap_or(0))
        .collect();
    let (a, b, c) = (
        *nums.first().unwrap_or(&0),
        *nums.get(1).unwrap_or(&0),
        *nums.get(2).unwrap_or(&0),
    );
    (a, b, c) >= (0, 1, 11)
}

#[cfg(test)]
mod capability_tests {
    use super::schema_supports_jobs;

    #[test]
    fn binding_schema_or_a_release_that_shipped_it() {
        assert!(schema_supports_jobs(r#"{"components":{"schemas":{"LumidJobBinding":{}}}}"#));
        assert!(schema_supports_jobs(r#"{"info":{"version":"0.1.11"}}"#));
        assert!(schema_supports_jobs(r#"{"info":{"version":"v0.2.0"}}"#));
        assert!(!schema_supports_jobs(r#"{"info":{"version":"0.1.10"}}"#));
        assert!(!schema_supports_jobs(r#"{"info":{}}"#));
        assert!(!schema_supports_jobs("not json"));
    }
}

/// The data URL the plan chose for this job's data (first data-plane source
/// with a route URL); `None` lets the runtime use its own default.
fn planned_data_url(job: &Value) -> Option<String> {
    job["sources"]
        .as_array()?
        .iter()
        .find_map(|s| s["url"].as_str().map(str::to_string))
}

/// Does this hub dispatch `runtime` jobs at `site` itself? When it doesn't, a
/// planned job stays `planned` for an external runtime adapter to claim (with
/// its runtime key: token + events) — the hub never fails a job just because
/// it has no dispatcher for it.
pub fn dispatches(st: &AppState, runtime: &str, site: &str) -> bool {
    matches!(runtime, "lumilake" | "flowmesh" | "sandbox") && base_url(st, runtime, site).is_ok()
}

/// The binding FlowMesh and the sandbox accept: the job, its planned data
/// URL, its site, and each source's planned route. Never a token.
fn full_binding(job_id: Uuid, job: &Value, site: &str) -> Result<Value, String> {
    let sources: Vec<Value> = job["sources"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| {
                    json!({
                        "ref": s["ref"],
                        "url": s["url"],
                        "route_kind": s["via"],
                        "local": s["via"] == "local" || s["site"].as_str() == Some(site),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let data_url = planned_data_url(job)
        .ok_or("no planned data URL for this job (it declares no data-plane source)")?;
    Ok(
        json!({"job_id": job_id.to_string(), "data_url": data_url, "site": site, "sources": sources}),
    )
}

/// Submit to a runtime that reports its own status back to the hub (FlowMesh,
/// the sandbox): check it understands `lumid_job`, post as the owner, record
/// the runtime ref it returns.
async fn submit_as_owner(
    st: &AppState,
    job_id: Uuid,
    runtime: &str,
    site: &str,
    owner: &str,
    path: &str,
    body: &Value,
    ref_of: fn(&Value) -> Option<String>,
) -> Result<(), String> {
    let base = base_url(st, runtime, site)?;
    let token = mint_for_owner(st, owner, runtime).await?;
    if !runtime_supports_jobs(st, &base, &token).await? {
        return Err(format!(
            "the {runtime} at {base} does not support job-scoped data (no LumidJobBinding in \
             its OpenAPI); refusing to dispatch rather than let it read unscoped"
        ));
    }
    let resp = st
        .http
        .post(format!("{base}{path}"))
        .bearer_auth(token)
        .json(body)
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| format!("{runtime} unreachable: {e}"))?;
    let status = resp.status();
    let out: Value = resp.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        let detail = out.get("detail").cloned().unwrap_or(out);
        return Err(format!(
            "{runtime} refused the job (HTTP {status}): {detail}"
        ));
    }
    let rref = ref_of(&out).ok_or_else(|| format!("{runtime} returned no job reference"))?;
    service::transition(
        st,
        job_id,
        "submitted",
        Some(&rref),
        json!({"by": "hub", "runtime": runtime, "site": site, "runtime_ref": rref}),
    )
    .await
    .map_err(|e| e.message)?;
    Ok(())
}

fn flowmesh_ref(v: &Value) -> Option<String> {
    v["workflow_id"]
        .as_str()
        .or_else(|| v["data"]["workflow_id"].as_str())
        .map(str::to_string)
}

fn sandbox_ref(v: &Value) -> Option<String> {
    v["runtime_ref"]
        .as_str()
        .or_else(|| v["data"]["runtime_ref"].as_str())
        .map(str::to_string)
}

/// Dispatch one planned job. Idempotent per state: only a `planned` job with
/// no runtime ref is sent.
pub async fn dispatch(st: &AppState, job_id: Uuid) {
    let outcome = dispatch_inner(st, job_id).await;
    if let Err(msg) = outcome {
        tracing::warn!(%job_id, "jobs: dispatch failed: {msg}");
        // Reason first, then status: a reader never sees `failed` without why.
        let _ = set_error(st, job_id, &msg).await;
        let _ = service::transition(
            st,
            job_id,
            "failed",
            None,
            json!({"by": "hub", "message": format!("dispatch failed: {msg}")}),
        )
        .await;
    }
}

async fn set_error(st: &AppState, job_id: Uuid, msg: &str) -> anyhow::Result<()> {
    let c = st.pool.get().await?;
    c.execute(
        "UPDATE jobs.jobs SET error = $2, updated_at = now() WHERE job_id = $1",
        &[&job_id, &json!({"code": "dispatch_failed", "message": msg})],
    )
    .await?;
    Ok(())
}

async fn dispatch_inner(st: &AppState, job_id: Uuid) -> Result<(), String> {
    let job = service::job_snapshot(st, job_id)
        .await
        .map_err(|e| e.message)?;
    if job["status"] != "planned" || !job["runtime_ref"].is_null() {
        return Ok(());
    }
    let runtime = job["runtime"].as_str().unwrap_or("");
    let site = job["site"].as_str().ok_or("job has no planned site")?;
    let owner = job["owner"].as_str().unwrap_or("");
    match runtime {
        "lumilake" => {
            let base = base_url(st, "lumilake", site)?;
            let mut payload = job["spec"]["payload"].clone();
            let obj = payload
                .as_object_mut()
                .ok_or("payload must be a Lumilake JobSubmitRequest object")?;
            if !obj.get("data").is_some_and(Value::is_array) {
                return Err("payload.data must be the Lumilake workflow entries".into());
            }
            let format = obj
                .remove("workflow_format")
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "native".into());
            // Lumilake's binding is exactly {job_id} (extra fields are refused);
            // it reads through its own LUMID_DATA_WORKER_URL.
            obj.insert("lumid_job".into(), json!({"job_id": job_id.to_string()}));
            let token = mint_for_owner(st, owner, "lumilake").await?;
            if !runtime_supports_jobs(st, &base, &token).await? {
                return Err(format!(
                    "the Lumilake at {base} does not support job-scoped data (needs \
                     mlsys-io/Lumilake#187); refusing to dispatch rather than let it \
                     read with its server-wide token"
                ));
            }
            let resp = st
                .http
                .post(format!("{base}/api/v1/jobs"))
                .bearer_auth(token)
                .header("Workflow-Format", format)
                .json(&payload)
                .timeout(Duration::from_secs(120))
                .send()
                .await
                .map_err(|e| format!("lumilake unreachable: {e}"))?;
            let status = resp.status();
            let body: Value = resp.json().await.unwrap_or(Value::Null);
            if !status.is_success() {
                let detail = body.get("detail").cloned().unwrap_or(body);
                return Err(format!(
                    "lumilake refused the job (HTTP {status}): {detail}"
                ));
            }
            let rref = body["data"]["job_id"]
                .as_str()
                .ok_or("lumilake returned no job id")?
                .to_string();
            service::transition(
                st,
                job_id,
                "submitted",
                Some(&rref),
                json!({"by": "hub", "runtime": "lumilake", "site": site, "runtime_ref": rref}),
            )
            .await
            .map_err(|e| e.message)?;
            Ok(())
        }
        "flowmesh" => {
            // FlowMesh's native body: {"yaml": "<workflow>"} (+ lumid_job).
            let mut body = job["spec"]["payload"].clone();
            let obj = body
                .as_object_mut()
                .ok_or("payload must be a FlowMesh submit object")?;
            if !obj.get("yaml").is_some_and(Value::is_string) {
                return Err("payload.yaml must be the FlowMesh workflow (YAML text)".into());
            }
            obj.insert("lumid_job".into(), full_binding(job_id, &job, site)?);
            submit_as_owner(
                st,
                job_id,
                "flowmesh",
                site,
                owner,
                "/api/v1/workflows",
                &body,
                flowmesh_ref,
            )
            .await
        }
        "sandbox" => {
            // sandbox-control's SandboxSpec (+ lumid_job).
            let mut body = job["spec"]["payload"].clone();
            let obj = body
                .as_object_mut()
                .ok_or("payload must be a sandbox spec object")?;
            obj.insert("lumid_job".into(), full_binding(job_id, &job, site)?);
            submit_as_owner(
                st,
                job_id,
                "sandbox",
                site,
                owner,
                "/api/sandboxes",
                &body,
                sandbox_ref,
            )
            .await
        }
        other => Err(format!("dispatch to {other} is not wired yet")),
    }
}

/// Lumilake job status → hub status.
pub fn map_lumilake_status(s: &str) -> Option<&'static str> {
    match s {
        "pending" => Some("submitted"),
        "running" => Some("running"),
        "completed" => Some("succeeded"),
        "failed" => Some("failed"),
        "cancelled" => Some("cancelled"),
        _ => None,
    }
}

/// Poll every active Lumilake job this hub owns and apply status changes.
pub async fn reconcile_once(st: &AppState) -> anyhow::Result<usize> {
    let rows = {
        let c = st.pool.get().await?;
        c.query(
            "SELECT job_id, owner_sub, site, runtime_ref, status FROM jobs.jobs
              WHERE home_hub = $1 AND runtime = 'lumilake' AND runtime_ref IS NOT NULL
                AND status IN ('submitted','running')
              ORDER BY updated_at LIMIT 200",
            &[&st.settings.hub_id],
        )
        .await?
    };
    let mut tokens: HashMap<String, String> = HashMap::new();
    let mut changed = 0;
    for r in rows {
        let job_id: Uuid = r.get("job_id");
        let owner: String = r.get("owner_sub");
        let site: Option<String> = r.get("site");
        let rref: String = r.get("runtime_ref");
        let current: String = r.get("status");
        let Some(site) = site else { continue };
        let Ok(base) = base_url(st, "lumilake", &site) else {
            continue;
        };
        let token = match tokens.get(&owner) {
            Some(t) => t.clone(),
            None => match mint_for_owner(st, &owner, "lumilake").await {
                Ok(t) => {
                    tokens.insert(owner.clone(), t.clone());
                    t
                }
                Err(e) => {
                    tracing::warn!(%job_id, "jobs: reconcile mint failed: {e}");
                    continue;
                }
            },
        };
        let resp = st
            .http
            .get(format!("{base}/api/v1/jobs/{rref}"))
            .bearer_auth(&token)
            .timeout(Duration::from_secs(20))
            .send()
            .await;
        let Ok(resp) = resp else { continue };
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            service::transition(
                st,
                job_id,
                "failed",
                None,
                json!({"by": "hub", "message": "runtime no longer knows this job"}),
            )
            .await
            .ok();
            changed += 1;
            continue;
        }
        let Ok(body) = resp.json::<Value>().await else {
            continue;
        };
        let Some(next) = body["data"]["status"]
            .as_str()
            .and_then(map_lumilake_status)
        else {
            continue;
        };
        if next != current && service::can_move(&current, next) {
            let msg = body["data"]["error"].as_str().map(str::to_string);
            service::transition(st, job_id, next, None, json!({"by": "reconciler", "runtime_status": body["data"]["status"], "message": msg}))
                .await
                .ok();
            changed += 1;
        }
    }
    Ok(changed)
}

/// Best-effort cancel at the runtime (the hub has already cancelled locally).
/// The sandbox needs no call: sandbox-control follows the hub and deletes the
/// sandbox once its job has ended.
pub async fn cancel_at_runtime(st: &AppState, job: &Value) {
    let runtime = job["runtime"].as_str().unwrap_or("");
    let (Some(site), Some(rref), Some(owner)) = (
        job["site"].as_str(),
        job["runtime_ref"].as_str(),
        job["owner"].as_str(),
    ) else {
        return;
    };
    let path = match runtime {
        "lumilake" => format!("/api/v1/jobs/{rref}/cancel"),
        "flowmesh" => format!("/api/v1/workflows/{rref}/cancel"),
        _ => return,
    };
    let (Ok(base), Ok(token)) = (
        base_url(st, runtime, site),
        mint_for_owner(st, owner, runtime).await,
    ) else {
        return;
    };
    let _ = st
        .http
        .post(format!("{base}{path}"))
        .bearer_auth(token)
        .timeout(Duration::from_secs(20))
        .send()
        .await;
}

/// Run the reconciler forever (spawned at boot when jobs are enabled).
pub fn spawn_reconciler(st: AppState) {
    let every = Duration::from_secs(
        crate::config::env_var("JOBS_RECONCILE_SECS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(30)
            .max(5),
    );
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        loop {
            tick.tick().await;
            match reconcile_once(&st).await {
                Ok(n) if n > 0 => tracing::info!("jobs: reconciler applied {n} status change(s)"),
                Ok(_) => {}
                Err(e) => tracing::warn!("jobs: reconcile failed: {e:#}"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtimes_config() {
        let r = parse_runtimes(r#"{"lumilake": {"home": "http://ll-home:9000", "cloud": "http://lumilake.lumid.svc:9000/"}}"#).unwrap();
        assert_eq!(r["lumilake"]["home"], "http://ll-home:9000");
        assert!(parse_runtimes("").unwrap().is_empty());
        assert!(parse_runtimes(r#"{"k8s": {}}"#).is_err());
        assert!(parse_runtimes(r#"{"lumilake": {"home": "ftp://x"}}"#).is_err());
    }

    #[test]
    fn status_mapping() {
        assert_eq!(map_lumilake_status("pending"), Some("submitted"));
        assert_eq!(map_lumilake_status("completed"), Some("succeeded"));
        assert_eq!(map_lumilake_status("cancelled"), Some("cancelled"));
        assert_eq!(map_lumilake_status("weird"), None);
    }

    #[test]
    fn planned_url_is_the_first_routed_source() {
        let j = json!({"sources": [{"url": null}, {"url": "http://192.168.7.155:5101"}, {"url": "https://lum.id/data"}]});
        assert_eq!(
            planned_data_url(&j).as_deref(),
            Some("http://192.168.7.155:5101")
        );
        assert_eq!(planned_data_url(&json!({"sources": []})), None);
    }
}
