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

/// A short-lived lum.id token for `sub`, so the runtime sees the real user.
async fn mint_for_owner(st: &AppState, sub: &str, audience: &str) -> Result<String, String> {
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
            st.settings.lumid_url.trim_end_matches('/')
        ))
        .header("X-Bridge-Secret", secret)
        .json(&json!({"sub": sub, "audience": audience, "scopes": [], "ttl_seconds": 900}))
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

/// Does the Lumilake at `base` understand `lumid_job`? Answered from its
/// published OpenAPI schema. Without it, Lumilake's request model silently
/// IGNORES the binding and the job would read with its server-wide token —
/// unscoped, while the hub believed otherwise — so the hub refuses instead.
async fn lumilake_supports_jobs(st: &AppState, base: &str, token: &str) -> Result<bool, String> {
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
        .map_err(|e| format!("lumilake unreachable (schema check): {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "lumilake schema check failed (HTTP {})",
            resp.status()
        ));
    }
    let body = resp
        .text()
        .await
        .map_err(|e| format!("lumilake schema: {e}"))?;
    let ok = body.contains("LumidJobBinding");
    CAPABILITY
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .insert(base.to_string(), (ok, Instant::now()));
    Ok(ok)
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
    runtime == "lumilake" && base_url(st, runtime, site).is_ok()
}

/// Dispatch one planned job. Idempotent per state: only a `planned` job with
/// no runtime ref is sent.
pub async fn dispatch(st: &AppState, job_id: Uuid) {
    let outcome = dispatch_inner(st, job_id).await;
    if let Err(msg) = outcome {
        tracing::warn!(%job_id, "jobs: dispatch failed: {msg}");
        let _ = service::transition(
            st,
            job_id,
            "failed",
            None,
            json!({"by": "hub", "message": format!("dispatch failed: {msg}")}),
        )
        .await;
        let _ = set_error(st, job_id, &msg).await;
    }
}

async fn set_error(st: &AppState, job_id: Uuid, msg: &str) -> anyhow::Result<()> {
    let c = st.pool.get().await?;
    c.execute(
        "UPDATE jobs.jobs SET error = $2 WHERE job_id = $1",
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
            obj.insert(
                "lumid_job".into(),
                json!({"job_id": job_id.to_string(), "data_url": planned_data_url(&job)}),
            );
            let token = mint_for_owner(st, owner, "lumilake").await?;
            if !lumilake_supports_jobs(st, &base, &token).await? {
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
pub async fn cancel_at_runtime(st: &AppState, job: &Value) {
    if job["runtime"] != "lumilake" {
        return;
    }
    let (Some(site), Some(rref), Some(owner)) = (
        job["site"].as_str(),
        job["runtime_ref"].as_str(),
        job["owner"].as_str(),
    ) else {
        return;
    };
    let (Ok(base), Ok(token)) = (
        base_url(st, "lumilake", site),
        mint_for_owner(st, owner, "lumilake").await,
    ) else {
        return;
    };
    let _ = st
        .http
        .post(format!("{base}/api/v1/jobs/{rref}/cancel"))
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
