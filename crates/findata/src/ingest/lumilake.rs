//! Lumilake handoff — port of `ingest/lumilake.py`.
//!
//! Fire-and-forget POST /api/v1/jobs after a successful, non-empty ingest.
//! No-op when `lumilake_base_url` is empty. Never blocks or fails the ingest.
//!
//! Config is read from process env at call time (the engine modules don't hold
//! a Settings handle); the route layer could thread Settings through instead,
//! but env-read keeps the hook self-contained and matches the Python module's
//! direct `settings.*` access.

use std::env;

use serde_json::json;

use super::core::IngestResult;

pub struct LumilakeInfo {
    pub target_schema: String,
    pub target_table: String,
    pub mode: String,
    pub declared_endpoint: Option<String>,
    pub submitted_by: Option<String>,
}

fn base_url() -> Option<String> {
    let v = env::var("FINDATA_LUMILAKE_BASE_URL").unwrap_or_default();
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}

/// on_finalize equivalent — spawns a detached task that POSTs the job event.
pub fn submit_after_ingest(result: &IngestResult, info: LumilakeInfo) {
    let base = match base_url() {
        Some(b) => b,
        None => return,
    };
    if (result.inserted + result.updated) == 0 {
        return;
    }
    let workflow =
        env::var("FINDATA_LUMILAKE_WORKFLOW").unwrap_or_else(|_| "findata-ingress-followup".into());
    let token = env::var("FINDATA_LUMILAKE_TOKEN").ok().filter(|s| !s.is_empty());
    let timeout_s = env::var("FINDATA_LUMILAKE_TIMEOUT_S")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(10.0);

    let payload = json!({
        "data": [{
            "workflow": workflow,
            "inputs": {
                "run_id": [result.run_id.clone()],
                "target_schema": [info.target_schema],
                "target_table": [info.target_table],
                "rows_inserted": [result.inserted.to_string()],
                "rows_updated": [result.updated.to_string()],
                "mode": [info.mode],
                "declared_endpoint": [info.declared_endpoint.unwrap_or_default()],
                "submitted_by": [info.submitted_by.unwrap_or_default()],
            }
        }]
    });

    tokio::spawn(async move {
        let url = format!("{}/api/v1/jobs", base.trim_end_matches('/'));
        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs_f64(timeout_s))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("lumilake client build failed: {e}");
                return;
            }
        };
        let mut req = client.post(&url).json(&payload);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::debug!("lumilake handoff ok ({})", resp.status());
            }
            Ok(resp) => {
                tracing::warn!("lumilake handoff {}", resp.status());
            }
            Err(e) => tracing::warn!("lumilake handoff failed: {e}"),
        }
    });
}
