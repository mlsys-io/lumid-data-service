//! MCP tools for jobs and sources — the same verbs as `/jobs/v1/*`, calling
//! the same [`super::service`] functions, so an agent gets exactly the rules
//! (and the coded errors) a person gets over HTTP.

use std::sync::Arc;

use serde_json::{json, Map, Value};
use uuid::Uuid;

use super::service::{self, GrantIn, JobSpec, JobsError, SourceIn};
use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::mcp::{BoxFut, McpTool, ToolHandler};
use crate::state::AppState;

/// A JobsError reaches the agent as its JSON body (`{"error":{code,…}}`),
/// so it can branch on `code` (e.g. `source_not_entitled`, `quota_exceeded`).
fn to_api(e: JobsError) -> ApiError {
    ApiError::BadRequest(e.to_string())
}

fn need_identity(id: Option<Identity>) -> ApiResult<Identity> {
    id.ok_or_else(|| ApiError::Unauthorized("authentication required".into()))
}

fn arg<T: serde::de::DeserializeOwned>(args: &Map<String, Value>, key: &str) -> ApiResult<T> {
    serde_json::from_value(args.get(key).cloned().unwrap_or(Value::Null))
        .map_err(|e| ApiError::BadRequest(format!("argument '{key}': {e}")))
}

fn opt_str(args: &Map<String, Value>, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(str::to_string)
}

fn job_id(args: &Map<String, Value>) -> ApiResult<Uuid> {
    opt_str(args, "job_id")
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| ApiError::BadRequest("job_id must be a UUID".into()))
}

fn tool<F, Fut>(name: &str, description: &str, schema: Value, f: F) -> McpTool
where
    F: Fn(AppState, Identity, Map<String, Value>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Value, JobsError>> + Send + 'static,
{
    let f = Arc::new(f);
    let handler: ToolHandler = Arc::new(
        move |st: AppState, id: Option<Identity>, args: Map<String, Value>| {
            let f = f.clone();
            Box::pin(async move {
                let id = need_identity(id)?;
                f(st, id, args).await.map_err(to_api)
            }) as BoxFut
        },
    );
    McpTool {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: schema,
        handler,
    }
}

const SPEC_SCHEMA: &str = r#"{
  "type": "object",
  "description": "A job: what it runs and which data it reads/writes. Refs name where data lives: findata://schema.table, lumid-data://schema[.table], nus://…, data://<hub>/<prefix>/, data://<hub>/blobs/<sha256>, src://<source_id> or src://me/<name>, hf://<org>/<model>, ext://<host>.",
  "properties": {
    "app": {"type": "string"},
    "runtime": {"type": "string", "enum": ["flowmesh", "lumilake", "sandbox", "auto"]},
    "name": {"type": "string"},
    "sources": {"type": "array", "items": {"type": "object", "properties": {"ref": {"type": "string"}, "max_bytes": {"type": "integer"}, "window": {"type": "string"}}, "required": ["ref"]}},
    "sinks": {"type": "array", "items": {"type": "object", "properties": {"ref": {"type": "string"}, "max_bytes": {"type": "integer"}}, "required": ["ref"]}},
    "payload": {"type": "object", "description": "Runtime-native body (FlowMesh task / Lumilake request / sandbox spec)."},
    "parent_job_id": {"type": "string"}
  },
  "required": ["app", "runtime"]
}"#;

/// The jobs tool set.
pub fn tools() -> Vec<McpTool> {
    let spec: Value = serde_json::from_str(SPEC_SCHEMA).expect("static schema");
    vec![
        tool(
            "jobs_sources",
            "List data sources you may use. scope: mine | shared (with you) | public | all (default). q: search.",
            json!({"type": "object", "properties": {"scope": {"type": "string"}, "q": {"type": "string"}}}),
            |st, id, a| async move {
                service::list_sources(&st, &id, opt_str(&a, "scope").as_deref().unwrap_or("all"), opt_str(&a, "q")).await
            },
        ),
        tool(
            "jobs_source_create",
            "Register a data source you own on this hub (you must be able to read it). Private until shared.",
            json!({"type": "object", "properties": {
                "name": {"type": "string"}, "ref": {"type": "string"},
                "visibility": {"type": "string", "enum": ["private", "shared", "public"]},
                "max_bytes": {"type": "integer"}, "description": {"type": "string"}},
                "required": ["name", "ref"]}),
            |st, id, a| async move {
                let s: SourceIn = serde_json::from_value(Value::Object(a))
                    .map_err(|e| JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_source", e.to_string()))?;
                service::create_source(&st, &id, s).await
            },
        ),
        tool(
            "jobs_source_share",
            "Share a source you own with another user (by lum.id sub). access: read (default) or write.",
            json!({"type": "object", "properties": {
                "source_id": {"type": "string"}, "grantee": {"type": "string"},
                "access": {"type": "string", "enum": ["read", "write"]}, "expires_at": {"type": "string"}},
                "required": ["source_id", "grantee"]}),
            |st, id, mut a| async move {
                let sid = opt_str(&a, "source_id").unwrap_or_default();
                a.remove("source_id");
                let g: GrantIn = serde_json::from_value(Value::Object(a))
                    .map_err(|e| JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_grant", e.to_string()))?;
                service::share(&st, &id, &sid, g).await
            },
        ),
        tool(
            "jobs_source_unshare",
            "Withdraw a share. Running jobs that relied on it lose access within 15 seconds.",
            json!({"type": "object", "properties": {
                "source_id": {"type": "string"}, "grantee": {"type": "string"},
                "access": {"type": "string", "enum": ["read", "write"]}},
                "required": ["source_id", "grantee"]}),
            |st, id, a| async move {
                service::unshare(
                    &st,
                    &id,
                    &opt_str(&a, "source_id").unwrap_or_default(),
                    &opt_str(&a, "grantee").unwrap_or_default(),
                    opt_str(&a, "access").as_deref().unwrap_or("read"),
                )
                .await
            },
        ),
        tool(
            "jobs_plan",
            "Dry run: resolve a job's sources, check your access, and return where it would run and how each source is reached (with reasons). Call this before jobs_submit.",
            json!({"type": "object", "properties": {"spec": spec.clone()}, "required": ["spec"]}),
            |st, id, a| async move {
                let s: JobSpec = arg(&a, "spec").map_err(|e| JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_spec", e.to_string()))?;
                service::plan(&st, &id, s).await
            },
        ),
        tool(
            "jobs_submit",
            "Submit a job from a spec or a plan_id from jobs_plan. Pass idempotency_key to make retries safe.",
            json!({"type": "object", "properties": {"spec": spec, "plan_id": {"type": "string"}, "idempotency_key": {"type": "string"}}}),
            |st, id, a| async move {
                let s: Option<JobSpec> = match a.get("spec") {
                    Some(v) if !v.is_null() => Some(serde_json::from_value(v.clone()).map_err(|e| {
                        JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_spec", e.to_string())
                    })?),
                    _ => None,
                };
                let pid = opt_str(&a, "plan_id").and_then(|p| Uuid::parse_str(&p).ok());
                let (_code, v) = service::submit(&st, &id, s, pid, opt_str(&a, "idempotency_key")).await?;
                Ok(v)
            },
        ),
        tool(
            "jobs_list",
            "Your jobs, newest first. Optional status and app filters.",
            json!({"type": "object", "properties": {"status": {"type": "string"}, "app": {"type": "string"}, "limit": {"type": "integer"}}}),
            |st, id, a| async move {
                let limit = a.get("limit").and_then(Value::as_i64).unwrap_or(50);
                service::list_jobs(&st, &id, opt_str(&a, "status"), opt_str(&a, "app"), limit).await
            },
        ),
        tool(
            "jobs_status",
            "One job: status, plan, and bytes read per source.",
            json!({"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]}),
            |st, id, a| async move {
                let j = job_id(&a).map_err(|e| JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_job_id", e.to_string()))?;
                service::get_job(&st, &id, j).await
            },
        ),
        tool(
            "jobs_events",
            "A job's event log (status changes and runtime messages).",
            json!({"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]}),
            |st, id, a| async move {
                let j = job_id(&a).map_err(|e| JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_job_id", e.to_string()))?;
                service::list_events(&st, &id, j).await
            },
        ),
        tool(
            "jobs_cancel",
            "Cancel a job you own. Its tokens stop working immediately.",
            json!({"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]}),
            |st, id, a| async move {
                let j = job_id(&a).map_err(|e| JobsError::new(axum::http::StatusCode::BAD_REQUEST, "invalid_job_id", e.to_string()))?;
                service::cancel(&st, &id, j).await
            },
        ),
    ]
}
