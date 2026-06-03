//! Tool-use loop — provider-agnostic agentic inference over the data plane.
//!
//! Response is a **buffered** SSE body (all frames collected before the HTTP
//! response is sent). TODO: convert to live streaming via `axum::response::sse::Sse`.
//!
//! On tool error the error is appended as a `tool` role message with
//! `"error": true`; the loop never aborts on a single tool failure.

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::agent::tools::{dispatch, AgentConfig, ToolRegistry};
use crate::error::ApiError;
use crate::state::AppState;

// ── Request / helpers ─────────────────────────────────────────────────────────

const SYSTEM_PROMPT_PREFIX: &str = "\
You are a data-retrieval assistant. Follow this workflow for every request:\n\
1. Call `get_schema_cards` to learn the available tables and columns.\n\
2. Compose a `RetrievalPlan` with only read-only SELECT statements that reference \
   exact column and table names from the cards.\n\
3. Call `replay_retrieval_plan` to materialize the result to object storage.\n\
4. In your final answer, reference the `materialized_uri` and key stats \
   (rowcount, size_bytes). Do NOT include raw data rows in your reply.\n\n\
Available tools:\n";

fn system_prompt(registry: &ToolRegistry) -> String {
    let mut s = SYSTEM_PROMPT_PREFIX.to_string();
    for schema in registry.schemas() {
        if let Some(f) = schema.get("function") {
            let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let desc = f.get("description").and_then(|v| v.as_str()).unwrap_or("");
            s.push_str(&format!("- **{name}**: {desc}\n"));
        }
    }
    s
}

fn sse_frame(payload: &Value) -> String {
    format!("data: {}\n\n", payload)
}

fn sse_body(frames: Vec<String>) -> Body {
    Body::from(frames.concat())
}

fn sse_response(body: Body) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ── Core loop ─────────────────────────────────────────────────────────────────

/// Run the tool-use loop and return all SSE frames.
///
/// Tracks the last successful `replay_retrieval_plan` result and includes it
/// in the final `done` frame so consumers can access the `RetrievalResult`
/// without parsing the transcript.
pub async fn run_loop(
    st: &AppState,
    registry: &ToolRegistry,
    cfg: &AgentConfig,
    mut messages: Vec<Value>,
    request_max_iter: Option<usize>,
    model_override: Option<String>,
) -> Vec<String> {
    let max_iter = request_max_iter
        .map(|n| n.min(cfg.max_iterations))
        .unwrap_or(cfg.max_iterations);

    let base_url = st
        .settings
        .llm_backend_url
        .trim_end_matches('/')
        .to_string();
    let model = model_override.filter(|m| !m.is_empty()).or_else(|| {
        let dm = &st.settings.llm_default_model;
        if dm.is_empty() {
            None
        } else {
            Some(dm.clone())
        }
    });

    messages.insert(
        0,
        json!({
            "role": "system",
            "content": system_prompt(registry)
        }),
    );

    let tool_schemas = registry.schemas();
    let mut frames: Vec<String> = Vec::new();
    // Track the last RetrievalResult from replay_retrieval_plan for the done frame.
    let mut last_retrieval_result: Option<Value> = None;

    for iteration in 0..max_iter {
        let mut payload = json!({
            "messages": messages,
            "tools": tool_schemas,
            "tool_choice": "auto",
            "stream": false,
        });
        if let Some(ref m) = model {
            payload["model"] = Value::String(m.clone());
        }

        frames.push(sse_frame(&json!({
            "type": "iteration",
            "iteration": iteration + 1,
            "max_iterations": max_iter,
        })));

        let url = format!("{base_url}/v1/chat/completions");
        let mut req = st.http.post(&url).json(&payload);
        if !st.settings.llm_api_key.is_empty() {
            req = req.bearer_auth(&st.settings.llm_api_key);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                let msg = format!("LLM call failed: {e}");
                tracing::warn!("{msg}");
                frames.push(sse_frame(&json!({"type":"error","error": msg})));
                return frames;
            }
        };

        if resp.status().as_u16() >= 400 {
            let code = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            let text: String = text.chars().take(512).collect();
            frames.push(sse_frame(&json!({
                "type": "error",
                "error": format!("LLM returned HTTP {code}: {text}"),
            })));
            return frames;
        }

        let llm_body: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                frames.push(sse_frame(&json!({
                    "type": "error",
                    "error": format!("LLM response parse error: {e}"),
                })));
                return frames;
            }
        };

        let choice = match llm_body
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
        {
            Some(c) => c.clone(),
            None => {
                frames.push(sse_frame(&json!({
                    "type": "error",
                    "error": "LLM response missing choices",
                })));
                return frames;
            }
        };

        let assistant_msg = match choice.get("message") {
            Some(m) => m.clone(),
            None => {
                frames.push(sse_frame(&json!({
                    "type": "error",
                    "error": "LLM choice missing message",
                })));
                return frames;
            }
        };

        messages.push(assistant_msg.clone());

        let tool_calls = assistant_msg
            .get("tool_calls")
            .and_then(|tc| tc.as_array())
            .cloned()
            .unwrap_or_default();

        if tool_calls.is_empty() {
            frames.push(sse_frame(&json!({
                "type": "done",
                "message": assistant_msg,
                "result": last_retrieval_result,
            })));
            return frames;
        }

        for tc in &tool_calls {
            let call_id = tc
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let function = tc.get("function").cloned().unwrap_or(Value::Null);
            let fn_name = function
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let fn_args: Map<String, Value> = function
                .get("arguments")
                .and_then(|a| {
                    if let Some(s) = a.as_str() {
                        serde_json::from_str(s).ok()
                    } else {
                        a.as_object().cloned()
                    }
                })
                .unwrap_or_default();

            frames.push(sse_frame(&json!({
                "type": "tool_call",
                "call_id": call_id,
                "tool": fn_name,
            })));

            let (tool_result_content, is_error) =
                match dispatch(st, &fn_name, &fn_args, cfg).await {
                    Ok(v) => {
                        // Track the last RetrievalResult for the done frame.
                        if fn_name == "replay_retrieval_plan" {
                            last_retrieval_result = Some(v.clone());
                        }
                        (v.to_string(), false)
                    }
                    Err(e) => {
                        let msg = format!("tool '{fn_name}' error: {e}");
                        tracing::warn!("{msg}");
                        frames.push(sse_frame(&json!({
                            "type": "tool_error",
                            "call_id": call_id,
                            "tool": fn_name,
                            "error": msg,
                        })));
                        (json!({"error": true, "message": msg}).to_string(), true)
                    }
                };

            let mut tool_msg = json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": tool_result_content,
            });
            if is_error {
                tool_msg["error"] = Value::Bool(true);
            }
            messages.push(tool_msg);

            if !is_error {
                frames.push(sse_frame(&json!({
                    "type": "tool_result",
                    "call_id": call_id,
                    "tool": fn_name,
                })));
            }
        }
    }

    let last_assistant = messages
        .iter()
        .rev()
        .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("assistant"))
        .cloned();
    frames.push(sse_frame(&json!({
        "type": "done",
        "truncated": true,
        "max_iterations_reached": max_iter,
        "message": last_assistant,
        "result": last_retrieval_result,
    })));
    frames
}

// ── Axum handler ─────────────────────────────────────────────────────────────

pub async fn agent_chat(State(st): State<AppState>, body: Json<Value>) -> Response {
    let body = match body.0.as_object() {
        Some(o) => o.clone(),
        None => {
            return ApiError::BadRequest("request body must be a JSON object".into())
                .into_response();
        }
    };

    let messages: Vec<Value> = match body.get("messages").and_then(|m| m.as_array()) {
        Some(arr) => arr.clone(),
        None => {
            return ApiError::BadRequest("'messages' array is required".into()).into_response();
        }
    };

    let max_iterations: Option<usize> = body
        .get("max_iterations")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);
    let model_override: Option<String> = body
        .get("model")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let cfg = AgentConfig::from_env();
    let registry = ToolRegistry::new();

    let frames = run_loop(
        &st,
        &registry,
        &cfg,
        messages,
        max_iterations,
        model_override,
    )
    .await;

    sse_response(sse_body(frames))
}
