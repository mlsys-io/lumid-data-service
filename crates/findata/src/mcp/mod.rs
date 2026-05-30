//! Generic MCP (Model Context Protocol) server — the PLATFORM half.
//!
//! Provides the transport (Streamable-HTTP JSON-RPC 2.0 at `POST /mcp`) and a
//! domain-agnostic tool registry. The *tools* are supplied by the application
//! (e.g. findata-ext auto-generates them from the declarative read specs), so
//! the platform carries no domain knowledge — mirroring the read-layer split.
//!
//! Mounted inside the gated router, so MCP inherits the same PAT / local-key
//! auth + rate limiting as every other data route.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures_util::future::BoxFuture;
use serde_json::{json, Map, Value};

use crate::error::ApiResult;
use crate::state::AppState;

/// A tool handler: takes the live AppState + the call arguments, returns a JSON
/// result (or an ApiError, surfaced to the client as an MCP tool error).
pub type BoxFut = BoxFuture<'static, ApiResult<Value>>;
pub type ToolHandler = Arc<dyn Fn(AppState, Map<String, Value>) -> BoxFut + Send + Sync>;

/// One MCP tool: its advertised name/description/JSON-schema + the handler.
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub handler: ToolHandler,
}

#[derive(Default)]
pub struct McpRegistry {
    pub tools: Vec<McpTool>,
}

impl McpRegistry {
    pub fn new(tools: Vec<McpTool>) -> Self {
        Self { tools }
    }
    pub fn len(&self) -> usize {
        self.tools.len()
    }
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
    fn find(&self, name: &str) -> Option<&McpTool> {
        self.tools.iter().find(|t| t.name == name)
    }
}

const PROTOCOL_VERSION: &str = "2025-03-26";

/// Mount `POST /mcp` (JSON-RPC). Merge the returned router into the gated group.
pub fn build_router(registry: Arc<McpRegistry>) -> Router<AppState> {
    Router::new().route(
        "/mcp",
        post(move |st: State<AppState>, body: Json<Value>| {
            let registry = registry.clone();
            async move { handle(&registry, st.0, body.0).await }
        })
        .get(|| async { Json(json!({"service": "mcp", "transport": "streamable-http"})) }),
    )
}

async fn handle(reg: &McpRegistry, st: AppState, body: Value) -> Response {
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = body.get("method").and_then(|m| m.as_str()).unwrap_or("");
    match method {
        "initialize" => rpc_ok(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": "findata", "version": env!("CARGO_PKG_VERSION")}
            }),
        ),
        // Fire-and-forget notification; no response body.
        "notifications/initialized" => axum::http::StatusCode::ACCEPTED.into_response(),
        "ping" => rpc_ok(id, json!({})),
        "tools/list" => {
            let tools: Vec<Value> = reg
                .tools
                .iter()
                .map(|t| {
                    json!({"name": t.name, "description": t.description, "inputSchema": t.input_schema})
                })
                .collect();
            rpc_ok(id, json!({"tools": tools}))
        }
        "tools/call" => {
            let params = body.get("params").cloned().unwrap_or(Value::Null);
            let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params
                .get("arguments")
                .and_then(|a| a.as_object())
                .cloned()
                .unwrap_or_default();
            match reg.find(name) {
                None => rpc_ok(id, tool_error(format!("unknown tool {name:?}"))),
                Some(t) => match (t.handler)(st, args).await {
                    Ok(v) => rpc_ok(id, tool_result(v)),
                    Err(e) => rpc_ok(id, tool_error(format!("{e}"))),
                },
            }
        }
        other => rpc_err(id, -32601, &format!("method not found: {other}")),
    }
}

/// MCP tool result: a single text content block holding the JSON payload.
fn tool_result(v: Value) -> Value {
    json!({"content": [{"type": "text", "text": v.to_string()}], "isError": false})
}
fn tool_error(msg: String) -> Value {
    json!({"content": [{"type": "text", "text": msg}], "isError": true})
}

fn rpc_ok(id: Value, result: Value) -> Response {
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}
fn rpc_err(id: Value, code: i64, message: &str) -> Response {
    Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})).into_response()
}
