//! Minimal OpenAPI 3.1 document generated from the declarative read specs
//! (same source as the MCP tools). Served public at `GET /openapi.json` so
//! consumers + tooling can introspect the read surface. Bespoke ext routes and
//! `/v1/*` aren't enumerated here (they're compiled, not declarative).

use std::sync::Arc;

use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Map, Value};

use crate::read::spec::{EndpointSpec, Kind};
use crate::state::AppState;

fn json_type(ty: &str) -> &'static str {
    match ty {
        "int" => "integer",
        "float" => "number",
        "bool" => "boolean",
        _ => "string",
    }
}

/// `:symbol` → `{symbol}` for OpenAPI path templating.
fn oapi_path(path: &str) -> String {
    path.split('/')
        .map(|seg| seg.strip_prefix(':').map(|n| format!("{{{n}}}")).unwrap_or_else(|| seg.to_string()))
        .collect::<Vec<_>>()
        .join("/")
}

fn generate(specs: &[Arc<EndpointSpec>], extra_paths: &Value, enable_llm: bool) -> Value {
    let mut paths = Map::new();
    for s in specs {
        let params: Vec<Value> = s
            .params
            .iter()
            .map(|p| {
                json!({
                    "name": p.name,
                    "in": if p.kind == Kind::Path { "path" } else { "query" },
                    "required": p.required || p.kind == Kind::Path,
                    "schema": {"type": json_type(&p.ty)},
                })
            })
            .collect();
        let op = json!({
            "summary": s.id,
            "operationId": s.id.replace('.', "_"),
            "parameters": params,
            "responses": {"200": {"description": "OK"}},
        });
        paths.insert(oapi_path(&s.path), json!({ s.method.to_lowercase(): op }));
    }

    // Platform routes that aren't declarative specs but ARE part of the
    // read-accessible surface: catalog/lineage, realtime (SSE/WS), MCP, and the
    // caller's own usage. Write/ingest/admin are intentionally OMITTED — these
    // are operator/submitter routes, not the end-user (read) surface.
    let p = |name: &str, loc: &str, required: bool| {
        json!({"name": name, "in": loc, "required": required, "schema": {"type": "string"}})
    };
    let mut add = |path: &str, method: &str, summary: &str, params: Vec<Value>, desc: &str| {
        let op = json!({
            "summary": summary,
            "operationId": format!("{}_{}", method, path.replace(['/', '{', '}'], "_").trim_matches('_')),
            "description": desc,
            "parameters": params,
            "responses": {"200": {"description": "OK"}},
        });
        let entry = paths.entry(path.to_string()).or_insert_with(|| json!({}));
        if let Some(m) = entry.as_object_mut() {
            m.insert(method.to_string(), op);
        }
    };

    // --- Catalog / lineage (discovery; read-only) ---
    add("/catalog/schemas", "get", "List schemas", vec![], "User schemas in the warehouse.");
    add("/catalog/schemas/{schema}/tables", "get", "List tables in a schema", vec![p("schema", "path", true)], "");
    add("/catalog/tables/{schema}/{table}", "get", "Table profile", vec![p("schema", "path", true), p("table", "path", true)], "Columns, row estimate, provenance.");
    add("/catalog/tables/{schema}/{table}/schema.json", "get", "Table JSON Schema", vec![p("schema", "path", true), p("table", "path", true)], "");
    add("/catalog/sources", "get", "Ingest sources", vec![], "");
    add("/catalog/submitters", "get", "Ingest submitters", vec![], "");
    add("/catalog/lineage/runs", "get", "Recent ingest runs", vec![], "");
    add("/catalog/lineage/run/{run_id}", "get", "Lineage for a run", vec![p("run_id", "path", true)], "");
    add("/catalog/lineage/row", "get", "Lineage for a row", vec![p("schema", "query", true), p("table", "query", true)], "Trace a row back to its ingest run.");

    // Realtime SSE/WebSocket routes are app-contributed (the platform names no
    // such path) — see `extra_paths` below.

    // --- Query cost profiling (EXPLAIN-based, no ANALYZE) ---
    add(
        "/profile",
        "post",
        "Query cost profile",
        vec![],
        "EXPLAIN-based query cost estimates for one or more planner-variant plans. \
         Returns raw_cost, estimated_rows, and relation/index footprints per variant. \
         The query is never executed (no ANALYZE). Same safety boundary as POST /retrieve.",
    );

    // --- Blob KV write plane (privileged: lumilake:write scope / local key) ---
    add(
        "/blobs/{key}",
        "put",
        "Write blob at key",
        vec![p("key", "path", true)],
        "Write (overwrite) the request body bytes to the object store at the caller-supplied key \
         (path-style, may contain '/', e.g. jobs/<id>/record.json). Body capped at blob_max_bytes. \
         Requires the `lumilake:write` scope (or an admin scope / local API key). Returns {\"key\", \"size\"}.",
    );
    add(
        "/blobs/{key}",
        "delete",
        "Delete blob at key",
        vec![p("key", "path", true)],
        "Delete the object at the caller-supplied key (path-style, may contain '/'). Idempotent: \
         already-absent keys succeed. Returns 204 No Content. Requires the `lumilake:write` scope (or an admin scope / local API key).",
    );

    // --- MCP + own usage ---
    add("/mcp", "post", "MCP JSON-RPC", vec![], "Model Context Protocol (JSON-RPC 2.0, Streamable-HTTP). One tool per read endpoint; tools/list + tools/call.");
    add("/usage/me", "get", "Your usage", vec![], "Calling identity's totals: total_calls, bytes_out, calls_last_24h, hourly_last_24h.");

    // --- LLM reverse-proxy (/v1/*) — only when this deployment mounts the LLM
    // plugin (ServeParts.enable_llm). OpenAI- + Anthropic-compatible, model-routed
    // across the backend pool. Enumerated here so the spec matches the mounted
    // surface (previously omitted — the "/v1/* are compiled, not declarative" gap). ---
    if enable_llm {
        add("/v1/models", "get", "List models",
            vec![], "OpenAI-compatible. Model ids available across the routed backend pool.");
        add("/v1/chat/completions", "post", "Chat completions",
            vec![], "OpenAI-compatible chat completions (streaming + non-streaming). Model-routed; `model` selects the backend.");
        add("/v1/completions", "post", "Text completions",
            vec![], "OpenAI-compatible legacy text completions. Model-routed.");
        add("/v1/embeddings", "post", "Embeddings",
            vec![], "OpenAI-compatible embeddings. Model-routed.");
        add("/v1/messages", "post", "Messages (Anthropic)",
            vec![], "Anthropic-compatible Messages API (streaming + non-streaming). Model-routed.");
        add("/v1/messages/count_tokens", "post", "Count tokens (Anthropic)",
            vec![], "Anthropic-compatible token counting for a Messages request.");
        add("/v1/images/generations", "post", "Image generation",
            vec![], "OpenAI-compatible image generation. Model-routed (e.g. `qwen-image`); returns `{data:[{b64_json}]}`.");
        add("/v1/audio/speech", "post", "Text to speech",
            vec![], "OpenAI-compatible text-to-speech. Model-routed (e.g. `qwen-tts`); returns binary audio (mp3/wav).");
    }

    // --- Platform public surfaces (no auth) ---
    add("/health", "get", "Liveness", vec![], "Liveness probe.");
    add("/status", "get", "Status board", vec![], "HTML health board: DB/Redis/pool + realtime feed health + endpoint-freshness SLA.");
    add("/freshness", "get", "Freshness (JSON)", vec![], "Per-endpoint freshness SLA counts + per-source realtime lag.");
    add("/usage", "get", "Usage dashboard", vec![], "Global request dashboard (all callers, aggregate).");
    add("/openapi.json", "get", "OpenAPI document", vec![], "This document.");

    // App-contributed paths (e.g. realtime SSE/WS the app mounts) — merged last
    // so the app can document routes the platform doesn't name. Shape is an
    // OpenAPI paths object: `{ "/path": { "get": {<operation>} } }`.
    if let Some(extra) = extra_paths.as_object() {
        for (path, item) in extra {
            paths.insert(path.clone(), item.clone());
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": crate::config::env_var("SERVICE_NAME").unwrap_or_else(|| "lumid".into()),
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Read + discovery + realtime + MCP surface. Declarative read \
                endpoints, catalog/lineage, SSE/WebSocket streams, POST /mcp, and — when this \
                deployment serves inference — the OpenAI/Anthropic-compatible /v1/* LLM proxy. \
                Write/ingest/admin routes are operator-only and intentionally not listed here.",
        },
        "servers": [{"url": "/"}],
        "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}},
        "security": [{"bearer": []}],
        "paths": paths,
    })
}

/// A router with `GET /openapi.json` (public — merge outside the gate).
/// `extra_paths` is an app-contributed OpenAPI paths object merged into the doc
/// (so apps can document routes the platform doesn't name, e.g. realtime SSE/WS).
pub fn build_router(specs: &[Arc<EndpointSpec>], extra_paths: &Value, enable_llm: bool) -> Router<AppState> {
    let doc = Arc::new(generate(specs, extra_paths, enable_llm));
    Router::new().route(
        "/openapi.json",
        get(move || {
            let doc = doc.clone();
            async move { Json((*doc).clone()) }
        }),
    )
}
