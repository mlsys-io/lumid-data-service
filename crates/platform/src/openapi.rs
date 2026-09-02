//! Minimal OpenAPI 3.1 document generated from the declarative read specs
//! (same source as the MCP tools). Served public at `GET /openapi.json` so
//! consumers + tooling can introspect the read surface. Bespoke ext routes and
//! `/v1/*` aren't enumerated here (they're compiled, not declarative).

use std::sync::Arc;

use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Map, Value};

use crate::read::spec::{EndpointSpec, Kind, Shape};
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

/// Insert one operation into `paths`. Free function (rather than the local
/// closure it used to be) so it can be called both from the `add(...)`
/// shorthand below (no request body) and directly for routes that need one
/// (`/retrieve`) — a closure capturing `paths` mutably can only be borrowed
/// once, so a second call site needs a plain function instead.
#[allow(clippy::too_many_arguments)]
fn add_op(
    paths: &mut Map<String, Value>,
    path: &str,
    method: &str,
    summary: &str,
    params: Vec<Value>,
    desc: &str,
    request_body: Option<Value>,
) {
    // `/health` is the one unauthenticated route, so it is the one route that
    // cannot answer 401/403. Everything else shares the gated error contract.
    let mut responses = error_responses(path != "/health");
    responses.insert(
        "200".into(),
        json!({"description": "OK", "content": {"application/json": {"schema": {"type": "object"}}}}),
    );
    let mut op = json!({
        "summary": summary,
        "operationId": format!("{}_{}", method, path.replace(['/', '{', '}'], "_").trim_matches('_')),
        "description": desc,
        "parameters": params,
        "responses": Value::Object(responses),
    });
    if let Some(rb) = request_body {
        if let Some(o) = op.as_object_mut() {
            o.insert("requestBody".to_string(), rb);
        }
    }
    let entry = paths.entry(path.to_string()).or_insert_with(|| json!({}));
    if let Some(m) = entry.as_object_mut() {
        m.insert(method.to_string(), op);
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// Response contracts (LUMID-011)
//
// Every operation used to declare exactly `{"200": {"description": "OK"}}` and
// no content schema — 172 operations, one status code, zero response bodies —
// while the service really returned structured 400/401/404/422/429/500. A
// generated client could not validate a success payload or tell a validation
// failure from an outage.
//
// Success bodies are derived from the SPEC ITSELF rather than hand-maintained:
// a declarative endpoint's output columns are exactly its SQL SELECT list, so
// that list is the contract. Deriving it means the document cannot drift from
// the query the way a hand-written schema does.
// ─────────────────────────────────────────────────────────────────────────────

/// Columns hidden from every response when `strip_lineage` is set. Kept in
/// sync with `db::lineage::HIDDEN_COLUMNS` — declaring a column the caller
/// never receives is worse than declaring nothing.
const LINEAGE_HIDDEN: [&str; 4] = ["source", "source_endpoint", "source_run_id", "raw"];

/// Split a SELECT list on commas that sit at paren depth 0.
fn split_top_level(list: &str) -> Vec<String> {
    let (mut out, mut depth, mut cur) = (Vec::new(), 0i32, String::new());
    for ch in list.chars() {
        match ch {
            '(' => { depth += 1; cur.push(ch); }
            ')' => { depth -= 1; cur.push(ch); }
            ',' if depth == 0 => { out.push(cur.trim().to_string()); cur = String::new(); }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() { out.push(cur.trim().to_string()); }
    out
}

/// The JSON type of a select item, ONLY when the SQL states it outright.
///
/// Deliberately conservative: an explicit `::float8` or a `count(...)` is a
/// fact, but `max(x)` depends on a column this function cannot see. An omitted
/// `type` is an honest "any JSON value"; a guessed one is a lie a client will
/// validate against.
fn column_type(expr: &str) -> Option<&'static str> {
    let e = expr.to_lowercase();
    if e.contains("::float8") || e.contains("::double precision") || e.contains("::numeric")
        || e.contains("::real") || e.contains("::float") {
        return Some("number");
    }
    if e.contains("::bigint") || e.contains("::int") || e.contains("::smallint") {
        return Some("integer");
    }
    if e.contains("::text") || e.contains("::varchar") { return Some("string"); }
    if e.contains("::bool") { return Some("boolean"); }
    if e.trim_start().starts_with("count(") { return Some("integer"); }
    None
}

/// Output name of one select item: its `AS` alias, else the trailing
/// identifier of a bare column reference. Returns None for an unaliased
/// expression, whose response key cannot be known from the SQL alone.
fn column_name(expr: &str) -> Option<String> {
    let lower = expr.to_lowercase();
    // Find " as " at depth 0, taking the LAST one (aliases end the item).
    let (mut depth, mut as_at) = (0i32, None);
    let bytes: Vec<char> = lower.chars().collect();
    for (i, ch) in bytes.iter().enumerate() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {
                if depth == 0 && i + 4 <= bytes.len() && lower[i..].starts_with(" as ") {
                    as_at = Some(i + 4);
                }
            }
        }
    }
    let raw = match as_at {
        Some(i) => expr[i..].trim().to_string(),
        None => {
            // Bare reference like `news.articles.published_at` or `published_at`.
            let t = expr.trim();
            if t.contains(['(', ' ', '*']) { return None; }
            t.rsplit('.').next().unwrap_or(t).to_string()
        }
    };
    let name = raw.trim().trim_matches('"').to_string();
    let ok = !name.is_empty()
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    ok.then_some(name)
}

/// Properties object for one row of a spec's result set.
fn row_properties(spec: &EndpointSpec) -> (Map<String, Value>, bool) {
    let sql = &spec.sql;
    let lower = sql.to_lowercase();
    let Some(sel) = lower.find("select") else { return (Map::new(), false) };
    let after = sel + "select".len();
    // Matching FROM at depth 0.
    let (mut depth, mut from_at) = (0i32, None);
    for (i, ch) in lower[after..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {
                if depth == 0 && lower[after + i..].starts_with("from ") && from_at.is_none() {
                    from_at = Some(after + i);
                }
            }
        }
        if from_at.is_some() { break; }
    }
    let Some(end) = from_at else { return (Map::new(), false) };
    let mut props = Map::new();
    let mut complete = true;
    for item in split_top_level(&sql[after..end]) {
        if item.trim() == "*" { return (Map::new(), false); } // shape unknowable
        match column_name(&item) {
            Some(n) => {
                if spec.strip_lineage && LINEAGE_HIDDEN.contains(&n.as_str()) { continue; }
                let mut schema = Map::new();
                if let Some(t) = column_type(&item) {
                    schema.insert("type".into(), json!(t));
                }
                props.insert(n, Value::Object(schema));
            }
            None => complete = false,
        }
    }
    (props, complete)
}

/// The 200 response for a declarative spec, shaped by its `shape`.
fn success_response(spec: &EndpointSpec) -> Value {
    let (props, complete) = row_properties(spec);
    let row = if props.is_empty() {
        json!({"type": "object"})
    } else {
        // `additionalProperties` stays true when a select item's name could not
        // be derived — the row genuinely has more keys than are listed, and
        // claiming otherwise would make a valid response fail validation.
        json!({"type": "object", "properties": props, "additionalProperties": !complete})
    };
    let schema = match spec.shape {
        Shape::One => row,
        Shape::Rows => json!({"type": "array", "items": row}),
        Shape::Envelope => {
            let key = spec.envelope_key.clone().unwrap_or_else(|| "data".to_string());
            json!({
                "type": "object",
                "properties": { key: {"type": "array", "items": row} },
            })
        }
    };
    json!({"description": "OK", "content": {"application/json": {"schema": schema}}})
}

/// Error responses every gated read operation can actually return.
pub fn error_responses(auth: bool) -> Map<String, Value> {
    let err = |d: &str| json!({
        "description": d,
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
    });
    let mut m = Map::new();
    m.insert("400".into(), err("Malformed parameter (wrong type, too long, unparseable)."));
    if auth {
        m.insert("401".into(), err("Missing or invalid bearer identity."));
        m.insert("403".into(), err("Authenticated but not permitted to read this resource."));
    }
    m.insert("404".into(), err("No such route, or no such resource."));
    m.insert("422".into(), json!({
        "description": "Parameter failed validation — e.g. outside its declared minimum/maximum.",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ValidationError"}}}
    }));
    m.insert("429".into(), json!({
        "description": "Rate limited. Carries Retry-After and X-RateLimit-Limit.",
        "headers": {
            "Retry-After": {"schema": {"type": "integer"}, "description": "Seconds to wait."},
            "X-RateLimit-Limit": {"schema": {"type": "string"}, "description": "The limit that was hit."}
        },
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
    }));
    m.insert("500".into(), json!({
        "description": "Internal error. The body carries a request_id echoed by the x-request-id header; quote it in a bug report.",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/InternalError"}}}
    }));
    m.insert("503".into(), err("Upstream or database unavailable."));
    m
}

/// Shared error schemas.
pub fn error_schemas() -> Value {
    json!({
        "Error": {
            "type": "object",
            "required": ["detail"],
            "properties": {"detail": {"type": "string", "description": "Human-readable cause."}}
        },
        "InternalError": {
            "type": "object",
            "required": ["detail"],
            "properties": {
                "detail": {"type": "string"},
                "request_id": {"type": "string", "format": "uuid",
                    "description": "Correlates with the server-side log line and the x-request-id header."}
            }
        },
        "ValidationError": {
            "type": "object",
            "required": ["detail"],
            "properties": {"detail": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "loc": {"type": "array", "items": {"type": "string"},
                        "description": "Where the bad value was, e.g. [\"query\",\"limit\"]."},
                    "msg": {"type": "string"},
                    "type": {"type": "string"},
                    "input": {"type": "string"},
                    "ctx": {"type": "object", "description": "Declared bounds, when the failure was a range check."}
                }
            }}}
        }
    })
}

fn generate(specs: &[Arc<EndpointSpec>], extra_paths: &Value) -> Value {
    let mut paths = Map::new();
    for s in specs {
        let params: Vec<Value> = s
            .params
            .iter()
            .map(|p| {
                // Constraints were declared in the spec all along and simply
                // never reached the document, so a generated client could not
                // know that `limit` tops out at 200 (LUMID-011).
                let mut sch = Map::new();
                sch.insert("type".into(), json!(json_type(&p.ty)));
                if let Some(mn) = p.min { sch.insert("minimum".into(), json!(mn)); }
                if let Some(mx) = p.max { sch.insert("maximum".into(), json!(mx)); }
                if let Some(ml) = p.max_len { sch.insert("maxLength".into(), json!(ml)); }
                if let Some(d) = &p.default {
                    if let Ok(v) = serde_json::to_value(d) { sch.insert("default".into(), v); }
                }
                if !p.select.is_empty() {
                    let mut vals: Vec<String> = p.select.keys().cloned().collect();
                    vals.sort();
                    sch.insert("enum".into(), json!(vals));
                }
                json!({
                    "name": p.name,
                    "in": if p.kind == Kind::Path { "path" } else { "query" },
                    "required": p.required || p.kind == Kind::Path,
                    "schema": Value::Object(sch),
                })
            })
            .collect();
        let mut responses = error_responses(true);
        responses.insert("200".into(), success_response(s));
        let op = json!({
            "summary": s.id,
            "operationId": s.id.replace('.', "_"),
            "description": if s.description.is_empty() { s.id.clone() } else { s.description.clone() },
            "parameters": params,
            "responses": Value::Object(responses),
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
        add_op(&mut paths, path, method, summary, params, desc, None);
    };

    // --- Catalog / lineage (discovery; read-only) ---
    add("/catalog/schemas", "get", "List schemas", vec![], "User schemas in the warehouse.");
    add("/catalog/schemas/{schema}/tables", "get", "List tables in a schema", vec![p("schema", "path", true)], "Tables + row estimates for one schema.");
    add("/catalog/tables/{schema}/{table}", "get", "Table profile", vec![p("schema", "path", true), p("table", "path", true)], "Columns, row estimate, provenance.");
    add("/catalog/tables/{schema}/{table}/schema.json", "get", "Table JSON Schema", vec![p("schema", "path", true), p("table", "path", true)], "Column shape (name, type) for a table.");
    add("/catalog/sources", "get", "Ingest sources", vec![], "Configured ingest source systems.");
    add("/catalog/submitters", "get", "Ingest submitters", vec![], "Who/what may submit to the ingress surface.");
    add("/catalog/lineage/runs", "get", "Recent ingest runs", vec![], "Recent ingest runs with status + row counts.");
    add("/catalog/lineage/run/{run_id}", "get", "Lineage for a run", vec![p("run_id", "path", true)], "The lineage chain for one ingest run.");
    add("/catalog/lineage/row", "get", "Lineage for a row", vec![p("schema", "query", true), p("table", "query", true)],
        "Trace a row back to its ingest run. Every query param besides 'schema'/'table' is a \
         natural-key column=value filter — the required key is per-table and is published as \
         the `natural_key` field of GET /catalog/tables/{schema}/{table}; omitting the key \
         returns 400 naming it.");

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

    // --- Platform public surfaces (no auth) ---
    add("/health", "get", "Liveness", vec![], "Liveness probe.");
    add("/status", "get", "Status board", vec![], "HTML health board: DB/Redis/pool + realtime feed health + endpoint-freshness SLA.");
    add("/freshness", "get", "Freshness (JSON)", vec![], "Per-endpoint freshness SLA counts + per-source realtime lag.");
    add("/usage", "get", "Usage dashboard", vec![], "Global request dashboard (all callers, aggregate).");
    add("/openapi.json", "get", "OpenAPI document", vec![], "This document.");

    // `add` (the closure above) is done being used past this point, so `paths`
    // can be borrowed directly again for the one route that needs a
    // requestBody (a closure's captured mutable borrow can't be interleaved
    // with a second direct borrow of the same variable while still in scope).
    //
    // --- Direct SQL/storage retrieval ---
    // Historically missing from this document (LUMID-004) even though /profile
    // above refers to it ("Same safety boundary as POST /retrieve") — a
    // read-only, gated surface that just never got an `add()` call.
    add_op(
        &mut paths,
        "/retrieve",
        "post",
        "Direct SQL/storage retrieval",
        vec![],
        "Executes a single read-only SELECT (or a pre-built retrieval plan) and materializes \
         the result to object storage, returning a materialized_uri to fetch it from. Safety \
         model: SELECT-only parser, the query runs in a READ ONLY transaction, and a row cap \
         is enforced by injecting LIMIT <cap>+1 server-side (exceeding it is a 400, not a \
         silently truncated result). The connection's statement_timeout \
         (STATEMENT_TIMEOUT_MS, default 30000ms) applies; a query that exceeds it returns 400 \
         naming the timeout rather than hanging. 'sql' and 'plan' are mutually exclusive — \
         exactly one is required.",
        Some(json!({
            "required": true,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "A single SELECT statement. Mutually exclusive with 'plan'."
                            },
                            "plan": {
                                "type": "object",
                                "description": "A pre-built RetrievalPlan (ops sequence). Mutually exclusive with 'sql'."
                            },
                            "output_format": {
                                "type": "string",
                                "enum": ["jsonl", "csv", "raw"],
                                "default": "jsonl",
                                "description": "'parquet' is not implemented and returns 400."
                            }
                        }
                    }
                }
            }
        })),
    );

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
                endpoints, catalog/lineage, SSE/WebSocket streams, and POST /mcp. \
                App-compiled reads and the optional /v1 LLM proxy are at /reference. \
                Write/ingest/admin routes are operator-only and intentionally not listed here. \
                \n\nAuthentication: every route below except /health requires a bearer identity \
                (`Authorization: Bearer <token>`) at this service. When reached through \
                https://lum.id/findata/*, the edge proxy grants a rate-limited, read-only \
                anonymous tier automatically to any request that presents no Authorization \
                header at all — this is intentional (research/reproducibility use), and a \
                caller's own PAT, when presented, always takes precedence and is passed through \
                unmodified.",
        },
        "servers": [{"url": "/"}],
        "components": {
            "securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}},
            "schemas": error_schemas(),
        },
        "security": [{"bearer": []}],
        "paths": paths,
    })
}

/// A router with `GET /openapi.json` (public — merge outside the gate).
/// `extra_paths` is an app-contributed OpenAPI paths object merged into the doc
/// (so apps can document routes the platform doesn't name, e.g. realtime SSE/WS).
pub fn build_router(specs: &[Arc<EndpointSpec>], extra_paths: &Value) -> Router<AppState> {
    let doc = Arc::new(generate(specs, extra_paths));
    Router::new().route(
        "/openapi.json",
        get(move || {
            let doc = doc.clone();
            async move { Json((*doc).clone()) }
        }),
    )
}
