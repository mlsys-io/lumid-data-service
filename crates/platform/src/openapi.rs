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

fn generate(specs: &[Arc<EndpointSpec>]) -> Value {
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
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": std::env::var("FINDATA_SERVICE_NAME").unwrap_or_else(|_| "lumid".into()),
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Declarative read endpoints. Bespoke routes (ohlc, quotes, screener, \
                prediction-markets, /v1 LLM, MCP) are documented at /reference.",
        },
        "servers": [{"url": "/"}],
        "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}},
        "security": [{"bearer": []}],
        "paths": paths,
    })
}

/// A router with `GET /openapi.json` (public — merge outside the gate).
pub fn build_router(specs: &[Arc<EndpointSpec>]) -> Router<AppState> {
    let doc = Arc::new(generate(specs));
    Router::new().route(
        "/openapi.json",
        get(move || {
            let doc = doc.clone();
            async move { Json((*doc).clone()) }
        }),
    )
}
