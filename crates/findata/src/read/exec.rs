//! Generic executor for declarative read endpoints + the router builder.
//!
//! One axum handler serves every `[[read.endpoint]]`: resolve params → bind →
//! (cache) → execute prepared SQL → `rows_to_objects` → strip lineage → shape →
//! serialize, with ETag / `If-None-Match` 304 + `Cache-Control` edge headers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{RawPathParams, RawQuery, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::{Map, Value};

use super::bind;
use super::cache::{CacheKey, CachedBody};
use super::spec::{EndpointSpec, Shape};
use crate::db::lineage::strip_lineage_rows;
use crate::db::rows::rows_to_objects;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// Build a router mounting every spec as a GET route (reads are GET-only).
pub fn build_router(specs: &[Arc<EndpointSpec>]) -> Router<AppState> {
    let mut r = Router::new();
    for spec in specs {
        let spec = spec.clone();
        let id: Arc<str> = Arc::from(spec.id.as_str());
        let path = spec.path.clone();
        let handler = move |raw_path: RawPathParams, raw_q: RawQuery, headers: HeaderMap, st: State<AppState>| {
            let spec = spec.clone();
            let id = id.clone();
            async move { run_spec(&st.0, &spec, id, raw_path, raw_q, &headers).await }
        };
        r = r.route(&path, get(handler));
    }
    r
}

async fn run_spec(
    st: &AppState,
    spec: &EndpointSpec,
    id: Arc<str>,
    raw_path: RawPathParams,
    raw_q: RawQuery,
    headers: &HeaderMap,
) -> Response {
    // Path + query maps.
    let mut path: HashMap<String, String> = HashMap::new();
    for (k, v) in &raw_path {
        path.insert(k.to_string(), v.to_string());
    }
    let query = parse_query(raw_q.0.as_deref());

    let bound = match bind::resolve(spec, &path, &query) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };

    let has_symbol = path.contains_key("symbol");
    let symbol = path.get("symbol").cloned();
    let ttl = spec.ttl_duration();

    // Produce (or fetch cached) the serialized body.
    let body: ApiResult<Arc<CachedBody>> = if spec.cache {
        let gen = st.read_cache.generation(&id);
        let key = CacheKey::new(id, gen, bound.canon.clone());
        st.read_cache
            .get_or_compute(key, ttl, true, || async {
                produce(st, spec, &bound, has_symbol, symbol.clone()).await
            })
            .await
    } else {
        produce(st, spec, &bound, has_symbol, symbol.clone())
            .await
            .map(|bytes| CachedBody::new(bytes, ttl))
    };

    match body {
        Ok(cb) => respond(&cb, spec, headers),
        Err(e) => e.into_response(),
    }
}

/// Execute the query and serialize the shaped JSON body.
async fn produce(
    st: &AppState,
    spec: &EndpointSpec,
    bound: &bind::Bound,
    has_symbol: bool,
    symbol: Option<String>,
) -> ApiResult<Vec<u8>> {
    let client = st.pool.get().await?;
    let rows = client.query(&bound.sql, &bound.refs()).await?;
    let mut objs = rows_to_objects(&rows);
    if spec.strip_lineage {
        objs = strip_lineage_rows(objs);
    }
    let value = shape(spec, objs, has_symbol, symbol)?;
    serde_json::to_vec(&value).map_err(|e| ApiError::Internal(anyhow::anyhow!(e)))
}

fn shape(
    spec: &EndpointSpec,
    objs: Vec<Map<String, Value>>,
    has_symbol: bool,
    symbol: Option<String>,
) -> ApiResult<Value> {
    match spec.shape {
        Shape::Rows => Ok(Value::Array(objs.into_iter().map(Value::Object).collect())),
        Shape::One => match objs.into_iter().next() {
            Some(o) => Ok(Value::Object(o)),
            None => Err(ApiError::NotFound("not found".into())),
        },
        Shape::Envelope => {
            let key = spec.envelope_key.clone().unwrap_or_else(|| "data".to_string());
            let mut env = Map::new();
            if has_symbol {
                if let Some(s) = symbol {
                    env.insert("symbol".into(), Value::String(s));
                }
            }
            env.insert("count".into(), Value::from(objs.len()));
            env.insert(key, Value::Array(objs.into_iter().map(Value::Object).collect()));
            Ok(Value::Object(env))
        }
    }
}

fn respond(cb: &CachedBody, spec: &EndpointSpec, headers: &HeaderMap) -> Response {
    let etag = cb.etag.as_ref();
    // If-None-Match → 304.
    if let Some(inm) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if inm.split(',').any(|t| t.trim() == etag) {
            return (
                StatusCode::NOT_MODIFIED,
                [
                    (header::ETAG, etag.to_string()),
                    (header::CACHE_CONTROL, cache_control(spec)),
                ],
            )
                .into_response();
        }
    }
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json".to_string()),
            (header::ETAG, etag.to_string()),
            (header::CACHE_CONTROL, cache_control(spec)),
        ],
        cb.bytes.clone(),
    )
        .into_response()
}

fn cache_control(spec: &EndpointSpec) -> String {
    if spec.cache {
        format!("public, max-age={}", spec.ttl_duration().as_secs())
    } else {
        "no-store".to_string()
    }
}

/// Minimal `a=b&c=d` query parser with `+`/`%XX` decoding.
fn parse_query(q: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(q) = q else { return out };
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.insert(pct_decode(k), pct_decode(v));
    }
    out
}

fn pct_decode(s: &str) -> String {
    let s = s.replace('+', " ");
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
