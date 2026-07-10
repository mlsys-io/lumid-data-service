//! Generic executor for declarative read endpoints + the router builder.
//!
//! One axum handler serves every `[[read.endpoint]]`: resolve params → bind →
//! (cache) → execute prepared SQL → `rows_to_objects` → strip lineage → shape →
//! serialize, with ETag / `If-None-Match` 304 + `Cache-Control` edge headers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, OriginalUri, RawPathParams, RawQuery, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::{Map, Value};

use super::bind;
use super::cache::{CacheKey, CachedBody};
use super::spec::{EndpointSpec, Shape};
use crate::auth::Identity;
use crate::db::lineage::strip_lineage_rows;
use crate::error::{ApiError, ApiResult};
use crate::federation::OriginIdentity;
use crate::state::AppState;

/// Build a router mounting every spec as a GET route (reads are GET-only).
pub fn build_router(specs: &[Arc<EndpointSpec>]) -> Router<AppState> {
    let mut r = Router::new();
    for spec in specs {
        let spec = spec.clone();
        let id: Arc<str> = Arc::from(spec.id.as_str());
        let path = spec.path.clone();
        let handler = move |raw_path: RawPathParams,
                            raw_q: RawQuery,
                            orig: OriginalUri,
                            ident: Option<Extension<Identity>>,
                            headers: HeaderMap,
                            st: State<AppState>| {
            let spec = spec.clone();
            let id = id.clone();
            async move {
                run_spec(&st.0, &spec, id, raw_path, raw_q, orig, ident, &headers).await
            }
        };
        r = r.route(&path, get(handler));
    }
    r
}

#[allow(clippy::too_many_arguments)]
async fn run_spec(
    st: &AppState,
    spec: &EndpointSpec,
    id: Arc<str>,
    raw_path: RawPathParams,
    raw_q: RawQuery,
    orig: OriginalUri,
    ident: Option<Extension<Identity>>,
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

    let id_kv = id_echo(&path);
    let ttl = spec.ttl_duration();

    // Federation default-route (F1): when `read_federate` names a configured
    // peer, declarative reads this instance doesn't own are forwarded to that
    // peer. MVP rule — shadow mode: every declarative read is treated as
    // federated (the shadow has no local warehouse). The forward is wrapped
    // INSIDE the read-cache below (keyed spec-id+params), so a warm entry serves
    // locally with no peer round-trip. When `read_federate` is unset, this is
    // `None` and reads run locally exactly as before.
    let fed_peer = st
        .settings
        .read_federate
        .as_deref()
        .and_then(|pid| st.federation.peer(pid));
    let origin = ident
        .map(|Extension(i)| OriginIdentity { sub: i.sub, role: i.role })
        .unwrap_or_default();

    // Produce (or fetch cached) the serialized body. The compute closure either
    // runs the local query (`produce`) or forwards to the peer — both return the
    // serialized JSON bytes, so the cache layer is identical for both paths.
    let raw_q_str = raw_q.0.clone();
    let compute = || async {
        match fed_peer {
            Some(peer) => {
                produce_federated(st, peer, orig.0.path(), raw_q_str.as_deref(), &origin).await
            }
            None => produce(st, spec, &bound, id_kv.clone()).await,
        }
    };

    let body: ApiResult<Arc<CachedBody>> = if spec.cache {
        let gen = st.read_cache.generation(&id);
        let key = CacheKey::new(id, gen, bound.canon.clone());
        st.read_cache.get_or_compute(key, ttl, true, compute).await
    } else {
        compute().await.map(|bytes| CachedBody::new(bytes, ttl))
    };

    match body {
        Ok(cb) => respond(&cb, spec, headers),
        Err(e) => e.into_response(),
    }
}

/// Forward a declarative read to a federation `peer`'s identical endpoint and
/// return the peer's JSON body bytes (to be cached + relayed by the caller).
///
/// Only a 2xx JSON response is cached; any other status maps to an `ApiError`
/// (NOT cached — errors must not be memoized) that surfaces the peer's status
/// class to the client. This is the compute half of the cache-wrapped forward:
/// the surrounding `get_or_compute` means a warm spec-id+params entry never
/// reaches here (no peer round-trip on a cache hit).
async fn produce_federated(
    st: &AppState,
    peer: &crate::config::Peer,
    path: &str,
    query: Option<&str>,
    origin: &OriginIdentity,
) -> ApiResult<Vec<u8>> {
    // Reads are GET-only; no request body.
    let resp = st
        .federation
        .forward(peer, reqwest::Method::GET, path, query, Vec::new(), origin)
        .await;
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("federation body read: {e}")))?;
    if status.is_success() {
        return Ok(bytes.to_vec());
    }
    // Map the peer's error class to ours (message from the peer body when JSON).
    let detail = serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|v| v.get("detail").and_then(|d| d.as_str()).map(str::to_string))
        .unwrap_or_else(|| format!("federation peer returned {}", status.as_u16()));
    Err(match status {
        StatusCode::NOT_FOUND => ApiError::NotFound(detail),
        StatusCode::BAD_REQUEST => ApiError::BadRequest(detail),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ApiError::Forbidden(detail),
        _ => ApiError::Unavailable(detail),
    })
}

/// Execute the query and serialize the shaped JSON body.
///
/// The row fetch is dispatched through the backend registry
/// (`backends.get(schema, table).query_rows(..)`) resolved on the spec's first
/// declared table; lineage-strip + shape + serialize stay here. Phase A: every
/// table resolves to Postgres, so this runs on `st.pool` exactly as before.
async fn produce(
    st: &AppState,
    spec: &EndpointSpec,
    bound: &bind::Bound,
    id_kv: Option<(String, String)>,
) -> ApiResult<Vec<u8>> {
    use crate::backend::BackendKind;
    use crate::read::dialect::{ClickHouseDialect, Dialect};

    let (schema, table) = read_target(spec);
    let backend = st.backends.get(&schema, &table).await?;

    // Dialect lowering (`T-READ-IR-001`). Postgres uses the already-lowered
    // `$N` SQL (byte-equivalent to legacy). ClickHouse, when the spec parsed to
    // an IR, lowers the IR to CH SQL (`?` binds, CH casts, PREWHERE/FINAL); when
    // it didn't, the CH backend falls back to translating the PG placeholders
    // (PR #9 behaviour) — so we pass the PG-shaped SQL and let it translate.
    let query_fut = async {
        match backend.kind() {
            BackendKind::ClickHouse if spec.ir.is_some() => {
                // Derive CH read knobs from the parsed IR: leading ORDER BY key for
                // PREWHERE hoist; FINAL is opt-in via the spec's `ch_final` flag.
                let ir = spec.ir.as_ref().unwrap();
                let dialect = ClickHouseDialect {
                    final_: spec.ch_final,
                    order_key_cols: ir.order_by.iter().map(|o| o.expr.clone()).collect(),
                };
                let lowered = dialect.lower(&bound.substituted, &bound.value_map, spec.ir.as_ref())?;
                backend
                    .query_rows(&crate::backend::BoundQuery {
                        sql: &lowered.sql,
                        params: Vec::new(),
                        binds: &lowered.values,
                        pre_lowered: true,
                    })
                    .await
            }
            _ => {
                // Postgres (or CH-without-IR fallback): hand over the PG-shaped SQL.
                backend
                    .query_rows(&crate::backend::BoundQuery {
                        sql: &bound.sql,
                        params: bound.refs(),
                        binds: &bound.values,
                        pre_lowered: false,
                    })
                    .await
            }
        }
    };

    let mut objs = if let Some(ms) = spec.query_timeout_ms {
        match tokio::time::timeout(std::time::Duration::from_millis(ms), query_fut).await {
            Ok(result) => result?,
            Err(_elapsed) => {
                tracing::warn!(spec = %spec.id, timeout_ms = ms, "spec query timed out — returning []");
                vec![]
            }
        }
    } else {
        query_fut.await?
    };
    if spec.strip_lineage {
        objs = strip_lineage_rows(objs);
    }
    let value = shape(spec, objs, id_kv)?;
    serde_json::to_vec(&value).map_err(|e| ApiError::Internal(anyhow::anyhow!(e)))
}

/// The id path-param to echo into an `Envelope` response, as `(name, value)`.
/// Recognizes a path param named `key` (canonical) or `symbol` (legacy alias) —
/// the platform names no domain field, but echoes whichever the route declared
/// under its own name, so existing `:symbol` routes keep emitting `"symbol"`.
fn id_echo(path: &HashMap<String, String>) -> Option<(String, String)> {
    for name in ["key", "symbol"] {
        if let Some(v) = path.get(name) {
            return Some((name.to_string(), v.clone()));
        }
    }
    None
}

/// The `(schema, table)` a read endpoint resolves its backend on — the first
/// declared source table (`schema.table`). Endpoints with no declared table (or
/// a bare name) default to the empty pair, which resolves to Postgres.
fn read_target(spec: &EndpointSpec) -> (String, String) {
    match spec.tables.first() {
        Some(t) => match t.split_once('.') {
            Some((s, tbl)) => (s.to_string(), tbl.to_string()),
            None => (String::new(), t.clone()),
        },
        None => (String::new(), String::new()),
    }
}

/// Execute a spec from plain param maps (no axum extractors) and return the
/// shaped JSON value — the read pipeline (bind → execute → shape) without the
/// HTTP/cache/ETag layer. Used by the MCP tool layer to reuse declarative specs
/// as tools. (Reads go straight to the DB here; the cache fronts the HTTP path.)
pub async fn execute_to_value(
    st: &AppState,
    spec: &EndpointSpec,
    path: HashMap<String, String>,
    query: HashMap<String, String>,
) -> ApiResult<Value> {
    let bound = bind::resolve(spec, &path, &query)?;
    let id_kv = id_echo(&path);
    let bytes = produce(st, spec, &bound, id_kv).await?;
    serde_json::from_slice(&bytes).map_err(|e| ApiError::Internal(anyhow::anyhow!(e)))
}

fn shape(
    spec: &EndpointSpec,
    objs: Vec<Map<String, Value>>,
    id_kv: Option<(String, String)>,
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
            if let Some((name, val)) = id_kv {
                env.insert(name, Value::String(val));
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
