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
    let mut query = parse_query(raw_q.0.as_deref());

    // Self-tenant scoping. The `tenant` bind is SERVER-SUPPLIED: we overwrite
    // whatever the client sent with the authenticated caller's `sub`, so a
    // crafted `?tenant=<someone-else>` cannot widen the read. Done BEFORE
    // `bind::resolve` so the bind resolves normally from there on.
    //
    // An admin elevating via `admin_cross_tenant` is exempt: that path reads
    // under the cross-tenant role deliberately, and forcing its own sub into
    // the predicate would defeat the oversight it exists to provide.
    if spec.self_tenant {
        let sub = ident.as_ref().map(|Extension(i)| i.sub.as_str()).unwrap_or("");
        let role = ident.as_ref().map(|Extension(i)| i.role.as_str()).unwrap_or("");
        let elevating = admin_read_elevation(spec, &st.settings.admin_read_role, role).is_some();
        if !elevating {
            if sub.is_empty() {
                // Previously this fell through and died on the unresolved bind
                // as a 500. A caller with no subject cannot be scoped, so say so.
                return ApiError::Forbidden(
                    "this endpoint is self-tenant scoped and the caller has no subject".into(),
                )
                .into_response();
            }
            query.insert("tenant".to_string(), sub.to_string());
        }
    }

    let bound = match bind::resolve(spec, &path, &query) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };

    let id_kv = id_echo(&path);
    let ttl = spec.ttl_duration();

    // Admin cross-tenant oversight (Phase D4). The elevation decision is made
    // here from the trusted `Identity` extension — never from a client-supplied
    // field. A `user` (or an anonymous/absent identity) never elevates, so the
    // non-admin path is byte-identical to today (same query, same self-scoped RLS
    // binding). See `admin_read_elevation` for the exact gate.
    let caller_role = ident.as_ref().map(|Extension(i)| i.role.as_str()).unwrap_or("");
    // The role actually handed to the query path: the configured cross-tenant
    // role when elevating, else empty (⇒ normal self-scoped `query_rows`).
    let effective_read_role =
        admin_read_elevation(spec, &st.settings.admin_read_role, caller_role).unwrap_or_default();
    let elevate = !effective_read_role.is_empty();

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
    let read_role = effective_read_role.clone();
    let compute = || async {
        match fed_peer {
            Some(peer) => {
                produce_federated(st, peer, orig.0.path(), raw_q_str.as_deref(), &origin).await
            }
            None => produce(st, spec, &bound, id_kv.clone(), &read_role).await,
        }
    };

    let body: ApiResult<Arc<CachedBody>> = if spec.cache {
        let gen = st.read_cache.generation(&id);
        // Fold the elevation into the cache key so an admin's cross-tenant result
        // is NEVER served to a self-scoped (non-admin) caller and vice-versa —
        // the two share a spec id + params but must not share a cached body.
        let canon = if elevate {
            format!("{}\u{1}admin_read={}", bound.canon, effective_read_role)
        } else {
            bound.canon.clone()
        };
        let key = CacheKey::new(id, gen, canon);
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
    read_role: &str,
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
                // `read_role` re-scopes the read for admin cross-tenant oversight;
                // empty ⇒ the normal self-scoped path. The ClickHouse backend's
                // default impl ignores the role, so this is byte-identical for CH.
                backend
                    .query_rows_as_role(&crate::backend::BoundQuery {
                        sql: &lowered.sql,
                        params: Vec::new(),
                        binds: &lowered.values,
                        pre_lowered: true,
                    }, read_role)
                    .await
            }
            _ => {
                // Postgres (or CH-without-IR fallback): hand over the PG-shaped SQL.
                backend
                    .query_rows_as_role(&crate::backend::BoundQuery {
                        sql: &bound.sql,
                        params: bound.refs(),
                        binds: &bound.values,
                        pre_lowered: false,
                    }, read_role)
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

/// Decide the effective DB read-role for admin cross-tenant oversight (Phase D4).
///
/// Returns `Some(role)` — the configured cross-tenant `admin_read_role` — ONLY
/// when all three hold: (1) the endpoint opts in (`spec.admin_cross_tenant`),
/// (2) a non-empty `admin_read_role` is configured, and (3) the SERVER-introspected
/// `caller_role` is `admin`/`super_admin`/`local`. Otherwise `None` ⇒ the caller
/// runs the normal self-scoped read (byte-identical to today). `caller_role` must
/// come from the trusted `Identity`, never a client-supplied field — so a `user`
/// (or anonymous, empty role) can never cross tenants and no leak is possible.
fn admin_read_elevation(spec: &EndpointSpec, admin_read_role: &str, caller_role: &str) -> Option<String> {
    let role = admin_read_role.trim();
    if spec.admin_cross_tenant
        && !role.is_empty()
        && matches!(caller_role, "admin" | "super_admin" | "local")
    {
        Some(role.to_string())
    } else {
        None
    }
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
    // No HTTP identity on the MCP tool path, so a self-tenant endpoint has no
    // subject to scope by. Refuse explicitly instead of dying on the unresolved
    // `:tenant` bind with an opaque 500.
    if spec.self_tenant {
        return Err(ApiError::Forbidden(
            "self-tenant endpoints require an authenticated caller; not available on this path"
                .into(),
        ));
    }
    let bound = bind::resolve(spec, &path, &query)?;
    let id_kv = id_echo(&path);
    // MCP tool path: no HTTP identity here, so never elevate — empty role runs
    // the normal self-scoped read (byte-identical to before Phase D4).
    let bytes = produce(st, spec, &bound, id_kv, "").await?;
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

#[cfg(test)]
mod admin_cross_tenant_tests {
    use super::*;
    use crate::read::spec;

    fn spec_with(admin_cross_tenant: bool) -> EndpointSpec {
        // Build via the real TOML loader so the spec is shaped exactly as at
        // runtime (defaults applied, IR compiled). `admin_cross_tenant` is the
        // only knob these tests vary.
        let toml = format!(
            r#"
[[read.endpoint]]
id = "runtime.cycles"
path = "/runtime/cycles/:box"
ttl = "30s"
sql = "SELECT box, ts FROM obs.runtime_cycles WHERE box = :box ORDER BY ts DESC LIMIT :limit"
tables = ["obs.runtime_cycles"]
admin_cross_tenant = {admin_cross_tenant}
  [[read.endpoint.param]]
  name = "box"
  kind = "path"
  type = "key"
  [[read.endpoint.param]]
  name = "limit"
  kind = "query"
  type = "int"
  default = 100
"#
        );
        spec::parse(&toml).expect("parse spec").pop().expect("one spec")
    }

    // A flagged endpoint elevates ONLY for admin roles, and ONLY when a role is
    // configured. This is the cross-tenant unlock — admins see all tenants.
    #[test]
    fn admin_roles_elevate_when_flagged_and_configured() {
        let sp = spec_with(true);
        for role in ["admin", "super_admin", "local"] {
            assert_eq!(
                admin_read_elevation(&sp, "lqt_admin_read", role),
                Some("lqt_admin_read".to_string()),
                "role {role} must elevate on a flagged endpoint"
            );
        }
    }

    // A `user` (or anonymous/unknown role) must NEVER elevate — this is the
    // no-leak invariant. A cross-tenant read for a non-admin MUST fail to elevate,
    // so the caller stays on the self-scoped RLS path (byte-equivalent to today).
    #[test]
    fn non_admin_roles_never_elevate() {
        let sp = spec_with(true);
        for role in ["user", "", "reader", "trader", "USER", "Admin", "superadmin"] {
            assert_eq!(
                admin_read_elevation(&sp, "lqt_admin_read", role),
                None,
                "role {role:?} must NOT elevate (no cross-tenant leak)"
            );
        }
    }

    // Feature disabled (empty `admin_read_role`) ⇒ even an admin stays self-scoped
    // — the deployment hasn't opted in, so behavior is byte-identical to today.
    #[test]
    fn unconfigured_role_disables_elevation_for_everyone() {
        let sp = spec_with(true);
        for role in ["admin", "super_admin", "local", "user"] {
            assert_eq!(admin_read_elevation(&sp, "", role), None);
            assert_eq!(admin_read_elevation(&sp, "   ", role), None, "whitespace-only role is empty");
        }
    }

    // An endpoint that did NOT opt in is never elevated, regardless of caller role
    // — most endpoints stay strictly self-scoped even for admins.
    #[test]
    fn unflagged_endpoint_never_elevates() {
        let sp = spec_with(false);
        for role in ["admin", "super_admin", "local", "user"] {
            assert_eq!(admin_read_elevation(&sp, "lqt_admin_read", role), None);
        }
    }
}
