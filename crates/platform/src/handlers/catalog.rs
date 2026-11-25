//! Catalog read-plane handlers — port of the pure-asyncpg routes in
//! `api/routes/catalog.py`.
//!
//! These EXPOSE provenance (no lineage stripping). The 4 ingress-discovery
//! endpoints that proxy to the injection service (schema.json, /catalog/ingress,
//! /catalog/ingress/adapters, /catalog/ingress/proposals) are handled separately
//! and are NOT ported here.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::queries::catalog as q;
use crate::state::AppState;

/// How long a catalog listing is served without asking Postgres again.
const CATALOG_TTL: Duration = Duration::from_secs(300);

type Rows = Vec<Map<String, Value>>;

fn catalog_cache() -> &'static Mutex<HashMap<String, (Instant, Rows)>> {
    static C: OnceLock<Mutex<HashMap<String, (Instant, Rows)>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Catalog listings, cached: fresh for `CATALOG_TTL`, and on a failed refresh
/// the last good answer is served instead of an error.
///
/// The listing calls TimescaleDB's per-hypertable size/row functions across
/// every chunk. On FinData (32k relations) that is ~2 s normally, but when the
/// data NAS is I/O-stalled it runs past the 30 s statement timeout, and Studio
/// showed the whole warehouse "offline" (500, 2026-11-25). The numbers are
/// estimates; a few minutes old is fine, an outage is not.
async fn catalog_cached<F, Fut>(key: String, load: F) -> ApiResult<Rows>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ApiResult<Rows>> + Send + 'static,
{
    let cached = catalog_cache()
        .lock()
        .ok()
        .and_then(|c| c.get(&key).cloned());
    match cached {
        // Fresh: serve it.
        Some((at, rows)) if at.elapsed() < CATALOG_TTL => Ok(rows),
        // Stale: serve it now and refresh in the background. Re-stamping first
        // means concurrent requests don't all start their own refresh.
        Some((at, rows)) => {
            if let Ok(mut c) = catalog_cache().lock() {
                if let Some(e) = c.get_mut(&key) {
                    e.0 = Instant::now();
                }
            }
            let fut = load();
            tokio::spawn(async move {
                match fut.await {
                    Ok(fresh) => {
                        if let Ok(mut c) = catalog_cache().lock() {
                            c.insert(key, (Instant::now(), fresh));
                        }
                    }
                    Err(e) => tracing::warn!(%key, age_s = at.elapsed().as_secs(),
                        "catalog refresh failed; still serving the last good listing: {e}"),
                }
            });
            Ok(rows)
        }
        // Nothing yet: this request pays for it.
        None => {
            let rows = load().await?;
            if let Ok(mut c) = catalog_cache().lock() {
                c.insert(key, (Instant::now(), rows.clone()));
            }
            Ok(rows)
        }
    }
}

/// GET /catalog/schemas
pub async fn get_schemas(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
) -> ApiResult<Json<Value>> {
    let caller = identity.as_ref().map(|Extension(id)| id);
    let effective = q::effective_schemas(&st.settings.user_schemas);
    let (pool, eff) = (st.pool.clone(), effective.clone());
    let schemas = catalog_cached(
        format!("schemas:{}", effective.join(",")),
        move || async move { q::list_schemas(&pool, &eff).await },
    )
    .await?;
    // COVERAGE NOTE, served with the schema list.
    //
    // The warehouse holds 6.2B rows of 1-minute equity OHLCV, which reads as
    // "equities are covered" — so a researcher reasonably goes looking for
    // equity tick trades and L2 depth, finds nothing under `trade`, `l2`,
    // `depth` or `nbbo`, and cannot tell an absent dataset from a broken
    // search. Reported 2026-08-30 and verified: the only equity matches are
    // `macro.commitment_of_traders*` (CFTC positioning, a false hit on the
    // substring "trade").
    //
    // Tick-level and order-book data exist ONLY for prediction markets. Saying
    // so here costs one field and saves the search.
    //
    // Each note is about one schema's data, so it is served only where that schema
    // exists. Unconditional, they told the NUS hub's arXiv catalog (and the home
    // hub's) about equity OHLCV they do not hold (2026-11-25).
    // Restricted schemas are left out for anyone not on their list.
    let schemas: Vec<_> = schemas
        .into_iter()
        .filter(|row| {
            let name = row.get("schema").and_then(Value::as_str).unwrap_or("");
            crate::restricted::may_see(&st.settings, caller, name)
        })
        .collect();
    let notes = coverage_notes(&schemas);
    let mut body = json!({ "schemas": schemas });
    if !notes.is_empty() {
        body["coverage_notes"] = Value::Object(notes);
    }
    Ok(Json(body))
}

/// Coverage notes for the schemas this instance actually lists.
fn coverage_notes(schemas: &[Map<String, Value>]) -> Map<String, Value> {
    let has = |name: &str| {
        schemas
            .iter()
            .any(|s| s.get("schema").and_then(Value::as_str) == Some(name))
    };
    let mut notes = Map::new();
    if has("market") {
        notes.insert(
            "equities".into(),
            "OHLCV only (market.ohlc_1min, 1-minute bars). No tick/trade tape and no L2 depth."
                .into(),
        );
    }
    if has("prediction_markets") {
        notes.insert(
            "prediction_markets".into(),
            "Full depth: trades, L2 orderbook snapshots, and derived candles for Polymarket and Kalshi.".into(),
        );
    }
    notes
}

#[cfg(test)]
mod coverage_notes_tests {
    use super::*;

    fn schemas(names: &[&str]) -> Vec<Map<String, Value>> {
        names
            .iter()
            .map(|n| {
                let mut m = Map::new();
                m.insert("schema".into(), Value::from(*n));
                m
            })
            .collect()
    }

    #[test]
    fn notes_only_for_schemas_that_exist() {
        assert!(coverage_notes(&schemas(&["arxiv", "provenance"])).is_empty());
        let fin = coverage_notes(&schemas(&["market", "prediction_markets", "macro"]));
        assert!(fin.contains_key("equities") && fin.contains_key("prediction_markets"));
        let only_pm = coverage_notes(&schemas(&["prediction_markets"]));
        assert!(!only_pm.contains_key("equities") && only_pm.contains_key("prediction_markets"));
    }
}

/// GET /catalog/schemas/{schema}/tables
pub async fn get_schema_tables(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Path(schema): Path<String>,
) -> ApiResult<Json<Value>> {
    let effective = q::effective_schemas(&st.settings.user_schemas);
    let caller = identity.as_ref().map(|Extension(id)| id);
    if !q::is_user_schema(&schema, &effective)
        || !crate::restricted::may_see(&st.settings, caller, &schema)
    {
        return Err(ApiError::NotFound(format!("unknown schema '{schema}'")));
    }
    let (pool, sch) = (st.pool.clone(), schema.clone());
    let tables = catalog_cached(format!("tables:{schema}"), move || async move {
        q::list_tables(&pool, &sch).await
    })
    .await?;
    Ok(Json(json!({ "schema": schema, "tables": tables })))
}

/// GET /catalog/tables/{schema}/{table}
pub async fn get_table_profile(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Path((schema, table)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let effective = q::effective_schemas(&st.settings.user_schemas);
    let caller = identity.as_ref().map(|Extension(id)| id);
    if !q::is_user_schema(&schema, &effective)
        || !crate::restricted::may_see(&st.settings, caller, &schema)
    {
        return Err(ApiError::NotFound(format!(
            "unknown table {schema}.{table}"
        )));
    }
    match q::table_profile(&st.pool, &schema, &table).await? {
        Some(profile) => Ok(Json(profile)),
        None => Err(ApiError::NotFound(format!(
            "unknown table {schema}.{table}"
        ))),
    }
}

/// GET /catalog/tables/{schema}/{table}/schema.json — read-facing JSON Schema
/// (every live column). Moved here from `handlers::ingest` (2026-08-30): it was
/// built from the write/ingest input model (`validation::schema_json_for`),
/// which silently excluded server-stamped + generated columns (LUMID-001) and
/// only ever covered `information_schema.columns`, which excludes materialized
/// views (LUMID-003). See `queries::catalog::table_schema_json`.
pub async fn get_table_schema_json(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Path((schema, table)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    // Column lists of a restricted schema are part of what the allow-list hides.
    let caller = identity.as_ref().map(|Extension(id)| id);
    if !crate::restricted::may_see(&st.settings, caller, &schema) {
        return Err(ApiError::NotFound(format!("unknown table: {schema}.{table}")));
    }
    // No `is_user_schema` gate, matching the route's prior behavior (it
    // introspected any schema/table via `write::introspect::table_meta`,
    // unrestricted) — some ACL-only write targets live outside `USER_SCHEMAS`
    // and their `schema_url` (see `list_writable_for_role`) must keep resolving.
    match q::table_schema_json(&st.pool, &schema, &table).await? {
        Some(schema_json) => Ok(Json(schema_json)),
        None => Err(ApiError::NotFound(format!(
            "unknown table: {schema}.{table}"
        ))),
    }
}

/// GET /catalog/ingress/writable — what THIS identity can write.
pub async fn get_writable(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<Value>> {
    let rows = q::list_writable_for_role(&st.pool, &identity.role).await?;
    Ok(Json(json!({
        "role": identity.role,
        "sub": identity.sub,
        "count": rows.len(),
        "tables": rows,
    })))
}

/// GET /catalog/lineage/run/{run_id}
pub async fn get_lineage_run(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    // Same rule as `/catalog/lineage/runs`: non-admin callers see only their
    // own runs. Someone else's run answers exactly like an unknown id (404),
    // so a probe cannot confirm it exists.
    let not_found = || ApiError::NotFound(format!("unknown run_id '{run_id}'"));
    let row = q::trace_run(&st.pool, &run_id)
        .await?
        .ok_or_else(not_found)?;
    if may_see_run(&identity, &row) {
        Ok(Json(row))
    } else {
        Err(not_found())
    }
}

/// Admins and local keys see every run; everyone else only runs they submitted.
fn may_see_run(identity: &Identity, run: &Value) -> bool {
    identity.role == "super_admin"
        || identity.role == "local"
        || run.get("submitted_by").and_then(Value::as_str) == Some(identity.sub.as_str())
}

#[derive(Deserialize)]
pub struct RunsParams {
    pub submitted_by: Option<String>,
    pub target_schema: Option<String>,
    pub target_table: Option<String>,
    pub status: Option<String>,
    pub since: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}
fn default_limit() -> i64 {
    50
}

/// GET /catalog/lineage/runs
pub async fn get_lineage_runs(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Query(p): Query<RunsParams>,
) -> ApiResult<Json<Value>> {
    // Non-admin callers are restricted to their own submitter id.
    let mut submitted_by = p.submitted_by.clone();
    if identity.role != "super_admin" && identity.role != "local" {
        match &submitted_by {
            None => submitted_by = Some(identity.sub.clone()),
            Some(s) if *s != identity.sub => {
                return Err(ApiError::Forbidden("can only inspect your own runs".into()));
            }
            _ => {}
        }
    }
    let limit = p.limit.clamp(1, 500);
    let rows = q::list_runs_for(
        &st.pool,
        submitted_by.as_deref(),
        p.target_schema.as_deref(),
        p.target_table.as_deref(),
        p.status.as_deref(),
        p.since.as_deref(),
        limit,
    )
    .await?;
    Ok(Json(json!({ "count": rows.len(), "runs": rows })))
}

/// GET /catalog/lineage/row — natural-key lineage trace.
/// Every query param except `schema`/`table` is a natural-key filter.
pub async fn get_lineage_row(
    State(st): State<AppState>,
    Query(mut raw): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let schema = raw
        .remove("schema")
        .ok_or_else(|| ApiError::BadRequest("missing 'schema' query parameter".into()))?;
    let table = raw
        .remove("table")
        .ok_or_else(|| ApiError::BadRequest("missing 'table' query parameter".into()))?;
    let effective = q::effective_schemas(&st.settings.user_schemas);
    if !q::is_user_schema(&schema, &effective) {
        return Err(ApiError::NotFound(format!("unknown schema '{schema}'")));
    }
    if raw.is_empty() {
        // LUMID-007: name the actual natural key instead of a generic 400, so
        // the caller doesn't have to separately fetch the table profile first.
        let client = st.pool.get().await?;
        let key_cols = q::natural_key_for(&client, &schema, &table)
            .await
            .unwrap_or_default();
        let msg = if key_cols.is_empty() {
            "supply at least one <column>=<value> query parameter (natural key)".to_string()
        } else {
            format!(
                "supply at least one <column>=<value> query parameter — natural key for {schema}.{table} is ({})",
                key_cols.join(", ")
            )
        };
        return Err(ApiError::BadRequest(msg));
    }
    match q::trace_by_natural_key(&st.pool, &schema, &table, &raw, &effective).await? {
        Some(out) => Ok(Json(out)),
        None => Err(ApiError::NotFound(format!(
            "no rows in {schema}.{table} match the supplied key (or schema/columns invalid)"
        ))),
    }
}

#[derive(Deserialize)]
pub struct SourcesParams {
    pub schema: Option<String>,
    pub table: Option<String>,
}

/// GET /catalog/sources
pub async fn get_sources(
    State(st): State<AppState>,
    Query(p): Query<SourcesParams>,
) -> ApiResult<Json<Value>> {
    let effective = q::effective_schemas(&st.settings.user_schemas);
    if let Some(schema) = p.schema.as_deref() {
        if !schema.is_empty() && !q::is_user_schema(schema, &effective) {
            return Err(ApiError::NotFound(format!("unknown schema '{schema}'")));
        }
    }
    let sources = q::list_sources(&st.pool, p.schema.as_deref(), p.table.as_deref()).await?;
    Ok(Json(json!({ "sources": sources })))
}

/// GET /catalog/submitters
pub async fn get_submitters(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> ApiResult<Json<Value>> {
    let only_self = if identity.role != "super_admin" && identity.role != "local" {
        Some(identity.sub.as_str())
    } else {
        None
    };
    let submitters = q::list_submitters(&st.pool, only_self).await?;
    Ok(Json(json!({ "submitters": submitters })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(sub: &str, role: &str) -> Identity {
        Identity {
            sub: sub.into(),
            role: role.into(),
            email: None,
            active: true,
            scopes: vec![],
        }
    }

    #[test]
    fn lineage_run_is_visible_to_its_submitter_and_privileged_roles_only() {
        let run = json!({"run_id": "r", "submitted_by": "user-a"});
        assert!(may_see_run(&ident("user-a", "user"), &run));
        assert!(!may_see_run(&ident("user-b", "user"), &run));
        assert!(may_see_run(&ident("ops", "super_admin"), &run));
        assert!(may_see_run(&ident("ops", "local"), &run));
        // Mirrors /catalog/lineage/runs, which does not widen for plain admin.
        assert!(!may_see_run(&ident("ops", "admin"), &run));
        // A run with no submitter (scraper/sync rows) is privileged-only.
        let unowned = json!({"run_id": "r", "submitted_by": null});
        assert!(!may_see_run(&ident("user-a", "user"), &unowned));
        assert!(may_see_run(&ident("ops", "local"), &unowned));
    }
}
