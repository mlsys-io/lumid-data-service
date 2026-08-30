//! Catalog read-plane handlers — port of the pure-asyncpg routes in
//! `api/routes/catalog.py`.
//!
//! These EXPOSE provenance (no lineage stripping). The 4 ingress-discovery
//! endpoints that proxy to the injection service (schema.json, /catalog/ingress,
//! /catalog/ingress/adapters, /catalog/ingress/proposals) are handled separately
//! and are NOT ported here.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::queries::catalog as q;
use crate::state::AppState;

/// GET /catalog/schemas
pub async fn get_schemas(State(st): State<AppState>) -> ApiResult<Json<Value>> {
    let effective = q::effective_schemas(&st.settings.user_schemas);
    let schemas = q::list_schemas(&st.pool, &effective).await?;
    Ok(Json(json!({ "schemas": schemas })))
}

/// GET /catalog/schemas/{schema}/tables
pub async fn get_schema_tables(
    State(st): State<AppState>,
    Path(schema): Path<String>,
) -> ApiResult<Json<Value>> {
    let effective = q::effective_schemas(&st.settings.user_schemas);
    if !q::is_user_schema(&schema, &effective) {
        return Err(ApiError::NotFound(format!("unknown schema '{schema}'")));
    }
    let tables = q::list_tables(&st.pool, &schema).await?;
    Ok(Json(json!({ "schema": schema, "tables": tables })))
}

/// GET /catalog/tables/{schema}/{table}
pub async fn get_table_profile(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let effective = q::effective_schemas(&st.settings.user_schemas);
    if !q::is_user_schema(&schema, &effective) {
        return Err(ApiError::NotFound(format!("unknown table {schema}.{table}")));
    }
    match q::table_profile(&st.pool, &schema, &table).await? {
        Some(profile) => Ok(Json(profile)),
        None => Err(ApiError::NotFound(format!("unknown table {schema}.{table}"))),
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
    Path((schema, table)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    // No `is_user_schema` gate, matching the route's prior behavior (it
    // introspected any schema/table via `write::introspect::table_meta`,
    // unrestricted) — some ACL-only write targets live outside `USER_SCHEMAS`
    // and their `schema_url` (see `list_writable_for_role`) must keep resolving.
    match q::table_schema_json(&st.pool, &schema, &table).await? {
        Some(schema_json) => Ok(Json(schema_json)),
        None => Err(ApiError::NotFound(format!("unknown table: {schema}.{table}"))),
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
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    match q::trace_run(&st.pool, &run_id).await? {
        Some(row) => Ok(Json(row)),
        None => Err(ApiError::NotFound(format!("unknown run_id '{run_id}'"))),
    }
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
        let key_cols = q::natural_key_for(&client, &schema, &table).await.unwrap_or_default();
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
