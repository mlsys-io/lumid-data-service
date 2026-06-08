//! `GET /admin/export/:schema/:table` — paginated NDJSON table dump.
//!
//! Returns rows as newline-delimited JSON, backend-agnostic (Postgres or
//! ClickHouse). Pair with `POST /ingest/:schema/:table/stream` on the
//! destination instance to migrate data over a single HTTP port.
//!
//! ```text
//! # page through source, stream into target
//! offset=0
//! while true; do
//!   body=$(curl -sf "https://src:5012/admin/export/myschema/mytable?offset=$offset&limit=10000" \
//!     -H "Authorization: Bearer $SRC_TOKEN")
//!   [ -z "$body" ] && break
//!   echo "$body" | curl -sf -X POST "https://dst:5012/ingest/myschema/mytable/stream" \
//!     -H "Authorization: Bearer $DST_TOKEN" \
//!     -H "Content-Type: application/x-ndjson" --data-binary @-
//!   offset=$((offset + 10000))
//! done
//! ```

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::Extension;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::types::ToSql;

use crate::auth::Identity;
use crate::backend::postgres::norm_ident;
use crate::backend::BoundQuery;
use crate::error::ApiError;
use crate::read::bind::BindValue;
use crate::state::AppState;
use crate::sync::push::strip_server_side_cols;

use super::ingest::require_admin;

#[derive(Deserialize)]
pub struct ExportQuery {
    /// Starting row offset (0-based, default 0).
    pub offset: Option<i64>,
    /// Rows per page (default 10 000, max 100 000).
    pub limit: Option<i64>,
    /// Filter rows by `status` column value, e.g. `?status=pending`.
    pub status: Option<String>,
    /// Return only rows with `created_at` strictly after this ISO-8601 timestamp,
    /// e.g. `?after=2026-06-08T03:00:00Z`. Useful for incremental polling.
    pub after: Option<String>,
}

/// `GET /admin/export/:schema/:table`
///
/// Returns the requested page as NDJSON (one JSON object per line).
/// An empty body signals the end of the table — stop paging when you get it.
/// Requires a local key or `super_admin` role.
///
/// Optional query params:
/// - `?status=pending`  — filter by the `status` column (for mailbox tables)
/// - `?after=<iso8601>` — return only rows with `created_at` after the given timestamp
pub async fn get_export(
    State(st): State<AppState>,
    Extension(identity): Extension<Identity>,
    Path((schema, table)): Path<(String, String)>,
    Query(q): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    require_admin(&identity)?;

    let schema_n = norm_ident(&schema)
        .ok_or_else(|| ApiError::BadRequest("invalid schema name".into()))?;
    let table_n = norm_ident(&table)
        .ok_or_else(|| ApiError::BadRequest("invalid table name".into()))?;

    let backend = st.backends.get(&schema_n, &table_n).await?;
    if backend.table_meta(&schema_n, &table_n).await?.is_none() {
        return Err(ApiError::NotFound(format!("unknown table: {schema_n}.{table_n}")));
    }

    let limit: i64 = q.limit.unwrap_or(10_000).clamp(1, 100_000);
    let offset: i64 = q.offset.unwrap_or(0).max(0);

    // Build optional WHERE clauses. $1/$2 are reserved for LIMIT/OFFSET.
    let mut where_parts: Vec<String> = Vec::new();
    let mut next_param = 3usize;

    let status_val = q.status;
    // Parse after= into a typed timestamp so tokio-postgres can bind it as timestamptz.
    let after_val: Option<DateTime<Utc>> = match q.after {
        Some(ref s) => s.parse().ok().or_else(|| {
            // accept bare date "YYYY-MM-DD" too
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .ok()
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
        }),
        None => None,
    };

    if status_val.is_some() {
        where_parts.push(format!("status = ${next_param}"));
        next_param += 1;
    }
    if after_val.is_some() {
        where_parts.push(format!("created_at > ${next_param}"));
    }

    let where_sql = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };

    // When filtering, impose a stable order so cursor-based polling is consistent.
    let order_sql = if where_parts.is_empty() {
        String::new()
    } else {
        " ORDER BY created_at, msg_id".to_string()
    };

    let sql = format!(
        r#"SELECT * FROM "{schema_n}"."{table_n}"{where_sql}{order_sql} LIMIT $1 OFFSET $2"#
    );

    // Postgres backend only uses `params` (ignores `binds`).
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&limit, &offset];
    if let Some(ref s) = status_val {
        params.push(s);
    }
    if let Some(ref a) = after_val {
        params.push(a);
    }

    let rows = backend
        .query_rows(&BoundQuery {
            sql: &sql,
            params,
            binds: &[BindValue::Int(limit), BindValue::Int(offset)],
            pre_lowered: false,
        })
        .await?;

    let mut body: Vec<u8> = Vec::with_capacity(rows.len() * 128);
    for row in &rows {
        let clean = strip_server_side_cols(&Value::Object(row.clone()));
        serde_json::to_writer(&mut body, &clean)
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("{e}")))?;
        body.push(b'\n');
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(body))
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("{e}")))
}
