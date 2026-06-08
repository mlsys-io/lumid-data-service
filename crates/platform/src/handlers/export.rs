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
}

/// `GET /admin/export/:schema/:table`
///
/// Returns the requested page as NDJSON (one JSON object per line).
/// An empty body signals the end of the table — stop paging when you get it.
/// Requires a local key or `super_admin` role.
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

    let sql = format!(r#"SELECT * FROM "{schema_n}"."{table_n}" LIMIT $1 OFFSET $2"#);
    let binds = [BindValue::Int(limit), BindValue::Int(offset)];
    let params: Vec<&(dyn ToSql + Sync)> = vec![&limit, &offset];

    let rows = backend
        .query_rows(&BoundQuery {
            sql: &sql,
            params,
            binds: &binds,
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
