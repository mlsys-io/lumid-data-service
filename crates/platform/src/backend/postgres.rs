//! The Postgres backend — wraps the existing PG code paths so the legacy
//! behavior is byte-identical. Nothing here is new logic; it is an extraction:
//!
//!   * [`PostgresBackend::table_meta`]    → [`crate::write::introspect::table_meta`]
//!   * [`PostgresBackend::create_table`]  → [`build_create_table_ddl`] + execute
//!     (lifted verbatim from the inline DDL builder in `ingest/proposals.rs::approve`)
//!   * [`PostgresBackend::write_records`] → the former `ingest/core.rs::write_parsed`
//!     body (column intersection + transaction + [`crate::write::engine::copy_and_merge`])
//!   * [`PostgresBackend::query_rows`]    → the query+`rows_to_objects` half of the
//!     former `read/exec.rs::produce` (lineage/shape stay in `exec.rs`)

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use serde_json::{Map, Value};

use super::{Backend, BackendKind, BoundQuery, CreateTablePlan, WriteRequest};
use crate::db::rows::rows_to_objects;
use crate::error::{ApiError, ApiResult};
use crate::validation::SERVER_STAMPED_COLS;
use crate::write::introspect::{self, TableMeta};
use crate::write::engine;

/// Validate + normalise a SQL identifier (schema/table/column) to
/// `^[a-z_][a-z0-9_]{0,62}$`. Lifted from `ingest/proposals.rs::norm_ident` so
/// the DDL builder can live here unchanged.
pub(crate) fn norm_ident(s: &str) -> Option<String> {
    let l = s.trim().to_lowercase();
    let ok = !l.is_empty()
        && l.len() <= 63
        && l.chars().next().map(|c| c.is_ascii_lowercase() || c == '_').unwrap_or(false)
        && l.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    ok.then_some(l)
}

/// Build the `CREATE TABLE IF NOT EXISTS` DDL for an approved proposal. This is
/// the exact builder that used to live inline in `ingest/proposals.rs::approve`
/// — lifted here so both the approve path and any future caller share one impl.
/// Returns `(schema_n, table_n, ddl)` with all identifiers re-validated +
/// double-quoted (defence in depth against injected JSON keys).
pub fn build_create_table_ddl(plan: &CreateTablePlan<'_>) -> ApiResult<(String, String, String)> {
    let schema_n = norm_ident(plan.schema).ok_or_else(|| ApiError::BadRequest("bad schema".into()))?;
    let table_n = norm_ident(plan.table).ok_or_else(|| ApiError::BadRequest("bad table".into()))?;
    let obj = plan.inferred;
    let mut col_ddl = Vec::new();
    for (c, ty) in obj {
        let c_n = norm_ident(c).ok_or_else(|| ApiError::BadRequest(format!("bad column {c:?}")))?;
        let ty_s = match ty.as_str().unwrap_or("text") {
            "text" | "bigint" | "double precision" | "boolean" | "jsonb" => ty.as_str().unwrap(),
            _ => "text",
        };
        col_ddl.push(format!("\"{c_n}\" {ty_s}"));
    }
    // PK = inferred key (+ source for multi-source safety) if all present; else a surrogate.
    let key_n: Vec<String> = plan
        .key
        .iter()
        .filter_map(|k| norm_ident(k))
        .filter(|k| obj.contains_key(k))
        .collect();
    let pk = if key_n.is_empty() {
        "  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n".to_string()
    } else {
        String::new()
    };
    let pk_constraint = if key_n.is_empty() {
        String::new()
    } else {
        let mut cols = key_n.clone();
        cols.push("source".into());
        format!(
            ",\n  PRIMARY KEY ({})",
            cols.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ")
        )
    };

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS \"{schema_n}\".\"{table_n}\" (\n{pk}  {cols},\n\
           source text NOT NULL,\n  source_endpoint text NOT NULL,\n\
           source_run_id uuid NOT NULL REFERENCES provenance.runs(run_id),\n\
           ingest_ts timestamptz NOT NULL DEFAULT now(),\n  raw jsonb{pkc}\n)",
        cols = col_ddl.join(",\n  "),
        pkc = pk_constraint,
    );
    Ok((schema_n, table_n, ddl))
}

/// Postgres storage backend — the default and (in Phase A) only backend.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: Pool,
}

impl PostgresBackend {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// The underlying pool (used by callers that still need direct access during
    /// the incremental migration — e.g. provenance/run bookkeeping).
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }

    async fn table_meta(&self, schema: &str, table: &str) -> ApiResult<Option<Arc<TableMeta>>> {
        let client = self.pool.get().await?;
        Ok(introspect::table_meta(&client, schema, table).await?)
    }

    async fn create_table(&self, plan: &CreateTablePlan<'_>) -> ApiResult<()> {
        let (schema_n, _table_n, ddl) = build_create_table_ddl(plan)?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema_n}\""))
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("create schema: {e}")))?;
        tx.batch_execute(&ddl)
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("create table: {e}")))?;
        tx.commit().await?;
        Ok(())
    }

    async fn write_records(&self, req: &WriteRequest<'_>) -> ApiResult<(i64, i64)> {
        // Verbatim former `ingest/core.rs::write_parsed` body.
        if req.records.is_empty() {
            return Ok((0, 0));
        }
        let meta: &TableMeta = req.meta;
        let writable: HashSet<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();

        // Column union across records ∩ writable − server-stamped (raw kept).
        let mut present: HashSet<String> = HashSet::new();
        for rec in req.records {
            if let Some(o) = rec.as_object() {
                for k in o.keys() {
                    present.insert(k.clone());
                }
            }
        }
        // Preserve target column order.
        let cols: Vec<String> = meta
            .columns
            .iter()
            .map(|c| c.name.clone())
            .filter(|c| {
                present.contains(c)
                    && writable.contains(c.as_str())
                    && !SERVER_STAMPED_COLS.contains(c.as_str())
            })
            .collect();

        if cols.is_empty() {
            return Err(ApiError::Internal(anyhow::anyhow!(
                "no usable columns after intersection with {}.{}",
                req.schema,
                req.table
            )));
        }
        if meta.conflict_cols.is_empty() {
            return Err(ApiError::Internal(anyhow::anyhow!(
                "{}.{} has no UNIQUE/PRIMARY KEY — refusing to upsert",
                req.schema,
                req.table
            )));
        }

        let mut client = self.pool.get().await?;
        let tx = client.transaction().await.map_err(|e| ApiError::Internal(e.into()))?;
        let (ins, upd) = engine::copy_and_merge(
            &tx,
            req.schema,
            req.table,
            &cols,
            req.records,
            req.source,
            req.source_endpoint,
            req.source_run_id,
            &meta.conflict_cols,
        )
        .await
        .map_err(ApiError::Internal)?;
        tx.commit().await.map_err(|e| ApiError::Internal(e.into()))?;
        Ok((ins, upd))
    }

    async fn query_rows(&self, q: &BoundQuery<'_>) -> ApiResult<Vec<Map<String, Value>>> {
        // Verbatim former `read/exec.rs::produce` query half.
        let client = self.pool.get().await?;
        let rows = client.query(q.sql, &q.params).await?;
        Ok(rows_to_objects(&rows))
    }

    async fn query_rows_as_role(&self, q: &BoundQuery<'_>, role: &str) -> ApiResult<Vec<Map<String, Value>>> {
        // Empty role ⇒ no elevation configured; run the normal (self-scoped) path.
        // This keeps the feature strictly opt-in and the default byte-identical.
        if role.trim().is_empty() {
            return self.query_rows(q).await;
        }
        // Run the read inside an explicit READ ONLY transaction with the session
        // role re-scoped to `role` for this query only (Phase D4). Ordering:
        // `SET TRANSACTION READ ONLY` first (blocks any mutation even via a
        // writable CTE), then `SET LOCAL ROLE` (reverts at txn end — no pool
        // leakage). The connecting pool role must be a member of `role`. This is
        // how an admin caller reads across tenants WITHOUT a `bypassrls` grant:
        // the deployment points `role` at a cross-tenant-visible RLS role.
        let mut client = self.pool.get().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("begin admin-read txn: {e}")))?;
        let setup = format!(
            "SET TRANSACTION READ ONLY; SET LOCAL ROLE {}",
            super::quote_role_ident(role.trim())
        );
        tx.batch_execute(&setup)
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("configure admin-read txn: {e}")))?;
        let rows = tx.query(q.sql, &q.params).await?;
        // READ ONLY txn — drop (implicit rollback) is fine, nothing to commit.
        drop(tx);
        Ok(rows_to_objects(&rows))
    }

    async fn query_rows_as_tenant(&self, q: &BoundQuery<'_>, sub: &str) -> ApiResult<Vec<Map<String, Value>>> {
        // Empty sub ⇒ nothing to pin; run the normal path (defensive — the read
        // handler never reaches here without an authenticated sub).
        if sub.trim().is_empty() {
            return self.query_rows(q).await;
        }
        // Run the read inside an explicit READ ONLY transaction with the RLS
        // tenant GUC pinned to the caller's sub for this query only (Phase 0c).
        // `SET TRANSACTION READ ONLY` first (blocks any mutation even via a
        // writable CTE), then `set_config('app.tenant_id', $1, true)` — the
        // `true` scopes it to the txn so it reverts on commit/rollback (no pool
        // leakage). The sub is bound as a parameter (never string-interpolated),
        // so a hostile-shaped value cannot escape into SQL. RLS-scoped tables
        // then filter on `current_setting('app.tenant_id')::uuid`.
        let mut client = self.pool.get().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("begin self-tenant read txn: {e}")))?;
        tx.batch_execute("SET TRANSACTION READ ONLY")
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("configure self-tenant read txn: {e}")))?;
        tx.execute("SELECT set_config('app.tenant_id', $1, true)", &[&sub])
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("pin app.tenant_id GUC: {e}")))?;
        let rows = tx.query(q.sql, &q.params).await?;
        // READ ONLY txn — drop (implicit rollback) is fine, nothing to commit.
        drop(tx);
        Ok(rows_to_objects(&rows))
    }
}
