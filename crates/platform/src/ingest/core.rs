//! `ingest_records` orchestration — port of `ingest/core.py`.
//!
//! Validates each record against the per-table metadata, opens (or adopts) a
//! `provenance.runs` row, COPY-stages + DISTINCT-FROM-merges, and stamps the
//! run with status + counts. The `IngestResult` JSON shape matches the Python
//! dataclass exactly (run_id, target_schema, target_table, received, inserted,
//! updated, failed, rejected, status).

use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::backend::{Registry, WriteRequest};
use crate::error::ApiError;
use crate::validation::{self, Rejected};
use crate::write::run;

use super::lumilake::{self, LumilakeInfo};

/// Partner-declared source_endpoint: 1..=200 chars from [A-Za-z0-9_:/?=&.-].
/// Hand-rolled to avoid a regex dependency (mirrors the Python `_SOURCE_ENDPOINT_RE`).
fn valid_source_endpoint(s: &str) -> bool {
    let len = s.chars().count();
    if !(1..=200).contains(&len) {
        return false;
    }
    s.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '/' | '?' | '=' | '&' | '.' | '-')
    })
}

#[derive(Serialize, Clone)]
pub struct IngestResult {
    /// `None` when all records were rejected before a run row was opened.
    pub run_id: Option<String>,
    pub target_schema: String,
    pub target_table: String,
    pub received: usize,
    pub inserted: i64,
    pub updated: i64,
    pub failed: usize,
    pub rejected: Vec<Rejected>,
    pub status: String,
}

impl IngestResult {
    pub fn to_json(&self) -> Value {
        json!({
            "run_id": self.run_id,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "received": self.received,
            "inserted": self.inserted,
            "updated": self.updated,
            "failed": self.failed,
            "rejected": self.rejected,
            "status": self.status,
        })
    }
}

/// Distinguishes "table unknown" (→ caller decides sandbox/404) from other
/// ingest failures.
pub enum IngestErr {
    /// Target table doesn't exist / can't be introspected (mirrors
    /// SchemaIntrospectionError).
    UnknownTable(String),
    /// Any other ingest failure (→ 400/500 via Into<ApiError>).
    Failed(ApiError),
}

impl From<IngestErr> for ApiError {
    fn from(e: IngestErr) -> Self {
        match e {
            IngestErr::UnknownTable(t) => ApiError::NotFound(format!("unknown table: {t}")),
            IngestErr::Failed(a) => a,
        }
    }
}

/// Options carried into a single ingest call.
pub struct IngestParams<'a> {
    pub target_schema: &'a str,
    pub target_table: &'a str,
    pub source: &'a str,
    pub source_endpoint: &'a str,
    pub submitted_by: Option<&'a str>,
    /// When Some, the caller owns the run lifecycle (stream mode); we WILL NOT
    /// close the run.
    pub run_id: Option<Uuid>,
    pub declared_endpoint: Option<&'a str>,
    pub mode: &'a str,
    pub user_agent: Option<&'a str>,
    /// When true (default), validate each record against the per-table model.
    pub validate: bool,
    /// Fire the lumilake handoff after a successful, non-empty write.
    pub fire_lumilake: bool,
}

/// Write `records` to `target_schema.target_table` with full provenance.
///
/// The actual upsert is dispatched through the backend registry
/// (`reg.get(schema, table).write_records(..)`); provenance/run bookkeeping +
/// validation introspection stay on the shared Postgres pool (`reg.pool()`).
/// Phase A: every table resolves to the Postgres backend, so this is identical
/// to the former direct-to-PG path.
pub async fn ingest_records(
    reg: &Registry,
    p: &IngestParams<'_>,
    records: &[Value],
) -> Result<IngestResult, IngestErr> {
    if p.target_schema.is_empty() || p.target_table.is_empty() {
        return Err(IngestErr::Failed(ApiError::BadRequest(
            "target_schema and target_table are required".into(),
        )));
    }
    if p.source.is_empty() {
        return Err(IngestErr::Failed(ApiError::BadRequest(
            "source is required".into(),
        )));
    }
    if p.source_endpoint.is_empty() || !valid_source_endpoint(p.source_endpoint) {
        return Err(IngestErr::Failed(ApiError::BadRequest(format!(
            "source_endpoint must match [A-Za-z0-9_:/?=&.-]{{1,200}} (got {:?})",
            p.source_endpoint
        ))));
    }

    let received = records.len();

    let client = reg
        .pool()
        .get()
        .await
        .map_err(|e| IngestErr::Failed(e.into()))?;

    // Resolve the backend that OWNS this table (Phase B/C: Postgres or
    // ClickHouse, per the `provenance.table_backend` registry; an unknown table
    // defaults to Postgres, preserving the net-new-table → proposal flow).
    // Introspect existence + columns through THAT backend — a ClickHouse-backed
    // table isn't visible in Postgres's information_schema, so the prior
    // PG-only introspect re-proposed every write to a CH table instead of
    // upserting it.
    let backend = reg
        .get(p.target_schema, p.target_table)
        .await
        .map_err(IngestErr::Failed)?;

    // Introspect target (also confirms existence). Done before opening a run.
    let meta = match backend
        .table_meta(p.target_schema, p.target_table)
        .await
        .map_err(IngestErr::Failed)?
    {
        Some(m) => m,
        None => {
            return Err(IngestErr::UnknownTable(format!(
                "{}.{}",
                p.target_schema, p.target_table
            )))
        }
    };

    // Validate before opening a run row (a pure-validation failure leaves no
    // 0/0 'failed' run behind).
    let (parsed, rejected): (Vec<Value>, Vec<Rejected>) = if p.validate {
        validation::validate_batch(&meta, records)
    } else {
        (records.to_vec(), Vec::new())
    };

    // All rejected, none parsed → short-circuit before opening a run row (422).
    if p.validate && parsed.is_empty() && !rejected.is_empty() {
        return Ok(IngestResult {
            run_id: None,
            target_schema: p.target_schema.to_string(),
            target_table: p.target_table.to_string(),
            received,
            inserted: 0,
            updated: 0,
            failed: rejected.len(),
            rejected,
            status: "failed".to_string(),
        });
    }

    // Open or adopt the run row.
    let owned_run = p.run_id.is_none();
    let run_id = match p.run_id {
        Some(rid) => rid,
        None => {
            let mut args = json!({
                "target_schema": p.target_schema,
                "target_table": p.target_table,
                "mode": p.mode,
                "n_records_received": received,
            });
            let o = args.as_object_mut().unwrap();
            if let Some(d) = p.declared_endpoint {
                o.insert("declared_endpoint".into(), json!(d));
            }
            if let Some(ua) = p.user_agent {
                o.insert("user_agent".into(), json!(ua));
            }
            if let Some(sb) = p.submitted_by {
                o.insert("submitted_by".into(), json!(sb));
            }
            let rid = run::open_run(&client, "ingress:generic", &args, None)
                .await
                .map_err(|e| IngestErr::Failed(e.into()))?;
            if let Some(sb) = p.submitted_by {
                run::set_submitted_by(&client, &rid, sb)
                    .await
                    .map_err(|e| IngestErr::Failed(e.into()))?;
            }
            rid
        }
    };

    // Dispatch the upsert through the backend resolved above (Postgres or
    // ClickHouse). Surface the inner anyhow message for `ApiError::Internal`
    // (the write engine's bail strings) so the failed-run error_text stays
    // identical.
    let to_anyhow = |e: ApiError| match e {
        ApiError::Internal(inner) => inner,
        other => anyhow::anyhow!("{other}"),
    };
    let result = backend
        .write_records(&WriteRequest {
            schema: p.target_schema,
            table: p.target_table,
            meta: &meta,
            records: &parsed,
            source: p.source,
            source_endpoint: p.source_endpoint,
            source_run_id: &run_id,
        })
        .await
        .map_err(to_anyhow);

    match result {
        Ok((inserted, updated)) => {
            let status = if rejected.is_empty() { "ok" } else { "partial" };
            if owned_run {
                if let Err(e) = run::close_run(
                    &client,
                    &run_id,
                    status,
                    inserted,
                    updated,
                    rejected.len() as i64,
                    None,
                )
                .await
                {
                    tracing::error!("close_run failed for {run_id}: {e}");
                }
            }
            let out = IngestResult {
                run_id: Some(run_id.to_string()),
                target_schema: p.target_schema.to_string(),
                target_table: p.target_table.to_string(),
                received,
                inserted,
                updated,
                failed: rejected.len(),
                rejected,
                status: status.to_string(),
            };
            if p.fire_lumilake && (inserted + updated) > 0 {
                lumilake::submit_after_ingest(
                    &out,
                    LumilakeInfo {
                        target_schema: p.target_schema.to_string(),
                        target_table: p.target_table.to_string(),
                        mode: p.mode.to_string(),
                        declared_endpoint: p.declared_endpoint.map(|s| s.to_string()),
                        submitted_by: p.submitted_by.map(|s| s.to_string()),
                    },
                );
            }
            Ok(out)
        }
        Err(e) => {
            let error_text = format!("{e:#}");
            // Keep the head (root cause first in anyhow's {:#} format).
            let trunc = &error_text[..error_text.len().min(4000)];
            if owned_run {
                if let Err(ce) = run::close_run(
                    &client,
                    &run_id,
                    "failed",
                    0,
                    0,
                    rejected.len() as i64,
                    Some(trunc),
                )
                .await
                {
                    tracing::error!("close_run failed for {run_id}: {ce}");
                }
            }
            tracing::error!(
                "ingest_records to {}.{} failed: {error_text}",
                p.target_schema,
                p.target_table
            );
            // Log the raw error but return a sanitised message so DB internals
            // (constraint names, column types) don't leak to the caller.
            Err(IngestErr::Failed(ApiError::Internal(anyhow::anyhow!(
                "ingest write failed; check server logs for run {run_id}"
            ))))
        }
    }
}
