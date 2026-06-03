//! Deterministic plan executor.
//!
//! Walks a `RetrievalPlan`, executes each op against Postgres or object
//! storage, materializes output, and returns a `RetrievalResult`.

use std::collections::BTreeMap;
use std::time::Instant;

use deadpool_postgres::Pool;
use object_store::path::Path as ObjPath;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

use super::materialize::{write_to_store, Materializer, OutputFormat};
use super::plan::{AccessChainStep, RetrievalOp, RetrievalPlan, RetrievalResult};

/// Execute a `RetrievalPlan` and return a `RetrievalResult`.
///
/// - SQL ops are validated as read-only SELECTs before execution.
/// - Row cap is enforced; the call returns `ApiError::BadRequest` if exceeded.
/// - Output is materialized to object storage under `<prefix>/<run_id>.<fmt>`.
pub async fn replay(
    plan: &RetrievalPlan,
    st: &AppState,
    output_format: &OutputFormat,
    run_id: &str,
) -> ApiResult<RetrievalResult> {
    if plan.plan.is_empty() {
        return Err(ApiError::BadRequest("plan has no ops to execute".into()));
    }

    let stmt_timeout_ms = st.settings.retrieval_stmt_timeout_ms;
    let row_cap = st.settings.retrieval_row_cap;
    let prefix = &st.settings.retrieval_prefix;
    let ext = output_format.extension();
    let key = format!("{prefix}/{run_id}/result.{ext}");

    let started = Instant::now();
    let mut access_chain: Vec<AccessChainStep> = Vec::new();
    let mut materializer = Materializer::new(match output_format {
        OutputFormat::Csv => OutputFormat::Csv,
        OutputFormat::Jsonl => OutputFormat::Jsonl,
        OutputFormat::Raw => OutputFormat::Raw,
    });
    let mut total_rows: i64 = 0;

    for op in &plan.plan {
        match op {
            RetrievalOp::Sql(sql_op) => {
                let op_start = Instant::now();
                // Safety check: only read-only SELECT allowed.
                if !super::plan::is_safe_select(&sql_op.query) {
                    return Err(ApiError::BadRequest(format!(
                        "SQL rejected: only a single read-only SELECT is allowed; got: {}",
                        sql_op.query.chars().take(200).collect::<String>()
                    )));
                }
                let (rows_written, rows_count) = execute_sql_op(
                    &st.pool,
                    &sql_op.query,
                    stmt_timeout_ms,
                    row_cap,
                    &mut materializer,
                )
                .await?;
                total_rows += rows_count;
                let elapsed = op_start.elapsed().as_millis() as u64;
                access_chain.push(AccessChainStep {
                    op: "sql".to_string(),
                    query: Some(sql_op.query.clone()),
                    bucket: None,
                    key: None,
                    rows_or_bytes: rows_written,
                    ms: elapsed,
                });
            }
            RetrievalOp::StorageGet(sg_op) => {
                let op_start = Instant::now();
                // Validate and resolve the key.
                let resolved_key = crate::handlers::blobs::sanitize_blob_key(&sg_op.key)?;
                let path = ObjPath::from(resolved_key.as_str());
                let result = st
                    .blob_store
                    .get(&path)
                    .await
                    .map_err(|e| ApiError::NotFound(format!("blob '{}' not found: {e}", sg_op.key)))?;

                // Guard against unbounded buffering: check object size before reading
                // the body. blob_max_bytes == 0 means no cap (skip the check).
                let blob_max = st.settings.blob_max_bytes;
                let object_size = result.meta.size as u64;
                if blob_max > 0 && object_size > blob_max {
                    return Err(ApiError::BadRequest(format!(
                        "blob {} exceeds LUMID_BLOB_MAX_BYTES ({} bytes)",
                        sg_op.key, object_size
                    )));
                }

                let bytes_data = result
                    .bytes()
                    .await
                    .map_err(|e| ApiError::Internal(anyhow::anyhow!("reading blob: {e}")))?;
                let size = bytes_data.len() as i64;

                match output_format {
                    OutputFormat::Raw => {
                        materializer.write_raw_bytes(&bytes_data);
                    }
                    _ => {
                        // Mixed plans: emit a descriptor record.
                        let sha = {
                            use sha2::{Digest, Sha256};
                            hex::encode(Sha256::digest(&bytes_data))
                        };
                        let mut record = BTreeMap::new();
                        record.insert(
                            "op".to_string(),
                            serde_json::Value::String("storage_get".to_string()),
                        );
                        record.insert(
                            "key".to_string(),
                            serde_json::Value::String(sg_op.key.clone()),
                        );
                        record.insert(
                            "size".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(size)),
                        );
                        record.insert("sha256".to_string(), serde_json::Value::String(sha));
                        materializer.write_row(&record);
                        total_rows += 1;
                    }
                }

                let elapsed = op_start.elapsed().as_millis() as u64;
                access_chain.push(AccessChainStep {
                    op: "storage_get".to_string(),
                    query: None,
                    bucket: None,
                    key: Some(sg_op.key.clone()),
                    rows_or_bytes: size,
                    ms: elapsed,
                });
            }
        }
    }

    let output_bytes = materializer.into_bytes();
    let size_bytes = output_bytes.len() as i64;
    let (materialized_uri, _) =
        write_to_store(&st.blob_store, &key, output_bytes).await?;

    let signed_url = super::materialize::try_presign_url(&st.blob_store, &key);
    let replay_latency_ms = started.elapsed().as_millis() as i64;

    Ok(RetrievalResult {
        run_id: run_id.to_string(),
        materialized_uri,
        signed_url,
        output_format: output_format.format_name().to_string(),
        access_chain,
        rowcount: total_rows,
        size_bytes,
        tokens_in: 0,
        tokens_out: 0,
        steps_taken: 0,
        replay_latency_ms,
        transcript_url: String::new(),
    })
}

/// Execute a single SQL SELECT op and stream rows into the materializer.
/// Returns `(rows_written, rowcount)`.
///
/// Row cap is enforced by injecting `LIMIT row_cap+1` so Postgres stops
/// scanning early rather than buffering the full result set.  If the probe
/// row is present the call is rejected before writing any output.
///
/// The statement timeout is set inside an explicit transaction so that
/// `SET LOCAL` applies to the subsequent SELECT (autocommit mode would
/// discard it immediately).
async fn execute_sql_op(
    pool: &Pool,
    query: &str,
    stmt_timeout_ms: u32,
    row_cap: u64,
    mat: &mut Materializer,
) -> ApiResult<(i64, i64)> {
    let mut client = pool.get().await?;

    let tx = client
        .transaction()
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("begin transaction: {e}")))?;

    // READ ONLY enforces non-mutation at the DB level — a writable CTE
    // (WITH t AS (DELETE ... RETURNING ...) SELECT ...) is rejected by Postgres
    // even if the statement parser is bypassed. Defense-in-depth beyond the
    // SELECT-only check in `plan::is_safe_select`.
    tx.batch_execute(&format!(
        "SET TRANSACTION READ ONLY; SET LOCAL statement_timeout = {stmt_timeout_ms}"
    ))
    .await
    .map_err(|e| ApiError::Internal(anyhow::anyhow!("configure read txn: {e}")))?;

    let cleaned = query.trim_end().trim_end_matches(';').trim_end();

    // Wrap as a subquery so an existing LIMIT in the user query is respected
    // while still allowing us to detect cap exceedance at the DB level.
    let probe_limit = row_cap.saturating_add(1);
    let limited = format!("SELECT * FROM ( {cleaned} ) AS __retrieval_outer LIMIT {probe_limit}");

    let rows = tx.query(&limited, &[]).await.map_err(|e| {
        ApiError::BadRequest(format!("SQL execution failed: {e}"))
    })?;

    // Transaction is read-only; drop (implicit rollback) is fine.
    drop(tx);

    if rows.len() as u64 > row_cap {
        return Err(ApiError::BadRequest(format!(
            "SQL result set exceeds row cap ({row_cap}); refine your query"
        )));
    }

    let rowcount = rows.len() as i64;
    let mut written = 0i64;

    for row in &rows {
        let cols = row.columns();
        let mut record: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        for col in cols {
            let val = pg_value_to_json(row, col.name());
            record.insert(col.name().to_string(), val);
        }
        mat.write_row(&record);
        written += 1;
    }

    Ok((written, rowcount))
}

fn pg_value_to_json(row: &tokio_postgres::Row, col: &str) -> serde_json::Value {
    // Try common types in order.  Numeric types before String so we don't
    // accidentally coerce numbers via the Display impl.
    if let Ok(Some(v)) = row.try_get::<_, Option<i64>>(col) {
        return serde_json::json!(v);
    }
    if let Ok(Some(v)) = row.try_get::<_, Option<i32>>(col) {
        return serde_json::json!(v);
    }
    if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(col) {
        return serde_json::json!(v);
    }
    if let Ok(Some(v)) = row.try_get::<_, Option<bool>>(col) {
        return serde_json::json!(v);
    }
    // `numeric` / DECIMAL — rendered as a decimal string so precision is
    // preserved (matches Python's json.dumps(row, default=str) behaviour).
    if let Ok(Some(v)) = row.try_get::<_, Option<rust_decimal::Decimal>>(col) {
        return serde_json::Value::String(v.to_string());
    }
    // `timestamptz` — ISO-8601 UTC string (e.g. "2024-01-15T10:30:00Z").
    if let Ok(Some(v)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(col) {
        use chrono::SecondsFormat;
        let fmt = if v.timestamp_subsec_nanos() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        };
        return serde_json::Value::String(v.to_rfc3339_opts(fmt, true));
    }
    // `timestamp` (no tz) — ISO-8601 string without offset.
    if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(col) {
        return serde_json::Value::String(v.format("%Y-%m-%dT%H:%M:%S%.6f").to_string());
    }
    // `date` — ISO-8601 date string.
    if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDate>>(col) {
        return serde_json::Value::String(v.to_string());
    }
    // `uuid` — standard hyphenated string.
    if let Ok(Some(v)) = row.try_get::<_, Option<uuid::Uuid>>(col) {
        return serde_json::Value::String(v.to_string());
    }
    if let Ok(Some(v)) = row.try_get::<_, Option<String>>(col) {
        return serde_json::json!(v);
    }
    if let Ok(Some(v)) = row.try_get::<_, Option<serde_json::Value>>(col) {
        return v;
    }
    serde_json::Value::Null
}
