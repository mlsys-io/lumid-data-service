//! COPY-staging + DISTINCT-FROM merge — port of
//! `writeengine.copy_into_staging` + `merge_staging_into_target`.
//!
//! The whole sequence runs inside ONE transaction:
//!   1. CREATE TEMP TABLE _stg_... (LIKE schema.table INCLUDING DEFAULTS) ON COMMIT DROP
//!   2. drop generated columns from the temp clone
//!   3. COPY the payload (+ provenance triplet + ingest_ts) into the temp table
//!   4. INSERT ... SELECT DISTINCT ON(key) ... ON CONFLICT(key) DO UPDATE
//!      SET <flat=EXCLUDED + prov> WHERE <any flat IS DISTINCT FROM EXCLUDED>
//!      RETURNING (xmax = 0)  -- true = fresh insert, false = update
//!   5. COMMIT  (fires ON COMMIT DROP on the temp table)
//!
//! `(inserted, updated)` are counted from the returned bool column. Re-running
//! the same rows yields (0, 0) because the WHERE-distinct guard suppresses
//! no-op updates — load-bearing for idempotency.

use std::collections::HashSet;

use bytes::Bytes;
use csv::{QuoteStyle, WriterBuilder};
use futures_util::SinkExt;
use serde_json::Value;
use tokio_postgres::Transaction;
use uuid::Uuid;

use super::coerce::{cell_for, NULL_TOKEN};
use super::introspect::{generated_columns, get_target_columns, ColumnInfo};

/// Provenance columns always re-stamped from EXCLUDED on update.
const PROV_REFRESH: &[&str] = &["source_endpoint", "source_run_id", "ingest_ts", "raw"];

/// Columns never compared for change-detection / never SET as a flat column.
fn is_prov_or_id(c: &str) -> bool {
    matches!(
        c,
        "source" | "source_endpoint" | "source_run_id" | "ingest_ts" | "raw" | "payload" | "id"
    )
}

/// COPY `records` (already column-aligned via `cols`) into a fresh temp table,
/// then merge into `schema.table`. Returns `(inserted, updated)`.
///
/// `cols` are the payload columns present across the batch (already intersected
/// with writable cols and with server-stamped columns removed except `raw`).
pub async fn copy_and_merge(
    tx: &Transaction<'_>,
    schema: &str,
    table: &str,
    cols: &[String],
    records: &[Value],
    source: &str,
    source_endpoint: &str,
    source_run_id: &Uuid,
    conflict_cols: &[String],
) -> anyhow::Result<(i64, i64)> {
    // Re-read target columns inside the txn (authoritative; the merge needs the
    // full set + nullability).
    let target_cols = get_target_columns(tx.client(), schema, table).await?;
    if target_cols.is_empty() {
        anyhow::bail!("no columns found for {schema}.{table}");
    }
    let has_ingest_ts = target_cols.iter().any(|c| c.name == "ingest_ts");

    // 1) temp table cloned from target.
    let tmp = format!("_stg_{}_{}", table, &Uuid::new_v4().simple().to_string()[..10]);
    tx.batch_execute(&format!(
        "CREATE TEMP TABLE {tmp} (LIKE {schema}.{table} INCLUDING DEFAULTS) ON COMMIT DROP"
    ))
    .await?;
    // 2) drop generated columns.
    for gcol in generated_columns(tx.client(), schema, table).await? {
        tx.batch_execute(&format!("ALTER TABLE {tmp} DROP COLUMN IF EXISTS {gcol}"))
            .await?;
    }

    // Full column ordering for the COPY: payload cols + provenance triplet
    // (+ ingest_ts when present).
    let mut full_cols: Vec<String> = cols.to_vec();
    full_cols.push("source".into());
    full_cols.push("source_endpoint".into());
    full_cols.push("source_run_id".into());
    if has_ingest_ts {
        full_cols.push("ingest_ts".into());
    }

    // data_type lookup per full column (default "text").
    let dtype_of = |name: &str| -> String {
        target_cols
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.data_type.clone())
            .unwrap_or_else(|| "text".to_string())
    };

    // 3) Build the CSV bytes and COPY.
    let now_iso = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
    let csv_bytes = build_csv(
        cols,
        records,
        source,
        source_endpoint,
        &source_run_id.to_string(),
        has_ingest_ts.then_some(now_iso.as_str()),
        &dtype_of,
    )?;

    let copy_sql = format!(
        "COPY {tmp} ({}) FROM STDIN WITH (FORMAT csv, NULL '{}', QUOTE '\"')",
        full_cols.join(","),
        NULL_TOKEN
    );
    let sink = tx.copy_in(&copy_sql).await?;
    futures_util::pin_mut!(sink);
    sink.send(Bytes::from(csv_bytes)).await?;
    sink.finish().await?;

    // 4) Merge.
    let (inserted, updated) =
        merge(tx, schema, table, &tmp, &target_cols, conflict_cols).await?;
    Ok((inserted, updated))
}

/// Serialize the batch into one CSV blob (QUOTE_MINIMAL ≈ QuoteStyle::Necessary,
/// `\n` line terminator). NULL cells emit the NULL sentinel string.
fn build_csv(
    cols: &[String],
    records: &[Value],
    source: &str,
    source_endpoint: &str,
    source_run_id: &str,
    ingest_ts: Option<&str>,
    dtype_of: &dyn Fn(&str) -> String,
) -> anyhow::Result<Vec<u8>> {
    let mut wtr = WriterBuilder::new()
        .quote_style(QuoteStyle::Necessary)
        .terminator(csv::Terminator::Any(b'\n'))
        .has_headers(false)
        .from_writer(Vec::new());

    // Precompute dtypes for payload cols.
    let dtypes: Vec<String> = cols.iter().map(|c| dtype_of(c)).collect();

    for rec in records {
        let obj = rec.as_object();
        let mut field: Vec<String> = Vec::with_capacity(cols.len() + 4);
        for (i, c) in cols.iter().enumerate() {
            let v = obj.and_then(|o| o.get(c)).unwrap_or(&Value::Null);
            field.push(cell_for(v, &dtypes[i]).unwrap_or_else(|| NULL_TOKEN.to_string()));
        }
        // provenance triplet (text)
        field.push(source.to_string());
        field.push(source_endpoint.to_string());
        field.push(source_run_id.to_string());
        if let Some(ts) = ingest_ts {
            field.push(ts.to_string());
        }
        wtr.write_record(&field)?;
    }
    wtr.flush()?;
    Ok(wtr.into_inner()?)
}

/// Port of `merge_staging_into_target` — builds the INSERT ... ON CONFLICT
/// DO UPDATE ... WHERE distinct, runs it, and counts xmax=0 vs not.
async fn merge(
    tx: &Transaction<'_>,
    schema: &str,
    table: &str,
    tmp: &str,
    target_cols: &[ColumnInfo],
    conflict_cols: &[String],
) -> anyhow::Result<(i64, i64)> {
    let all_cols: Vec<&str> = target_cols.iter().map(|c| c.name.as_str()).collect();
    let key_set: HashSet<&str> = conflict_cols.iter().map(|s| s.as_str()).collect();

    // Flat columns: compared for change + set from EXCLUDED.
    let flat_cols: Vec<&str> = all_cols
        .iter()
        .copied()
        .filter(|c| !key_set.contains(c) && !is_prov_or_id(c))
        .collect();

    let mut update_targets: Vec<String> = Vec::new();
    let mut distinct_clauses: Vec<String> = Vec::new();
    for c in &flat_cols {
        update_targets.push(format!("{c} = EXCLUDED.{c}"));
        distinct_clauses.push(format!(
            "{schema}.{table}.{c} IS DISTINCT FROM EXCLUDED.{c}"
        ));
    }
    // Provenance always re-stamped.
    for c in PROV_REFRESH {
        if all_cols.contains(c) {
            update_targets.push(format!("{c} = EXCLUDED.{c}"));
        }
    }
    // raw distinct guard when raw exists but isn't a flat col.
    if all_cols.contains(&"raw") && !flat_cols.contains(&"raw") {
        distinct_clauses.push(format!(
            "{schema}.{table}.raw IS DISTINCT FROM EXCLUDED.raw"
        ));
    }
    if all_cols.contains(&"payload") {
        update_targets.push("payload = EXCLUDED.payload".to_string());
        distinct_clauses.push(format!(
            "{schema}.{table}.payload IS DISTINCT FROM EXCLUDED.payload"
        ));
    }

    let insert_cols: Vec<&str> = all_cols.iter().copied().filter(|c| *c != "id").collect();
    let insert_cols_str = insert_cols.join(", ");
    let select_cols_str = insert_cols_str.clone();

    let on_conflict = conflict_cols.join(", ");
    let set_clause = update_targets.join(", ");
    let where_clause = if distinct_clauses.is_empty() {
        "FALSE".to_string()
    } else {
        distinct_clauses.join(" OR ")
    };

    // Dedupe within the batch: DISTINCT ON(key) over the NOT-NULL key filter.
    let nullable_of = |name: &str| -> bool {
        target_cols
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.is_nullable)
            .unwrap_or(true)
    };
    let not_null_keys: Vec<&String> =
        conflict_cols.iter().filter(|c| !nullable_of(c)).collect();
    let where_keys = if not_null_keys.is_empty() {
        "TRUE".to_string()
    } else {
        not_null_keys
            .iter()
            .map(|c| format!("{c} IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let on_conflict_quoted = conflict_cols.join(", ");
    let dedupe_select = format!(
        "SELECT DISTINCT ON ({on_conflict_quoted}) {select_cols_str} \
           FROM {tmp} \
          WHERE {where_keys} \
          ORDER BY {on_conflict_quoted}, ctid DESC"
    );

    let sql = format!(
        "INSERT INTO {schema}.{table} ({insert_cols_str}) \
         {dedupe_select} \
         ON CONFLICT ({on_conflict}) DO UPDATE \
            SET {set_clause} \
          WHERE {where_clause} \
         RETURNING (xmax = 0) AS inserted"
    );

    let rows = tx.query(&sql, &[]).await?;
    let mut inserted = 0i64;
    let mut updated = 0i64;
    for r in &rows {
        let was_insert: bool = r.get("inserted");
        if was_insert {
            inserted += 1;
        } else {
            updated += 1;
        }
    }
    Ok((inserted, updated))
}
