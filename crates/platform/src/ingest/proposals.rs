//! Ingress proposals — the "I don't know the schema yet" path.
//!
//! When a caller POSTs records to a table that doesn't exist *and* their role
//! has propose rights, instead of 404 we infer a schema from the records and
//! stage a row in `provenance.ingress_proposals` (status `pending`). An admin
//! reviews it (`GET /catalog/ingress/proposals`) and approves
//! (`POST /admin/ingress/proposals/:id/approve`) — which CREATEs the table
//! (inferred columns + the universal provenance columns + a primary key) and
//! grants the proposer's role write ACL. Net-new data, no DDL by hand.
//!
//! Safety: every identifier (schema/table/column) is normalised + validated
//! against `^[a-z_][a-z0-9_]{0,62}$` and emitted double-quoted, so caller JSON
//! keys can never inject SQL.

use std::collections::BTreeMap;

use deadpool_postgres::Pool;
use serde_json::{json, Map, Value};

use crate::backend::CreateTablePlan;
use crate::error::{ApiError, ApiResult};

fn norm_ident(s: &str) -> Option<String> {
    let l = s.trim().to_lowercase();
    let ok = !l.is_empty()
        && l.len() <= 63
        && l.chars().next().map(|c| c.is_ascii_lowercase() || c == '_').unwrap_or(false)
        && l.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    ok.then_some(l)
}

/// Universal provenance columns every fact table carries (stamped on write).
const PROVENANCE_COLS: &[&str] = &["source", "source_endpoint", "source_run_id", "ingest_ts", "raw"];

/// Infer a Postgres type for a column from its values across the sample.
fn infer_type(values: &[&Value]) -> &'static str {
    let mut any_str = false;
    let mut any_float = false;
    let mut any_int = false;
    let mut any_bool = false;
    let mut any_json = false;
    for v in values {
        match v {
            Value::String(_) => any_str = true,
            Value::Bool(_) => any_bool = true,
            Value::Number(n) => {
                if n.is_f64() && n.as_i64().is_none() { any_float = true } else { any_int = true }
            }
            Value::Array(_) | Value::Object(_) => any_json = true,
            Value::Null => {}
        }
    }
    if any_json { return "jsonb" }
    if any_str { return "text" }
    if any_float { return "double precision" }
    if any_int { return "bigint" }
    if any_bool { return "boolean" }
    "text"
}

/// Infer a natural upsert key from the proposed columns.
///
/// Returns a **composite** entity-dimension key (most-significant first) plus the
/// finest event-time column when present — e.g. market-data
/// `(tenant_id, venue, instrument_id, ts_event_ns)` gives correct newest-wins
/// dedup; reference data `(tenant_id, venue, class_id, instrument_id)` keys on the
/// entity. Falls back to legacy single identity-ish names, then — as a last
/// resort — to ALL columns, so the result is **never empty** for a non-empty
/// column set. Empty key ⇒ a generated-identity PK, which the COPY+merge upsert
/// cannot target; this function exists to avoid that (see `create`).
fn infer_natural_key(cols: &BTreeMap<String, Vec<&Value>>) -> Vec<String> {
    let has = |c: &str| cols.contains_key(c);
    // Entity dimensions in key order (coarse → fine).
    let entity: Vec<&str> = [
        "tenant_id", "venue", "class_id", "instrument_id", "symbol", "event_id",
        "market_id", "asset_id", "ticker",
    ]
    .into_iter()
    .filter(|c| has(c))
    .collect();
    if !entity.is_empty() {
        let mut key: Vec<String> = entity.iter().map(|s| (*s).to_string()).collect();
        // Append the finest event-time column, if any (first match wins).
        if let Some(t) = ["ts_event_ns", "ts_event", "ts", "timestamp", "date"]
            .into_iter()
            .find(|c| has(c))
        {
            key.push(t.to_string());
        }
        return key;
    }
    // Legacy single identity-ish column.
    let legacy: Vec<String> = ["symbol", "id", "date", "ts", "timestamp"]
        .into_iter()
        .filter(|c| has(c))
        .map(String::from)
        .collect();
    if !legacy.is_empty() {
        return legacy;
    }
    // Last resort: all columns (guarantees a valid ON CONFLICT target; at worst
    // dedups exact-duplicate rows). Never returns empty for a non-empty `cols`.
    cols.keys().cloned().collect()
}

/// Create a pending proposal from the records. Returns the response body.
pub async fn create(
    pool: &Pool,
    schema: &str,
    table: &str,
    role: &str,
    sub: &str,
    records: &[Value],
) -> ApiResult<Value> {
    let schema_n = norm_ident(schema).ok_or_else(|| ApiError::BadRequest(format!("invalid schema {schema:?}")))?;
    let table_n = norm_ident(table).ok_or_else(|| ApiError::BadRequest(format!("invalid table {table:?}")))?;

    // Union of keys → inferred column types (provenance cols excluded; added at create).
    let mut cols: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    let mut skipped: Vec<String> = Vec::new();
    for rec in records {
        let Some(obj) = rec.as_object() else { continue };
        for (k, v) in obj {
            match norm_ident(k) {
                Some(c) if !PROVENANCE_COLS.contains(&c.as_str()) => cols.entry(c).or_default().push(v),
                Some(_) => {} // a provenance col supplied by caller — ignore
                None => { if !skipped.contains(k) { skipped.push(k.clone()) } }
            }
        }
    }
    if cols.is_empty() {
        return Err(ApiError::BadRequest("no usable columns inferred from records".into()));
    }
    let inferred: Map<String, Value> =
        cols.iter().map(|(c, vs)| (c.clone(), Value::String(infer_type(vs).into()))).collect();

    // Natural key for the upsert. MUST be non-empty for any table the COPY+merge
    // engine will upsert into: a keyless table falls back to a generated-identity
    // `id` PK, and the merge's `ON CONFLICT (id)` then references a GENERATED
    // ALWAYS column absent from the payload → "column id does not exist". So we
    // infer a real composite key (entity dimensions + finest event-time) and only
    // ever return empty when there are genuinely no columns (guarded above).
    let key = infer_natural_key(&cols);

    let sample: Vec<Value> = records.iter().take(3).cloned().collect();
    let client = pool.get().await?;
    let row = client
        .query_one(
            "INSERT INTO provenance.ingress_proposals \
               (declared_schema, declared_table, proposer_sub, proposer_role, \
                inferred_schema, inferred_key, sample_records, drop_count, status) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending') \
             RETURNING proposal_id::text",
            &[&schema_n, &table_n, &sub, &role, &Value::Object(inferred.clone()),
              &key, &Value::Array(sample), &(skipped.len() as i64)],
        )
        .await?;
    let pid: String = row.get(0);
    Ok(json!({
        "status": "proposed",
        "proposal_id": pid,
        "target": format!("{schema_n}.{table_n}"),
        "inferred_columns": inferred,
        "inferred_key": key,
        "skipped_keys": skipped,
        "note": "net-new table — pending admin approval (GET /catalog/ingress/proposals)"
    }))
}

/// List proposals, optionally filtered by status.
pub async fn list(pool: &Pool, status: Option<&str>) -> ApiResult<Value> {
    let client = pool.get().await?;
    let rows = if let Some(s) = status {
        client.query("SELECT proposal_id::text, declared_schema, declared_table, proposer_role, \
            inferred_schema, inferred_key, drop_count, status, applied_table, created_at \
            FROM provenance.ingress_proposals WHERE status=$1 ORDER BY created_at DESC LIMIT 200", &[&s]).await?
    } else {
        client.query("SELECT proposal_id::text, declared_schema, declared_table, proposer_role, \
            inferred_schema, inferred_key, drop_count, status, applied_table, created_at \
            FROM provenance.ingress_proposals ORDER BY created_at DESC LIMIT 200", &[]).await?
    };
    let items: Vec<Value> = rows.iter().map(|r| json!({
        "proposal_id": r.get::<_, String>(0),
        "schema": r.get::<_, String>(1),
        "table": r.get::<_, String>(2),
        "proposer_role": r.get::<_, String>(3),
        "inferred_schema": r.get::<_, Value>(4),
        "inferred_key": r.get::<_, Vec<String>>(5),
        "status": r.get::<_, String>(7),
        "applied_table": r.get::<_, Option<String>>(8),
    })).collect();
    Ok(json!({"count": items.len(), "proposals": items}))
}

/// Approve a proposal: CREATE the table (inferred + provenance cols + PK) and
/// grant the proposer's role write ACL. Idempotent-ish (CREATE IF NOT EXISTS).
pub async fn approve(pool: &Pool, proposal_id: &str, reviewer: &str) -> ApiResult<Value> {
    let mut client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT declared_schema, declared_table, proposer_role, inferred_schema, inferred_key \
             FROM provenance.ingress_proposals WHERE proposal_id::text=$1 AND status='pending'",
            &[&proposal_id],
        )
        .await?
        .ok_or_else(|| ApiError::NotFound("no pending proposal with that id".into()))?;
    let schema: String = row.get(0);
    let table: String = row.get(1);
    let role: String = row.get(2);
    let inferred: Value = row.get(3);
    let key: Vec<String> = row.get(4);

    // Re-validate identifiers (defence in depth) + build the CREATE TABLE DDL via
    // the shared builder (same fn the PostgresBackend uses — single source of DDL).
    let obj = inferred.as_object().ok_or_else(|| ApiError::Internal(anyhow::anyhow!("bad inferred_schema")))?;
    let (schema_n, table_n, ddl) = crate::backend::postgres::build_create_table_ddl(&CreateTablePlan {
        schema: &schema,
        table: &table,
        inferred: obj,
        key: &key,
    })?;

    let tx = client.transaction().await?;
    tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema_n}\"")).await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("create schema: {e}")))?;
    tx.batch_execute(&ddl).await.map_err(|e| ApiError::Internal(anyhow::anyhow!("create table: {e}")))?;
    // Record the storage backend for this table (Phase A: always 'postgres'),
    // in the same transaction as the CREATE so the registry never sees a created
    // table without a backend row.
    tx.execute(
        "INSERT INTO provenance.table_backend (target_schema, target_table, backend) \
         VALUES ($1,$2,'postgres') \
         ON CONFLICT (target_schema, target_table) DO UPDATE SET backend=EXCLUDED.backend",
        &[&schema_n, &table_n],
    ).await?;
    tx.execute(
        "INSERT INTO provenance.ingress_acl (role, target_schema, target_table, can_write, notes) \
         VALUES ($1,$2,$3,true,'auto-granted on proposal approval') \
         ON CONFLICT (role, target_schema, target_table) DO UPDATE SET can_write=true",
        &[&role, &schema_n, &table_n],
    ).await?;
    tx.execute(
        "UPDATE provenance.ingress_proposals SET status='applied', applied_table=$2, \
         reviewer_sub=$3, reviewed_at=now(), updated_at=now() WHERE proposal_id::text=$1",
        &[&proposal_id, &format!("{schema_n}.{table_n}"), &reviewer],
    ).await?;
    tx.commit().await?;
    super::acl::invalidate();
    Ok(json!({"status": "applied", "table": format!("{schema_n}.{table_n}"), "granted_role": role}))
}

/// Reject a pending proposal.
pub async fn reject(pool: &Pool, proposal_id: &str, reviewer: &str, notes: Option<&str>) -> ApiResult<Value> {
    let client = pool.get().await?;
    let n = client.execute(
        "UPDATE provenance.ingress_proposals SET status='rejected', reviewer_sub=$2, \
         review_notes=$3, reviewed_at=now(), updated_at=now() WHERE proposal_id::text=$1 AND status='pending'",
        &[&proposal_id, &reviewer, &notes],
    ).await?;
    if n == 0 { return Err(ApiError::NotFound("no pending proposal with that id".into())); }
    Ok(json!({"status": "rejected", "proposal_id": proposal_id}))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a column map with the given names (values irrelevant to keying).
    fn cols_of(names: &[&str]) -> BTreeMap<String, Vec<&'static Value>> {
        const NULL: Value = Value::Null;
        names
            .iter()
            .map(|n| ((*n).to_string(), vec![&NULL]))
            .collect()
    }

    #[test]
    fn market_data_gets_composite_entity_plus_time_key() {
        let cols = cols_of(&[
            "tenant_id", "venue", "instrument_id", "bid_price_ticks", "ask_price_ticks",
            "ts_event_ns", "ts_recv_ns",
        ]);
        assert_eq!(
            infer_natural_key(&cols),
            vec!["tenant_id", "venue", "instrument_id", "ts_event_ns"]
        );
    }

    #[test]
    fn reference_data_without_time_gets_entity_key() {
        let cols = cols_of(&["tenant_id", "venue", "instrument_id", "class_id"]);
        // entity order: tenant_id, venue, class_id, instrument_id; no time col appended.
        assert_eq!(
            infer_natural_key(&cols),
            vec!["tenant_id", "venue", "class_id", "instrument_id"]
        );
    }

    #[test]
    fn legacy_single_column_key_still_works() {
        let cols = cols_of(&["symbol", "open", "close"]);
        assert_eq!(infer_natural_key(&cols), vec!["symbol"]);
    }

    #[test]
    fn keyless_table_falls_back_to_all_columns_never_empty() {
        let cols = cols_of(&["foo", "bar", "baz"]);
        // BTreeMap-sorted; non-empty guarantees a valid ON CONFLICT target (no identity-PK).
        let key = infer_natural_key(&cols);
        assert_eq!(key, vec!["bar", "baz", "foo"]);
        assert!(!key.is_empty());
    }
}
