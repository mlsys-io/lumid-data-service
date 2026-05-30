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

use deadpool_postgres::Pool;
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use super::schema_suggest::norm_ident;

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

    // Rules-based suggestion (round 0, author=platform).
    let (inferred, key, skipped, time_col) = super::schema_suggest::rules_suggest(records);
    if inferred.is_empty() {
        return Err(ApiError::BadRequest("no usable columns inferred from records".into()));
    }
    let round0 = json!({
        "author": "platform", "kind": "suggestion", "reason": "rules-inferred",
        "columns": &inferred, "key": &key, "time_col": time_col,
    });

    let sample: Vec<Value> = records.iter().take(5).cloned().collect();
    let client = pool.get().await?;
    let row = client
        .query_one(
            "INSERT INTO provenance.ingress_proposals \
               (declared_schema, declared_table, proposer_sub, proposer_role, \
                inferred_schema, inferred_key, sample_records, drop_count, status, rounds) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9) \
             RETURNING proposal_id::text",
            &[&schema_n, &table_n, &sub, &role, &Value::Object(inferred.clone()),
              &key, &Value::Array(sample), &(skipped.len() as i64), &json!([round0])],
        )
        .await?;
    let pid: String = row.get(0);
    Ok(json!({
        "status": "proposed",
        "proposal_id": pid,
        "target": format!("{schema_n}.{table_n}"),
        "suggested_columns": inferred,
        "suggested_key": key,
        "skipped_keys": skipped,
        "note": "net-new table — negotiate the schema: GET /catalog/ingress/proposals/{id}, \
                 then approve / reject / counter (POST /ingress/proposals/{id}/{approve,reject,counter})."
    }))
}

/// Full detail of one proposal: current suggested schema + the round history.
pub async fn get_detail(pool: &Pool, proposal_id: &str) -> ApiResult<Value> {
    let client = pool.get().await?;
    let r = client.query_opt(
        "SELECT proposal_id::text, declared_schema, declared_table, proposer_sub, proposer_role, \
            inferred_schema, inferred_key, status, applied_table, rounds, sample_records, created_at::text \
         FROM provenance.ingress_proposals WHERE proposal_id::text=$1", &[&proposal_id],
    ).await?.ok_or_else(|| ApiError::NotFound("no such proposal".into()))?;
    Ok(json!({
        "proposal_id": r.get::<_, String>(0),
        "target": format!("{}.{}", r.get::<_, String>(1), r.get::<_, String>(2)),
        "proposer": r.get::<_, String>(3),
        "proposer_role": r.get::<_, String>(4),
        "current_columns": r.get::<_, Value>(5),
        "current_key": r.get::<_, Vec<String>>(6),
        "status": r.get::<_, String>(7),
        "applied_table": r.get::<_, Option<String>>(8),
        "rounds": r.get::<_, Value>(9),
        "sample_records": r.get::<_, Value>(10),
        "created_at": r.get::<_, String>(11),
    }))
}

/// Is `sub` the original proposer of this (still-pending) proposal?
pub async fn is_proposer(pool: &Pool, proposal_id: &str, sub: &str) -> ApiResult<bool> {
    let client = pool.get().await?;
    Ok(client.query_opt(
        "SELECT 1 FROM provenance.ingress_proposals WHERE proposal_id::text=$1 AND proposer_sub=$2",
        &[&proposal_id, &sub],
    ).await?.is_some())
}

/// Counter-propose a schema. The platform validates + normalises the caller's
/// columns/key, optionally refines them through the wired LLM, records a builder
/// round + a platform round, and updates the current suggestion — leaving the
/// proposal `pending` for another approve/reject/counter cycle.
pub async fn counter(
    pool: &Pool,
    settings: &crate::config::Settings,
    http: &reqwest::Client,
    proposal_id: &str,
    sub: &str,
    columns: &Value,
    key: &[String],
    records_hint: &[Value],
) -> ApiResult<Value> {
    // 1) Validate the builder's proposal (safe identifiers + allow-listed types).
    let (b_cols, b_key) = super::schema_suggest::validate(columns, key)
        .map_err(ApiError::BadRequest)?;

    // 2) Optional LLM refine (best-effort; falls back to the builder's proposal).
    let (cur_cols, cur_key, refined_by) =
        match super::schema_suggest::llm_refine(settings, http, &b_cols, &b_key, records_hint).await {
            Some((c, k)) => (c, k, "rules+ai"),
            None => (b_cols.clone(), b_key.clone(), "rules"),
        };

    let builder_round = json!({"author": "builder", "kind": "counter", "columns": &b_cols, "key": &b_key});
    let platform_round = json!({"author": "platform", "kind": "suggestion", "reason": refined_by,
                                "columns": &cur_cols, "key": &cur_key});

    let client = pool.get().await?;
    let n = client.execute(
        "UPDATE provenance.ingress_proposals \
            SET inferred_schema=$2, inferred_key=$3, \
                rounds = rounds || $4::jsonb, updated_at=now() \
          WHERE proposal_id::text=$1 AND status='pending'",
        &[&proposal_id, &Value::Object(cur_cols.clone()), &cur_key,
          &json!([builder_round, platform_round])],
    ).await?;
    if n == 0 {
        return Err(ApiError::NotFound("no pending proposal with that id".into()));
    }
    let _ = sub;
    Ok(json!({
        "status": "countered",
        "proposal_id": proposal_id,
        "refined_by": refined_by,
        "current_columns": cur_cols,
        "current_key": cur_key,
        "note": "schema updated — approve to apply, or counter again."
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

    // Re-validate identifiers (defence in depth) + build column DDL.
    let schema_n = norm_ident(&schema).ok_or_else(|| ApiError::BadRequest("bad schema".into()))?;
    let table_n = norm_ident(&table).ok_or_else(|| ApiError::BadRequest("bad table".into()))?;
    let obj = inferred.as_object().ok_or_else(|| ApiError::Internal(anyhow::anyhow!("bad inferred_schema")))?;
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
    let key_n: Vec<String> = key.iter().filter_map(|k| norm_ident(k)).filter(|k| obj.contains_key(k)).collect();
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
        format!(",\n  PRIMARY KEY ({})", cols.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", "))
    };

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS \"{schema_n}\".\"{table_n}\" (\n{pk}  {cols},\n\
           source text NOT NULL,\n  source_endpoint text NOT NULL,\n\
           source_run_id uuid NOT NULL REFERENCES provenance.runs(run_id),\n\
           ingest_ts timestamptz NOT NULL DEFAULT now(),\n  raw jsonb{pkc}\n)",
        cols = col_ddl.join(",\n  "),
        pkc = pk_constraint,
    );

    let tx = client.transaction().await?;
    tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema_n}\"")).await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("create schema: {e}")))?;
    tx.batch_execute(&ddl).await.map_err(|e| ApiError::Internal(anyhow::anyhow!("create table: {e}")))?;
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
