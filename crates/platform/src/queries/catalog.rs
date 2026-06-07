//! Catalog read-plane queries — port of `api/catalog/core.py` + `catalog/lineage.py`.
//!
//! Provenance columns are NOT stripped from catalog responses — exposing them is
//! the whole point. All SQL here is parameterized except where a (schema, table)
//! identifier must be interpolated; those identifiers are always validated against
//! `USER_SCHEMAS` / `information_schema` before interpolation.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::{json, Map, Value};
use tokio_postgres::types::ToSql;

use crate::db::rows::rows_to_objects;
use crate::error::{ApiError, ApiResult};

/// Returns true when a `tokio_postgres::Error` is an `UndefinedTable` (42P01),
/// which happens when `_timescaledb_catalog.hypertable` doesn't exist (vanilla PG).
fn is_undefined_table(e: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    e.as_db_error()
        .map(|db| *db.code() == SqlState::UNDEFINED_TABLE)
        .unwrap_or(false)
}

/// Compile-time allowlist of schemas this platform knows about.
/// Used as both the security whitelist (identifiers safe to interpolate) and
/// the default display list when `LUMID_USER_SCHEMAS` is not set.
/// Other schemas (pg_*, information_schema, _timescaledb_*) are always hidden.
pub const USER_SCHEMAS: &[&str] = &[
    "reference",
    "market",
    "fundamentals",
    "estimates",
    "ownership",
    "events",
    "news",
    "regulatory",
    "macro",
    "prediction_markets",
    "raw",
    "provenance",
];

/// Always-blocked Postgres system schema prefixes/names. These are never
/// surfaced regardless of what `LUMID_USER_SCHEMAS` says, so an accidental
/// `USER_SCHEMAS=pg_catalog` can't expose credential catalogs.
fn is_system_schema(s: &str) -> bool {
    s.starts_with("pg_")
        || s.starts_with("_timescaledb")
        || s.starts_with("timescaledb")
        || matches!(s, "information_schema" | "public")
}

/// Resolve the effective schema list from the runtime configuration.
///
/// - When `configured` is empty the compile-time `USER_SCHEMAS` default is returned.
/// - When `configured` is non-empty it is trusted directly — operators can expose
///   custom schemas not in `USER_SCHEMAS` without a code change. System schemas
///   (`pg_*`, `_timescaledb*`, `information_schema`, `public`) are stripped
///   regardless.
pub fn effective_schemas(configured: &[String]) -> Vec<String> {
    if configured.is_empty() {
        USER_SCHEMAS.iter().map(|s| s.to_string()).collect()
    } else {
        configured
            .iter()
            .filter(|s| !is_system_schema(s))
            .cloned()
            .collect()
    }
}

/// Returns true only when `schema` is in the effective schema list and is not
/// a system schema. Used as a 404 gate and before identifier interpolation.
pub fn is_user_schema(schema: &str, effective: &[String]) -> bool {
    !is_system_schema(schema) && effective.iter().any(|s| s == schema)
}

/// A pg identifier (schema/table name) is safe to interpolate when it is a
/// non-empty snake_case-ish token. We never interpolate user-supplied identifiers
/// without also validating against information_schema, but this guards against
/// anything pathological slipping through.
fn ident_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

// --------------------------------------------------------------------- core

/// Per-schema stats: table count, total estimated rows, size on disk.
pub async fn list_schemas(pool: &Pool, effective: &[String]) -> ApiResult<Vec<Map<String, Value>>> {
    let sql = "
    SELECT n.nspname AS schema,
           count(*) FILTER (WHERE c.relkind = 'r')  AS tables,
           count(*) FILTER (WHERE c.relkind = 'v')  AS views,
           count(*) FILTER (WHERE c.relkind = 'm')  AS materialized_views,
           coalesce(sum(c.reltuples) FILTER (WHERE c.relkind = 'r'), 0)::bigint AS est_rows,
           coalesce(sum(pg_total_relation_size(c.oid)) FILTER (WHERE c.relkind IN ('r','m')), 0)::bigint AS size_bytes
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = ANY($1)
     GROUP BY n.nspname
     ORDER BY n.nspname";
    let schemas: Vec<String> = effective.to_vec();
    let client = pool.get().await?;
    let rows = client.query(sql, &[&schemas]).await?;
    Ok(rows_to_objects(&rows))
}

/// Per-table stats inside one schema.
pub async fn list_tables(pool: &Pool, schema: &str) -> ApiResult<Vec<Map<String, Value>>> {
    let sql_ts = "
    SELECT c.relname AS table,
           c.reltuples::bigint AS est_rows,
           pg_total_relation_size(c.oid)::bigint AS size_bytes,
           EXISTS (
               SELECT 1 FROM _timescaledb_catalog.hypertable h
                WHERE h.schema_name = n.nspname AND h.table_name = c.relname
           ) AS is_hypertable,
           obj_description(c.oid, 'pg_class') AS comment
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1
       AND c.relkind IN ('r', 'm')
     ORDER BY c.relname";
    // Vanilla Postgres without TimescaleDB: fall back to a version without the
    // _timescaledb_catalog reference (returns is_hypertable=false for all tables).
    let sql_plain = "
    SELECT c.relname AS table,
           c.reltuples::bigint AS est_rows,
           pg_total_relation_size(c.oid)::bigint AS size_bytes,
           false AS is_hypertable,
           obj_description(c.oid, 'pg_class') AS comment
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1
       AND c.relkind IN ('r', 'm')
     ORDER BY c.relname";
    let client = pool.get().await?;
    let rows = match client.query(sql_ts, &[&schema]).await {
        Ok(r) => r,
        Err(ref e) if is_undefined_table(e) => client.query(sql_plain, &[&schema]).await?,
        Err(e) => return Err(e.into()),
    };
    Ok(rows_to_objects(&rows))
}

const PROV_SET: &[&str] = &["source", "source_endpoint", "source_run_id", "ingest_ts", "raw"];

/// Full profile of one table. Returns None (→ 404) when the table doesn't exist.
pub async fn table_profile(
    pool: &Pool,
    schema: &str,
    table: &str,
) -> ApiResult<Option<Value>> {
    let client = pool.get().await?;

    // Table meta — also tells us whether the relation exists at all.
    // Two SQL variants: TimescaleDB-aware (with hypertable check) and plain fallback.
    let meta_ts = "
    SELECT pg_total_relation_size(c.oid)::bigint AS size_bytes,
           c.reltuples::bigint                   AS est_rows,
           obj_description(c.oid, 'pg_class')    AS comment,
           EXISTS (
              SELECT 1 FROM _timescaledb_catalog.hypertable h
               WHERE h.schema_name = n.nspname AND h.table_name = c.relname
           ) AS is_hypertable
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname = $2";
    let meta_plain = "
    SELECT pg_total_relation_size(c.oid)::bigint AS size_bytes,
           c.reltuples::bigint                   AS est_rows,
           obj_description(c.oid, 'pg_class')    AS comment,
           false                                 AS is_hypertable
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname = $2";
    let meta = match client.query_opt(meta_ts, &[&schema, &table]).await {
        Ok(r) => r,
        Err(ref e) if is_undefined_table(e) => {
            client.query_opt(meta_plain, &[&schema, &table]).await?
        }
        Err(e) => return Err(e.into()),
    };
    let meta = match meta {
        Some(m) => m,
        None => return Ok(None),
    };

    // Columns.
    let col_rows = client
        .query(
            "
    SELECT column_name AS name, data_type AS type, udt_name,
           is_nullable = 'YES' AS nullable,
           column_default AS default_val,
           is_generated, identity_generation,
           ordinal_position AS pos
      FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await?;
    let mut cols: Vec<Value> = Vec::with_capacity(col_rows.len());
    let mut has_source = false;
    for r in &col_rows {
        let name: String = r.get("name");
        if name == "source" {
            has_source = true;
        }
        let is_generated: Option<String> = r.try_get("is_generated").ok();
        let identity_generation: Option<String> = r.try_get("identity_generation").ok();
        let generated = is_generated.as_deref() == Some("ALWAYS")
            || identity_generation.as_deref() == Some("ALWAYS");
        let default_val: Option<String> = r.try_get("default_val").ok().flatten();
        let ty: Option<String> = r.try_get("type").ok().flatten();
        let udt: Option<String> = r.try_get("udt_name").ok().flatten();
        let nullable: Option<bool> = r.try_get("nullable").ok().flatten();
        cols.push(json!({
            "name": name,
            "type": ty,
            "udt": udt,
            "nullable": nullable.unwrap_or(false),
            "default": default_val,
            "is_generated": generated,
            "is_provenance": PROV_SET.contains(&name.as_str()),
        }));
    }

    // UNIQUE / PRIMARY KEY constraints → natural key.
    let unique_rows = client
        .query(
            "
    SELECT kcu.column_name, tc.constraint_type, tc.constraint_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema    = kcu.table_schema
     WHERE tc.table_schema = $1 AND tc.table_name = $2
       AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
     ORDER BY tc.constraint_type DESC, kcu.ordinal_position",
            &[&schema, &table],
        )
        .await?;
    // Preserve insertion order of constraint groups (Python relied on dict order).
    let mut group_order: Vec<(String, String)> = Vec::new();
    let mut groups: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    for r in &unique_rows {
        let ctype: String = r.get("constraint_type");
        let cname: String = r.get("constraint_name");
        let col: String = r.get("column_name");
        let key = (ctype, cname);
        if !groups.contains_key(&key) {
            group_order.push(key.clone());
        }
        groups.entry(key).or_default().push(col);
    }
    let mut natural_key: Vec<String> = Vec::new();
    for key in &group_order {
        let gcols = &groups[key];
        if key.0 == "UNIQUE" && !gcols.iter().any(|c| c == "id") {
            natural_key = gcols.clone();
            break;
        }
    }
    if natural_key.is_empty() {
        if let Some(first) = group_order.first() {
            natural_key = groups[first].clone();
        }
    }

    // Top-5 (source, source_endpoint) by row count — only if a `source` column
    // exists. Best-effort: failures are swallowed (Python logged + continued).
    let mut top_sources: Vec<Map<String, Value>> = Vec::new();
    if has_source && ident_ok(schema) && ident_ok(table) {
        let sources_sql = format!(
            "SELECT source, source_endpoint, count(*) AS rows
               FROM {schema}.{table}
              GROUP BY source, source_endpoint
              ORDER BY rows DESC
              LIMIT 5"
        );
        if let Ok(src_rows) = client.query(sources_sql.as_str(), &[]).await {
            top_sources = rows_to_objects(&src_rows);
        }
    }

    // Last 5 provenance.runs touching this table.
    let runs_sql = "
    SELECT r.run_id, r.endpoint_id, r.started_at, r.ended_at, r.status,
           r.rows_inserted, r.rows_updated, r.rows_failed, r.submitted_by, r.args
      FROM provenance.runs r
      JOIN provenance.endpoints e ON e.endpoint_id = r.endpoint_id
     WHERE (e.target_schema = $1 AND e.target_table = $2)
        OR (r.args ? 'target_schema' AND r.args->>'target_schema' = $1
                                    AND r.args->>'target_table' = $2)
     ORDER BY r.started_at DESC
     LIMIT 5";
    let mut runs: Vec<Value> = Vec::new();
    if let Ok(run_rows) = client.query(runs_sql, &[&schema, &table]).await {
        for r in &run_rows {
            runs.push(serialize_profile_run(r));
        }
    }

    let size_bytes: Option<i64> = meta.try_get("size_bytes").ok().flatten();
    let est_rows: Option<i64> = meta.try_get("est_rows").ok().flatten();
    let comment: Option<String> = meta.try_get("comment").ok().flatten();
    let is_hypertable: Option<bool> = meta.try_get("is_hypertable").ok().flatten();

    Ok(Some(json!({
        "schema": schema,
        "table": table,
        "is_hypertable": is_hypertable.unwrap_or(false),
        "est_rows": est_rows,
        "size_bytes": size_bytes,
        "comment": comment,
        "columns": cols,
        "natural_key": natural_key,
        "top_sources": top_sources,
        "recent_runs": runs,
    })))
}

fn ts_iso(row: &tokio_postgres::Row, col: &str) -> Value {
    match row.try_get::<_, Option<DateTime<Utc>>>(col) {
        Ok(Some(t)) => Value::String(iso_z(t)),
        _ => Value::Null,
    }
}

fn iso_z(t: DateTime<Utc>) -> String {
    use chrono::SecondsFormat;
    let fmt = if t.timestamp_subsec_nanos() == 0 {
        SecondsFormat::Secs
    } else {
        SecondsFormat::Micros
    };
    t.to_rfc3339_opts(fmt, true)
}

fn serialize_profile_run(r: &tokio_postgres::Row) -> Value {
    let run_id: Option<uuid::Uuid> = r.try_get("run_id").ok().flatten();
    let endpoint_id: Option<String> = r.try_get("endpoint_id").ok().flatten();
    let status: Option<String> = r.try_get("status").ok().flatten();
    let submitted_by: Option<String> = r.try_get("submitted_by").ok().flatten();
    let args: Option<Value> = r.try_get("args").ok().flatten();
    let rows_inserted: Option<i64> = r.try_get("rows_inserted").ok().flatten();
    let rows_updated: Option<i64> = r.try_get("rows_updated").ok().flatten();
    let rows_failed: Option<i64> = r.try_get("rows_failed").ok().flatten();
    json!({
        "run_id": run_id.map(|u| u.to_string()),
        "endpoint_id": endpoint_id,
        "started_at": ts_iso(r, "started_at"),
        "ended_at": ts_iso(r, "ended_at"),
        "status": status,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_failed": rows_failed,
        "submitted_by": submitted_by,
        "args": args.unwrap_or(Value::Null),
    })
}

/// Resolve the ACL to a concrete list of (schema, table) for one role.
/// Wildcards (`*`,`*`) and (schema,`*`) are expanded against information_schema.
pub async fn list_writable_for_role(pool: &Pool, role: &str) -> ApiResult<Vec<Value>> {
    let client = pool.get().await?;
    let rule_rows = client
        .query(
            "
    SELECT target_schema, target_table, can_write, notes
      FROM provenance.ingress_acl
     WHERE role = $1 AND can_write = true
     ORDER BY target_schema, target_table",
            &[&role],
        )
        .await?;

    let schemas: Vec<String> = USER_SCHEMAS.iter().map(|s| s.to_string()).collect(); // ACL expansion uses full whitelist
    let mut out: Vec<Value> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();

    for rule in &rule_rows {
        let sch: String = rule.get("target_schema");
        let tbl: String = rule.get("target_table");
        let notes: Option<String> = rule.try_get("notes").ok().flatten();

        if sch == "*" && tbl == "*" {
            let rows = client
                .query(
                    "
                    SELECT table_schema, table_name
                      FROM information_schema.tables
                     WHERE table_schema = ANY($1)
                       AND table_type = 'BASE TABLE'
                     ORDER BY table_schema, table_name",
                    &[&schemas],
                )
                .await?;
            for row in &rows {
                let s: String = row.get("table_schema");
                let t: String = row.get("table_name");
                let key = (s.clone(), t.clone());
                if !seen.insert(key) {
                    continue;
                }
                out.push(json!({
                    "schema": s,
                    "table": t,
                    "rule_source": "wildcard",
                    "schema_url": format!("/catalog/tables/{s}/{t}/schema.json"),
                }));
            }
        } else if tbl == "*" {
            let rows = client
                .query(
                    "
                    SELECT table_name
                      FROM information_schema.tables
                     WHERE table_schema = $1
                       AND table_type = 'BASE TABLE'
                     ORDER BY table_name",
                    &[&sch],
                )
                .await?;
            for row in &rows {
                let t: String = row.get("table_name");
                let key = (sch.clone(), t.clone());
                if !seen.insert(key) {
                    continue;
                }
                out.push(json!({
                    "schema": sch,
                    "table": t,
                    "rule_source": "wildcard",
                    "schema_url": format!("/catalog/tables/{sch}/{t}/schema.json"),
                }));
            }
        } else {
            let key = (sch.clone(), tbl.clone());
            if !seen.insert(key) {
                continue;
            }
            out.push(json!({
                "schema": sch,
                "table": tbl,
                "rule_source": "explicit",
                "notes": notes,
                "schema_url": format!("/catalog/tables/{sch}/{tbl}/schema.json"),
            }));
        }
    }
    Ok(out)
}

// ------------------------------------------------------------------ lineage

const RUN_SELECT: &str = "
    SELECT r.run_id, r.endpoint_id, r.started_at, r.ended_at, r.status,
           r.submitted_by, r.args, r.rows_inserted, r.rows_updated,
           r.rows_failed, r.error_text,
           e.source, e.path_template, e.target_schema, e.target_table, e.scope
      FROM provenance.runs r
      JOIN provenance.endpoints e ON e.endpoint_id = r.endpoint_id";

fn serialize_run(r: &tokio_postgres::Row) -> Value {
    let run_id: Option<uuid::Uuid> = r.try_get("run_id").ok().flatten();
    let endpoint_id: Option<String> = r.try_get("endpoint_id").ok().flatten();
    let status: Option<String> = r.try_get("status").ok().flatten();
    let submitted_by: Option<String> = r.try_get("submitted_by").ok().flatten();
    let args: Option<Value> = r.try_get("args").ok().flatten();
    let rows_inserted: Option<i64> = r.try_get("rows_inserted").ok().flatten();
    let rows_updated: Option<i64> = r.try_get("rows_updated").ok().flatten();
    let rows_failed: Option<i64> = r.try_get("rows_failed").ok().flatten();
    let error_text: Option<String> = r.try_get("error_text").ok().flatten();
    let source: Option<String> = r.try_get("source").ok().flatten();
    let path_template: Option<String> = r.try_get("path_template").ok().flatten();
    let target_schema: Option<String> = r.try_get("target_schema").ok().flatten();
    let target_table: Option<String> = r.try_get("target_table").ok().flatten();
    let scope: Option<String> = r.try_get("scope").ok().flatten();
    json!({
        "run_id": run_id.map(|u| u.to_string()),
        "endpoint_id": endpoint_id,
        "started_at": ts_iso(r, "started_at"),
        "ended_at": ts_iso(r, "ended_at"),
        "status": status,
        "submitted_by": submitted_by,
        "args": args.unwrap_or(Value::Null),
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_failed": rows_failed,
        "error_text": error_text,
        "endpoint": {
            "source": source,
            "path_template": path_template,
            "target_schema": target_schema,
            "target_table": target_table,
            "scope": scope,
        },
    })
}

pub async fn trace_run(pool: &Pool, run_id: &str) -> ApiResult<Option<Value>> {
    let uuid = match uuid::Uuid::parse_str(run_id) {
        Ok(u) => u,
        Err(_) => return Ok(None),
    };
    let sql = format!("{RUN_SELECT}\n     WHERE r.run_id = $1");
    let client = pool.get().await?;
    let row = client.query_opt(sql.as_str(), &[&uuid]).await?;
    Ok(row.as_ref().map(serialize_run))
}

#[allow(clippy::too_many_arguments)]
pub async fn list_runs_for(
    pool: &Pool,
    submitted_by: Option<&str>,
    target_schema: Option<&str>,
    target_table: Option<&str>,
    status: Option<&str>,
    since: Option<&str>,
    limit: i64,
) -> ApiResult<Vec<Value>> {
    let mut where_: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

    if let Some(s) = submitted_by {
        params.push(Box::new(s.to_string()));
        where_.push(format!("r.submitted_by = ${}", params.len()));
    }
    if let Some(s) = status {
        params.push(Box::new(s.to_string()));
        where_.push(format!("r.status = ${}", params.len()));
    }
    // Generate only the conditions for filters actually supplied. Using `= NULL`
    // (SQL null equality) always returns false — use separate clauses so a caller
    // that supplies only target_schema still matches runs without a table constraint.
    if let Some(s) = target_schema {
        params.push(Box::new(s.to_string()));
        let i = params.len();
        if let Some(t) = target_table {
            params.push(Box::new(t.to_string()));
            let j = params.len();
            where_.push(format!(
                "((e.target_schema = ${i} AND e.target_table = ${j}) \
                 OR (r.args->>'target_schema' = ${i} AND r.args->>'target_table' = ${j}))"
            ));
        } else {
            where_.push(format!(
                "(e.target_schema = ${i} OR r.args->>'target_schema' = ${i})"
            ));
        }
    } else if let Some(t) = target_table {
        params.push(Box::new(t.to_string()));
        let j = params.len();
        where_.push(format!(
            "(e.target_table = ${j} OR r.args->>'target_table' = ${j})"
        ));
    }
    if let Some(s) = since {
        params.push(Box::new(s.to_string()));
        where_.push(format!("r.started_at >= ${}::timestamptz", params.len()));
    }
    let where_sql = if where_.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_.join(" AND "))
    };
    params.push(Box::new(limit));
    let limit_idx = params.len();
    let sql = format!(
        "{RUN_SELECT}\n    {where_sql}\n     ORDER BY r.started_at DESC\n     LIMIT ${limit_idx}"
    );
    let refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let client = pool.get().await?;
    let rows = client.query(sql.as_str(), &refs).await?;
    Ok(rows.iter().map(serialize_run).collect())
}

/// Coerce a query-string value to the SQL type the column expects, so the
/// tokio-postgres binary protocol binds the right type. Mirrors `_coerce_param`.
enum Param {
    Text(String),
    Ts(DateTime<Utc>),
    Date(NaiveDate),
    Int(i64),
    Float(f64),
    Bool(bool),
}

fn coerce_param(value: &str, pg_type: &str) -> Param {
    let t = pg_type.to_lowercase();
    match t.as_str() {
        "timestamp with time zone" | "timestamp without time zone" => {
            let s = value.replace('Z', "+00:00");
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                return Param::Ts(dt.with_timezone(&Utc));
            }
            // +HH short form → +HH:00
            if s.len() >= 3 {
                let bytes = s.as_bytes();
                let c = bytes[bytes.len() - 3] as char;
                if (c == '+' || c == '-') && s[s.len() - 2..].chars().all(|d| d.is_ascii_digit()) {
                    let s2 = format!("{s}:00");
                    if let Ok(dt) = DateTime::parse_from_rfc3339(&s2) {
                        return Param::Ts(dt.with_timezone(&Utc));
                    }
                }
            }
            // naive (no offset): try parse as naive then assume UTC.
            if let Ok(ndt) = NaiveDateTime::parse_from_str(&value.replace('Z', ""), "%Y-%m-%dT%H:%M:%S") {
                return Param::Ts(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
            }
            Param::Text(value.to_string())
        }
        "date" => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Param::Date)
            .unwrap_or_else(|_| Param::Text(value.to_string())),
        "integer" | "bigint" | "smallint" => value
            .parse::<i64>()
            .map(Param::Int)
            .unwrap_or_else(|_| Param::Text(value.to_string())),
        "numeric" | "real" | "double precision" => value
            .parse::<f64>()
            .map(Param::Float)
            .unwrap_or_else(|_| Param::Text(value.to_string())),
        "boolean" => match value.to_lowercase().trim() {
            "true" | "t" | "1" | "yes" => Param::Bool(true),
            "false" | "f" | "0" | "no" => Param::Bool(false),
            _ => Param::Text(value.to_string()),
        },
        _ => Param::Text(value.to_string()),
    }
}

/// Trace by natural-key filter. Returns None (→ 404) when schema/columns invalid
/// or no fact row matches. Column names are validated against information_schema.
pub async fn trace_by_natural_key(
    pool: &Pool,
    schema: &str,
    table: &str,
    key_filters: &BTreeMap<String, String>,
    effective: &[String],
) -> ApiResult<Option<Value>> {
    if !is_user_schema(schema, effective) {
        return Ok(None);
    }
    if !ident_ok(schema) || !ident_ok(table) {
        return Ok(None);
    }
    let client = pool.get().await?;
    let col_rows = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns \
              WHERE table_schema=$1 AND table_name=$2",
            &[&schema, &table],
        )
        .await?;
    if col_rows.is_empty() {
        return Ok(None);
    }
    let mut col_types: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for r in &col_rows {
        let name: String = r.get("column_name");
        let ty: String = r.get("data_type");
        col_types.insert(name, ty);
    }
    // Validate every requested key column; silent reject (never echo columns).
    for k in key_filters.keys() {
        if !col_types.contains_key(k) {
            return Ok(None);
        }
    }

    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    for (k, v) in key_filters {
        let pg_type = &col_types[k];
        match coerce_param(v, pg_type) {
            Param::Text(s) => params.push(Box::new(s)),
            Param::Ts(t) => params.push(Box::new(t)),
            Param::Date(d) => params.push(Box::new(d)),
            Param::Int(i) => params.push(Box::new(i)),
            Param::Float(f) => params.push(Box::new(f)),
            Param::Bool(b) => params.push(Box::new(b)),
        }
        // Column name is validated against information_schema above; safe to interpolate.
        clauses.push(format!("{k} = ${}", params.len()));
    }
    if clauses.is_empty() {
        return Ok(None);
    }
    let sql = format!(
        "SELECT source, source_endpoint, source_run_id::text AS source_run_id, ingest_ts \
           FROM {schema}.{table} \
          WHERE {} \
          ORDER BY ingest_ts DESC \
          LIMIT 5",
        clauses.join(" AND ")
    );
    let refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let fact_rows = client.query(sql.as_str(), &refs).await?;
    if fact_rows.is_empty() {
        return Ok(None);
    }

    let mut matches: Vec<Value> = Vec::new();
    for fr in &fact_rows {
        let source: Option<String> = fr.try_get("source").ok().flatten();
        let source_endpoint: Option<String> = fr.try_get("source_endpoint").ok().flatten();
        let source_run_id: Option<String> = fr.try_get("source_run_id").ok().flatten();
        let run = match &source_run_id {
            Some(rid) => trace_run(pool, rid).await?,
            None => None,
        };
        matches.push(json!({
            "fact_row": {
                "source": source,
                "source_endpoint": source_endpoint,
                "source_run_id": source_run_id,
                "ingest_ts": ts_iso(fr, "ingest_ts"),
            },
            "run": run,
        }));
    }
    // Echo the key as a JSON object of string values (matches the input shape).
    let key_obj: Map<String, Value> = key_filters
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    Ok(Some(json!({
        "schema": schema,
        "table": table,
        "key": Value::Object(key_obj),
        "matches": matches,
    })))
}

/// Distinct (source, source_endpoint) tuples for one table, or the aggregate
/// across the warehouse (via provenance.runs) when no filter is given.
pub async fn list_sources(
    pool: &Pool,
    schema: Option<&str>,
    table: Option<&str>,
) -> ApiResult<Vec<Value>> {
    let client = pool.get().await?;
    if let (Some(sch), Some(tbl)) = (schema, table) {
        if !ident_ok(sch) || !ident_ok(tbl) {
            return Err(ApiError::BadRequest("invalid schema/table".into()));
        }
        let sql = format!(
            "SELECT source, source_endpoint, count(*) AS rows
               FROM {sch}.{tbl}
              GROUP BY source, source_endpoint
              ORDER BY rows DESC
              LIMIT 100"
        );
        let rows = client.query(sql.as_str(), &[]).await?;
        return Ok(rows
            .iter()
            .map(|r| {
                let source: Option<String> = r.try_get("source").ok().flatten();
                let se: Option<String> = r.try_get("source_endpoint").ok().flatten();
                let cnt: i64 = r.try_get("rows").ok().flatten().unwrap_or(0);
                json!({
                    "schema": sch,
                    "table": tbl,
                    "source": source,
                    "source_endpoint": se,
                    "rows": cnt,
                })
            })
            .collect());
    }
    let sql = "
    SELECT e.source, r.endpoint_id, count(*) AS run_count,
           sum(coalesce(r.rows_inserted, 0))::bigint AS rows_inserted_total,
           min(r.started_at) AS first_seen,
           max(r.started_at) AS last_seen
      FROM provenance.runs r
      JOIN provenance.endpoints e ON e.endpoint_id = r.endpoint_id
     GROUP BY e.source, r.endpoint_id
     ORDER BY rows_inserted_total DESC NULLS LAST
     LIMIT 100";
    let rows = client.query(sql, &[]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let source: Option<String> = r.try_get("source").ok().flatten();
            let endpoint_id: Option<String> = r.try_get("endpoint_id").ok().flatten();
            let run_count: i64 = r.try_get("run_count").ok().flatten().unwrap_or(0);
            let rit: i64 = r.try_get("rows_inserted_total").ok().flatten().unwrap_or(0);
            json!({
                "source": source,
                "endpoint_id": endpoint_id,
                "runs": run_count,
                "rows_inserted_total": rit,
                "first_seen": ts_iso(r, "first_seen"),
                "last_seen": ts_iso(r, "last_seen"),
            })
        })
        .collect())
}

/// Distinct submitted_by values (ingress runs only — scraper runs have NULL).
pub async fn list_submitters(pool: &Pool, only_self: Option<&str>) -> ApiResult<Vec<Value>> {
    let client = pool.get().await?;
    let rows = if let Some(s) = only_self {
        let sql = "
    SELECT submitted_by,
           count(*) AS runs,
           sum(coalesce(rows_inserted, 0))::bigint AS rows_inserted_total,
           sum(coalesce(rows_updated, 0))::bigint  AS rows_updated_total,
           sum(coalesce(rows_failed, 0))::bigint   AS rows_failed_total,
           min(started_at) AS first_seen,
           max(started_at) AS last_seen
      FROM provenance.runs
     WHERE submitted_by IS NOT NULL AND submitted_by = $1
     GROUP BY submitted_by
     ORDER BY rows_inserted_total DESC NULLS LAST
     LIMIT 200";
        client.query(sql, &[&s]).await?
    } else {
        let sql = "
    SELECT submitted_by,
           count(*) AS runs,
           sum(coalesce(rows_inserted, 0))::bigint AS rows_inserted_total,
           sum(coalesce(rows_updated, 0))::bigint  AS rows_updated_total,
           sum(coalesce(rows_failed, 0))::bigint   AS rows_failed_total,
           min(started_at) AS first_seen,
           max(started_at) AS last_seen
      FROM provenance.runs
     WHERE submitted_by IS NOT NULL
     GROUP BY submitted_by
     ORDER BY rows_inserted_total DESC NULLS LAST
     LIMIT 200";
        client.query(sql, &[]).await?
    };
    Ok(rows
        .iter()
        .map(|r| {
            let submitted_by: Option<String> = r.try_get("submitted_by").ok().flatten();
            let runs: i64 = r.try_get("runs").ok().flatten().unwrap_or(0);
            let rit: i64 = r.try_get("rows_inserted_total").ok().flatten().unwrap_or(0);
            let rut: i64 = r.try_get("rows_updated_total").ok().flatten().unwrap_or(0);
            let rft: i64 = r.try_get("rows_failed_total").ok().flatten().unwrap_or(0);
            json!({
                "submitted_by": submitted_by,
                "runs": runs,
                "rows_inserted_total": rit,
                "rows_updated_total": rut,
                "rows_failed_total": rft,
                "first_seen": ts_iso(r, "first_seen"),
                "last_seen": ts_iso(r, "last_seen"),
            })
        })
        .collect())
}
