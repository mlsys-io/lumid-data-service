//! Build `SchemaCard` instances from a live Postgres connection pool.
//!
//! Uses `pg_stats`, `pg_attribute`, `pg_constraint`, and
//! `information_schema` — the same catalog surfaces as the Python
//! `SchemaCardBuilder`. Falls back gracefully when tables are
//! un-ANALYZEd (stats stay `None`; structural fields still populate).

use chrono::Utc;
use deadpool_postgres::Pool;

use crate::error::ApiResult;

use super::schema_card::{ColumnCard, ForeignKeyHint, SchemaCard};

/// Build cards for the given schemas. When `schemas` is empty, query all
/// non-system schemas (matching `effective_user_schemas` logic). `sample_rows`
/// controls how many histogram/MCV values are included as `sample_values`.
pub async fn build_cards(
    pool: &Pool,
    schemas: &[String],
    sample_rows: usize,
) -> ApiResult<Vec<SchemaCard>> {
    let client = pool.get().await?;

    // Resolve the list of (schema, table) pairs to card.
    let effective_schemas: Vec<String> = if schemas.is_empty() {
        let rows = client
            .query(
                "SELECT nspname FROM pg_namespace \
                  WHERE nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
                    AND nspname NOT IN ('information_schema', 'lumid_data_meta') \
                  ORDER BY nspname",
                &[],
            )
            .await?;
        rows.iter()
            .map(|r| r.get::<_, String>("nspname"))
            .collect()
    } else {
        schemas.to_vec()
    };

    // Collect (schema, table) pairs.
    let schema_refs: Vec<String> = effective_schemas.clone();
    let table_rows = client
        .query(
            "SELECT table_schema, table_name \
               FROM information_schema.tables \
              WHERE table_schema = ANY($1) \
                AND table_type = 'BASE TABLE' \
              ORDER BY table_schema, table_name",
            &[&schema_refs],
        )
        .await?;

    let mut cards: Vec<SchemaCard> = Vec::new();
    for row in &table_rows {
        let schema: String = row.get("table_schema");
        let table: String = row.get("table_name");
        match build_one_card(pool, &schema, &table, sample_rows).await {
            Ok(card) => cards.push(card),
            Err(e) => {
                tracing::warn!("card build failed for {schema}.{table}: {e}");
            }
        }
    }
    Ok(cards)
}

// SQL constants extracted for testability — guards against direct-param-to-regclass cast regression.
const SQL_COL_ROWS: &str = "SELECT a.attname AS name, \
        format_type(a.atttypid, a.atttypmod) AS type, \
        NOT a.attnotnull AS nullable, \
        col_description(a.attrelid, a.attnum) AS description \
   FROM pg_attribute a \
  WHERE a.attrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass \
    AND a.attnum > 0 \
    AND NOT a.attisdropped \
  ORDER BY a.attnum";

const SQL_PK_ROWS: &str = "SELECT a.attname AS name \
   FROM pg_index i \
   JOIN pg_attribute a \
     ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
  WHERE i.indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass AND i.indisprimary \
  ORDER BY array_position(i.indkey, a.attnum)";

const SQL_FK_ROWS: &str = "SELECT \
    child_a.attname  AS column, \
    refclass.relname AS ref_table, \
    refschema.nspname AS ref_schema, \
    ref_a.attname    AS ref_column \
  FROM pg_constraint c \
  JOIN pg_class childclass    ON childclass.oid = c.conrelid \
  JOIN pg_class refclass      ON refclass.oid = c.confrelid \
  JOIN pg_namespace refschema ON refschema.oid = refclass.relnamespace \
  JOIN unnest(c.conkey) WITH ORDINALITY AS k(attnum, idx) ON true \
  JOIN unnest(c.confkey) WITH ORDINALITY AS r(attnum, idx) ON r.idx = k.idx \
  JOIN pg_attribute child_a \
    ON child_a.attrelid = c.conrelid AND child_a.attnum = k.attnum \
  JOIN pg_attribute ref_a \
    ON ref_a.attrelid = c.confrelid AND ref_a.attnum = r.attnum \
 WHERE c.conrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass AND c.contype = 'f' \
 ORDER BY c.conname, k.idx";

async fn build_one_card(
    pool: &Pool,
    schema: &str,
    table: &str,
    sample_rows: usize,
) -> ApiResult<SchemaCard> {
    let client = pool.get().await?;
    let fqname = format!("{schema}.{table}");

    // Approximate row count + description.
    let meta_row = client
        .query_opt(
            "SELECT c.reltuples::bigint AS n, \
                    pg_total_relation_size(c.oid)::bigint AS size_bytes, \
                    obj_description(c.oid, 'pg_class') AS comment \
               FROM pg_class c \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await?;

    let (approx_row_count, size_bytes, description) = match meta_row {
        Some(r) => {
            let n: Option<i64> = r.try_get("n").ok().flatten();
            let sb: Option<i64> = r.try_get("size_bytes").ok().flatten();
            let comment: Option<String> = r.try_get("comment").ok().flatten();
            let approx = n.filter(|&v| v >= 0);
            (approx, sb, comment)
        }
        None => (None, None, None),
    };

    // Columns from pg_attribute.
    // quote_ident params avoid direct-param-to-regclass cast which fails to encode Rust String.
    let col_rows = client
        .query(SQL_COL_ROWS, &[&schema, &table])
        .await?;

    // Primary key columns.
    let pk_rows = client
        .query(SQL_PK_ROWS, &[&schema, &table])
        .await?;
    let pk_set: std::collections::HashSet<String> = pk_rows
        .iter()
        .map(|r| r.get::<_, String>("name"))
        .collect();
    let pk: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>("name")).collect();

    // Foreign keys.
    let fk_rows = client
        .query(SQL_FK_ROWS, &[&schema, &table])
        .await?;
    let fk_set: std::collections::HashSet<String> = fk_rows
        .iter()
        .map(|r| r.get::<_, String>("column"))
        .collect();
    let fks: Vec<ForeignKeyHint> = fk_rows
        .iter()
        .map(|r| {
            let ref_schema: String = r.get("ref_schema");
            let ref_table: String = r.get("ref_table");
            ForeignKeyHint {
                column: r.get("column"),
                ref_table: format!("{ref_schema}.{ref_table}"),
                ref_column: r.get("ref_column"),
            }
        })
        .collect();

    // Column stats from pg_stats.
    let stat_rows = client
        .query(
            "SELECT attname, n_distinct, null_frac, \
                    most_common_vals::text AS mcv_text, \
                    most_common_freqs, \
                    histogram_bounds::text AS hist_text \
               FROM pg_stats \
              WHERE schemaname = $1 AND tablename = $2",
            &[&schema, &table],
        )
        .await?;
    let mut stats: std::collections::HashMap<String, ColStat> =
        std::collections::HashMap::new();
    for r in &stat_rows {
        let col_name: String = r.get("attname");
        let n_distinct: Option<f64> = r.try_get("n_distinct").ok().flatten();
        let null_frac: Option<f32> = r.try_get("null_frac").ok().flatten();
        let mcv_text: Option<String> = r.try_get("mcv_text").ok().flatten();
        let hist_text: Option<String> = r.try_get("hist_text").ok().flatten();
        let mcv = parse_pg_array(mcv_text.as_deref());
        let hist = parse_pg_array(hist_text.as_deref());
        let distinct_count = n_distinct.and_then(|v| if v >= 0.0 { Some(v as i64) } else { None });
        let null_pct = null_frac.map(|f| f as f64);
        let mut sample_values: Vec<serde_json::Value> = mcv
            .iter()
            .take(sample_rows)
            .map(|s| serde_json::Value::String(s.clone()))
            .collect();
        if sample_values.is_empty() {
            sample_values = hist
                .iter()
                .take(sample_rows)
                .map(|s| serde_json::Value::String(s.clone()))
                .collect();
        }
        let min = hist.first().map(|s| serde_json::Value::String(s.clone()));
        let max = hist.last().map(|s| serde_json::Value::String(s.clone()));
        stats.insert(
            col_name,
            ColStat {
                distinct_count,
                null_pct,
                sample_values,
                min,
                max,
            },
        );
    }

    // Assemble ColumnCard list.
    let columns: Vec<ColumnCard> = col_rows
        .iter()
        .map(|r| {
            let name: String = r.get("name");
            let col_type: String = r.try_get("type").ok().flatten().unwrap_or_default();
            let nullable: Option<bool> = r.try_get("nullable").ok().flatten();
            let description: Option<String> = r.try_get("description").ok().flatten();
            let is_pk = pk_set.contains(&name);
            let is_fk = fk_set.contains(&name);
            let stat = stats.get(&name);
            ColumnCard {
                col_type,
                nullable: nullable.unwrap_or(true),
                description,
                is_pk,
                is_fk,
                distinct_count: stat.and_then(|s| s.distinct_count),
                null_pct: stat.and_then(|s| s.null_pct),
                sample_values: stat
                    .map(|s| s.sample_values.clone())
                    .unwrap_or_default(),
                min: stat.and_then(|s| s.min.clone()),
                max: stat.and_then(|s| s.max.clone()),
                name,
            }
        })
        .collect();

    Ok(SchemaCard {
        fqname,
        description,
        approx_row_count,
        size_bytes,
        columns,
        pk,
        fks,
        built_at: Utc::now().to_rfc3339(),
    })
}

struct ColStat {
    distinct_count: Option<i64>,
    null_pct: Option<f64>,
    sample_values: Vec<serde_json::Value>,
    min: Option<serde_json::Value>,
    max: Option<serde_json::Value>,
}

/// Parse the Postgres canonical array text format `{a,b,"c d"}`.
fn parse_pg_array(text: Option<&str>) -> Vec<String> {
    let text = match text {
        Some(t) => t.trim(),
        None => return vec![],
    };
    if !text.starts_with('{') || !text.ends_with('}') {
        return vec![];
    }
    let inner = &text[1..text.len() - 1];
    if inner.is_empty() {
        return vec![];
    }
    let mut out: Vec<String> = Vec::new();
    let chars: Vec<char> = inner.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        if chars[i] == '"' {
            let mut buf = String::new();
            i += 1;
            while i < n {
                if chars[i] == '\\' && i + 1 < n {
                    buf.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                if chars[i] == '"' {
                    i += 1;
                    break;
                }
                buf.push(chars[i]);
                i += 1;
            }
            out.push(buf);
        } else {
            let start = i;
            while i < n && chars[i] != ',' {
                i += 1;
            }
            let piece: String = chars[start..i].iter().collect();
            let piece = piece.trim();
            if !piece.is_empty() && piece != "NULL" {
                out.push(piece.to_string());
            }
        }
        if i < n && chars[i] == ',' {
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_array_basic() {
        assert_eq!(parse_pg_array(Some("{a,b,c}")), vec!["a", "b", "c"]);
        assert_eq!(
            parse_pg_array(Some(r#"{"hello world","foo"}"#)),
            vec!["hello world", "foo"]
        );
        assert_eq!(parse_pg_array(None), Vec::<String>::new());
        assert_eq!(parse_pg_array(Some("{}")), Vec::<String>::new());
    }

    #[test]
    fn parse_pg_array_null_items_dropped() {
        assert_eq!(parse_pg_array(Some("{a,NULL,b}")), vec!["a", "b"]);
    }

    /// Regression guard: none of the catalog SQL strings may cast a bare bind
    /// parameter directly to regclass (which fails to encode Rust String as OID 2205).
    /// Weak test — checks string content only — but locks the specific regression.
    #[test]
    fn catalog_queries_use_quote_ident_not_direct_regclass_cast() {
        for (name, sql) in [
            ("SQL_COL_ROWS", SQL_COL_ROWS),
            ("SQL_PK_ROWS", SQL_PK_ROWS),
            ("SQL_FK_ROWS", SQL_FK_ROWS),
        ] {
            assert!(
                !sql.contains("($1)::regclass") && !sql.contains("($2)::regclass"),
                "{name} must not cast a bare param directly to regclass"
            );
            assert!(
                sql.contains("quote_ident($1)") && sql.contains("quote_ident($2)"),
                "{name} must use quote_ident($1) and quote_ident($2) for safe regclass resolution"
            );
        }
    }
}
