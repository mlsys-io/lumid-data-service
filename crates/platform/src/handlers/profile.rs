//! `POST /profile` — EXPLAIN-based query cost estimation endpoint.
//!
//! Returns planner cost estimates for one or more GUC-variant plans without
//! executing the query. Identical safety boundaries as `/retrieve` apply:
//! SELECT-only parser, READ ONLY transaction, statement timeout, and optional
//! de-escalated DB role. EXPLAIN is run plain (no ANALYZE) so the query is
//! never executed against live data.
//!
//! The response feeds lumilake's HALO cost model. Field semantics match
//! `DataProfileCostEstimate` in `lumilake_OSS/src/lumilake_server/data_profile_models.py`
//! and the footprint extraction logic mirrors `_extract_plan_footprints` /
//! `_aggregate_plan_footprints` in `lumilake_OSS/.../utils/data_profile_offload.py`.

use std::collections::HashMap;

use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{ApiError, ApiResult};
use crate::retrieve::plan::is_safe_select;
use crate::state::AppState;

// ── Request / response types ──────────────────────────────────────────────────

/// One planner-variant request: a `plan_id` label and GUC overrides to apply
/// via `SET LOCAL` before running EXPLAIN.
#[derive(Debug, Clone, Deserialize)]
pub struct PlanVariant {
    pub plan_id: String,
    #[serde(default)]
    pub settings: HashMap<String, String>,
}

/// Parsed, validated profile request.
#[derive(Debug)]
pub struct ProfileRequest {
    pub sql: String,
    pub plans: Vec<PlanVariant>,
}

/// One variant's cost estimate — matches `DataProfileCostEstimate` field names.
#[derive(Debug, Serialize)]
pub struct PlanCostEstimate {
    pub plan_id: String,
    pub raw_cost: f64,
    pub estimated_rows: i64,
    /// Relation / index name → block-footprint weight.
    /// Populated by walking the EXPLAIN plan tree: for each node, weight =
    /// `Shared Hit Blocks + Shared Read Blocks` (defaulting to 1 when both are
    /// absent or zero), accumulated per `Relation Name` / `Index Name` key.
    /// Relation and index footprints are merged into a single map, mirroring
    /// `_extract_plan_footprints` in `data_profile_offload.py`.
    pub footprints: HashMap<String, i64>,
    /// The raw EXPLAIN JSON first element (the object that contains `"Plan"`).
    pub explain_json: Value,
}

/// Top-level profile response.
#[derive(Debug, Serialize)]
pub struct ProfileResponse {
    pub variants: Vec<PlanCostEstimate>,
}

// ── GUC allowlist ─────────────────────────────────────────────────────────────

/// Allowed planner-toggle GUC names. Only `enable_*` planner booleans are
/// permitted; arbitrary `SET LOCAL` injection is rejected with 400.
const ALLOWED_GUC_KEYS: &[&str] = &[
    "enable_seqscan",
    "enable_indexscan",
    "enable_bitmapscan",
    "enable_hashjoin",
    "enable_mergejoin",
    "enable_nestloop",
    "enable_indexonlyscan",
    "enable_material",
    "enable_sort",
];

/// Returns `true` iff `key` is an allowed planner-toggle GUC name.
///
/// The key must match `^enable_[a-z]+$` **and** appear in `ALLOWED_GUC_KEYS`.
/// This two-layer check ensures that:
/// 1. The key is structurally a planner toggle (no arbitrary SQL injection).
/// 2. It is in the explicit allowlist (no unknown `enable_*` names).
pub fn is_allowed_guc_key(key: &str) -> bool {
    // Structural check: must match ^enable_[a-z]+$
    let structurally_ok = key.starts_with("enable_")
        && key.len() > "enable_".len()
        && key["enable_".len()..]
            .bytes()
            .all(|b| b.is_ascii_lowercase());
    if !structurally_ok {
        return false;
    }
    ALLOWED_GUC_KEYS.contains(&key)
}

/// Returns `true` iff `value` is exactly `"on"` or `"off"`.
pub fn is_allowed_guc_value(value: &str) -> bool {
    value == "on" || value == "off"
}

// ── Request parsing ───────────────────────────────────────────────────────────

/// Parse and validate the `POST /profile` request body.
///
/// Extracted as a pure function for unit-testability without `AppState`.
pub fn parse_profile_request(body: &Value) -> ApiResult<ProfileRequest> {
    let sql = body
        .get("sql")
        .ok_or_else(|| ApiError::BadRequest("'sql' is required".into()))?
        .as_str()
        .ok_or_else(|| ApiError::BadRequest("'sql' must be a string".into()))?
        .to_string();

    // Validate SQL safety (same boundary as /retrieve).
    if !is_safe_select(&sql) {
        return Err(ApiError::BadRequest(format!(
            "SQL rejected: only a single read-only SELECT is allowed; got: {}",
            sql.chars().take(200).collect::<String>()
        )));
    }

    let plans = match body.get("plans") {
        None => vec![PlanVariant {
            plan_id: "default".into(),
            settings: HashMap::new(),
        }],
        Some(v) => {
            let arr = v
                .as_array()
                .ok_or_else(|| ApiError::BadRequest("'plans' must be an array".into()))?;
            if arr.is_empty() {
                return Err(ApiError::BadRequest(
                    "'plans' must have at least one entry".into(),
                ));
            }
            let mut parsed: Vec<PlanVariant> = Vec::with_capacity(arr.len());
            for (i, item) in arr.iter().enumerate() {
                let variant: PlanVariant = serde_json::from_value(item.clone())
                    .map_err(|e| ApiError::BadRequest(format!("plans[{i}] is invalid: {e}")))?;
                if variant.plan_id.trim().is_empty() {
                    return Err(ApiError::BadRequest(format!(
                        "plans[{i}].plan_id must not be empty"
                    )));
                }
                // Validate GUC keys and values.
                for (key, val) in &variant.settings {
                    if !is_allowed_guc_key(key) {
                        return Err(ApiError::BadRequest(format!(
                            "plans[{i}]: GUC key '{key}' is not allowed; \
                             only planner enable_* toggles are permitted"
                        )));
                    }
                    if !is_allowed_guc_value(val) {
                        return Err(ApiError::BadRequest(format!(
                            "plans[{i}]: GUC value '{val}' for key '{key}' is not allowed; \
                             only 'on' or 'off' are permitted"
                        )));
                    }
                }
                parsed.push(variant);
            }
            parsed
        }
    };

    Ok(ProfileRequest { sql, plans })
}

// ── EXPLAIN JSON parsing ──────────────────────────────────────────────────────

/// Walk a single plan node and its `"Plans"` children recursively, accumulating
/// footprint weights per relation and index name.
///
/// Weight for each node = `Shared Hit Blocks + Shared Read Blocks`.
/// When both are absent or their sum is zero, the node contributes a weight of 1
/// (so every touched relation/index appears in the map regardless of EXPLAIN
/// verbosity settings). This matches the defaulting logic in
/// `_aggregate_plan_footprints` in `data_profile_offload.py`.
fn aggregate_plan_footprints(
    node: &Value,
    relations: &mut HashMap<String, i64>,
    indexes: &mut HashMap<String, i64>,
) {
    if let Some(obj) = node.as_object() {
        let hit = obj
            .get("Shared Hit Blocks")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let read = obj
            .get("Shared Read Blocks")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let footprint = hit + read;
        let footprint = if footprint <= 0 { 1 } else { footprint };

        if let Some(relation_name) = obj.get("Relation Name").and_then(|v| v.as_str()) {
            if !relation_name.is_empty() {
                *relations.entry(relation_name.to_string()).or_insert(0) += footprint;
            }
        }
        if let Some(index_name) = obj.get("Index Name").and_then(|v| v.as_str()) {
            if !index_name.is_empty() {
                *indexes.entry(index_name.to_string()).or_insert(0) += footprint;
            }
        }

        if let Some(children) = obj.get("Plans").and_then(|v| v.as_array()) {
            for child in children {
                aggregate_plan_footprints(child, relations, indexes);
            }
        }
    }
}

/// Extract `{raw_cost, estimated_rows, footprints}` from an EXPLAIN JSON value.
///
/// `explain_json` is expected to be the array returned by
/// `EXPLAIN (FORMAT JSON, VERBOSE) <sql>` — i.e. `[{"Plan": {...}, ...}]`.
///
/// Returns `None` if the structure is not a non-empty array with a `"Plan"` object.
pub fn parse_explain_json(explain_json: &Value) -> Option<(f64, i64, HashMap<String, i64>)> {
    let arr = explain_json.as_array()?;
    let top = arr.first()?.as_object()?;
    let plan = top.get("Plan")?.as_object()?;

    let raw_cost = plan.get("Total Cost")?.as_f64()?;
    let estimated_rows = plan.get("Plan Rows")?.as_i64().unwrap_or_else(|| {
        plan.get("Plan Rows")
            .and_then(|v| v.as_f64())
            .map(|f| f as i64)
            .unwrap_or(0)
    });

    let mut relations: HashMap<String, i64> = HashMap::new();
    let mut indexes: HashMap<String, i64> = HashMap::new();
    aggregate_plan_footprints(&Value::Object(plan.clone()), &mut relations, &mut indexes);

    // Merge relations and indexes into one footprints map (matching
    // `_extract_plan_footprints` which merges both dicts).
    let mut footprints = relations;
    for (k, v) in indexes {
        *footprints.entry(k).or_insert(0) += v;
    }

    Some((raw_cost, estimated_rows, footprints))
}

// ── SQL helper ────────────────────────────────────────────────────────────────

/// Quote an identifier for safe interpolation into SQL. Mirrors the implementation
/// in `retrieve::replayer`.
fn quote_pg_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ── Per-plan EXPLAIN execution ────────────────────────────────────────────────

/// Run `EXPLAIN (FORMAT JSON, VERBOSE) <sql>` for one plan variant and parse the result.
///
/// Opens an explicit transaction, applies `SET TRANSACTION READ ONLY`,
/// `SET LOCAL statement_timeout`, optional `SET LOCAL ROLE`, and the variant's
/// GUC overrides — then runs plain EXPLAIN (never ANALYZE). The transaction is
/// always rolled back / dropped after the call.
async fn explain_one_variant(
    pool: &deadpool_postgres::Pool,
    sql: &str,
    variant: &PlanVariant,
    stmt_timeout_ms: u32,
    db_role: &str,
) -> ApiResult<PlanCostEstimate> {
    let mut client = pool.get().await?;

    let tx = client
        .transaction()
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("begin transaction: {e}")))?;

    // Build the setup batch: READ ONLY + statement timeout + optional role.
    let mut setup =
        format!("SET TRANSACTION READ ONLY; SET LOCAL statement_timeout = {stmt_timeout_ms}");
    if !db_role.trim().is_empty() {
        setup.push_str(&format!(
            "; SET LOCAL ROLE {}",
            quote_pg_ident(db_role.trim())
        ));
    }
    tx.batch_execute(&setup)
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("configure read txn: {e}")))?;

    // Apply per-variant GUC overrides. Keys and values have already been
    // validated against the allowlist in `parse_profile_request`.
    for (key, val) in &variant.settings {
        // Safety: key matches ^enable_[a-z]+$ (enforced by is_allowed_guc_key);
        // val is exactly "on" or "off" (enforced by is_allowed_guc_value).
        // No string interpolation of user-controlled SQL is possible here.
        let set_stmt = format!("SET LOCAL {key} = {val}");
        tx.execute(&set_stmt, &[])
            .await
            .map_err(|e| ApiError::BadRequest(format!("SET LOCAL {key} = {val} failed: {e}")))?;
    }

    // Strip trailing semicolons (EXPLAIN requires a bare statement).
    let cleaned = sql.trim_end().trim_end_matches(';').trim_end();
    let explain_stmt = format!("EXPLAIN (FORMAT JSON, VERBOSE) {cleaned}");

    let rows = tx
        .query(&explain_stmt, &[])
        .await
        .map_err(|e| ApiError::BadRequest(format!("EXPLAIN failed: {e}")))?;

    // Transaction is read-only; implicit rollback on drop is correct.
    drop(tx);

    let raw: Value = rows
        .first()
        .and_then(|row| row.try_get::<_, Value>(0).ok())
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("EXPLAIN returned no rows")))?;

    let (raw_cost, estimated_rows, footprints) = parse_explain_json(&raw)
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("unexpected EXPLAIN JSON shape")))?;

    // explain_json is the first element of the array (the object with "Plan").
    let explain_json = raw
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or(Value::Null);

    Ok(PlanCostEstimate {
        plan_id: variant.plan_id.clone(),
        raw_cost,
        estimated_rows,
        footprints,
        explain_json,
    })
}

// ── Handler ───────────────────────────────────────────────────────────────────

pub async fn post_profile(
    State(st): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let req = parse_profile_request(&body)?;

    let stmt_timeout_ms = st.settings.retrieval_stmt_timeout_ms;
    let db_role = &st.settings.retrieval_db_role;

    let mut variants: Vec<PlanCostEstimate> = Vec::with_capacity(req.plans.len());
    for variant in &req.plans {
        let estimate =
            explain_one_variant(&st.pool, &req.sql, variant, stmt_timeout_ms, db_role).await?;
        variants.push(estimate);
    }

    let response = ProfileResponse { variants };
    Ok(Json(serde_json::to_value(&response).map_err(|e| {
        ApiError::Internal(anyhow::anyhow!("serializing response: {e}"))
    })?))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── GUC allowlist ─────────────────────────────────────────────────────────

    #[test]
    fn guc_key_allowlist_accepts_known_planner_toggles() {
        for key in ALLOWED_GUC_KEYS {
            assert!(
                is_allowed_guc_key(key),
                "known planner toggle '{key}' must be allowed"
            );
        }
    }

    #[test]
    fn guc_key_allowlist_rejects_non_enable_keys() {
        assert!(!is_allowed_guc_key("work_mem"));
        assert!(!is_allowed_guc_key("max_parallel_workers"));
        assert!(!is_allowed_guc_key("statement_timeout"));
        assert!(!is_allowed_guc_key("role"));
        assert!(!is_allowed_guc_key("search_path"));
        assert!(!is_allowed_guc_key(""));
    }

    #[test]
    fn guc_key_allowlist_rejects_unknown_enable_keys() {
        // Structurally matches ^enable_[a-z]+$ but not in the explicit allowlist.
        assert!(!is_allowed_guc_key("enable_unknownfeature"));
        assert!(!is_allowed_guc_key("enable_parallel"));
    }

    #[test]
    fn guc_key_allowlist_rejects_enable_with_non_lowercase() {
        assert!(!is_allowed_guc_key("enable_SeqScan"));
        assert!(!is_allowed_guc_key("ENABLE_seqscan"));
        assert!(!is_allowed_guc_key("enable_seq_scan")); // underscore in suffix
    }

    #[test]
    fn guc_value_allowlist_accepts_on_off() {
        assert!(is_allowed_guc_value("on"));
        assert!(is_allowed_guc_value("off"));
    }

    #[test]
    fn guc_value_allowlist_rejects_other_values() {
        assert!(!is_allowed_guc_value("true"));
        assert!(!is_allowed_guc_value("false"));
        assert!(!is_allowed_guc_value("1"));
        assert!(!is_allowed_guc_value("0"));
        assert!(!is_allowed_guc_value("ON"));
        assert!(!is_allowed_guc_value("OFF"));
        assert!(!is_allowed_guc_value("'));DROP TABLE t;--"));
        assert!(!is_allowed_guc_value(""));
    }

    // ── Request parsing ───────────────────────────────────────────────────────

    #[test]
    fn parse_request_missing_sql_returns_error() {
        let body = json!({"plans": []});
        let err = parse_profile_request(&body).unwrap_err();
        assert!(
            err.to_string().contains("'sql' is required"),
            "error must mention sql required; got: {err}"
        );
    }

    #[test]
    fn parse_request_non_string_sql_returns_error() {
        let body = json!({"sql": 42});
        let err = parse_profile_request(&body).unwrap_err();
        assert!(
            err.to_string().contains("must be a string"),
            "error must mention string; got: {err}"
        );
    }

    #[test]
    fn parse_request_dml_sql_returns_error() {
        let body = json!({"sql": "DELETE FROM foo"});
        let err = parse_profile_request(&body).unwrap_err();
        assert!(
            err.to_string().contains("SQL rejected"),
            "DML must be rejected; got: {err}"
        );
    }

    #[test]
    fn parse_request_no_plans_field_defaults_to_single_default_plan() {
        let body = json!({"sql": "SELECT 1"});
        let req = parse_profile_request(&body).unwrap();
        assert_eq!(req.plans.len(), 1);
        assert_eq!(req.plans[0].plan_id, "default");
        assert!(req.plans[0].settings.is_empty());
    }

    #[test]
    fn parse_request_explicit_plans_parsed() {
        let body = json!({
            "sql": "SELECT 1",
            "plans": [
                {"plan_id": "default", "settings": {}},
                {"plan_id": "prefer_index", "settings": {"enable_seqscan": "off"}}
            ]
        });
        let req = parse_profile_request(&body).unwrap();
        assert_eq!(req.plans.len(), 2);
        assert_eq!(req.plans[1].plan_id, "prefer_index");
        assert_eq!(
            req.plans[1]
                .settings
                .get("enable_seqscan")
                .map(String::as_str),
            Some("off")
        );
    }

    #[test]
    fn parse_request_disallowed_guc_key_returns_error() {
        let body = json!({
            "sql": "SELECT 1",
            "plans": [
                {"plan_id": "p", "settings": {"work_mem": "off"}}
            ]
        });
        let err = parse_profile_request(&body).unwrap_err();
        assert!(
            err.to_string()
                .contains("GUC key 'work_mem' is not allowed"),
            "must report disallowed key; got: {err}"
        );
    }

    #[test]
    fn parse_request_disallowed_guc_value_returns_error() {
        let body = json!({
            "sql": "SELECT 1",
            "plans": [
                {"plan_id": "p", "settings": {"enable_seqscan": "true"}}
            ]
        });
        let err = parse_profile_request(&body).unwrap_err();
        assert!(
            err.to_string()
                .contains("GUC value 'true' for key 'enable_seqscan' is not allowed"),
            "must report disallowed value; got: {err}"
        );
    }

    #[test]
    fn parse_request_empty_plan_id_returns_error() {
        let body = json!({
            "sql": "SELECT 1",
            "plans": [{"plan_id": "  ", "settings": {}}]
        });
        let err = parse_profile_request(&body).unwrap_err();
        assert!(
            err.to_string().contains("plan_id must not be empty"),
            "must report empty plan_id; got: {err}"
        );
    }

    #[test]
    fn parse_request_all_four_lumilake_variants_accepted() {
        // Verify all four variants from _local_data_profile_variants() pass validation.
        let body = json!({
            "sql": "SELECT 1",
            "plans": [
                {"plan_id": "default", "settings": {}},
                {"plan_id": "prefer_index", "settings": {"enable_seqscan": "off"}},
                {"plan_id": "prefer_seq", "settings": {"enable_indexscan": "off", "enable_bitmapscan": "off"}},
                {"plan_id": "prefer_nestloop", "settings": {"enable_hashjoin": "off", "enable_mergejoin": "off"}}
            ]
        });
        let req = parse_profile_request(&body).unwrap();
        assert_eq!(req.plans.len(), 4);
    }

    // ── EXPLAIN JSON parsing ──────────────────────────────────────────────────

    /// A realistic EXPLAIN (FORMAT JSON, VERBOSE) output for a simple seq scan.
    fn sample_explain_seqscan() -> Value {
        json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "orders",
                "Schema": "public",
                "Alias": "orders",
                "Startup Cost": 0.0,
                "Total Cost": 1450.0,
                "Plan Rows": 50000,
                "Plan Width": 64,
                "Shared Hit Blocks": 300,
                "Shared Read Blocks": 150
            }
        }])
    }

    /// A nested EXPLAIN with an index scan child.
    fn sample_explain_nested() -> Value {
        json!([{
            "Plan": {
                "Node Type": "Hash Join",
                "Startup Cost": 10.0,
                "Total Cost": 2500.75,
                "Plan Rows": 1000,
                "Plan Width": 100,
                "Shared Hit Blocks": 0,
                "Shared Read Blocks": 0,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "orders",
                        "Schema": "public",
                        "Alias": "o",
                        "Startup Cost": 0.0,
                        "Total Cost": 900.0,
                        "Plan Rows": 5000,
                        "Plan Width": 60,
                        "Shared Hit Blocks": 200,
                        "Shared Read Blocks": 100
                    },
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "customers",
                        "Index Name": "customers_pkey",
                        "Schema": "public",
                        "Alias": "c",
                        "Startup Cost": 0.0,
                        "Total Cost": 500.0,
                        "Plan Rows": 1000,
                        "Plan Width": 40,
                        "Shared Hit Blocks": 50,
                        "Shared Read Blocks": 10
                    }
                ]
            }
        }])
    }

    #[test]
    fn parse_explain_json_extracts_cost_and_rows() {
        let explain = sample_explain_seqscan();
        let (raw_cost, estimated_rows, _footprints) = parse_explain_json(&explain).unwrap();
        assert_eq!(raw_cost, 1450.0, "Total Cost must match");
        assert_eq!(estimated_rows, 50000, "Plan Rows must match");
    }

    #[test]
    fn parse_explain_json_extracts_relation_footprint() {
        let explain = sample_explain_seqscan();
        let (_cost, _rows, footprints) = parse_explain_json(&explain).unwrap();
        // Shared Hit Blocks (300) + Shared Read Blocks (150) = 450
        assert_eq!(
            footprints.get("orders").copied(),
            Some(450),
            "orders footprint must be Shared Hit + Shared Read blocks"
        );
    }

    #[test]
    fn parse_explain_json_defaults_to_one_when_no_block_info() {
        // A plan node with no block stats should contribute footprint=1.
        let explain = json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "small_table",
                "Total Cost": 5.0,
                "Plan Rows": 10
            }
        }]);
        let (_cost, _rows, footprints) = parse_explain_json(&explain).unwrap();
        assert_eq!(
            footprints.get("small_table").copied(),
            Some(1),
            "footprint must default to 1 when block stats absent"
        );
    }

    #[test]
    fn parse_explain_json_accumulates_nested_footprints() {
        let explain = sample_explain_nested();
        let (raw_cost, estimated_rows, footprints) = parse_explain_json(&explain).unwrap();
        assert_eq!(raw_cost, 2500.75, "Total Cost from top node");
        assert_eq!(estimated_rows, 1000, "Plan Rows from top node");

        // Top Hash Join node: Shared Hit=0, Shared Read=0 → footprint=1,
        // but it has no Relation Name, so nothing accumulated there.
        // orders child: 200+100=300
        assert_eq!(
            footprints.get("orders").copied(),
            Some(300),
            "orders footprint from child seq scan"
        );
        // customers child: 50+10=60 for the relation
        assert_eq!(
            footprints.get("customers").copied(),
            Some(60),
            "customers footprint from index scan"
        );
        // customers_pkey index: 50+10=60
        assert_eq!(
            footprints.get("customers_pkey").copied(),
            Some(60),
            "customers_pkey index footprint"
        );
    }

    #[test]
    fn parse_explain_json_merges_relation_and_index_footprints() {
        // Same relation accessed via both a seq scan and an index scan — footprints merge.
        let explain = json!([{
            "Plan": {
                "Node Type": "Append",
                "Total Cost": 100.0,
                "Plan Rows": 200,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "events",
                        "Total Cost": 50.0,
                        "Plan Rows": 100,
                        "Shared Hit Blocks": 20,
                        "Shared Read Blocks": 5
                    },
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "events",
                        "Index Name": "events_ts_idx",
                        "Total Cost": 50.0,
                        "Plan Rows": 100,
                        "Shared Hit Blocks": 10,
                        "Shared Read Blocks": 3
                    }
                ]
            }
        }]);
        let (_cost, _rows, footprints) = parse_explain_json(&explain).unwrap();
        // events from seq scan: 20+5=25; events from index scan: 10+3=13 → total 38
        assert_eq!(
            footprints.get("events").copied(),
            Some(38),
            "relation footprints across plan nodes must be summed"
        );
        // events_ts_idx index: 10+3=13
        assert_eq!(
            footprints.get("events_ts_idx").copied(),
            Some(13),
            "index footprint must be present"
        );
    }

    #[test]
    fn parse_explain_json_returns_none_for_empty_array() {
        assert!(parse_explain_json(&json!([])).is_none());
    }

    #[test]
    fn parse_explain_json_returns_none_for_non_array() {
        assert!(parse_explain_json(&json!({"Plan": {}})).is_none());
    }

    #[test]
    fn parse_explain_json_returns_none_when_plan_missing() {
        assert!(parse_explain_json(&json!([{"not_plan": {}}])).is_none());
    }

    #[test]
    fn parse_explain_json_returns_none_when_total_cost_missing() {
        assert!(parse_explain_json(&json!([{"Plan": {"Plan Rows": 100}}])).is_none());
    }
}
