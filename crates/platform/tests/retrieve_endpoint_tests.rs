//! Tests for `handlers::retrieve::parse_request`.
//!
//! All tests run in-memory; no Postgres or object store required.
//! The `parse_request` helper is unit-tested directly so the parsing
//! contract is verified without standing up an `AppState`.

use lumid_platform::handlers::retrieve::parse_request;
use serde_json::json;

/// Unwrap Ok without requiring T: Debug (OutputFormat doesn't derive Debug).
macro_rules! assert_ok {
    ($result:expr, $msg:literal) => {
        match $result {
            Ok(v) => v,
            Err(e) => panic!("{}: error = {e}", $msg),
        }
    };
}

/// Unwrap Err without requiring T: Debug.
macro_rules! assert_err {
    ($result:expr, $msg:literal) => {
        match $result {
            Err(e) => e,
            Ok(_) => panic!("{}: expected Err but got Ok", $msg),
        }
    };
}

// ── sql form ──────────────────────────────────────────────────────────────────

#[test]
fn sql_form_builds_single_op_plan() {
    use lumid_platform::retrieve::plan::RetrievalOp;

    let body = json!({"sql": "SELECT 1"});
    let (plan, _fmt) = assert_ok!(parse_request(&body), "valid sql body");
    assert_eq!(plan.plan.len(), 1, "single-op plan");
    match &plan.plan[0] {
        RetrievalOp::Sql(op) => assert_eq!(op.query, "SELECT 1"),
        other => panic!("expected Sql op, got {other:?}"),
    }
}

#[test]
fn sql_form_default_output_format_is_jsonl() {
    let body = json!({"sql": "SELECT 1"});
    let (_plan, fmt) = assert_ok!(parse_request(&body), "valid sql body");
    // format_name() == "jsonl" is the observable contract.
    assert_eq!(fmt.format_name(), "jsonl");
}

#[test]
fn sql_form_explicit_output_format_csv() {
    let body = json!({"sql": "SELECT 1", "output_format": "csv"});
    let (_plan, fmt) = assert_ok!(parse_request(&body), "valid sql+csv body");
    assert_eq!(fmt.format_name(), "csv");
}

// ── plan form ─────────────────────────────────────────────────────────────────

#[test]
fn plan_form_deserializes_plan_object() {
    use lumid_platform::retrieve::plan::RetrievalOp;

    let body = json!({
        "plan": {
            "plan": [{"op": "sql", "query": "SELECT ticker FROM market.ohlc LIMIT 5"}]
        }
    });
    let (plan, _fmt) = assert_ok!(parse_request(&body), "valid plan body");
    assert_eq!(plan.plan.len(), 1);
    match &plan.plan[0] {
        RetrievalOp::Sql(op) => assert!(op.query.contains("SELECT ticker")),
        other => panic!("expected Sql op, got {other:?}"),
    }
}

#[test]
fn plan_form_default_output_format_is_jsonl() {
    let body = json!({
        "plan": {"plan": [{"op": "sql", "query": "SELECT 1"}]}
    });
    let (_plan, fmt) = assert_ok!(parse_request(&body), "valid plan body");
    assert_eq!(fmt.format_name(), "jsonl");
}

// ── error cases ───────────────────────────────────────────────────────────────

#[test]
fn missing_both_returns_bad_request() {
    let body = json!({"output_format": "jsonl"});
    let err = assert_err!(parse_request(&body), "expected error for missing both");
    assert!(
        err.to_string().contains("provide exactly one"),
        "error must mention 'provide exactly one'; got: {err}"
    );
}

#[test]
fn both_present_returns_bad_request() {
    let body = json!({
        "sql": "SELECT 1",
        "plan": {"plan": [{"op": "sql", "query": "SELECT 1"}]}
    });
    let err = assert_err!(parse_request(&body), "expected error for both-present");
    assert!(
        err.to_string().contains("provide exactly one"),
        "error must mention 'provide exactly one'; got: {err}"
    );
}

#[test]
fn unknown_output_format_returns_bad_request() {
    let body = json!({"sql": "SELECT 1", "output_format": "parquet"});
    let err = assert_err!(parse_request(&body), "expected error for bad format");
    assert!(
        err.to_string().contains("unknown output_format"),
        "error must mention 'unknown output_format'; got: {err}"
    );
}

#[test]
fn invalid_plan_json_returns_bad_request() {
    // plan value is not a valid RetrievalPlan — missing inner "plan" array.
    let body = json!({"plan": {"not_a_plan": true}});
    let err = assert_err!(parse_request(&body), "expected error for invalid plan");
    assert!(
        err.to_string().contains("invalid plan JSON"),
        "error must mention 'invalid plan JSON'; got: {err}"
    );
}
