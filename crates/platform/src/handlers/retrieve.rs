//! `POST /retrieve` — direct (non-LLM) SQL/storage retrieval endpoint.
//!
//! Exposes `retrieve::replayer::replay` directly over HTTP, bypassing the agent
//! loop. Identical safety boundaries apply: SELECT-only parser, READ ONLY
//! transaction, row cap, key sanitization.

use axum::extract::State;
use axum::Json;
use serde_json::Value;
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::retrieve::materialize::OutputFormat;
use crate::retrieve::plan::{RetrievalOp, RetrievalPlan, SqlOp};
use crate::retrieve::replayer;
use crate::state::AppState;

/// Parse the request body `{sql, plan, output_format}` into
/// `(RetrievalPlan, OutputFormat)`.
///
/// Extracted as a pure function so it can be unit-tested without `AppState`.
pub fn parse_request(body: &Value) -> ApiResult<(RetrievalPlan, OutputFormat)> {
    let has_sql = body.get("sql").is_some();
    let has_plan = body.get("plan").is_some();

    if has_sql == has_plan {
        // both present or both absent
        return Err(ApiError::BadRequest(
            "provide exactly one of 'sql' or 'plan'".into(),
        ));
    }

    let plan = if has_sql {
        let query = body["sql"]
            .as_str()
            .ok_or_else(|| ApiError::BadRequest("'sql' must be a string".into()))?
            .to_string();
        RetrievalPlan {
            plan: vec![RetrievalOp::Sql(SqlOp { query })],
            expected_rowcount_or_size: None,
            rationale: None,
        }
    } else {
        let plan_val = body["plan"].clone();
        serde_json::from_value(plan_val)
            .map_err(|e| ApiError::BadRequest(format!("invalid plan JSON: {e}")))?
    };

    let fmt_str = body
        .get("output_format")
        .and_then(|v| v.as_str())
        .unwrap_or("jsonl");

    let output_format = OutputFormat::from_str(fmt_str)
        .ok_or_else(|| ApiError::BadRequest(format!("unknown output_format: {fmt_str}")))?;

    Ok((plan, output_format))
}

pub async fn post_retrieve(
    State(st): State<AppState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let (plan, output_format) = parse_request(&body)?;
    let run_id = Uuid::new_v4().to_string();
    let result = replayer::replay(&plan, &st, &output_format, &run_id).await?;
    Ok(Json(
        serde_json::to_value(&result)
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("serializing result: {e}")))?,
    ))
}
