//! `replay_retrieval_plan` tool — executes and materializes a `RetrievalPlan`.

use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::retrieve::materialize::OutputFormat;
use crate::retrieve::plan::RetrievalPlan;
use crate::retrieve::replayer;
use crate::state::AppState;

pub struct ReplayRetrievalPlanTool;

impl super::Tool for ReplayRetrievalPlanTool {
    fn name(&self) -> &str {
        "replay_retrieval_plan"
    }

    fn description(&self) -> &str {
        "Execute and materialize a structured retrieval plan. Call get_schema_cards \
         first to learn the schema. The plan must contain only read-only SELECT \
         statements; no INSERT, UPDATE, DELETE, or DDL is allowed. Returns a \
         materialized_uri, rowcount, size_bytes, and access chain. Do NOT include \
         data rows in your final answer — reference the URI instead."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "plan": {
                    "type": "object",
                    "description": "Retrieval plan: {\"plan\": [{\"op\": \"sql\", \"query\": \"SELECT ...\"} | {\"op\": \"storage_get\", \"bucket\": \"...\", \"key\": \"...\"}]}"
                },
                "output_format": {
                    "type": "string",
                    "enum": ["csv", "jsonl", "raw"],
                    "description": "Materialized output format. Default: jsonl."
                }
            },
            "required": ["plan"],
            "additionalProperties": false
        })
    }
}

pub async fn replay_retrieval_plan(st: &AppState, args: &Map<String, Value>) -> ApiResult<Value> {
    let plan_val = args
        .get("plan")
        .cloned()
        .ok_or_else(|| ApiError::BadRequest("replay_retrieval_plan requires 'plan'".into()))?;

    let plan: RetrievalPlan = serde_json::from_value(plan_val)
        .map_err(|e| ApiError::BadRequest(format!("invalid plan JSON: {e}")))?;

    let fmt_str = args
        .get("output_format")
        .and_then(|v| v.as_str())
        .unwrap_or("jsonl");

    let output_format = OutputFormat::from_str(fmt_str)
        .ok_or_else(|| ApiError::BadRequest(format!("unknown output_format: {fmt_str}")))?;

    let run_id = Uuid::new_v4().to_string();
    let result = replayer::replay(&plan, st, &output_format, &run_id).await?;

    Ok(serde_json::to_value(&result)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("serializing result: {e}")))?)
}
