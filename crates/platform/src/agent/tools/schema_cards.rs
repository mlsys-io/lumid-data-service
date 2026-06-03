//! `get_schema_cards` tool — returns cached schema cards for LLM planning.

use serde_json::{json, Map, Value};

use crate::error::ApiResult;
use crate::retrieve::schema_card::render_bundle_for_prompt;
use crate::state::AppState;

pub struct GetSchemaCardsTool;

impl super::Tool for GetSchemaCardsTool {
    fn name(&self) -> &str {
        "get_schema_cards"
    }

    fn description(&self) -> &str {
        "Return compact schema cards for SQL planning. Call this before composing \
         SQL for a natural-language data request. The result contains table and \
         column names, useful stats, samples, foreign keys, and join hints. Use \
         the exact identifiers shown — do not guess or invent names."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "description": "Optional comma-separated schema names to restrict the card scope, \
                                    e.g. \"market,fundamentals\". Omit or leave empty for all schemas."
                }
            },
            "required": [],
            "additionalProperties": false
        })
    }
}

pub async fn get_schema_cards(st: &AppState, args: &Map<String, Value>) -> ApiResult<Value> {
    let scope_raw = args
        .get("scope")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let schemas: Vec<String> = if scope_raw.is_empty() {
        // When no explicit scope is supplied, fall back to the operator-configured
        // allowlist so tenants see only their permitted schemas.
        st.settings.user_schemas.clone()
    } else {
        scope_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    let store = &st.card_store;
    let cards = store.get_or_build(&schemas).await?;

    let rendered = render_bundle_for_prompt(&cards);

    Ok(json!({
        "table_count": cards.len(),
        "schema_cards": rendered,
    }))
}
