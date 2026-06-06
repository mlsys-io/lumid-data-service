//! Agent tools: `get_schema_cards` and `replay_retrieval_plan`.
//!
//! The old `list_tables`, `describe_table`, and `read_blob` tools are replaced
//! by these two deterministic retrieval tools.

pub mod replay;
pub mod schema_cards;

use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

// ── Tool trait ────────────────────────────────────────────────────────────────

pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters_schema(&self) -> Value;
}

// ── Registry ──────────────────────────────────────────────────────────────────

pub struct ToolRegistry {
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: vec![
                Arc::new(schema_cards::GetSchemaCardsTool),
                Arc::new(replay::ReplayRetrievalPlanTool),
            ],
        }
    }

    pub fn schemas(&self) -> Vec<Value> {
        self.tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name(),
                        "description": t.description(),
                        "parameters": t.parameters_schema(),
                    }
                })
            })
            .collect()
    }

    pub fn find(&self, name: &str) -> Option<&dyn Tool> {
        self.tools
            .iter()
            .find(|t| t.name() == name)
            .map(|t| t.as_ref())
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Dispatch a tool call by name.
pub async fn dispatch(
    st: &AppState,
    name: &str,
    args: &Map<String, Value>,
    _cfg: &AgentConfig,
) -> ApiResult<Value> {
    match name {
        "get_schema_cards" => schema_cards::get_schema_cards(st, args).await,
        "replay_retrieval_plan" => replay::replay_retrieval_plan(st, args).await,
        other => Err(ApiError::NotFound(format!("unknown tool '{other}'"))),
    }
}

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub max_iterations: usize,
}

impl AgentConfig {
    pub fn from_env() -> Self {
        let max_iterations = std::env::var("LUMID_AGENT_MAX_ITERATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        Self { max_iterations }
    }
}
