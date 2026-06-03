//! Agent tool-use loop — mounts at `/agent/v1` when `enable_agent` is true.
//!
//! See `agent_loop` for the loop logic and `tools` for the tool implementations.

pub mod agent_loop;
pub mod tools;

use axum::routing::post;
use axum::Router;

use crate::state::AppState;

/// The `/agent/v1` route. Merge into the gated router alongside `/v1/*`.
pub fn routes() -> Router<AppState> {
    Router::new().route("/agent/v1", post(agent_loop::agent_chat))
}
