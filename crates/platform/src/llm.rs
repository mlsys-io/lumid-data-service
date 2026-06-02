//! LLM reverse-proxy **plugin**. Optional: an app opts in by merging `routes()`
//! into its router (e.g. `my_ext::routes().merge(lumid_platform::llm::routes())`).
//! Apps that don't serve an LLM (e.g. mint) simply omit it — no `/v1/*` surface.
//!
//! OpenAI- + Anthropic-compatible; model-routed across the primary
//! `LUMID_LLM_BACKEND_URL` + any `LUMID_LLM_BACKENDS` (`model=url;…`) backends
//! (handlers return 503 when none is set). Gated like every data route once
//! merged into the app's gated group.

use axum::routing::{get, post};
use axum::Router;

use crate::handlers;
use crate::state::AppState;

/// The `/v1/*` proxy surface. Merge into an app's router to enable LLM serving.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/models", get(handlers::llm::list_models))
        .route("/v1/chat/completions", post(handlers::llm::chat_completions))
        .route("/v1/completions", post(handlers::llm::completions))
        .route("/v1/embeddings", post(handlers::llm::embeddings))
        .route("/v1/messages", post(handlers::llm::messages))
        .route("/v1/messages/count_tokens", post(handlers::llm::count_tokens))
}
