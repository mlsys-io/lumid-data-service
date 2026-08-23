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

/// OpenAPI paths for the `/v1/*` surface, contributed by the PLATFORM whenever
/// the LLM plane is enabled.
///
/// `openapi.rs` generates its document from the declarative read specs and says
/// so in its module doc: "`/v1/*` aren't enumerated here (they're compiled, not
/// declarative)". The consequence was that `lum.id/llm` — a product whose
/// entire surface is `/v1/*` — served an `/openapi.json` listing 18 paths, none
/// of them LLM, and no `/v1/chat/completions` anywhere in it. `ServeParts`
/// already had an `openapi_paths` escape hatch, but leaving it to each app
/// means every LLM deployment has to remember; the routes are the platform's,
/// so the documentation is too.
///
/// Kept deliberately next to `routes()` above: if you add a route there and not
/// here, `openapi_routes_and_paths_agree` fails.
pub fn openapi_paths() -> serde_json::Value {
    use serde_json::json;
    let bearer = json!([{ "bearerAuth": [] }]);
    let chat_body = json!({
        "required": true,
        "content": { "application/json": { "schema": {
            "type": "object",
            "required": ["model", "messages"],
            "properties": {
                "model": { "type": "string",
                    "description": "Must be an id GET /v1/models advertises. An id in neither \
                                    LUMID_LLM_BACKENDS nor LUMID_LLM_OPENROUTER_MODEL_MAP is \
                                    refused with 503 — never forwarded to a metered upstream." },
                "messages": { "type": "array", "items": { "type": "object" } },
                "stream": { "type": "boolean",
                    "description": "SSE. Keepalive comments are emitted every 15s and MUST be \
                                    relayed by anything proxying this, or a long prefill looks idle." },
                "max_tokens": { "type": "integer" }
            }
        } } }
    });
    let unavailable = json!({
        "description": "No backend can serve this model: unknown/unconfigured id, a refused \
                        claude-* id, or the pool is down."
    });
    json!({
        "/v1/models": { "get": {
            "summary": "List routable models",
            "description": "Every model this gateway will actually serve — the local pool plus the \
                            configured OpenRouter roster, deduped and labelled with the id you must \
                            send in `model`. Deliberately NOT the upstream provider's full catalog.",
            "tags": ["llm"], "security": bearer,
            "responses": { "200": { "description": "OpenAI-shaped model list" },
                           "503": { "description": "No backend configured" } }
        } },
        "/v1/chat/completions": { "post": {
            "summary": "Chat completion (OpenAI-compatible)",
            "tags": ["llm"], "security": bearer, "requestBody": chat_body,
            "responses": { "200": { "description": "Completion, or an SSE stream when `stream` is true" },
                           "503": unavailable }
        } },
        "/v1/completions": { "post": {
            "summary": "Legacy text completion (OpenAI-compatible)",
            "tags": ["llm"], "security": bearer,
            "responses": { "200": { "description": "Completion" }, "503": unavailable }
        } },
        "/v1/embeddings": { "post": {
            "summary": "Embeddings (OpenAI-compatible)",
            "description": "Requires an embedding model in the roster; 503 when none is configured.",
            "tags": ["llm"], "security": bearer,
            "responses": { "200": { "description": "Embedding vectors" }, "503": unavailable }
        } },
        "/v1/messages": { "post": {
            "summary": "Anthropic-compatible messages",
            "description": "Same routing as /v1/chat/completions. NOTE: `claude-*` ids are refused \
                            here — those are served by the pooled Anthropic accounts via \
                            claude-proxy (lum.id/claude), never from this gateway.",
            "tags": ["llm"], "security": bearer,
            "responses": { "200": { "description": "Anthropic-shaped message, or an SSE stream" },
                           "503": unavailable }
        } },
        "/v1/messages/count_tokens": { "post": {
            "summary": "Token count for an Anthropic-shaped request",
            "tags": ["llm"], "security": bearer,
            "responses": { "200": { "description": "{ input_tokens }" }, "503": unavailable }
        } }
    })
}

#[cfg(test)]
mod llm_openapi_tests {
    /// The documented paths must match the mounted routes exactly. Drift here is
    /// invisible at runtime -- a route works while being absent from the spec
    /// (which is the state this function was written to fix), or the spec
    /// promises a route that 404s.
    #[test]
    fn openapi_routes_and_paths_agree() {
        // Parsed from routes() above rather than re-listed, so the two cannot be
        // edited apart without this failing.
        let src = include_str!("llm.rs");
        let body = src
            .split("pub fn routes()")
            .nth(1)
            .expect("routes() present")
            .split("\n}")
            .next()
            .expect("routes() body");
        let mut mounted: Vec<String> = body
            .lines()
            .filter_map(|l| l.split(".route(\"").nth(1))
            .filter_map(|l| l.split('"').next())
            .map(str::to_string)
            .collect();
        mounted.sort();
        assert!(!mounted.is_empty(), "failed to parse routes() — fix the test's parser");

        let doc = super::openapi_paths();
        let mut documented: Vec<String> =
            doc.as_object().expect("paths object").keys().cloned().collect();
        documented.sort();

        assert_eq!(
            mounted, documented,
            "mounted /v1 routes and documented openapi paths have drifted"
        );
    }
}
