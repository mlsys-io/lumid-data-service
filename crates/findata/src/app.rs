//! Router assembly. Phase 0 wires health + one real read (symbols/search) to
//! prove the query → row-convert → lineage-strip → JSON path end-to-end.

use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health::health))
        .route("/health/db", get(handlers::health::health_db))
        .route("/symbols/search", get(handlers::symbols::search))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
