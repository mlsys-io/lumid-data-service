//! Router assembly.
//!
//! `/health` + `/health/db` are public. All data routes go through the `gate`
//! middleware (require identity + tiered rate limit). Phase 1 wires the canary
//! read set (symbols, ohlc, fundamentals, news, freshness); later phases append
//! the remaining endpoints + the write plane + the proxy gateway.

use axum::middleware::from_fn_with_state;
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    let public = Router::new()
        .route("/health", get(handlers::health::health))
        .route("/health/db", get(handlers::health::health_db));

    let gated = Router::new()
        .route("/symbols/search", get(handlers::symbols::search))
        .route("/symbols/:symbol", get(handlers::symbols::get_one))
        .route("/universe", get(handlers::symbols::universe))
        .route("/ohlc/:symbol", get(handlers::ohlc::ohlc))
        .route("/fundamentals/:symbol/latest", get(handlers::fundamentals::latest))
        .route("/fundamentals/:symbol/history", get(handlers::fundamentals::history))
        .route("/news/:symbol", get(handlers::news::for_symbol))
        .route("/freshness", get(handlers::freshness::freshness))
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
