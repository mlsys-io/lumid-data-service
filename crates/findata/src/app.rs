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
        // Estimates
        .route("/estimates/:symbol/price-target", get(handlers::estimates::price_target))
        .route("/grades/:symbol", get(handlers::estimates::grades))
        .route("/recommendation/:symbol", get(handlers::estimates::recommendation))
        .route("/analyst-estimates/:symbol", get(handlers::estimates::analyst_estimates))
        // Analysis
        .route("/ratios/:symbol", get(handlers::analysis::ratios))
        .route("/key-metrics/:symbol", get(handlers::analysis::key_metrics))
        .route("/financial-growth/:symbol", get(handlers::analysis::financial_growth))
        .route("/income-statement-growth/:symbol", get(handlers::analysis::income_statement_growth))
        .route("/balance-sheet-growth/:symbol", get(handlers::analysis::balance_sheet_growth))
        .route("/cash-flow-growth/:symbol", get(handlers::analysis::cash_flow_growth))
        // Investors
        .route("/holders/:symbol/top", get(handlers::investors::holders_top))
        .route("/insider/:symbol/transactions", get(handlers::investors::insider_transactions))
        .route("/insider/:symbol/sentiment", get(handlers::investors::insider_sentiment))
        .route("/insider/:symbol/statistics", get(handlers::investors::insider_statistics))
        .route("/fund-ownership/:symbol", get(handlers::investors::fund_ownership))
        .route("/funds-disclosure/:symbol", get(handlers::investors::funds_disclosure))
        // Earnings
        .route("/earnings", get(handlers::earnings::earnings_calendar))
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
