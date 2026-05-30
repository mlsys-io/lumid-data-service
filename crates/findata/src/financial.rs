//! Financial application routes (the `findata-ext` boundary, in-crate for now).
//!
//! The bespoke read endpoints that can't be expressed as declarative
//! `financial.toml` specs (jsonb pivots, Redis-backed KOL, multi-query
//! orchestration, asset-class rollups, dynamic filters) — registered into the
//! platform router via [`routes`]. On the repo split these handlers + their
//! queries move into a separate `findata-ext` crate that depends on the
//! platform lib; `routes()` becomes its `register()` contribution.

use axum::routing::get;
use axum::Router;

use crate::handlers;
use crate::state::AppState;

/// The financial bespoke routes merged into the platform's gated group.
pub fn routes() -> Router<AppState> {
    Router::new()
        // Symbols + OHLC (3-tier fallback / asset-class rollup)
        .route("/symbols/search", get(handlers::symbols::search))
        .route("/symbols/:symbol", get(handlers::symbols::get_one))
        .route("/universe", get(handlers::symbols::universe))
        .route("/ohlc/:symbol", get(handlers::ohlc::ohlc))
        // Analysis (jsonb pivot_raw)
        .route("/analyst-estimates/:symbol", get(handlers::estimates::analyst_estimates))
        .route("/ratios/:symbol", get(handlers::analysis::ratios))
        .route("/financial-growth/:symbol", get(handlers::analysis::financial_growth))
        .route("/income-statement-growth/:symbol", get(handlers::analysis::income_statement_growth))
        .route("/balance-sheet-growth/:symbol", get(handlers::analysis::balance_sheet_growth))
        .route("/cash-flow-growth/:symbol", get(handlers::analysis::cash_flow_growth))
        // Investors (computed as_of envelope)
        .route("/holders/:symbol/top", get(handlers::investors::holders_top))
        .route("/fund-ownership/:symbol", get(handlers::investors::fund_ownership))
        // ETF holdings (multi-query jsonb envelope)
        .route("/etf/:symbol/holdings", get(handlers::etf::holdings))
        // Reference (flat-array peers / computed is_open)
        .route("/peers/:symbol", get(handlers::reference::peers))
        .route("/exchange-market-hours", get(handlers::reference::exchange_market_hours))
        // Screener (dynamic WHERE)
        .route("/screener", get(handlers::screener::screener))
        // Quotes (Redis last-tick)
        .route("/quotes", get(handlers::quotes::quotes_snapshot))
        .route("/quote-stats/:symbol", get(handlers::quotes::quote_stats))
        .route("/metrics-snapshot/:symbol", get(handlers::quotes::metrics_snapshot))
        // Institutional industries summary
        .route("/institutional/holder/:cik/industries", get(handlers::institutional::holder_industries))
        // XBRL (jsonb shaping)
        .route("/xbrl/:symbol/filings", get(handlers::xbrl::xbrl_index))
        .route("/xbrl/:symbol/filing/:accession", get(handlers::xbrl::xbrl_filing))
        // Prediction markets (dynamic UNION / cagg)
        .route("/prediction-markets/markets/search", get(handlers::prediction_markets::search_markets))
        .route("/prediction-markets/candles/:venue/:market_id", get(handlers::prediction_markets::candles))
        // KOL live tweets (Redis)
        .route("/kols/tweets", get(handlers::kols::recent_tweets))
        .route("/kols/tweets/by-symbol/:symbol", get(handlers::kols::tweets_for_symbol))
        .route("/kols/:handle/tweets", get(handlers::kols::tweets_for_handle))
}
