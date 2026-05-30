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
        .route("/earnings/:symbol/history", get(handlers::earnings_history::history))
        .route("/earnings-quality/:symbol", get(handlers::earnings_history::quality))
        // Corp actions
        .route("/dividends/:symbol", get(handlers::corp_actions::dividends))
        .route("/splits/:symbol", get(handlers::corp_actions::splits))
        .route("/market-cap/:symbol/history", get(handlers::corp_actions::market_cap_history))
        // Valuation
        .route("/dcf/:symbol", get(handlers::valuation::dcf))
        .route("/enterprise-value/:symbol", get(handlers::valuation::enterprise_value))
        .route("/financial-scores/:symbol", get(handlers::valuation::financial_scores))
        .route("/owner-earnings/:symbol", get(handlers::valuation::owner_earnings))
        // ETF
        .route("/etf/:symbol/info", get(handlers::etf::info))
        .route("/etf/:symbol/holdings", get(handlers::etf::holdings))
        .route("/etf/:symbol/sector-weightings", get(handlers::etf::sector_weightings))
        .route("/etf/:symbol/country-weightings", get(handlers::etf::country_weightings))
        .route("/symbol/:symbol/etf-exposure", get(handlers::etf::symbol_etf_exposure))
        // Investors (acquisitions)
        .route("/acquisitions/:symbol", get(handlers::investors::acquisitions))
        // Regulatory + ESG + filings
        .route("/filings/:symbol", get(handlers::regulatory::filings))
        .route("/esg/:symbol/disclosures", get(handlers::regulatory::esg_disclosures))
        .route("/esg/:symbol/ratings", get(handlers::regulatory::esg_ratings))
        .route("/esg/:symbol/historical", get(handlers::regulatory::esg_historical))
        .route("/lobbying/:symbol", get(handlers::regulatory::lobbying))
        .route("/usa-spending/:symbol", get(handlers::regulatory::usa_spending))
        .route("/uspto-patents/:symbol", get(handlers::regulatory::uspto_patents))
        .route("/visa-applications/:symbol", get(handlers::regulatory::visa_applications))
        // Reference depth + misc
        .route("/executives/:symbol", get(handlers::reference::executives))
        .route("/governance/:symbol/compensation", get(handlers::reference::compensation))
        .route("/peers/:symbol", get(handlers::reference::peers))
        .route("/supply-chain/:symbol", get(handlers::reference::supply_chain))
        .route("/shares-float/:symbol", get(handlers::reference::shares_float))
        .route("/employee-count/:symbol", get(handlers::reference::employee_count))
        .route("/symbol-changes", get(handlers::reference::symbol_changes))
        .route("/exchange/:exchange/holidays", get(handlers::reference::exchange_holidays))
        .route("/exchange-market-hours", get(handlers::reference::exchange_market_hours))
        // Macro
        .route("/macro/treasury-rates", get(handlers::macro_data::treasury_rates))
        .route("/macro/economic-indicators", get(handlers::macro_data::economic_indicators))
        .route("/macro/economic-calendar", get(handlers::macro_data::economic_calendar))
        .route("/macro/cot/:symbol", get(handlers::macro_data::cot))
        // Events extras
        .route("/ipos", get(handlers::events_extra::ipos))
        .route("/mergers-acquisitions", get(handlers::events_extra::mergers_acquisitions))
        .route("/fda-calendar", get(handlers::events_extra::fda_calendar))
        // Transcripts
        .route("/transcripts/:symbol", get(handlers::transcripts::list_transcripts))
        .route("/transcripts/:symbol/:year/:quarter", get(handlers::transcripts::one_transcript))
        // News meta
        .route("/news/social-sentiment/:symbol", get(handlers::news::social_sentiment))
        .route("/news/symbol-sentiment/:symbol", get(handlers::news::symbol_sentiment))
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
