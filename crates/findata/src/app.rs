//! Router assembly.
//!
//! `/health` + `/health/db` are public. All data routes go through the `gate`
//! middleware (require identity + tiered rate limit). Phase 1 wires the canary
//! read set (symbols, ohlc, fundamentals, news, freshness); later phases append
//! the remaining endpoints + the write plane + the proxy gateway.

use axum::middleware::from_fn_with_state;
use axum::routing::{delete, get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    let public = Router::new()
        .route("/health", get(handlers::health::health))
        .route("/health/db", get(handlers::health::health_db))
        // Public landing surfaces (no auth) — ports of api/landing.py + llm_landing.py.
        .route("/", get(handlers::landing::landing))
        .route("/reference", get(handlers::landing::landing))
        .route("/llm", get(handlers::landing::llm_landing))
        // Webhook ingress: HMAC-authenticated, mounted OUTSIDE the gate.
        .route("/webhook/:webhook_id", post(handlers::ingest::post_webhook))
        // WebSocket realtime: self-authenticating (the WS upgrade can't carry
        // the gate's 401 body), so mounted OUTSIDE the gate.
        .route("/ws/quotes", get(handlers::ws::quotes))
        .route("/ws/news", get(handlers::ws::news));

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
        // News global feeds
        .route("/news/latest", get(handlers::news::latest))
        .route("/news/search", get(handlers::news::search))
        .route("/news/stats", get(handlers::news::stats))
        // Screener
        .route("/screener", get(handlers::screener::screener))
        // Quotes snapshot + stats + metrics
        .route("/quotes", get(handlers::quotes::quotes_snapshot))
        .route("/quote-stats/:symbol", get(handlers::quotes::quote_stats))
        .route("/metrics-snapshot/:symbol", get(handlers::quotes::metrics_snapshot))
        // Technical
        .route("/technical/:symbol", get(handlers::technical::technical))
        .route("/technical/:symbol/latest", get(handlers::technical::technical_latest))
        // Institutional 13-F
        .route("/institutional/:symbol/holders/analytics", get(handlers::institutional::holder_analytics))
        .route("/institutional/holder/:cik/performance", get(handlers::institutional::holder_performance))
        .route("/institutional/holder/:cik/industries", get(handlers::institutional::holder_industries))
        .route("/institutional/holder/:cik/dates", get(handlers::institutional::holder_dates))
        .route("/institutional/industries", get(handlers::institutional::industries_summary))
        // XBRL
        .route("/xbrl/:symbol/filings", get(handlers::xbrl::xbrl_index))
        .route("/xbrl/:symbol/filing/:accession", get(handlers::xbrl::xbrl_filing))
        // Market extras
        .route("/market-movers", get(handlers::market_extras::market_movers))
        .route("/dividends-calendar", get(handlers::market_extras::dividends_calendar))
        .route("/splits-calendar", get(handlers::market_extras::splits_calendar))
        .route("/sectors/pe", get(handlers::market_extras::sectors_pe))
        .route("/sectors/performance", get(handlers::market_extras::sectors_perf))
        .route("/industries/pe", get(handlers::market_extras::industries_pe))
        .route("/industries/performance", get(handlers::market_extras::industries_perf))
        .route("/exec-comp-benchmark/:industry", get(handlers::market_extras::exec_comp))
        .route("/universe/actively-trading", get(handlers::market_extras::universe_active))
        .route("/index/:index_symbol/constituents", get(handlers::market_extras::index_constituents))
        // Prediction markets
        .route("/prediction-markets/markets/search", get(handlers::prediction_markets::search_markets))
        .route("/prediction-markets/markets/polymarket/:condition_id", get(handlers::prediction_markets::get_polymarket_market))
        .route("/prediction-markets/markets/kalshi/:ticker", get(handlers::prediction_markets::get_kalshi_market))
        .route("/prediction-markets/trades/polymarket/:condition_id", get(handlers::prediction_markets::polymarket_trades))
        .route("/prediction-markets/trades/kalshi/:ticker", get(handlers::prediction_markets::kalshi_trades))
        .route("/prediction-markets/orderbook/polymarket/:asset_id", get(handlers::prediction_markets::polymarket_orderbook))
        .route("/prediction-markets/orderbook/kalshi/:ticker", get(handlers::prediction_markets::kalshi_orderbook))
        .route("/prediction-markets/candles/:venue/:market_id", get(handlers::prediction_markets::candles))
        .route("/prediction-markets/open-interest/:venue/:market_id", get(handlers::prediction_markets::open_interest))
        .route("/prediction-markets/top-holders/:venue/:market_id", get(handlers::prediction_markets::top_holders))
        .route("/prediction-markets/wallet/:address", get(handlers::prediction_markets::wallet_profile))
        .route("/prediction-markets/wallet/:address/pnl", get(handlers::prediction_markets::wallet_pnl))
        .route("/prediction-markets/wallet/:address/positions", get(handlers::prediction_markets::wallet_positions))
        .route("/prediction-markets/wallet/:address/activity", get(handlers::prediction_markets::wallet_activity))
        .route("/prediction-markets/leaderboard", get(handlers::prediction_markets::leaderboard))
        .route("/prediction-markets/matched-pairs/:venue/:venue_id", get(handlers::prediction_markets::matched_pairs))
        .route("/prediction-markets/events", get(handlers::prediction_markets::polymarket_events))
        // KOL
        .route("/kols", get(handlers::kols::list_kols))
        .route("/kols/tweets", get(handlers::kols::recent_tweets))
        .route("/kols/tweets/search", get(handlers::kols::search_archive))
        .route("/kols/tweets/by-symbol/:symbol", get(handlers::kols::tweets_for_symbol))
        .route("/kols/tweets/by-symbol/:symbol/history", get(handlers::kols::history_for_symbol))
        .route("/kols/archive/stats", get(handlers::kols::archive_stats))
        .route("/kols/:handle/tweets", get(handlers::kols::tweets_for_handle))
        .route("/kols/:handle/tweets/history", get(handlers::kols::history_for_handle))

        // Catalog read plane (provenance-exposing) — port of api/routes/catalog.py.
        .route("/catalog/schemas", get(handlers::catalog::get_schemas))
        .route("/catalog/schemas/:schema/tables", get(handlers::catalog::get_schema_tables))
        .route("/catalog/tables/:schema/:table", get(handlers::catalog::get_table_profile))
        .route("/catalog/ingress/writable", get(handlers::catalog::get_writable))
        .route("/catalog/lineage/run/:run_id", get(handlers::catalog::get_lineage_run))
        .route("/catalog/lineage/runs", get(handlers::catalog::get_lineage_runs))
        .route("/catalog/lineage/row", get(handlers::catalog::get_lineage_row))
        .route("/catalog/sources", get(handlers::catalog::get_sources))
        .route("/catalog/submitters", get(handlers::catalog::get_submitters))
        .route("/catalog/tables/:schema/:table/schema.json", get(handlers::ingest::get_table_schema_json))

        // Ingress write plane — port of injection/routes/ingest_*.py.
        .route("/ingest/:schema/:table", post(handlers::ingest::post_typed))
        .route("/ingest/:schema/:table/stream", post(handlers::ingest::post_stream))
        .route("/ingest/:schema/:table/file", post(handlers::ingest::post_file))
        .route("/ingest/blob", post(handlers::ingest::post_blob))

        // Ingress admin (super_admin / local key) — port of injection/routes/ingest_admin.py.
        .route("/admin/ingress/acl", post(handlers::ingest::grant_acl).delete(handlers::ingest::revoke_acl))
        .route("/admin/ingress/refresh-schemas", post(handlers::ingest::refresh_schemas))
        .route("/admin/ingress/refresh-acl", post(handlers::ingest::refresh_acl))

        // API usage dashboard — port of api/metrics.py::render_usage.
        .route("/usage", get(handlers::usage::usage))

        // LLM reverse proxy (OpenAI + Anthropic compatible) — port of api/routes/llm.py.
        .route("/v1/models", get(handlers::llm::list_models))
        .route("/v1/chat/completions", post(handlers::llm::chat_completions))
        .route("/v1/completions", post(handlers::llm::completions))
        .route("/v1/embeddings", post(handlers::llm::embeddings))
        .route("/v1/messages", post(handlers::llm::messages))
        .route("/v1/messages/count_tokens", post(handlers::llm::count_tokens))

        // Government trades — port of api/routes/gov_trades.py.
        .route("/gov-trades/:symbol", get(handlers::gov_trades::for_symbol))

        // Blob serving (read side) — port of api/routes/blobs.py.
        .route("/blobs/*key", get(handlers::blobs::serve_blob))
        .route("/storage/v1/object/findata/*path", get(handlers::blobs::legacy_storage_alias))

        // KOL media — port of api/routes/kol_media.py.
        .route("/kols/media", get(handlers::kol_media::info))
        .route("/kols/media/by-url", get(handlers::kol_media::by_url))
        .route("/kols/media/*rel", get(handlers::kol_media::serve))

        // KOL roster admin (super_admin / local) — port of api/routes/admin_kols.py.
        .route("/admin/kols", post(handlers::admin_kols::add_kol))
        .route("/admin/kols/:handle", delete(handlers::admin_kols::remove_kol))

        // Realtime SSE (normal GET → gated through the auth middleware).
        .route("/quotes/stream", get(handlers::sse_quotes::quotes_stream))
        .route("/prediction-markets/stream", get(handlers::pm_stream::stream))

        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
