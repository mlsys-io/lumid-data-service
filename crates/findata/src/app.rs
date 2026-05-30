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

pub fn build_router(state: AppState, read_router: Router<AppState>) -> Router {
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
        .route("/freshness", get(handlers::freshness::freshness))
        // Estimates
        .route("/analyst-estimates/:symbol", get(handlers::estimates::analyst_estimates))
        // Analysis
        .route("/ratios/:symbol", get(handlers::analysis::ratios))
        .route("/financial-growth/:symbol", get(handlers::analysis::financial_growth))
        .route("/income-statement-growth/:symbol", get(handlers::analysis::income_statement_growth))
        .route("/balance-sheet-growth/:symbol", get(handlers::analysis::balance_sheet_growth))
        .route("/cash-flow-growth/:symbol", get(handlers::analysis::cash_flow_growth))
        // Investors
        // Earnings
        // Corp actions
        // Valuation
        // ETF
        .route("/etf/:symbol/holdings", get(handlers::etf::holdings))
        // Investors (acquisitions)
        // Regulatory + ESG + filings
        // Reference depth + misc
        .route("/peers/:symbol", get(handlers::reference::peers))
        .route("/exchange-market-hours", get(handlers::reference::exchange_market_hours))
        // Macro
        // Events extras
        // Transcripts
        // News meta
        // News global feeds
        // Screener
        .route("/screener", get(handlers::screener::screener))
        // Quotes snapshot + stats + metrics
        .route("/quotes", get(handlers::quotes::quotes_snapshot))
        .route("/quote-stats/:symbol", get(handlers::quotes::quote_stats))
        .route("/metrics-snapshot/:symbol", get(handlers::quotes::metrics_snapshot))
        // Technical
        // Institutional 13-F
        .route("/institutional/holder/:cik/industries", get(handlers::institutional::holder_industries))
        // XBRL
        .route("/xbrl/:symbol/filings", get(handlers::xbrl::xbrl_index))
        .route("/xbrl/:symbol/filing/:accession", get(handlers::xbrl::xbrl_filing))
        // Market extras
        // Prediction markets
        .route("/prediction-markets/markets/search", get(handlers::prediction_markets::search_markets))
        .route("/prediction-markets/candles/:venue/:market_id", get(handlers::prediction_markets::candles))
        // KOL
        .route("/kols/tweets", get(handlers::kols::recent_tweets))
        .route("/kols/tweets/by-symbol/:symbol", get(handlers::kols::tweets_for_symbol))
        .route("/kols/:handle/tweets", get(handlers::kols::tweets_for_handle))

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

        .merge(read_router)
        .layer(from_fn_with_state(state.clone(), crate::auth::gate));

    public
        .merge(gated)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
