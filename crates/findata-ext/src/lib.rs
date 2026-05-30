//! findata-ext — the financial application layer on top of the portable
//! platform (`findata` lib). Owns the bespoke read routes that can't be
//! declarative `financial.toml` specs, and the provider upstream workers.
//!
//! Owns the financial handler/query/upstream module bodies (under
//! `handlers`/`queries`/`upstream`) — they depend on the platform via
//! `findata::…`. The platform never depends on this crate and contains no
//! financial provider/table names; the financial layer is this crate +
//! `financial.toml`.

pub mod handlers;
pub mod queries;
pub mod upstream;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use deadpool_postgres::Pool;
use futures_util::future::BoxFuture;

use findata::config::Settings;
use findata::realtime::hub::Hub;
use findata::realtime::upstream::UpstreamWorker;
use findata::state::AppState;

/// The financial bespoke routes, merged into the platform's gated router.
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

// ---- Provider upstream workers (impl the platform's UpstreamWorker trait) ----

macro_rules! worker {
    ($ty:ident, $name:literal, |$h:ident, $m:ident, $s:ident, $p:ident| $body:expr) => {
        pub struct $ty;
        impl UpstreamWorker for $ty {
            fn name(&self) -> &'static str {
                $name
            }
            fn start(
                &self,
                $h: Arc<Hub>,
                $m: redis::aio::MultiplexedConnection,
                $s: Arc<Settings>,
                $p: Pool,
            ) -> BoxFuture<'static, anyhow::Result<()>> {
                Box::pin(async move { $body })
            }
        }
    };
}

worker!(FmpWs, "fmp_ws", |hub, mux, settings, _pool| crate::upstream::fmp_ws::start(hub, mux, settings).await);
worker!(FinnhubWs, "finnhub_ws", |hub, mux, settings, _pool| crate::upstream::finnhub_ws::start(hub, mux, settings).await);
worker!(News, "news", |hub, mux, settings, _pool| crate::upstream::news::start(hub, mux, settings).await);
worker!(Kol, "kol", |hub, mux, settings, pool| crate::upstream::kol::start(hub, mux, settings, pool).await);
worker!(Polling, "polling", |hub, mux, settings, _pool| crate::upstream::polling::start(hub, mux, settings).await);

/// Provider set in registration order (FMP → Finnhub → news → kol → polling) —
/// preserves the crypto/forex claim precedence (bite #28).
pub fn workers() -> Vec<Box<dyn UpstreamWorker>> {
    vec![
        Box::new(FmpWs),
        Box::new(FinnhubWs),
        Box::new(News),
        Box::new(Kol),
        Box::new(Polling),
    ]
}
