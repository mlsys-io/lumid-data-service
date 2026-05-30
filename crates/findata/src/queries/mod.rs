//! Query layer — one module per domain, mirroring `api/queries/`. Each fn runs
//! parameterized SQL and returns JSON objects via `db::rows`.

pub mod analysis;
pub mod corp_actions;
pub mod earnings;
pub mod earnings_history;
pub mod estimates;
pub mod etf;
pub mod events_extra;
pub mod freshness;
pub mod fundamentals;
pub mod institutional;
pub mod investors;
pub mod macro_data;
pub mod market_extras;
pub mod news;
pub mod ohlc;
pub mod quotes;
pub mod reference;
pub mod regulatory;
pub mod screener;
pub mod symbols;
pub mod technical;
pub mod transcripts;
pub mod valuation;
pub mod xbrl;
