//! HTTP handlers — one module per route group, mirroring `api/routes/`.

pub mod analysis;
pub mod corp_actions;
pub mod earnings;
pub mod earnings_history;
pub mod estimates;
pub mod etf;
pub mod events_extra;
pub mod freshness;
pub mod fundamentals;
pub mod health;
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
