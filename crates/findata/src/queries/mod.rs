//! Query layer — one module per domain, mirroring `api/queries/`. Each fn runs
//! parameterized SQL and returns JSON objects via `db::rows`.

pub mod analysis;
pub mod earnings;
pub mod estimates;
pub mod freshness;
pub mod fundamentals;
pub mod investors;
pub mod news;
pub mod ohlc;
pub mod symbols;
