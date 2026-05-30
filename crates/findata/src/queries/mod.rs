//! Query layer — one module per domain, mirroring `api/queries/`. Each fn runs
//! parameterized SQL and returns JSON objects via `db::rows`.

pub mod symbols;
