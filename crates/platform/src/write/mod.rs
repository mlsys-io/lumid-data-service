//! The write engine — the COPY-staging + DISTINCT-FROM merge that backs every
//! ingress mode. Port of `injection/writeengine.py`.
//!
//!   - `introspect`: target-table column metadata + natural-key discovery
//!     (cached in moka, cleared by admin refresh-schemas).
//!   - `coerce`: per-pg-type value → CSV cell rendering (tolerant numeric/date
//!     parsing, NULL sentinel).
//!   - `run`: provenance.runs lifecycle (open_run / close_run).
//!   - `engine`: temp-table COPY + INSERT…ON CONFLICT DO UPDATE WHERE-distinct,
//!     all in one transaction.

pub mod coerce;
pub mod engine;
pub mod introspect;
pub mod run;
