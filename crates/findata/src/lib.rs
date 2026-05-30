//! lumid-data-service — the portable data platform library.
//!
//! Exposes the generic read/write/realtime data service as a library so a thin
//! binary (and, after the repo split, a separate `findata-ext` crate) can build
//! on it. The crate is domain-agnostic: the financial specifics live in
//! `financial.toml` (declarative reads) + the bespoke handlers/upstreams that
//! register into it (the `findata-ext` boundary, established incrementally).

pub mod app;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod handlers;
pub mod ingest;
pub mod parsers;
pub mod queries;
pub mod read;
pub mod realtime;
pub mod state;
pub mod validation;
pub mod write;
