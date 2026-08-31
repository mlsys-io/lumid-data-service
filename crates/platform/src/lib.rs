//! lumid-data-service — the portable data platform library.
//!
//! Exposes the generic read/write/realtime data service as a library so a thin
//! binary (and, after the repo split, a separate `my_ext` crate) can build
//! on it. The crate is domain-agnostic: the app specifics live in
//! the read-config TOML (declarative reads) + the bespoke handlers/upstreams that
//! register into it (the `my_ext` boundary, established incrementally).

pub mod agent;
pub mod app;
pub mod auth;
pub mod backend;
pub mod boot;
pub mod config;
pub mod db;
pub mod error;
pub mod federation;
pub mod handlers;
pub mod ingest;
pub mod llm;
pub mod llm_pool;
pub mod mcp;
pub mod objstore;
pub mod openapi;
pub mod parsers;
pub mod queries;
pub mod read;
pub mod realtime;
pub mod retrieve;
pub mod state;
pub mod sync;
pub mod validation;
pub mod write;

pub use boot::{check_serve_parts, serve, ServeParts};
