//! Query layer — platform (generic) only. Financial query modules moved to the
//! `my_ext` crate; the config-driven read engine uses `db::rows` directly.

pub mod blobs;
pub mod catalog;
pub mod freshness;
