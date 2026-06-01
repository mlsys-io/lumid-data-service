//! Query layer — platform (generic) only. App query modules live in the
//! `my_ext` crate; the config-driven read engine uses `db::rows` directly.

pub mod blobs;
pub mod catalog;
pub mod freshness;
