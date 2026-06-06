//! HTTP handlers — platform (generic) only. App read handlers live in the
//! `my_ext` crate.

pub mod blobs;
pub mod catalog;
pub mod freshness;
pub mod health;
pub mod ingest;
pub mod landing;
pub mod llm;
pub mod profile;
pub mod retrieve;
pub mod sse_quotes;
pub mod usage;
pub mod ws;
