//! HTTP handlers — platform (generic) only. Financial read handlers moved to
//! the `findata-ext` crate.

pub mod blobs;
pub mod catalog;
pub mod freshness;
pub mod health;
pub mod ingest;
pub mod landing;
pub mod llm;
pub mod pm_stream;
pub mod sse_quotes;
pub mod usage;
pub mod ws;
