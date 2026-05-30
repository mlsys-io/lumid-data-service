//! Ingress orchestration layer — ACL, core write orchestration, blob plane,
//! webhook HMAC auth, and the Lumilake handoff. Mirrors `injection/ingest/`.

pub mod acl;
pub mod blob;
pub mod core;
pub mod lumilake;
pub mod webhook;
