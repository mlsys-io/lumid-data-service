//! Config-driven read layer (the "config" half of the platform/app split).
//!
//! Endpoints are declared in `financial.toml` (`[[read.endpoint]]` blocks):
//! `{ id, method, path, sql (named `:binds` + allow-listed `{{fragments}}`),
//! params[], tables[], ttl, shape, strip_lineage, row_cap }`. At startup the
//! platform parses them, lowers each to prepared SQL, and mounts them as axum
//! routes behind the same `gate`. Per-request cost is bind + execute, fronted
//! by the multi-tier [`cache`]. Logic-heavy endpoints that can't be a single
//! parameterized SELECT stay compiled in `findata-ext` (see DEFERRED_TO_EXT.md).
//!
//! Implemented so far: the [`cache`] layer. The spec parser / param binder /
//! generic executor land next; until then nothing mounts these routes.

pub mod cache;
