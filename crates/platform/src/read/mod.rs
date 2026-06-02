//! Config-driven read layer (the "config" half of the platform/app split).
//!
//! Endpoints are declared in the read-config TOML (`[[read.endpoint]]` blocks):
//! `{ id, method, path, sql (named `:binds` + allow-listed `{{fragments}}`),
//! params[], tables[], ttl, shape, strip_lineage, row_cap }`. At startup the
//! platform parses them, lowers each to prepared SQL, and mounts them as axum
//! routes behind the same `gate`. Per-request cost is bind + execute, fronted
//! by the multi-tier [`cache`]. Logic-heavy endpoints that can't be a single
//! parameterized SELECT stay compiled in `my_ext` (see DEFERRED_TO_EXT.md).
//!
pub mod bind;
pub mod cache;
pub mod dialect;
pub mod exec;
pub mod ir;
pub mod parse;
pub mod spec;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use spec::EndpointSpec;

/// Load + validate the read-config TOML into Arc'd specs.
pub fn load_specs(path: &str) -> Result<Vec<Arc<EndpointSpec>>, crate::error::ApiError> {
    Ok(spec::load(path)?.into_iter().map(Arc::new).collect())
}

/// Build the `"schema.table" → {endpoint_id}` reverse index for cache
/// invalidation, from each spec's declared `tables`.
pub fn build_reverse(specs: &[Arc<EndpointSpec>]) -> HashMap<String, HashSet<Arc<str>>> {
    let mut rev: HashMap<String, HashSet<Arc<str>>> = HashMap::new();
    for s in specs {
        let id: Arc<str> = Arc::from(s.id.as_str());
        for t in &s.tables {
            rev.entry(t.clone()).or_default().insert(id.clone());
        }
    }
    rev
}
