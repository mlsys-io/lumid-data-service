//! Lineage stripping — mirrors `api/db.py:strip_lineage`.
//!
//! Read responses hide the provenance columns; the API picks the canonical
//! source per surface server-side. Catalog/lineage handlers deliberately do
//! NOT strip (they expose provenance), so this is applied per-handler, not
//! globally.

use serde_json::{Map, Value};

pub const HIDDEN_COLUMNS: [&str; 4] = ["source", "source_endpoint", "source_run_id", "raw"];

pub fn strip_lineage(mut row: Map<String, Value>) -> Map<String, Value> {
    for k in HIDDEN_COLUMNS {
        row.remove(k);
    }
    row
}

pub fn strip_lineage_rows(rows: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    rows.into_iter().map(strip_lineage).collect()
}
