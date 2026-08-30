//! Per-target-table record validation — port of `ingest/validation.py`.
//!
//! Rather than build a Pydantic model, we validate each record directly against
//! the cached column metadata (`write::introspect::TableMeta`):
//!   - server-stamped cols {source, source_endpoint, source_run_id, ingest_ts,
//!     id} are forbidden (extra-key reject)
//!   - unknown columns (not in the table) are forbidden (extra-key reject)
//!   - NOT-NULL columns without a default and not server-stamped are required
//!   - `raw` is always optional (never required, even if NOT NULL)
//!   - present values are accepted as-is (the engine's coercer handles pg
//!     typing at COPY time, matching Python where coercion happens in the
//!     write engine, not the validator)
//!
//! Rejects use the 422 shape `{index, error, record}`.

use std::collections::HashSet;

use once_cell::sync::Lazy;
use serde_json::Value;

use crate::write::introspect::TableMeta;

/// Columns the server fills in itself — partners must NOT supply these.
pub static SERVER_STAMPED_COLS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    ["source", "source_endpoint", "source_run_id", "ingest_ts", "id"]
        .into_iter()
        .collect()
});

/// One rejected record, 422-shape.
#[derive(serde::Serialize, Clone)]
pub struct Rejected {
    pub index: usize,
    pub error: String,
    pub record: Value,
}

/// Validate one record. Ok(()) = accepted; Err(msg) = rejected with that error.
fn validate_one(meta: &TableMeta, rec: &Value) -> Result<(), String> {
    let obj = match rec.as_object() {
        Some(o) => o,
        None => return Err("record must be a JSON object".to_string()),
    };
    let known: HashSet<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();

    // Reject server-stamped + unknown keys (extra='forbid').
    for k in obj.keys() {
        if SERVER_STAMPED_COLS.contains(k.as_str()) {
            return Err(format!(
                "field '{k}' is set server-side and must not be supplied"
            ));
        }
        if !known.contains(k.as_str()) {
            return Err(format!("unknown field '{k}'"));
        }
    }

    // Required = NOT NULL, no default, not server-stamped, not `raw`.
    for c in &meta.columns {
        if SERVER_STAMPED_COLS.contains(c.name.as_str()) {
            continue;
        }
        if c.name == "raw" {
            continue; // always optional
        }
        let required = !c.is_nullable && !c.has_default;
        if required {
            match obj.get(&c.name) {
                None => return Err(format!("missing required field '{}'", c.name)),
                Some(Value::Null) => {
                    return Err(format!("field '{}' must not be null", c.name))
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Validate a batch. Returns `(parsed, rejected)` — parsed are the accepted
/// records (clones), rejected carry index/error/record.
pub fn validate_batch(
    meta: &TableMeta,
    records: &[Value],
) -> (Vec<Value>, Vec<Rejected>) {
    let mut parsed = Vec::new();
    let mut rejected = Vec::new();
    for (idx, rec) in records.iter().enumerate() {
        match validate_one(meta, rec) {
            Ok(()) => parsed.push(rec.clone()),
            Err(e) => rejected.push(Rejected {
                index: idx,
                error: e,
                record: rec.clone(),
            }),
        }
    }
    (parsed, rejected)
}

