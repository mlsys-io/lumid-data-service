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
//! Rejects use the 422 shape `{index, error, record}`. `schema_json_for`
//! returns a draft-2020-12 JSON Schema for the table's writable input model.

use std::collections::HashSet;
use std::sync::Arc;

use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};

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

/// Map an `information_schema` data_type → JSON Schema type fragment.
fn json_schema_type(data_type: &str, udt_name: &str) -> Value {
    let t = data_type.to_lowercase();
    if t == "array" {
        return json!({"type": "array", "items": json_schema_type_for_udt(udt_name)});
    }
    match t.as_str() {
        "text" | "character varying" | "varchar" | "character" | "char" | "uuid"
        | "tsvector" => json!({"type": "string"}),
        "boolean" | "bool" => json!({"type": "boolean"}),
        "bigint" | "integer" | "smallint" | "int8" | "int4" | "int2" => {
            json!({"type": "integer"})
        }
        "numeric" | "decimal" | "real" | "double precision" | "float8" | "float4" => {
            json!({"type": "number"})
        }
        "date" => json!({"type": "string", "format": "date"}),
        "time without time zone" | "time with time zone" => {
            json!({"type": "string", "format": "time"})
        }
        "timestamp without time zone" | "timestamp with time zone" => {
            json!({"type": "string", "format": "date-time"})
        }
        "json" | "jsonb" => json!({"type": ["object", "array", "string", "number", "boolean", "null"]}),
        "bytea" => json!({"type": "string", "contentEncoding": "base64"}),
        _ => json!({"type": "string"}),
    }
}

fn json_schema_type_for_udt(udt_name: &str) -> Value {
    // udt like "_text", "_int4" → element type.
    match udt_name.trim_start_matches('_') {
        "int2" | "int4" | "int8" => json!({"type": "integer"}),
        "numeric" | "float4" | "float8" => json!({"type": "number"}),
        "bool" => json!({"type": "boolean"}),
        _ => json!({"type": "string"}),
    }
}

/// Build the draft-2020-12 JSON Schema for the writable input model.
pub fn schema_json_for(schema: &str, table: &str, meta: &Arc<TableMeta>) -> Value {
    let mut properties = Map::new();
    let mut required: Vec<Value> = Vec::new();
    for c in &meta.columns {
        if SERVER_STAMPED_COLS.contains(c.name.as_str()) {
            continue;
        }
        let mut frag = json_schema_type(&c.data_type, &c.udt_name);
        // nullable / raw → allow null too.
        if c.is_nullable || c.name == "raw" {
            if let Some(o) = frag.as_object_mut() {
                o.insert("nullable".into(), Value::Bool(true));
            }
        }
        properties.insert(c.name.clone(), frag);
        let is_required = !c.is_nullable && !c.has_default && c.name != "raw";
        if is_required {
            required.push(Value::String(c.name.clone()));
        }
    }
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": format!("Ingest_{schema}_{table}"),
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required,
    })
}
