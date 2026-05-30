//! Value coercion + CSV-cell serialization for COPY — port of
//! `writeengine.coerce_value` + `_format_value_for_copy`.
//!
//! The Python path coerces a Python value to a target-typed Python value, then
//! `_format_value_for_copy` renders it to a CSV cell string. Here we collapse
//! both into one step: `cell_for(value, data_type) -> Option<String>` where
//! `None` means "emit the NULL sentinel". The NULL sentinel itself
//! (`__FINAI_NULL_b8e3__`) is injected by the engine when it builds the CSV
//! record, matching `COPY ... NULL '<token>'`.

use serde_json::Value;

pub const NULL_TOKEN: &str = "__FINAI_NULL_b8e3__";

const BAD_FLOAT_TOKENS: &[&str] =
    &["", "n/a", "na", "null", "none", "nan", "-", "--", "inf", "-inf"];

/// True for pg types where a string value passes through verbatim (no
/// "none"/"null" → NULL coercion). Mirrors the text-ish allowlist in
/// `_format_value_for_copy`.
fn is_texty(dtype: &str) -> bool {
    let t = dtype.to_lowercase();
    t.starts_with("text")
        || t.starts_with("varchar")
        || t.starts_with("char")
        || t == "character varying"
        || t == "character"
        || t == "tsvector"
        || t == "jsonb"
        || t == "json"
        || t == "uuid"
}

/// JSON number → canonical decimal string (no scientific notation drift).
fn num_to_string(n: &serde_json::Number) -> String {
    n.to_string()
}

/// Parse a tolerant numeric out of a string (strip `$`, `%`, commas), matching
/// `_parse_numeric`. Returns the cleaned numeric string or None.
fn parse_numeric_str(s: &str) -> Option<String> {
    let s = s.trim().replace(',', "");
    if s.is_empty() || BAD_FLOAT_TOKENS.contains(&s.to_lowercase().as_str()) {
        return None;
    }
    let s2 = s.trim_start_matches('$').trim_end_matches('%');
    // Validate it parses as a float (the DB does the exact numeric parse; we
    // only gate obviously-bad tokens — same as Python's Decimal() attempt).
    if s2.parse::<f64>().is_ok() {
        Some(s2.to_string())
    } else {
        None
    }
}

/// Coerce + render one value into a CSV cell. `None` → emit NULL sentinel.
///
/// `data_type` is the unqualified `information_schema` data_type string.
pub fn cell_for(value: &Value, data_type: &str) -> Option<String> {
    let t = data_type.to_lowercase();
    match value {
        Value::Null => None,

        // jsonb / json — serialize objects/arrays; pass strings verbatim.
        _ if t == "jsonb" || t == "json" => match value {
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        },

        Value::Bool(b) => {
            if t == "boolean" || t == "bool" {
                Some(if *b { "true".into() } else { "false".into() })
            } else if t.starts_with("text")
                || t.starts_with("varchar")
                || t.starts_with("char")
                || t == "character varying"
            {
                Some(if *b { "true".into() } else { "false".into() })
            } else if is_numeric(&t) {
                Some(if *b { "1".into() } else { "0".into() })
            } else {
                Some(if *b { "true".into() } else { "false".into() })
            }
        }

        Value::Number(n) => {
            if is_numeric(&t) || is_integer(&t) {
                Some(num_to_string(n))
            } else if t == "boolean" || t == "bool" {
                // Numeric → bool only for 0/1.
                match n.as_i64() {
                    Some(1) => Some("true".into()),
                    Some(0) => Some("false".into()),
                    _ => None,
                }
            } else if t.starts_with("timestamp") {
                // Epoch seconds → timestamp; let PG parse via to_timestamp is
                // not available in COPY, so emit ISO. Best-effort: pass the
                // number through as text (rare path; Python used utcfromtimestamp).
                n.as_f64().map(epoch_to_iso).unwrap_or(None)
            } else {
                // text/date/other — stringify.
                Some(num_to_string(n))
            }
        }

        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None; // empty string → NULL sentinel (matches Python)
            }
            if !is_texty(&t) {
                let low = trimmed.to_lowercase();
                if low == "none" || low == "null" || low == "nan" || low == "n/a" {
                    return None;
                }
            }
            if is_numeric(&t) || is_integer(&t) {
                return parse_numeric_str(trimmed);
            }
            if t == "boolean" || t == "bool" {
                return match trimmed.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" | "y" => Some("true".into()),
                    "false" | "f" | "0" | "no" | "n" => Some("false".into()),
                    _ => None,
                };
            }
            if t == "date" {
                // Take the leading YYYY-MM-DD if present; else pass through and
                // let PG reject. Python parsed strictly; we keep the date prefix.
                return Some(date_prefix(trimmed).unwrap_or_else(|| trimmed.to_string()));
            }
            // text / timestamp / uuid / everything else → verbatim.
            Some(s.clone())
        }

        // Arrays + objects landing on a non-json column: serialize to JSON text
        // (PG will reject if the column truly isn't json/array; matches the
        // Python fallback `json.dumps`).
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

fn is_numeric(t: &str) -> bool {
    matches!(
        t,
        "numeric"
            | "decimal"
            | "double precision"
            | "real"
            | "float"
            | "float8"
            | "float4"
    )
}

fn is_integer(t: &str) -> bool {
    matches!(
        t,
        "int" | "integer" | "int4" | "smallint" | "int2" | "bigint" | "int8"
    )
}

fn date_prefix(s: &str) -> Option<String> {
    let b = s.as_bytes();
    if b.len() >= 10
        && b[..4].iter().all(|c| c.is_ascii_digit())
        && b[4] == b'-'
        && b[5..7].iter().all(|c| c.is_ascii_digit())
        && b[7] == b'-'
        && b[8..10].iter().all(|c| c.is_ascii_digit())
    {
        Some(s[..10].to_string())
    } else {
        None
    }
}

fn epoch_to_iso(secs: f64) -> Option<String> {
    use chrono::DateTime;
    DateTime::from_timestamp(secs as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
}
