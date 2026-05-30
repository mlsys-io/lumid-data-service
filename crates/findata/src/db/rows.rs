//! Dynamic `tokio_postgres::Row` → `serde_json` conversion.
//!
//! This single converter backs every read handler (the Python side did
//! `[dict(r) for r in rows]`). It dispatches on the column's Postgres type and
//! maps NULLs to `Value::Null`. Numeric/Decimal is preserved without f64
//! lossiness via serde_json's `arbitrary_precision`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use tokio_postgres::Row;
use uuid::Uuid;

/// Convert one cell to JSON. NULL → Value::Null. Unknown types fall back to a
/// text read, then Null.
fn cell_to_json(row: &Row, idx: usize) -> Value {
    let col = &row.columns()[idx];
    let tname = col.type_().name();
    match tname {
        "text" | "varchar" | "bpchar" | "char" | "name" | "citext" | "tsvector" => {
            row.try_get::<_, Option<String>>(idx).ok().flatten()
                .map(Value::String).unwrap_or(Value::Null)
        }
        "int2" => num_i64(row.try_get::<_, Option<i16>>(idx).ok().flatten().map(|v| v as i64)),
        "int4" => num_i64(row.try_get::<_, Option<i32>>(idx).ok().flatten().map(|v| v as i64)),
        "int8" => num_i64(row.try_get::<_, Option<i64>>(idx).ok().flatten()),
        "float4" => row.try_get::<_, Option<f32>>(idx).ok().flatten()
            .map(|v| json_num_f64(v as f64)).unwrap_or(Value::Null),
        "float8" => row.try_get::<_, Option<f64>>(idx).ok().flatten()
            .map(json_num_f64).unwrap_or(Value::Null),
        "numeric" => row.try_get::<_, Option<Decimal>>(idx).ok().flatten()
            .map(decimal_to_json).unwrap_or(Value::Null),
        "bool" => row.try_get::<_, Option<bool>>(idx).ok().flatten()
            .map(Value::Bool).unwrap_or(Value::Null),
        "date" => row.try_get::<_, Option<NaiveDate>>(idx).ok().flatten()
            .map(|d| Value::String(d.to_string())).unwrap_or(Value::Null),
        "timestamp" => row.try_get::<_, Option<NaiveDateTime>>(idx).ok().flatten()
            .map(naive_ts_to_json).unwrap_or(Value::Null),
        "timestamptz" => row.try_get::<_, Option<DateTime<Utc>>>(idx).ok().flatten()
            .map(|t| Value::String(t.to_rfc3339())).unwrap_or(Value::Null),
        "json" | "jsonb" => row.try_get::<_, Option<Value>>(idx).ok().flatten()
            .unwrap_or(Value::Null),
        "uuid" => row.try_get::<_, Option<Uuid>>(idx).ok().flatten()
            .map(|u| Value::String(u.to_string())).unwrap_or(Value::Null),
        "_text" | "_varchar" => row.try_get::<_, Option<Vec<String>>>(idx).ok().flatten()
            .map(|v| Value::Array(v.into_iter().map(Value::String).collect()))
            .unwrap_or(Value::Null),
        "_int4" => row.try_get::<_, Option<Vec<i32>>>(idx).ok().flatten()
            .map(|v| Value::Array(v.into_iter().map(|x| Value::from(x as i64)).collect()))
            .unwrap_or(Value::Null),
        "_int8" => row.try_get::<_, Option<Vec<i64>>>(idx).ok().flatten()
            .map(|v| Value::Array(v.into_iter().map(Value::from).collect()))
            .unwrap_or(Value::Null),
        "_float8" => row.try_get::<_, Option<Vec<f64>>>(idx).ok().flatten()
            .map(|v| Value::Array(v.into_iter().map(json_num_f64).collect()))
            .unwrap_or(Value::Null),
        // Fallback: try to read as text; if that fails, Null.
        _ => row.try_get::<_, Option<String>>(idx).ok().flatten()
            .map(Value::String).unwrap_or(Value::Null),
    }
}

fn num_i64(v: Option<i64>) -> Value {
    v.map(Value::from).unwrap_or(Value::Null)
}

/// f64 → JSON number, guarding against NaN/Inf (which aren't valid JSON).
fn json_num_f64(v: f64) -> Value {
    serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
}

/// Decimal → JSON number preserving exact digits (requires arbitrary_precision).
fn decimal_to_json(d: Decimal) -> Value {
    serde_json::from_str::<Value>(&d.normalize().to_string()).unwrap_or(Value::Null)
}

/// Naive timestamp → ISO-8601 with 'T', omitting microseconds when zero (Python
/// `datetime.isoformat()` parity).
fn naive_ts_to_json(t: NaiveDateTime) -> Value {
    let fmt = if t.and_utc().timestamp_subsec_nanos() == 0 {
        t.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else {
        t.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    };
    Value::String(fmt)
}

/// Convert a full row to a JSON object keyed by column name.
pub fn row_to_object(row: &Row) -> Map<String, Value> {
    let mut obj = Map::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        obj.insert(col.name().to_string(), cell_to_json(row, i));
    }
    obj
}

/// Convert rows to a Vec of JSON objects.
pub fn rows_to_objects(rows: &[Row]) -> Vec<Map<String, Value>> {
    rows.iter().map(row_to_object).collect()
}
