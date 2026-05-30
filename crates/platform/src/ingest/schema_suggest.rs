//! Schema suggestion engine for the ingress-proposal negotiation loop.
//!
//! Two layers, rules-first / AI-optional:
//! - [`rules_suggest`] — deterministic: infer column types from sample records,
//!   pick a heuristic natural key, detect a time-series. Always available.
//! - [`llm_refine`] — best-effort: when an LLM backend is wired
//!   (`FINDATA_LLM_BACKEND_URL`), ask it to refine the rules suggestion; the
//!   result is run back through [`validate`] before it's ever used. If the LLM
//!   is unset / down / returns junk, we silently keep the rules suggestion.
//! - [`validate`] — normalises every identifier (`^[a-z_][a-z0-9_]{0,62}$`) and
//!   clamps types to an allow-list, so neither caller JSON nor LLM output can
//!   inject SQL or unknown types.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use crate::config::Settings;

/// Reserved columns the platform manages itself — never created from a
/// proposal/counter. The provenance columns are stamped on write, and `id` is
/// the auto-generated surrogate PK (validation + the write engine both treat a
/// supplied `id` as server-side). A user identifier must be named something
/// else (e.g. `widget_id`); a bare `id` in the records is dropped here so the
/// created table stays consistent with the ingest engine.
pub const PROVENANCE_COLS: &[&str] =
    &["source", "source_endpoint", "source_run_id", "ingest_ts", "raw", "id"];

/// Postgres types a proposed column may use (everything else → text).
pub const ALLOWED_TYPES: &[&str] = &[
    "text", "bigint", "integer", "double precision", "numeric", "boolean", "jsonb",
    "timestamptz", "date", "uuid",
];

pub fn norm_ident(s: &str) -> Option<String> {
    let l = s.trim().to_lowercase();
    let ok = !l.is_empty()
        && l.len() <= 63
        && l.chars().next().map(|c| c.is_ascii_lowercase() || c == '_').unwrap_or(false)
        && l.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    ok.then_some(l)
}

fn infer_type(values: &[&Value]) -> &'static str {
    let (mut s, mut f, mut i, mut b, mut j) = (false, false, false, false, false);
    for v in values {
        match v {
            Value::String(_) => s = true,
            Value::Bool(_) => b = true,
            Value::Number(n) => {
                if n.is_f64() && n.as_i64().is_none() { f = true } else { i = true }
            }
            Value::Array(_) | Value::Object(_) => j = true,
            Value::Null => {}
        }
    }
    if j { "jsonb" } else if s { "text" } else if f { "double precision" } else if i { "bigint" } else if b { "boolean" } else { "text" }
}

/// Deterministic suggestion from the records: (columns col→pgtype, key cols,
/// skipped-key names, optional hypertable time column).
pub fn rules_suggest(records: &[Value]) -> (Map<String, Value>, Vec<String>, Vec<String>, Option<String>) {
    let mut cols: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    let mut skipped: Vec<String> = Vec::new();
    for rec in records {
        let Some(obj) = rec.as_object() else { continue };
        for (k, v) in obj {
            match norm_ident(k) {
                Some(c) if !PROVENANCE_COLS.contains(&c.as_str()) => cols.entry(c).or_default().push(v),
                Some(_) => {}
                None => { if !skipped.contains(k) { skipped.push(k.clone()) } }
            }
        }
    }
    let columns: Map<String, Value> =
        cols.iter().map(|(c, vs)| (c.clone(), Value::String(infer_type(vs).into()))).collect();
    // Heuristic natural key: identity-ish + a time column when present.
    // (`id` is reserved for the surrogate PK — excluded above — so it's not a
    // candidate here; key-less proposals get the auto `id` at apply time.)
    let key: Vec<String> = ["symbol", "date", "ts", "timestamp"]
        .iter().filter(|k| cols.contains_key(**k)).map(|k| k.to_string()).collect();
    // Time-series hint: a timestamp/date column → candidate hypertable axis.
    let time_col = ["ts", "timestamp", "time", "date", "created_at"]
        .iter().find(|k| cols.contains_key(**k)).map(|s| s.to_string());
    (columns, key, skipped, time_col)
}

/// Normalise + clamp an arbitrary columns/key proposal (builder- or LLM-supplied)
/// into a safe form. Drops invalid identifiers and unknown types (→ text).
pub fn validate(columns: &Value, key: &[String]) -> Result<(Map<String, Value>, Vec<String>), String> {
    let obj = columns.as_object().ok_or("columns must be an object {name: pgtype}")?;
    let mut out = Map::new();
    for (c, ty) in obj {
        let Some(cn) = norm_ident(c) else { continue };
        if PROVENANCE_COLS.contains(&cn.as_str()) { continue; }
        let tin = ty.as_str().unwrap_or("text").trim().to_lowercase();
        let tn = if ALLOWED_TYPES.contains(&tin.as_str()) { tin } else { "text".to_string() };
        out.insert(cn, Value::String(tn));
    }
    if out.is_empty() {
        return Err("no usable columns after validation".into());
    }
    let key_n: Vec<String> =
        key.iter().filter_map(|k| norm_ident(k)).filter(|k| out.contains_key(k)).collect();
    Ok((out, key_n))
}

/// Best-effort LLM refinement of a rules suggestion. Returns a *validated*
/// (columns, key) or `None` if no LLM is wired or anything fails.
pub async fn llm_refine(
    settings: &Settings,
    http: &reqwest::Client,
    columns: &Map<String, Value>,
    key: &[String],
    records: &[Value],
) -> Option<(Map<String, Value>, Vec<String>)> {
    if settings.llm_backend_url.trim().is_empty() {
        return None;
    }
    let sample: Vec<Value> = records.iter().take(5).cloned().collect();
    let sys = "You are a Postgres schema designer. Given sample JSON records and a \
        draft schema, return ONLY minified JSON: {\"columns\":{\"name\":\"pgtype\"},\"key\":[\"col\"]}. \
        Allowed pgtypes: text,bigint,integer,double precision,numeric,boolean,jsonb,timestamptz,date,uuid. \
        snake_case names. Do not include provenance columns. No prose, no code fences.";
    let user = json!({
        "draft_columns": columns, "draft_key": key, "sample_records": sample
    }).to_string();
    let body = json!({
        "model": settings.llm_default_model,
        "temperature": 0,
        "messages": [{"role": "system", "content": sys}, {"role": "user", "content": user}],
    });
    let url = format!("{}/v1/chat/completions", settings.llm_backend_url.trim_end_matches('/'));
    let resp = http.post(&url)
        .json(&body)
        .timeout(std::time::Duration::from_secs(12))
        .send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: Value = resp.json().await.ok()?;
    let content = v.get("choices")?.get(0)?.get("message")?.get("content")?.as_str()?;
    // Strip accidental code fences.
    let c = content.trim().trim_start_matches("```json").trim_start_matches("```").trim_end_matches("```").trim();
    let parsed: Value = serde_json::from_str(c).ok()?;
    let cols = parsed.get("columns")?;
    let key_v: Vec<String> = parsed.get("key")
        .and_then(|k| k.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
        .unwrap_or_default();
    validate(cols, &key_v).ok()
}
