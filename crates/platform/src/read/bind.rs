//! Per-request resolution of a [`EndpointSpec`] into executable SQL + bound
//! params + a canonical cache-key string.
//!
//! Steps: validate/coerce each param (type + transform + clamp/default) →
//! choose `{{fragment}}`s from enum `select` / presence `present|absent` maps →
//! substitute fragments into the SQL → lower the remaining `:name` binds to
//! positional `$N` (skipping `::type` casts) collecting values in order.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use tokio_postgres::types::ToSql;

use super::spec::{EndpointSpec, Kind, ParamSpec, Transform};
use crate::error::ApiError;

/// A coerced, cloneable bind value (cloned per `:name` occurrence).
#[derive(Clone)]
enum Bv {
    Text(String),
    Int(i64),
    Float(f64),
    Date(NaiveDate),
    Ts(DateTime<Utc>),
    Bool(bool),
}

impl Bv {
    fn boxed(&self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            Bv::Text(s) => Box::new(s.clone()),
            Bv::Int(i) => Box::new(*i),
            Bv::Float(f) => Box::new(*f),
            Bv::Date(d) => Box::new(*d),
            Bv::Ts(t) => Box::new(*t),
            Bv::Bool(b) => Box::new(*b),
        }
    }
    fn canon(&self) -> String {
        match self {
            Bv::Text(s) => s.clone(),
            Bv::Int(i) => i.to_string(),
            Bv::Float(f) => f.to_string(),
            Bv::Date(d) => d.to_string(),
            Bv::Ts(t) => t.to_rfc3339(),
            Bv::Bool(b) => b.to_string(),
        }
    }
}

pub struct Bound {
    pub sql: String,
    pub params: Vec<Box<dyn ToSql + Sync + Send>>,
    /// Canonical "name=value&…" of resolved params (sorted) for the cache key.
    pub canon: String,
}

impl Bound {
    pub fn refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
    }
}

fn apply_transform(s: String, t: Transform) -> String {
    match t {
        Transform::None => s,
        Transform::Upper => s.to_uppercase(),
        Transform::Lower => s.to_lowercase(),
    }
}

fn default_str(p: &ParamSpec) -> Option<String> {
    p.default.as_ref().map(|v| match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        other => other.to_string(),
    })
}

fn coerce(p: &ParamSpec, raw: &str) -> Result<Bv, ApiError> {
    let bad = |m: String| ApiError::BadRequest(m);
    match p.ty.as_str() {
        "symbol" => {
            let s = apply_transform(raw.trim().to_uppercase(), p.transform);
            if s.is_empty() || s.len() > 20 || !s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_')) {
                return Err(bad(format!("invalid symbol '{raw}'")));
            }
            Ok(Bv::Text(s))
        }
        "str" => {
            let s = apply_transform(raw.to_string(), p.transform);
            if let Some(ml) = p.max_len {
                if s.len() > ml {
                    return Err(bad(format!("'{}' too long (max {ml})", p.name)));
                }
            }
            Ok(Bv::Text(s))
        }
        "int" => {
            let mut n: i64 = raw.trim().parse().map_err(|_| bad(format!("'{}' must be an integer", p.name)))?;
            if let Some(mn) = p.min { n = n.max(mn as i64); }
            if let Some(mx) = p.max { n = n.min(mx as i64); }
            Ok(Bv::Int(n))
        }
        "float" => {
            let mut f: f64 = raw.trim().parse().map_err(|_| bad(format!("'{}' must be a number", p.name)))?;
            if let Some(mn) = p.min { if f < mn { f = mn; } }
            if let Some(mx) = p.max { if f > mx { f = mx; } }
            Ok(Bv::Float(f))
        }
        "date" => {
            let d = NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")
                .map_err(|_| bad(format!("'{}' must be YYYY-MM-DD", p.name)))?;
            Ok(Bv::Date(d))
        }
        "timestamp" => {
            // Accept a full RFC3339 timestamp, or a bare `YYYY-MM-DD` date
            // (interpreted as 00:00:00 UTC) — clients routinely pass a date
            // for `since`-style filters.
            let s = raw.trim();
            let t = DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .or_else(|_| {
                    NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|nd| {
                        DateTime::from_naive_utc_and_offset(
                            nd.and_hms_opt(0, 0, 0).unwrap_or_default(),
                            Utc,
                        )
                    })
                })
                .map_err(|_| bad(format!("'{}' must be RFC3339 or YYYY-MM-DD", p.name)))?;
            Ok(Bv::Ts(t))
        }
        "bool" => {
            let b = matches!(raw.trim().to_lowercase().as_str(), "1" | "true" | "yes");
            Ok(Bv::Bool(b))
        }
        // enum: the "value" is the variant string (used for fragment selection).
        "enum" => Ok(Bv::Text(apply_transform(raw.to_string(), p.transform))),
        other => Err(bad(format!("unknown param type '{other}'"))),
    }
}

/// Resolve a request into executable SQL + ordered binds + cache canon.
pub fn resolve(
    spec: &EndpointSpec,
    path: &HashMap<String, String>,
    query: &HashMap<String, String>,
) -> Result<Bound, ApiError> {
    // 1) Resolve each param's value (or mark absent).
    let mut values: HashMap<String, Bv> = HashMap::new();
    let mut enum_choice: HashMap<String, String> = HashMap::new();
    let mut present: HashMap<String, bool> = HashMap::new();
    let mut canon_parts: Vec<(String, String)> = Vec::new();

    for p in &spec.params {
        let raw: Option<String> = match p.kind {
            Kind::Path => path.get(&p.name).cloned(),
            Kind::Query => query.get(&p.name).cloned().or_else(|| default_str(p)),
        };
        match raw {
            None => {
                if p.effectively_required() {
                    return Err(ApiError::BadRequest(format!("missing required '{}'", p.name)));
                }
                present.insert(p.name.clone(), false);
            }
            Some(r) => {
                if p.is_enum() {
                    let choice = apply_transform(r.clone(), p.transform);
                    if !p.select.contains_key(&choice) {
                        let mut keys: Vec<&String> = p.select.keys().collect();
                        keys.sort();
                        return Err(ApiError::BadRequest(format!(
                            "'{}' must be one of {:?}", p.name, keys
                        )));
                    }
                    enum_choice.insert(p.name.clone(), choice.clone());
                    canon_parts.push((p.name.clone(), choice.clone()));
                    // An enum's value also drives fragment selection, but may
                    // ALSO be bound directly (`:name`). When the select map
                    // carries a fragment named like the param (the canonical
                    // value, e.g. `7d -> seven_day`), bind THAT mapped value so
                    // `:name` matches stored data — not the raw alias. Falls
                    // back to the choice when no such mapping exists.
                    let bound = p
                        .select
                        .get(&choice)
                        .and_then(|m| m.get(&p.name))
                        .cloned()
                        .unwrap_or_else(|| choice.clone());
                    values.insert(p.name.clone(), Bv::Text(bound));
                } else {
                    let v = coerce(p, &r)?;
                    canon_parts.push((p.name.clone(), v.canon()));
                    values.insert(p.name.clone(), v);
                }
                present.insert(p.name.clone(), true);
            }
        }
    }

    // 2) Build the fragment substitution map.
    let mut fragments: HashMap<String, String> = HashMap::new();
    for p in &spec.params {
        if p.is_enum() {
            if let Some(choice) = enum_choice.get(&p.name) {
                if let Some(map) = p.select.get(choice) {
                    for (k, v) in map {
                        fragments.insert(k.clone(), v.clone());
                    }
                }
            }
        } else if p.is_presence() {
            let map = if *present.get(&p.name).unwrap_or(&false) { &p.present } else { &p.absent };
            for (k, v) in map {
                fragments.insert(k.clone(), v.clone());
            }
        }
    }

    // 3) Substitute {{fragment}} → text.
    let substituted = substitute_fragments(&spec.sql, &fragments);

    // 4) Lower :name binds → $N (skip ::type casts), collecting values in order.
    let (sql, params) = lower_binds(&substituted, &values)?;

    // 5) Canonical cache key.
    canon_parts.sort();
    let canon = canon_parts.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");

    Ok(Bound { sql, params, canon })
}

fn substitute_fragments(sql: &str, fragments: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(end) = sql[i + 2..].find("}}") {
                let name = sql[i + 2..i + 2 + end].trim();
                out.push_str(fragments.get(name).map(|s| s.as_str()).unwrap_or(""));
                i = i + 2 + end + 2;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}
fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Replace `:name` binds with `$N`, pushing each occurrence's value. A `:`
/// preceded or followed by another `:` (a `::type` cast) is left untouched.
fn lower_binds(
    sql: &str,
    values: &HashMap<String, Bv>,
) -> Result<(String, Vec<Box<dyn ToSql + Sync + Send>>), ApiError> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b':' {
            let prev_colon = i > 0 && bytes[i - 1] == b':';
            let next_colon = i + 1 < bytes.len() && bytes[i + 1] == b':';
            if !prev_colon && !next_colon && i + 1 < bytes.len() && is_ident_start(bytes[i + 1]) {
                // Read the identifier.
                let mut j = i + 1;
                while j < bytes.len() && is_ident(bytes[j]) {
                    j += 1;
                }
                let name = &sql[i + 1..j];
                let v = values.get(name).ok_or_else(|| {
                    ApiError::Internal(anyhow::anyhow!("bind ':{name}' has no resolved value"))
                })?;
                params.push(v.boxed());
                out.push('$');
                out.push_str(&params.len().to_string());
                // Cast numeric binds so a single Rust width matches any column
                // width (i64 vs int4 / numeric): `$N::int8` / `$N::float8`.
                // Postgres implicitly promotes in comparisons (int4 = int8 etc).
                match v {
                    Bv::Int(_) => out.push_str("::int8"),
                    Bv::Float(_) => out.push_str("::float8"),
                    _ => {}
                }
                i = j;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Ok((out, params))
}
