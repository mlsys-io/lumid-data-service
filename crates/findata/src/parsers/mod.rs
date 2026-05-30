//! Wire-format parsers + Content-Encoding decode — port of
//! `ingest/parsers.py` + `ingest/decompress.py`.
//!
//! Structured formats supported natively: JSON, NDJSON, CSV, TSV. XML / YAML /
//! Parquet / Arrow return an "unsupported on this build" error (415) — they
//! stay a Python sidecar concern (see report). Compression: gzip (flate2) and
//! zstd (zstd crate) one-shot decode via `decode`.

use std::io::Read;

use serde_json::Value;

use crate::error::ApiError;

/// Resolved wire-format kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Json,
    Ndjson,
    Csv,
    Tsv,
    Xml,
    Yaml,
    Parquet,
    Arrow,
    Blob,
    TextBlob,
}

impl Kind {
    pub fn is_structured(self) -> bool {
        matches!(
            self,
            Kind::Json | Kind::Ndjson | Kind::Csv | Kind::Tsv | Kind::Xml | Kind::Yaml | Kind::Parquet | Kind::Arrow
        )
    }
    /// Natively parseable on this Rust build.
    pub fn is_native(self) -> bool {
        matches!(self, Kind::Json | Kind::Ndjson | Kind::Csv | Kind::Tsv)
    }
}

/// Resolve a kind from a Content-Type and/or filename — port of `kind_for`.
pub fn kind_for(content_type: Option<&str>, filename: Option<&str>) -> Result<Kind, ApiError> {
    let ct = content_type
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase();
    let by_ct = match ct.as_str() {
        "application/json" => Some(Kind::Json),
        "application/x-ndjson" | "application/jsonl" | "application/x-jsonlines" => {
            Some(Kind::Ndjson)
        }
        "text/csv" | "text/comma-separated-values" | "application/csv" => Some(Kind::Csv),
        "text/tab-separated-values" | "text/tsv" => Some(Kind::Tsv),
        "application/xml" | "text/xml" => Some(Kind::Xml),
        "application/yaml" | "application/x-yaml" | "text/yaml" | "text/x-yaml" => {
            Some(Kind::Yaml)
        }
        "application/vnd.apache.parquet" | "application/x-parquet" => Some(Kind::Parquet),
        "application/vnd.apache.arrow.stream" | "application/vnd.apache.arrow.file" => {
            Some(Kind::Arrow)
        }
        "application/octet-stream" | "application/pdf" => Some(Kind::Blob),
        "text/plain" => Some(Kind::TextBlob),
        _ => None,
    };
    if let Some(k) = by_ct {
        return Ok(k);
    }
    if ct.starts_with("image/")
        || ct.starts_with("audio/")
        || ct.starts_with("video/")
        || ct == "text/html"
    {
        return Ok(Kind::Blob);
    }
    if let Some(fname) = filename {
        let lower = fname.to_lowercase();
        let ext = lower.rsplit('.').next().unwrap_or("");
        let by_ext = match ext {
            "json" => Some(Kind::Json),
            "ndjson" | "jsonl" => Some(Kind::Ndjson),
            "csv" => Some(Kind::Csv),
            "tsv" => Some(Kind::Tsv),
            "xml" => Some(Kind::Xml),
            "yaml" | "yml" => Some(Kind::Yaml),
            "parquet" | "pq" => Some(Kind::Parquet),
            "arrow" => Some(Kind::Arrow),
            "pdf" => Some(Kind::Blob),
            "txt" | "md" => Some(Kind::TextBlob),
            _ => None,
        };
        if let Some(k) = by_ext {
            return Ok(k);
        }
    }
    Err(ApiError::BadRequest(format!(
        "unsupported wire format (content_type={content_type:?}, filename={filename:?})"
    )))
}

/// Parse a JSON envelope: `{records:[...]}`, a top-level list, or a bare object.
pub fn parse_json(body: &[u8]) -> Result<Vec<Value>, ApiError> {
    let body = if body.is_empty() { b"{}".as_slice() } else { body };
    let doc: Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;
    match doc {
        Value::Array(a) => Ok(a),
        Value::Object(mut o) => match o.remove("records") {
            None => Ok(vec![Value::Object(o)]),
            Some(Value::Array(a)) => Ok(a),
            Some(_) => Err(ApiError::BadRequest("'records' must be a list".into())),
        },
        _ => Err(ApiError::BadRequest(
            "top-level JSON must be an object or array".into(),
        )),
    }
}

/// Parse NDJSON from a buffer (one JSON object per non-blank line).
pub fn parse_ndjson(body: &[u8]) -> Result<Vec<Value>, ApiError> {
    let text = std::str::from_utf8(body)
        .map_err(|e| ApiError::BadRequest(format!("ndjson not utf-8: {e}")))?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let s = line.trim();
        if s.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(s)
            .map_err(|e| ApiError::BadRequest(format!("ndjson line {}: {e}", i + 1)))?;
        out.push(v);
    }
    Ok(out)
}

/// Parse CSV/TSV with a header row → records. Empty cells → JSON null.
pub fn parse_delimited(body: &[u8], delimiter: u8) -> Result<Vec<Value>, ApiError> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_reader(body);
    let headers = rdr
        .headers()
        .map_err(|e| ApiError::BadRequest(format!("csv header: {e}")))?
        .clone();
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| ApiError::BadRequest(format!("csv row: {e}")))?;
        let mut obj = serde_json::Map::new();
        for (h, field) in headers.iter().zip(rec.iter()) {
            if h.is_empty() {
                continue;
            }
            let v = if field.is_empty() {
                Value::Null
            } else {
                Value::String(field.to_string())
            };
            obj.insert(h.to_string(), v);
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Eager parse for the structured plane (typed/file). Streaming NDJSON uses
/// `parse_ndjson` on each accumulated chunk; XML/YAML/Parquet/Arrow are 415.
pub fn parse_to_records(body: &[u8], kind: Kind) -> Result<Vec<Value>, ApiError> {
    match kind {
        Kind::Json => parse_json(body),
        Kind::Ndjson => parse_ndjson(body),
        Kind::Csv => parse_delimited(body, b','),
        Kind::Tsv => parse_delimited(body, b'\t'),
        Kind::Xml | Kind::Yaml | Kind::Parquet | Kind::Arrow => Err(ApiError::Unavailable(
            format!("{kind:?} parsing is unsupported on this build (Python sidecar only)"),
        )),
        Kind::Blob | Kind::TextBlob => Err(ApiError::BadRequest(format!(
            "kind {kind:?} is not a structured format"
        ))),
    }
}

/// One-shot Content-Encoding decode — port of `decompress.decode`.
pub fn decode(body: Vec<u8>, content_encoding: Option<&str>) -> Result<Vec<u8>, ApiError> {
    let enc = content_encoding.unwrap_or("").trim().to_lowercase();
    if enc.is_empty() || enc == "identity" {
        return Ok(body);
    }
    if enc == "gzip" || enc == "x-gzip" {
        let mut dec = flate2::read::GzDecoder::new(&body[..]);
        let mut out = Vec::new();
        dec.read_to_end(&mut out)
            .map_err(|e| ApiError::BadRequest(format!("gzip decode failed: {e}")))?;
        return Ok(out);
    }
    if enc == "zstd" {
        let out = zstd::stream::decode_all(&body[..])
            .map_err(|e| ApiError::BadRequest(format!("zstd decode failed: {e}")))?;
        return Ok(out);
    }
    Err(ApiError::BadRequest(format!(
        "unsupported Content-Encoding {enc:?}"
    )))
}
