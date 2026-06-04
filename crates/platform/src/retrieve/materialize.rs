//! Stream-writes retrieved rows to object storage.
//!
//! Supports `csv`, `jsonl`, and `raw` formats. Buffers output in memory
//! (SQL rows are bounded by the row-cap in the replayer; storage_get objects
//! are bounded by `LUMID_BLOB_MAX_BYTES`), then writes to the configured
//! `Arc<dyn ObjectStore>`.

use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::ObjectStore;

use crate::error::{ApiError, ApiResult};

pub enum OutputFormat {
    Csv,
    Jsonl,
    Raw,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "csv" => Some(Self::Csv),
            "jsonl" => Some(Self::Jsonl),
            "raw" => Some(Self::Raw),
            _ => None,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Jsonl => "jsonl",
            Self::Raw => "bin",
        }
    }

    /// Wire-format name matching the Python `RetrievalResult.output_format` Literal.
    ///
    /// Distinct from `extension()` because `Raw` stores as `.bin` but
    /// the protocol value is `"raw"`.
    pub fn format_name(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Jsonl => "jsonl",
            Self::Raw => "raw",
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Csv => "text/csv",
            Self::Jsonl => "application/x-ndjson",
            Self::Raw => "application/octet-stream",
        }
    }
}

/// Handles incremental materialisation of rows or raw bytes into a buffer.
pub struct Materializer {
    format: OutputFormat,
    buf: Vec<u8>,
    csv_header_written: bool,
    csv_fields: Option<Vec<String>>,
}

impl Materializer {
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            buf: Vec::new(),
            csv_header_written: false,
            csv_fields: None,
        }
    }

    /// Append a single row (for `csv` / `jsonl` formats).
    pub fn write_row(&mut self, row: &std::collections::BTreeMap<String, serde_json::Value>) {
        match self.format {
            OutputFormat::Jsonl => {
                let json = serde_json::to_string(row).unwrap_or_default();
                self.buf.extend_from_slice(json.as_bytes());
                self.buf.push(b'\n');
            }
            OutputFormat::Csv => {
                if !self.csv_header_written {
                    let fields: Vec<String> = row.keys().cloned().collect();
                    let header = fields.join(",") + "\n";
                    self.buf.extend_from_slice(header.as_bytes());
                    self.csv_fields = Some(fields);
                    self.csv_header_written = true;
                }
                if let Some(ref fields) = self.csv_fields {
                    let values: Vec<String> = fields
                        .iter()
                        .map(|f| {
                            let v = row.get(f).cloned().unwrap_or(serde_json::Value::Null);
                            csv_escape(&v)
                        })
                        .collect();
                    let line = values.join(",") + "\n";
                    self.buf.extend_from_slice(line.as_bytes());
                }
            }
            OutputFormat::Raw => {
                // raw format should only be used for storage_get ops — bytes appended directly.
            }
        }
    }

    /// Append raw bytes (for `raw` format or mixed storage_get into jsonl).
    pub fn write_raw_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn size(&self) -> usize {
        self.buf.len()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

fn csv_escape(v: &serde_json::Value) -> String {
    let s = match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    // Quote on `\r` as well as `\n`: a bare CR can otherwise be read as a row
    // terminator by strict RFC-4180 parsers and corrupt row boundaries.
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

/// Write `bytes` to object storage at `key`, returning `(materialized_uri, size_bytes)`.
///
/// `materialized_uri` is the app-relative fetch path `/blobs/<key>` that
/// consumers pass to `GET {base_url}{materialized_uri}`.  The storage backend
/// (scheme, bucket) is never exposed.
pub async fn write_to_store(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    bytes: Vec<u8>,
) -> ApiResult<(String, usize)> {
    let path = ObjPath::from(key);
    let size = bytes.len();
    let payload = object_store::PutPayload::from(bytes::Bytes::from(bytes));
    store
        .put(&path, payload)
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("object store write failed: {e}")))?;
    let uri = format!("/blobs/{key}");
    Ok((uri, size))
}

/// Attempt to generate a presigned GET URL for `key`.
///
/// `object_store` 0.11 does not expose a stable presign API on the trait object,
/// so `signed_url` is always empty.  Consumers fetch the result via the
/// app-relative `/blobs/<key>` path returned in `materialized_uri`.
pub fn try_presign_url(_store: &Arc<dyn ObjectStore>, _key: &str) -> String {
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn row(pairs: &[(&str, &str)]) -> BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect()
    }

    #[test]
    fn jsonl_output() {
        let mut m = Materializer::new(OutputFormat::Jsonl);
        m.write_row(&row(&[("a", "1"), ("b", "2")]));
        m.write_row(&row(&[("a", "3"), ("b", "4")]));
        let bytes = m.into_bytes();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["a"], serde_json::json!("1"));
    }

    #[test]
    fn csv_output_has_header() {
        let mut m = Materializer::new(OutputFormat::Csv);
        m.write_row(&row(&[("col1", "val1"), ("col2", "val2")]));
        let bytes = m.into_bytes();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.starts_with("col1,col2\n") || text.starts_with("col2,col1\n"));
        assert!(text.contains("val1") && text.contains("val2"));
    }

    #[test]
    fn csv_escape_commas() {
        let v = serde_json::Value::String("a,b".to_string());
        assert_eq!(csv_escape(&v), "\"a,b\"");
    }

    #[test]
    fn csv_escape_carriage_return() {
        let v = serde_json::Value::String("a\rb".to_string());
        assert_eq!(csv_escape(&v), "\"a\rb\"");
    }

    #[test]
    fn format_name_matches_python_literal() {
        assert_eq!(OutputFormat::Csv.format_name(), "csv");
        assert_eq!(OutputFormat::Jsonl.format_name(), "jsonl");
        assert_eq!(OutputFormat::Raw.format_name(), "raw",
            "Raw format_name must be 'raw', not 'bin'");
    }

    #[test]
    fn extension_and_format_name_differ_for_raw() {
        assert_eq!(OutputFormat::Raw.extension(), "bin");
        assert_eq!(OutputFormat::Raw.format_name(), "raw");
    }

    /// write_to_store must return an app-relative `/blobs/<key>` URI — never
    /// an `s3://` URL or anything containing the bucket name.
    #[tokio::test]
    async fn write_to_store_uri_is_app_relative() {
        use object_store::memory::InMemory;
        use std::sync::Arc;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = "retrievals/abc123/result.jsonl";
        let (uri, size) = write_to_store(&store, key, b"hello\n".to_vec())
            .await
            .expect("write_to_store must succeed");

        assert_eq!(uri, format!("/blobs/{key}"), "URI must be /blobs/<key>");
        assert!(
            uri.starts_with("/blobs/"),
            "URI must start with /blobs/; got: {uri}"
        );
        assert!(
            !uri.contains("s3://"),
            "URI must not contain s3:// scheme; got: {uri}"
        );
        assert!(
            !uri.contains("lumilake"),
            "URI must not contain bucket name; got: {uri}"
        );
        assert_eq!(size, 6, "size must match byte count");
    }
}
