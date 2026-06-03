//! Unit tests for blob-listing helpers.
//!
//! All tests run in-memory; no live object store required.

use lumid_platform::handlers::blobs::{clamp_limit, sanitize_prefix, ListParams};

// ── clamp_limit ───────────────────────────────────────────────────────────────

#[test]
fn clamp_limit_defaults_to_1000_when_absent() {
    assert_eq!(clamp_limit(None), 1000);
}

#[test]
fn clamp_limit_returns_provided_value_within_range() {
    assert_eq!(clamp_limit(Some(500)), 500);
    assert_eq!(clamp_limit(Some(1)), 1);
    assert_eq!(clamp_limit(Some(10_000)), 10_000);
}

#[test]
fn clamp_limit_caps_at_hard_max_10000() {
    assert_eq!(clamp_limit(Some(10_001)), 10_000);
    assert_eq!(clamp_limit(Some(usize::MAX)), 10_000);
}

#[test]
fn clamp_limit_floors_at_1() {
    assert_eq!(clamp_limit(Some(0)), 1);
}

// ── sanitize_prefix ───────────────────────────────────────────────────────────

#[test]
fn sanitize_prefix_allows_empty_string() {
    let result = sanitize_prefix("").expect("empty prefix must be Ok");
    assert!(result.is_none(), "empty prefix should produce no ObjPath");
}

#[test]
fn sanitize_prefix_accepts_simple_prefix() {
    let result = sanitize_prefix("retrievals").expect("simple prefix must be Ok");
    assert!(result.is_some());
    assert_eq!(result.unwrap().to_string(), "retrievals");
}

#[test]
fn sanitize_prefix_accepts_nested_prefix() {
    let result = sanitize_prefix("retrievals/2024").expect("nested prefix must be Ok");
    assert!(result.is_some());
    assert_eq!(result.unwrap().to_string(), "retrievals/2024");
}

#[test]
fn sanitize_prefix_rejects_parent_traversal() {
    assert!(sanitize_prefix("../etc").is_err());
    assert!(sanitize_prefix("foo/../../etc").is_err());
    assert!(sanitize_prefix("..").is_err());
}

#[test]
fn sanitize_prefix_rejects_absolute_path() {
    assert!(sanitize_prefix("/etc/passwd").is_err());
}

// ── ListParams struct defaults ────────────────────────────────────────────────

#[test]
fn list_params_all_fields_absent_gives_none_defaults() {
    // Construct directly with all fields absent (as the axum Query extractor would
    // produce when no query params are present).
    let params = ListParams {
        prefix: None,
        delimiter: None,
        limit: None,
    };
    assert!(params.prefix.is_none());
    assert!(params.delimiter.is_none());
    // Confirm the limit helper gives 1000 when field absent.
    assert_eq!(clamp_limit(params.limit), 1000);
}

#[test]
fn list_params_limit_above_hard_max_clamps() {
    let params = ListParams {
        prefix: None,
        delimiter: None,
        limit: Some(99_999),
    };
    assert_eq!(clamp_limit(params.limit), 10_000);
}

#[test]
fn list_params_delimiter_present_signals_folder_mode() {
    let params = ListParams {
        prefix: Some("foo/".to_string()),
        delimiter: Some("/".to_string()),
        limit: Some(50),
    };
    // A non-empty delimiter triggers folder-style listing.
    let use_delimiter = params.delimiter.as_deref().is_some_and(|d| !d.is_empty());
    assert!(use_delimiter);
    assert_eq!(clamp_limit(params.limit), 50);
}

#[test]
fn list_params_empty_delimiter_does_not_trigger_folder_mode() {
    let params = ListParams {
        prefix: None,
        delimiter: Some(String::new()),
        limit: None,
    };
    let use_delimiter = params.delimiter.as_deref().is_some_and(|d| !d.is_empty());
    assert!(!use_delimiter);
}
