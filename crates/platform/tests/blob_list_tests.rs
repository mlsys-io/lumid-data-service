//! Unit tests for blob-listing helpers and the KV write-plane guards.
//!
//! All tests run in-memory; no live object store required.

use lumid_platform::auth::Identity;
use lumid_platform::error::ApiError;
use lumid_platform::handlers::blobs::{
    clamp_limit, is_hidden_key, is_hidden_prefix, require_blob_write, sanitize_blob_key,
    sanitize_prefix, ListParams,
};

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

// ── is_hidden_key ─────────────────────────────────────────────────────────────

#[test]
fn hidden_key_exact_prefix_is_hidden() {
    assert!(is_hidden_key("retrievals", "retrievals"));
}

#[test]
fn hidden_key_nested_under_prefix_is_hidden() {
    assert!(is_hidden_key("retrievals/abc/result.jsonl", "retrievals"));
}

#[test]
fn hidden_key_sibling_prefix_not_hidden() {
    // "retrievals-archive/x" must NOT be hidden — segment-boundary check.
    assert!(!is_hidden_key("retrievals-archive/x", "retrievals"));
}

#[test]
fn hidden_key_unrelated_prefix_not_hidden() {
    assert!(!is_hidden_key("demo/some/file.txt", "retrievals"));
}

#[test]
fn hidden_key_empty_retrieval_prefix_hides_nothing() {
    assert!(!is_hidden_key("retrievals/anything", ""));
    assert!(!is_hidden_key("", ""));
}

// ── is_hidden_prefix ──────────────────────────────────────────────────────────

#[test]
fn hidden_prefix_with_trailing_slash_is_hidden() {
    assert!(is_hidden_prefix("retrievals/", "retrievals"));
}

#[test]
fn hidden_prefix_without_trailing_slash_is_hidden() {
    assert!(is_hidden_prefix("retrievals", "retrievals"));
}

#[test]
fn hidden_prefix_sibling_not_hidden() {
    assert!(!is_hidden_prefix("retrievals-archive/", "retrievals"));
}

#[test]
fn hidden_prefix_empty_retrieval_prefix_hides_nothing() {
    assert!(!is_hidden_prefix("retrievals/", ""));
    assert!(!is_hidden_prefix("anything/", ""));
}

#[test]
fn hidden_prefix_nested_run_id_is_hidden() {
    // Security: GET /blobs?prefix=retrievals&delimiter=/ returns common_prefixes
    // like "retrievals/<run_id>/".  These must be hidden so run IDs don't leak.
    assert!(is_hidden_prefix("retrievals/abc123/", "retrievals"));
}

#[test]
fn hidden_prefix_deeply_nested_is_hidden() {
    // A two-level nested prefix also falls under the retrieval namespace.
    assert!(is_hidden_prefix("retrievals/abc/def/", "retrievals"));
}

#[test]
fn hidden_prefix_sibling_with_hyphen_not_hidden() {
    // Segment-boundary check: "retrievals-archive/" must NOT be hidden.
    assert!(!is_hidden_prefix("retrievals-archive/", "retrievals"));
}

// ── sanitize_blob_key — traversal rejection (the put_blob / delete_blob guard) ─

#[test]
fn sanitize_blob_key_rejects_parent_traversal() {
    assert!(matches!(sanitize_blob_key("../etc/passwd"), Err(ApiError::BadRequest(_))));
}

#[test]
fn sanitize_blob_key_rejects_double_dot_segment() {
    assert!(matches!(sanitize_blob_key("jobs/../../secrets"), Err(ApiError::BadRequest(_))));
}

#[test]
fn sanitize_blob_key_rejects_absolute_path() {
    assert!(matches!(sanitize_blob_key("/etc/passwd"), Err(ApiError::BadRequest(_))));
}

#[test]
fn sanitize_blob_key_rejects_empty_key() {
    assert!(matches!(sanitize_blob_key(""), Err(ApiError::BadRequest(_))));
}

#[test]
fn sanitize_blob_key_accepts_valid_nested_key() {
    let key = sanitize_blob_key("jobs/abc-123/record.json").expect("valid nested key accepted");
    assert_eq!(key, "jobs/abc-123/record.json");
}

// ── require_blob_write — PUT/DELETE gate: scope-based, not role-based ─────────

fn make_identity(role: &str) -> Identity {
    Identity {
        sub: format!("test:{role}"),
        role: role.to_string(),
        email: None,
        active: true,
        scopes: Vec::new(),
    }
}

fn make_scoped_identity(scopes: &[&str]) -> Identity {
    Identity {
        sub: "test:user".to_string(),
        role: "user".to_string(),
        email: None,
        active: true,
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn require_blob_write_allows_lumilake_write_scope() {
    require_blob_write(&make_scoped_identity(&["lumilake:write"])).expect("lumilake:write scope allowed");
}

#[test]
fn require_blob_write_allows_admin_scope_lumilake_star() {
    require_blob_write(&make_scoped_identity(&["lumilake:*"])).expect("lumilake:* admin scope allowed");
}

#[test]
fn require_blob_write_allows_admin_scope_lumilake_admin() {
    require_blob_write(&make_scoped_identity(&["lumilake:admin"])).expect("lumilake:admin scope allowed");
}

#[test]
fn require_blob_write_allows_wildcard_scope() {
    require_blob_write(&make_scoped_identity(&["*"])).expect("'*' wildcard scope allowed");
}

#[test]
fn require_blob_write_allows_local_role_without_scopes() {
    // Local API key: role="local", no scopes — must bypass the scope check entirely.
    require_blob_write(&make_identity("local")).expect("local key allowed");
}

#[test]
fn require_blob_write_rejects_unrelated_scope_only() {
    // A PAT with only non-lumilake scopes must be denied.
    assert!(matches!(
        require_blob_write(&make_scoped_identity(&["flowmesh:results:write"])),
        Err(ApiError::Forbidden(_))
    ));
}

#[test]
fn require_blob_write_rejects_user_with_no_scopes() {
    // A regular authed reader (no scopes) must NOT be able to write/delete blobs.
    assert!(matches!(
        require_blob_write(&make_identity("user")),
        Err(ApiError::Forbidden(_))
    ));
}

// ── archive-prefix visibility — "jobs/" must survive the retrieval-output filter ─

#[test]
fn jobs_key_not_hidden_by_retrieval_filter() {
    assert!(!is_hidden_key("jobs/abc/record.json", "retrievals"));
}

#[test]
fn jobs_common_prefix_not_hidden_by_retrieval_filter() {
    assert!(!is_hidden_prefix("jobs/", "retrievals"));
}
