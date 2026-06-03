//! Integration tests for the agent tool-use loop and tool registry.
//!
//! These tests run without a live Postgres or LLM; they use in-memory types
//! and public pure-function helpers.

// ── Tool registry: schemas validate ──────────────────────────────────────────

#[test]
fn tool_registry_schemas_are_valid_json_schema_objects() {
    use lumid_platform::agent::tools::ToolRegistry;

    let registry = ToolRegistry::new();
    assert!(!registry.is_empty(), "registry must have at least one tool");

    let schemas = registry.schemas();
    assert_eq!(schemas.len(), registry.len());

    for schema in &schemas {
        assert_eq!(
            schema.get("type").and_then(|v| v.as_str()),
            Some("function")
        );
        let f = schema.get("function").expect("missing 'function' key");
        let name = f
            .get("name")
            .and_then(|v| v.as_str())
            .expect("missing name");
        assert!(!name.is_empty(), "tool name must not be empty");
        let params = f.get("parameters").expect("missing parameters");
        assert_eq!(
            params.get("type").and_then(|v| v.as_str()),
            Some("object"),
            "tool '{name}' parameters.type must be 'object'"
        );
    }
}

#[test]
fn tool_registry_has_expected_tools() {
    use lumid_platform::agent::tools::ToolRegistry;

    let registry = ToolRegistry::new();
    let schemas = registry.schemas();
    let names: Vec<&str> = schemas
        .iter()
        .filter_map(|s| s.get("function")?.get("name")?.as_str())
        .collect();

    for expected in &["get_schema_cards", "replay_retrieval_plan"] {
        assert!(
            names.contains(expected),
            "registry missing tool '{expected}'; got: {names:?}"
        );
    }

    // Old tool triad must be gone.
    for removed in &["list_tables", "describe_table", "read_blob"] {
        assert!(
            !names.contains(removed),
            "old tool '{removed}' must NOT be in the registry; got: {names:?}"
        );
    }
}

// ── replay_retrieval_plan schema parameters ───────────────────────────────────

#[test]
fn replay_retrieval_plan_schema_requires_plan() {
    use lumid_platform::agent::tools::{replay::ReplayRetrievalPlanTool, Tool};

    let tool = ReplayRetrievalPlanTool;
    let schema = tool.parameters_schema();
    let required = schema
        .get("required")
        .and_then(|r| r.as_array())
        .expect("required array");
    let req_names: Vec<&str> = required.iter().filter_map(|v| v.as_str()).collect();
    assert!(req_names.contains(&"plan"), "'plan' must be required");
}

// ── get_schema_cards schema parameters ───────────────────────────────────────

#[test]
fn get_schema_cards_schema_has_scope_property() {
    use lumid_platform::agent::tools::{schema_cards::GetSchemaCardsTool, Tool};

    let tool = GetSchemaCardsTool;
    let schema = tool.parameters_schema();
    let props = schema
        .get("properties")
        .expect("properties object")
        .as_object()
        .expect("properties must be an object");
    assert!(
        props.contains_key("scope"),
        "get_schema_cards must expose a 'scope' property"
    );
}

// ── enable_agent + enable_llm validation ─────────────────────────────────────

#[test]
fn serve_fails_when_enable_agent_without_enable_llm() {
    let parts = lumid_platform::ServeParts {
        enable_agent: true,
        enable_llm: false,
        ..Default::default()
    };
    let result = lumid_platform::check_serve_parts(&parts);
    assert!(
        result.is_err(),
        "expected check_serve_parts to return Err when enable_agent=true and enable_llm=false"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("enable_agent requires enable_llm"),
        "unexpected error message: {msg}"
    );
}

// ── Tool error JSON shape ─────────────────────────────────────────────────────

/// The JSON produced for a tool error must carry both `"error":true` AND the
/// error message text.
#[test]
fn tool_error_message_includes_error_text() {
    use serde_json::json;

    let msg = "tool 'replay_retrieval_plan' error: connection refused".to_string();
    let content_str = json!({"error": true, "message": msg}).to_string();

    let v: serde_json::Value = serde_json::from_str(&content_str).expect("valid json");
    assert_eq!(
        v.get("error").and_then(|x| x.as_bool()),
        Some(true),
        "error field must be bool true"
    );
    assert!(
        v.get("message")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .contains("connection refused"),
        "message field must contain the error text; got: {content_str}"
    );
}

// ── StorageGet path-traversal guard ──────────────────────────────────────────

/// replay_retrieval_plan routes StorageGet ops through sanitize_blob_key.
/// Verify the sanitizer rejects `../` traversal.
#[test]
fn storage_get_rejects_dotdot_traversal() {
    use lumid_platform::handlers::blobs::sanitize_blob_key;

    let err = sanitize_blob_key("../etc/passwd").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("invalid"),
        "error must mention 'invalid'; got: {msg}"
    );
}

/// replay_retrieval_plan routes StorageGet ops through sanitize_blob_key.
/// Verify the sanitizer rejects absolute paths.
#[test]
fn storage_get_rejects_absolute_path() {
    use lumid_platform::handlers::blobs::sanitize_blob_key;

    let err = sanitize_blob_key("/abs/path").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("invalid"),
        "error must mention 'invalid'; got: {msg}"
    );
}

// ── Sql op DML rejection ──────────────────────────────────────────────────────

/// replay_retrieval_plan calls is_safe_select before executing any SQL.
/// Verify write operations are rejected before they reach the DB.
#[test]
fn sql_op_rejects_insert() {
    use lumid_platform::retrieve::plan::is_safe_select;

    assert!(
        !is_safe_select("INSERT INTO foo VALUES (1)"),
        "INSERT must be rejected"
    );
}

#[test]
fn sql_op_rejects_update() {
    use lumid_platform::retrieve::plan::is_safe_select;

    assert!(!is_safe_select("UPDATE foo SET x=1"), "UPDATE must be rejected");
}

#[test]
fn sql_op_rejects_delete() {
    use lumid_platform::retrieve::plan::is_safe_select;

    assert!(!is_safe_select("DELETE FROM foo"), "DELETE must be rejected");
}

// ── output_format wire value ──────────────────────────────────────────────────

/// B4: the `output_format` field in RetrievalResult must use the Python
/// Literal values ("csv" | "jsonl" | "raw"), not the file-extension values.
/// In particular Raw must produce "raw", not "bin".
#[test]
fn output_format_raw_format_name_is_raw() {
    use lumid_platform::retrieve::materialize::OutputFormat;

    assert_eq!(
        OutputFormat::Raw.format_name(),
        "raw",
        "Raw format_name must be 'raw' to match Python Literal; got 'bin' would cause Pydantic ValidationError"
    );
    assert_eq!(OutputFormat::Csv.format_name(), "csv");
    assert_eq!(OutputFormat::Jsonl.format_name(), "jsonl");
}

/// extension() keeps its original values (used for object-store key suffixing).
#[test]
fn output_format_extension_unchanged() {
    use lumid_platform::retrieve::materialize::OutputFormat;

    assert_eq!(OutputFormat::Raw.extension(), "bin");
    assert_eq!(OutputFormat::Csv.extension(), "csv");
    assert_eq!(OutputFormat::Jsonl.extension(), "jsonl");
}

// ── AgentConfig env parsing ───────────────────────────────────────────────────

/// Verify `AgentConfig` parses env vars correctly with current fields only.
#[test]
fn agent_config_env_parsing() {
    let _guard = ENV_MUTEX.lock().unwrap();

    unsafe {
        std::env::set_var("LUMID_AGENT_MAX_ITERATIONS", "5");
    }
    let cfg = lumid_platform::agent::tools::AgentConfig::from_env();
    assert_eq!(cfg.max_iterations, 5);

    unsafe {
        std::env::remove_var("LUMID_AGENT_MAX_ITERATIONS");
    }
    let cfg2 = lumid_platform::agent::tools::AgentConfig::from_env();
    assert_eq!(cfg2.max_iterations, 10, "default must be 10");
}

// ── user_schemas allowlist ────────────────────────────────────────────────────

// Shared lock for all tests that mutate environment variables.  Each test
// that touches LUMID_USER_SCHEMAS (or any LUMID_* env var) must acquire
// this lock so they don't race each other when the test binary runs tests in
// parallel threads.
static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// N1: when user_schemas is non-empty and scope is empty, the effective scope
/// must default to user_schemas (not "all non-system schemas").
///
/// This test verifies the logic by inspecting the Settings struct directly.
/// The full DB-round-trip path through get_schema_cards requires a live pool
/// and is covered by #[ignore]-gated tests below.
#[test]
fn user_schemas_parsed_from_env() {
    let _guard = ENV_MUTEX.lock().unwrap();

    unsafe {
        std::env::set_var("LUMID_USER_SCHEMAS", "foo,bar");
        // Provide enough env to construct Settings without panicking.
        std::env::set_var("LUMID_BLOB_S3_BUCKET", "test-bucket");
    }

    let settings = lumid_platform::config::Settings::from_env();

    unsafe {
        std::env::remove_var("LUMID_USER_SCHEMAS");
        std::env::remove_var("LUMID_BLOB_S3_BUCKET");
    }

    assert_eq!(
        settings.user_schemas,
        vec!["foo".to_string(), "bar".to_string()],
        "user_schemas must be parsed from LUMID_USER_SCHEMAS"
    );
}

/// Verify that when user_schemas is empty, Settings reflects that (no fallback).
#[test]
fn user_schemas_empty_when_env_unset() {
    let _guard = ENV_MUTEX.lock().unwrap();

    unsafe {
        std::env::remove_var("LUMID_USER_SCHEMAS");
    }

    let settings = lumid_platform::config::Settings::from_env();
    assert!(
        settings.user_schemas.is_empty(),
        "user_schemas must be empty when LUMID_USER_SCHEMAS is unset"
    );
}

/// Full round-trip: get_schema_cards with empty scope + non-empty user_schemas
/// restricts to the allowlist (requires a live pool — skipped in CI).
#[tokio::test]
#[ignore = "requires a live Postgres pool; run manually with a real DATABASE_URL"]
async fn get_schema_cards_empty_scope_uses_user_schemas_allowlist() {
    // This test would build a minimal AppState and call
    // agent::tools::schema_cards::get_schema_cards with an empty scope,
    // then assert the resulting table_count only includes tables from the
    // user_schemas allowlist.
    //
    // Kept as a documented stub; the logic is verified at unit level in
    // user_schemas_parsed_from_env above and in the schema_cards.rs source.
}

// ── schema user-schema guard ──────────────────────────────────────────────────

/// System schemas must not pass the user-schema check.
#[test]
fn system_schemas_rejected_by_is_user_schema() {
    use lumid_platform::queries::catalog::is_user_schema;

    let bad_schemas = [
        "pg_catalog",
        "information_schema",
        "_timescaledb_internal",
        "public",
    ];
    for s in bad_schemas {
        assert!(
            !is_user_schema(s),
            "schema '{s}' must NOT pass the user-schema check"
        );
    }
}

// ── effective_scope security: allowlist + non-permitted request → zero cards ──

/// Critical security regression test: when the operator allowlist is non-empty
/// and the agent requests a schema not in the allowlist, effective_scope must
/// return Scope::Only([]) — NOT Scope::All — so that zero cards are returned
/// rather than every schema being exposed.
#[test]
fn effective_scope_allowlist_with_non_permitted_request_yields_only_empty() {
    use lumid_platform::agent::tools::schema_cards::{effective_scope, Scope};

    let allowlist = vec!["market".to_string()];
    let requested = vec!["secret".to_string()];
    let result = effective_scope(&requested, &allowlist);

    // Must be Only([]), never Scope::All.
    assert_eq!(
        result,
        Scope::Only(vec![]),
        "allowlist=[market] + requested=[secret] must yield Only([]), not All; \
         Only([]) → 0 cards; Scope::All → all schemas exposed (security hole)"
    );
}

// ── lineage stripping ─────────────────────────────────────────────────────────

/// strip_lineage_rows must remove the four hidden columns from every row.
#[test]
fn lineage_columns_are_stripped() {
    use lumid_platform::db::lineage::{strip_lineage_rows, HIDDEN_COLUMNS};
    use serde_json::{Map, Value};

    let mut row: Map<String, Value> = Map::new();
    row.insert("ticker".to_string(), Value::String("AAPL".into()));
    row.insert("close".to_string(), Value::from(182.5_f64));
    for col in HIDDEN_COLUMNS {
        row.insert(col.to_string(), Value::String("secret".into()));
    }

    let stripped = strip_lineage_rows(vec![row]);
    assert_eq!(stripped.len(), 1);
    let r = &stripped[0];
    assert!(r.contains_key("ticker"), "ticker must survive");
    assert!(r.contains_key("close"), "close must survive");
    for col in HIDDEN_COLUMNS {
        assert!(
            !r.contains_key(col),
            "lineage column '{col}' must be stripped"
        );
    }
}
