//! Retrieval plan types, matching the Python `lumid_data.sdk.schemas` JSON contract.
//!
//! JSON wire shape:
//! ```json
//! {
//!   "plan": [
//!     {"op": "sql", "query": "SELECT ..."},
//!     {"op": "storage_get", "bucket": "b", "key": "k"}
//!   ]
//! }
//! ```

use serde::{Deserialize, Serialize};

/// A single SQL read operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SqlOp {
    pub query: String,
}

/// A single object-storage read operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageGetOp {
    /// Bucket name. Optional — defaults to `settings.blob_s3_bucket` at replay time.
    #[serde(default)]
    pub bucket: Option<String>,
    pub key: String,
}

/// One step in a retrieval plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RetrievalOp {
    Sql(SqlOp),
    StorageGet(StorageGetOp),
}

/// Top-level plan envelope, mirroring `RetrievalPlan` in Python.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalPlan {
    pub plan: Vec<RetrievalOp>,
    #[serde(default)]
    pub expected_rowcount_or_size: Option<i64>,
    #[serde(default)]
    pub rationale: Option<String>,
}

/// Per-op access record, mirroring `AccessStep` in Python.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessChainStep {
    /// `"sql"` or `"storage_get"`.
    pub op: String,
    pub query: Option<String>,
    pub bucket: Option<String>,
    pub key: Option<String>,
    /// Row count (for SQL) or bytes transferred (for storage_get).
    pub rows_or_bytes: i64,
    /// Wall-clock elapsed milliseconds for this op.
    pub ms: u64,
}

/// Full retrieval result returned to the agent and embedded in the `done` frame.
/// Field names match the Python `RetrievalResult` model exactly so FlowMesh's
/// `AgentConnector` works unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalResult {
    pub run_id: String,
    pub materialized_uri: String,
    pub signed_url: String,
    pub output_format: String,
    pub access_chain: Vec<AccessChainStep>,
    pub rowcount: i64,
    pub size_bytes: i64,
    pub tokens_in: i64,
    pub tokens_out: i64,
    pub steps_taken: i64,
    pub replay_latency_ms: i64,
    pub transcript_url: String,
}

// ── SQL safety ────────────────────────────────────────────────────────────────

/// Returns `true` iff `query` is a single, read-only SELECT statement.
///
/// Logic: lower-case the text, strip `--` and `/* */` comments, strip
/// leading whitespace, confirm it starts with `select`, and reject any
/// DML/DDL keywords appearing as the first token of any semicolon-separated
/// statement other than a trailing empty one.
pub fn is_safe_select(query: &str) -> bool {
    let stripped = strip_sql_comments(query);
    let lowered = stripped.to_lowercase();
    let stmts: Vec<&str> = lowered.split(';').collect();
    // Allow exactly one non-empty statement (trailing `;` → two items, last empty).
    let non_empty: Vec<&str> = stmts
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if non_empty.len() != 1 {
        return false;
    }
    let stmt = non_empty[0].trim();
    // Allow read-only CTEs (`WITH cte AS (...) SELECT ...`) in addition to a
    // bare `SELECT`. A *data-modifying* CTE necessarily contains an
    // insert/update/delete keyword (caught below) and is rejected anyway by the
    // `READ ONLY` transaction at execution time — so admitting a leading `with`
    // is safe and avoids rejecting the most common analytical query shape.
    let starts_ok = is_word_prefix(stmt, "select") || is_word_prefix(stmt, "with");
    if !starts_ok {
        return false;
    }
    // Reject any DML/DDL keyword anywhere in the (single) statement. This guards
    // writable CTEs and injected multi-statement via embedded semicolons inside
    // string literals (best-effort: trust the DB role + READ ONLY txn for the rest).
    if contains_dml_keyword(stmt) {
        return false;
    }
    // Reject superuser-only read primitives + sensitive catalogs. These are
    // SELECT-shaped (so the checks above miss them) and the `READ ONLY` txn does
    // NOT block reads — a superuser pool could otherwise exfiltrate host files
    // (`pg_read_file`) or credential hashes (`pg_authid`). Defense-in-depth that
    // matters most when the pool role is NOT de-escalated via `retrieval_db_role`
    // (see replayer). Word-boundary matched, so `pg_authid` ≠ a column called
    // `pg_authid_note`.
    !contains_dangerous_read(stmt)
}

/// True when `stmt` begins with `word` followed by a word boundary (so `selection`
/// or `withhold` don't count as `select` / `with`).
fn is_word_prefix(stmt: &str, word: &str) -> bool {
    match stmt.strip_prefix(word) {
        Some(rest) => rest
            .bytes()
            .next()
            .map(|b| !b.is_ascii_alphanumeric() && b != b'_')
            .unwrap_or(true),
        None => false,
    }
}

const FORBIDDEN_KEYWORDS: &[&str] = &[
    "insert", "update", "delete", "merge", "copy", "create", "drop", "alter",
    "truncate", "grant", "revoke", "call", "execute",
];

/// Superuser-only read functions + sensitive system catalogs/views. SELECT-shaped,
/// so the DML scan and `READ ONLY` txn don't catch them. Lower-case, matched on
/// word boundaries.
const FORBIDDEN_READ_TOKENS: &[&str] = &[
    // Filesystem / server-side read primitives (superuser / pg_read_server_files).
    "pg_read_file", "pg_read_binary_file", "pg_stat_file",
    "pg_ls_dir", "pg_ls_logdir", "pg_ls_waldir", "pg_ls_tmpdir", "pg_ls_archive_statusdir",
    // Large objects (can read server files via lo_import).
    "lo_import", "lo_export", "lo_get",
    // Outbound connections.
    "dblink", "dblink_connect",
    // Credential / secret-bearing catalogs.
    "pg_authid", "pg_shadow", "pg_user_mappings", "pg_user_mapping", "pg_largeobject",
    // DoS via blocking.
    "pg_sleep", "pg_sleep_for", "pg_sleep_until",
    // Session advisory locks (survive txn rollback → poison the pooled connection).
    "pg_advisory_lock", "pg_advisory_lock_shared",
    "pg_try_advisory_lock", "pg_try_advisory_lock_shared",
    "pg_advisory_unlock", "pg_advisory_unlock_shared", "pg_advisory_unlock_all",
    "pg_advisory_xact_lock", "pg_advisory_xact_lock_shared",
    "pg_try_advisory_xact_lock", "pg_try_advisory_xact_lock_shared",
    // Backend / server control (defense-in-depth; NOSUPERUSER blocks them anyway).
    "pg_terminate_backend", "pg_cancel_backend", "pg_reload_conf", "pg_rotate_logfile",
];

fn contains_dml_keyword(lowered_stmt: &str) -> bool {
    FORBIDDEN_KEYWORDS.iter().any(|kw| word_present(lowered_stmt, kw))
}

fn contains_dangerous_read(lowered_stmt: &str) -> bool {
    FORBIDDEN_READ_TOKENS.iter().any(|kw| word_present(lowered_stmt, kw))
}

/// True if `word` appears in `lowered_stmt` at least once delimited by word
/// boundaries (start/end or a non-`[A-Za-z0-9_]` char on each side), and the
/// occurrence is **outside** a single-quoted string literal. Scans every
/// occurrence so a non-boundary hit early in the string doesn't mask a real one
/// later (e.g. `xcopy ...; copy`).
fn word_present(lowered_stmt: &str, word: &str) -> bool {
    let bytes = lowered_stmt.as_bytes();
    let n = bytes.len();
    // Track whether we're inside a single-quoted string literal so that e.g.
    // `WHERE action = 'delete'` doesn't trigger the DML-keyword check.
    let mut in_string = false;
    let mut i = 0;
    while i < n {
        if in_string {
            if bytes[i] == b'\'' {
                // Handle escaped quote ('' inside a string).
                if i + 1 < n && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_string = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'\'' {
            in_string = true;
            i += 1;
            continue;
        }
        // Try to match `word` at position `i`.
        if lowered_stmt[i..].starts_with(word) {
            let pos = i;
            let before = pos == 0 || {
                let ch = bytes[pos - 1];
                !ch.is_ascii_alphanumeric() && ch != b'_'
            };
            let after_pos = pos + word.len();
            let after = after_pos >= n || {
                let ch = bytes[after_pos];
                !ch.is_ascii_alphanumeric() && ch != b'_'
            };
            if before && after {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Strip `--` line comments and `/* */` block comments from SQL text.
fn strip_sql_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        if i + 1 < n && chars[i] == '-' && chars[i + 1] == '-' {
            while i < n && chars[i] != '\n' {
                i += 1;
            }
        } else if i + 1 < n && chars[i] == '/' && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < n && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2; // skip */
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_json_round_trip() {
        let plan = RetrievalPlan {
            plan: vec![
                RetrievalOp::Sql(SqlOp {
                    query: "SELECT 1".to_string(),
                }),
                RetrievalOp::StorageGet(StorageGetOp {
                    bucket: Some("mybucket".to_string()),
                    key: "path/to/file.csv".to_string(),
                }),
            ],
            expected_rowcount_or_size: Some(100),
            rationale: Some("test".to_string()),
        };
        let json = serde_json::to_string(&plan).unwrap();
        let decoded: RetrievalPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(plan, decoded);
    }

    #[test]
    fn plan_from_python_sdk_example() {
        let raw = r#"{
            "plan": [
                {"op": "sql", "query": "SELECT ticker, close FROM market.ohlc_daily LIMIT 10"},
                {"op": "storage_get", "bucket": "lumid-data", "key": "reports/q1.pdf"}
            ]
        }"#;
        let plan: RetrievalPlan = serde_json::from_str(raw).unwrap();
        assert_eq!(plan.plan.len(), 2);
        match &plan.plan[0] {
            RetrievalOp::Sql(op) => assert!(op.query.contains("SELECT")),
            _ => panic!("expected sql op"),
        }
        match &plan.plan[1] {
            RetrievalOp::StorageGet(op) => assert_eq!(op.key, "reports/q1.pdf"),
            _ => panic!("expected storage_get op"),
        }
    }

    #[test]
    fn safe_select_accepts_valid_select() {
        assert!(is_safe_select("SELECT * FROM foo"));
        assert!(is_safe_select("  select id, name from bar where x = 1  "));
        assert!(is_safe_select("SELECT 1;"));
    }

    #[test]
    fn safe_select_accepts_read_only_cte() {
        assert!(is_safe_select(
            "WITH recent AS (SELECT id FROM t WHERE x > 1) SELECT * FROM recent"
        ));
        assert!(is_safe_select(
            "  with a as (select 1), b as (select 2) select * from a, b  "
        ));
    }

    #[test]
    fn safe_select_rejects_writable_cte() {
        // Data-modifying CTEs still contain a forbidden keyword → rejected.
        assert!(!is_safe_select(
            "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"
        ));
        assert!(!is_safe_select(
            "WITH i AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM i"
        ));
    }

    #[test]
    fn safe_select_rejects_lookalike_prefixes() {
        // `withhold` / `selection` must not be treated as `with` / `select` starts.
        assert!(!is_safe_select("withhold tax FROM t"));
        assert!(!is_safe_select("selection FROM t"));
    }

    #[test]
    fn safe_select_rejects_dangerous_read_primitives() {
        // Superuser file/secret reads are SELECT-shaped but must be rejected.
        assert!(!is_safe_select("SELECT pg_read_file('/etc/passwd')"));
        assert!(!is_safe_select("SELECT pg_read_binary_file('/etc/shadow')"));
        assert!(!is_safe_select("SELECT * FROM pg_ls_dir('/')"));
        assert!(!is_safe_select("SELECT rolpassword FROM pg_authid"));
        assert!(!is_safe_select("SELECT * FROM pg_shadow"));
        assert!(!is_safe_select("select lo_import('/etc/passwd')"));
        assert!(!is_safe_select("SELECT dblink('...', 'select 1')"));
    }

    #[test]
    fn safe_select_rejects_dos_sleep_functions() {
        assert!(!is_safe_select("SELECT pg_sleep(10)"));
        assert!(!is_safe_select("SELECT pg_sleep_for('5 seconds')"));
        assert!(!is_safe_select("SELECT pg_sleep_until('2030-01-01')"));
    }

    #[test]
    fn safe_select_rejects_advisory_lock_functions() {
        assert!(!is_safe_select("SELECT pg_advisory_lock(1)"));
        assert!(!is_safe_select("SELECT pg_advisory_lock_shared(1)"));
        assert!(!is_safe_select("SELECT pg_try_advisory_lock(1)"));
        assert!(!is_safe_select("SELECT pg_try_advisory_lock_shared(1)"));
        assert!(!is_safe_select("SELECT pg_advisory_unlock(1)"));
        assert!(!is_safe_select("SELECT pg_advisory_unlock_shared(1)"));
        assert!(!is_safe_select("SELECT pg_advisory_unlock_all()"));
        assert!(!is_safe_select("SELECT pg_advisory_xact_lock(1)"));
        assert!(!is_safe_select("SELECT pg_advisory_xact_lock_shared(1)"));
        assert!(!is_safe_select("SELECT pg_try_advisory_xact_lock(1)"));
        assert!(!is_safe_select("SELECT pg_try_advisory_xact_lock_shared(1)"));
    }

    #[test]
    fn safe_select_rejects_backend_control_functions() {
        assert!(!is_safe_select("SELECT pg_terminate_backend(123)"));
        assert!(!is_safe_select("SELECT pg_cancel_backend(123)"));
        assert!(!is_safe_select("SELECT pg_reload_conf()"));
        assert!(!is_safe_select("SELECT pg_rotate_logfile()"));
    }

    #[test]
    fn safe_select_still_accepts_normal_queries() {
        // Plain column query must not be affected by the new denylist entries.
        assert!(is_safe_select("SELECT col FROM schema.tbl WHERE id = 1"));
        // Read-only CTE must still be accepted.
        assert!(is_safe_select(
            "WITH recent AS (SELECT id, ts FROM events WHERE ts > NOW() - INTERVAL '1 day') \
             SELECT * FROM recent ORDER BY ts DESC LIMIT 100"
        ));
    }

    #[test]
    fn safe_select_allows_lookalike_identifiers() {
        // Word-boundary matching: a column whose name merely contains a token
        // must not be rejected.
        assert!(is_safe_select("SELECT pg_authid_note FROM app.t"));
        assert!(is_safe_select("SELECT my_copy_count FROM app.t"));
    }

    #[test]
    fn safe_select_rejects_dml() {
        assert!(!is_safe_select("INSERT INTO foo VALUES (1)"));
        assert!(!is_safe_select("UPDATE foo SET x=1"));
        assert!(!is_safe_select("DELETE FROM foo"));
        assert!(!is_safe_select("DROP TABLE foo"));
        assert!(!is_safe_select("TRUNCATE foo"));
        assert!(!is_safe_select("CREATE TABLE t (id int)"));
        assert!(!is_safe_select("GRANT SELECT ON foo TO r"));
        assert!(!is_safe_select("REVOKE SELECT ON foo FROM r"));
        assert!(!is_safe_select("ALTER TABLE foo ADD COLUMN x int"));
        assert!(!is_safe_select("CALL my_proc()"));
    }

    #[test]
    fn safe_select_rejects_multi_statement() {
        assert!(!is_safe_select("SELECT 1; SELECT 2"));
        assert!(!is_safe_select("SELECT 1; DELETE FROM foo"));
    }

    #[test]
    fn safe_select_strips_comments() {
        assert!(is_safe_select("-- pick data\nSELECT * FROM foo"));
        assert!(is_safe_select("/* comment */ SELECT * FROM foo"));
    }

    #[test]
    fn safe_select_rejects_non_select_start() {
        assert!(!is_safe_select("MERGE INTO t USING s ON (t.id=s.id)"));
        assert!(!is_safe_select("COPY foo TO STDOUT"));
    }
}
