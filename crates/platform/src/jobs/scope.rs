//! What a job token may touch: request classification, coverage of declared
//! sources, and a conservative SQL relation extractor. All pure.
//!
//! A job acts as its owner, but only within the sources it declared. This is
//! SCOPING (least privilege per job), layered on top of — not instead of — the
//! data plane's own boundaries: the retrieval reader role (public data only),
//! owner checks on tenant blobs / retrieval outputs, and the ingest ACL. So the
//! extractor is built to fail CLOSED: anything it cannot positively attribute
//! to a declared table is refused for job tokens.

use super::access::Access;
use super::refs::SourceRef;

/// What a request needs from the job's scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Need {
    /// Allowed for any live job (catalog metadata; its own retrieval outputs,
    /// which are owner-gated downstream).
    Allow,
    /// `GET /blobs/<key>` for a non-retrieval key.
    BlobKey(String),
    /// `GET /me/blobs/<sha256>`.
    MeBlob(String),
    /// `POST /retrieve` — the body decides (SQL relations, storage keys).
    Retrieve,
    /// `POST /profile` — EXPLAIN-only cost estimate; allowed when its SQL reads
    /// only declared sources (Lumilake's planner profiles a job's retrievals).
    Profile,
    /// `POST /ingest/<schema>/<table>[/stream|/file]`.
    Ingest { schema: String, table: String },
    /// A `/jobs/v1/jobs/<id>/…` route a job may use for ITSELF only
    /// (status, its events, reporting progress, renewing its token).
    OwnJob(String),
    /// Not available to job tokens.
    Deny(&'static str),
}

/// Classify a gated request for a job token.
pub fn classify(method: &str, path: &str, retrieval_prefix: &str) -> Need {
    let path = path.trim_end_matches('/');
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (method, segs.as_slice()) {
        // Catalog metadata: what exists, its shape and freshness. No rows.
        ("GET", ["catalog", "schemas"])
        | ("GET", ["catalog", "schemas", _, "tables"])
        | ("GET", ["catalog", "tables", _, _])
        | ("GET", ["catalog", "tables", _, _, "schema.json"])
        | ("GET", ["catalog", "sources"])
        | ("GET", ["freshness"]) => Need::Allow,
        ("GET", ["blobs", rest @ ..]) if !rest.is_empty() => {
            let key = rest.join("/");
            let pfx = retrieval_prefix.trim_matches('/');
            if !pfx.is_empty() && (key == pfx || key.starts_with(&format!("{pfx}/"))) {
                Need::Allow
            } else {
                Need::BlobKey(key)
            }
        }
        ("GET", ["me", "blobs", sha]) => Need::MeBlob(sha.to_ascii_lowercase()),
        ("POST", ["retrieve"]) => Need::Retrieve,
        ("POST", ["profile"]) => Need::Profile,
        ("POST", ["ingest", schema, table])
        | ("POST", ["ingest", schema, table, "stream" | "file"])
            if *schema != "blob" =>
        {
            Need::Ingest {
                schema: schema.to_string(),
                table: table.to_string(),
            }
        }
        ("GET", ["jobs", "v1", "jobs", id])
        | ("GET", ["jobs", "v1", "jobs", id, "events"])
        | ("POST", ["jobs", "v1", "jobs", id, "events" | "token"]) => Need::OwnJob(id.to_string()),
        ("GET", ["catalog", "lineage", ..]) => Need::Deny("job_tokens_cannot_read_lineage"),
        ("GET", ["blobs"]) | ("GET", ["me", "blobs"]) => Need::Deny("job_tokens_cannot_list"),
        _ => Need::Deny("route_not_available_to_job_tokens"),
    }
}

/// One declared source in a job's live scope.
#[derive(Clone, Debug)]
pub struct ScopedRef {
    /// The ref exactly as declared (the `jobs.job_sources.ref` key).
    pub raw: String,
    pub parsed: SourceRef,
    pub access: Access,
}

/// A job's live scope: the refs it declared that its owner may STILL use.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    /// This process's hub id (blob refs must name it).
    pub hub_id: String,
    /// The estate instance whose tables this process's SQL sees.
    pub instance: String,
    pub refs: Vec<ScopedRef>,
}

impl Scope {
    /// The first declared ref granting `access` (write implies read) that
    /// satisfies `pred`; `None` = not covered.
    fn find(&self, access: Access, pred: impl Fn(&SourceRef) -> bool) -> Option<&ScopedRef> {
        self.refs.iter().find(|r| {
            (r.access == access || (access == Access::Read && r.access == Access::Write))
                && pred(&r.parsed)
        })
    }

    /// Is `schema.table` on this instance covered for `access`?
    pub fn covers_table(&self, schema: &str, table: &str, access: Access) -> Option<&ScopedRef> {
        self.find(access, |r| match r {
            SourceRef::Table {
                instance,
                schema: s,
                table: t,
            } => {
                *instance == self.instance
                    && s.eq_ignore_ascii_case(schema)
                    && t.as_deref().is_none_or(|t| t.eq_ignore_ascii_case(table))
            }
            _ => false,
        })
    }

    /// Is the blob with this sha256 (on this hub) covered for reading?
    pub fn covers_sha(&self, sha: &str) -> Option<&ScopedRef> {
        self.find(Access::Read, |r| {
            matches!(r, SourceRef::Blob { hub, sha256 } if *hub == self.hub_id && sha256.eq_ignore_ascii_case(sha))
        })
    }

    /// Is an object key (as served by `GET /blobs/<key>`) covered for reading?
    /// Covered by a declared prefix on this hub, or — for content-addressed
    /// keys ending in `sha256=<hex>` — by a declared blob with that sha.
    pub fn covers_key(&self, key: &str) -> Option<&ScopedRef> {
        if key
            .split('/')
            .any(|s| s == ".." || s == "." || s.is_empty())
        {
            return None;
        }
        let by_prefix = self.find(Access::Read, |r| {
            matches!(r, SourceRef::BlobPrefix { hub, prefix } if *hub == self.hub_id && key.starts_with(prefix.as_str()))
        });
        if by_prefix.is_some() {
            return by_prefix;
        }
        key.rsplit('/')
            .next()
            .and_then(|last| last.strip_prefix("sha256="))
            .and_then(|sha| self.covers_sha(sha))
    }
}

// ─────────────────────────────────────────────── SQL relation extraction

/// Functions a job's SQL may never call: they read a relation named by a
/// string (invisible to relation extraction) or execute SQL text.
const JOB_FORBIDDEN_FUNCTIONS: &[&str] = &[
    "query_to_xml",
    "query_to_xmlschema",
    "query_to_xml_and_xmlschema",
    "cursor_to_xml",
    "cursor_to_xmlschema",
    "table_to_xml",
    "table_to_xmlschema",
    "table_to_xml_and_xmlschema",
    "schema_to_xml",
    "schema_to_xmlschema",
    "schema_to_xml_and_xmlschema",
    "database_to_xml",
    "database_to_xmlschema",
    "database_to_xml_and_xmlschema",
    "dblink",
    "dblink_exec",
    "dblink_open",
    "dblink_fetch",
    "pg_read_file",
    "pg_read_binary_file",
    "lo_get",
    "lo_import",
    "currval",
    "nextval",
    "setval",
    "set_config",
];

/// Words that may precede `(` without being a function call.
const NON_FUNCTION_WORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "and",
    "or",
    "not",
    "in",
    "exists",
    "any",
    "all",
    "some",
    "as",
    "on",
    "join",
    "lateral",
    "values",
    "with",
    "recursive",
    "union",
    "intersect",
    "except",
    "case",
    "when",
    "then",
    "else",
    "by",
    "over",
    "filter",
    "within",
    "array",
    "row",
    "using",
    "having",
    "limit",
    "offset",
    "materialized",
    "is",
    "between",
    "like",
    "ilike",
    "into",
];

/// Schemas whose relations are metadata, always readable (they are also what
/// `/catalog` serves).
const METADATA_SCHEMAS: &[&str] = &["pg_catalog", "information_schema"];

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Word(String),   // unquoted identifier/keyword, lowercased
    Quoted(String), // "Quoted" identifier, case kept
    Dot,
    Comma,
    LParen,
    RParen,
    Other,
}

fn tokenize(sql: &str) -> Result<Vec<Tok>, String> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 0usize;
                while i < b.len() {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                if depth != 0 {
                    return Err("unterminated comment".into());
                }
            }
            b'\'' => {
                i += 1;
                loop {
                    match b.get(i) {
                        None => return Err("unterminated string".into()),
                        Some(b'\'') if b.get(i + 1) == Some(&b'\'') => i += 2,
                        Some(b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(_) => i += 1,
                    }
                }
                out.push(Tok::Other);
            }
            b'$' => {
                // Dollar quote $tag$ … $tag$, or a positional parameter $1.
                let start = i;
                let mut j = i + 1;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                if b.get(j) == Some(&b'$') {
                    let tag = &sql[start..=j];
                    let body_start = j + 1;
                    match sql[body_start..].find(tag) {
                        Some(off) => i = body_start + off + tag.len(),
                        None => return Err("unterminated dollar quote".into()),
                    }
                    out.push(Tok::Other);
                } else {
                    i = j;
                    out.push(Tok::Other);
                }
            }
            b'"' => {
                let mut s = String::new();
                i += 1;
                loop {
                    match b.get(i) {
                        None => return Err("unterminated quoted identifier".into()),
                        Some(b'"') if b.get(i + 1) == Some(&b'"') => {
                            s.push('"');
                            i += 2;
                        }
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(_) => {
                            let ch = sql[i..].chars().next().unwrap();
                            s.push(ch);
                            i += ch.len_utf8();
                        }
                    }
                }
                out.push(Tok::Quoted(s));
            }
            b'.' => {
                // A dot between identifiers, or part of a number like 1.5.
                let prev_is_ident = matches!(out.last(), Some(Tok::Word(w)) if !w.starts_with(|c: char| c.is_ascii_digit()))
                    || matches!(out.last(), Some(Tok::Quoted(_)));
                out.push(if prev_is_ident { Tok::Dot } else { Tok::Other });
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            b'(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            b')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$')
                {
                    i += 1;
                }
                out.push(Tok::Word(sql[start..i].to_ascii_lowercase()));
            }
            c if c.is_ascii_whitespace() => i += 1,
            c if c >= 0x80 => {
                // Unquoted non-ASCII identifiers are not supported: refuse
                // rather than guess.
                return Err("non-ASCII outside quotes".into());
            }
            _ => {
                out.push(Tok::Other);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn ident_of(t: &Tok) -> Option<String> {
    match t {
        Tok::Word(w) => Some(w.clone()),
        Tok::Quoted(q) => Some(q.clone()),
        _ => None,
    }
}

/// A relation the query reads: `(schema, name)`; schema is `None` when the
/// reference is unqualified.
pub type Relation = (Option<String>, String);

/// Extract every relation a single SELECT reads, plus the CTE names it
/// defines. Errors on anything the extractor cannot attribute (which callers
/// treat as a refusal for job tokens).
pub fn extract_relations(sql: &str) -> Result<(Vec<Relation>, Vec<String>), String> {
    let toks = tokenize(sql)?;
    let mut rels: Vec<Relation> = Vec::new();
    let mut ctes: Vec<String> = Vec::new();
    // Paren stack: true = function-call parens (FROM inside means e.g.
    // `extract(year from ts)`, not a relation).
    let mut stack: Vec<bool> = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        match &toks[i] {
            Tok::LParen => {
                let is_func = matches!(i.checked_sub(1).map(|p| &toks[p]),
                    Some(Tok::Word(w)) if !NON_FUNCTION_WORDS.contains(&w.as_str()))
                    || matches!(i.checked_sub(1).map(|p| &toks[p]), Some(Tok::Quoted(_)));
                if is_func {
                    // Name of the called function (last identifier before '(').
                    if let Some(name) = i.checked_sub(1).and_then(|p| ident_of(&toks[p])) {
                        if JOB_FORBIDDEN_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str()) {
                            return Err(format!(
                                "function '{name}' is not available to job tokens"
                            ));
                        }
                    }
                }
                stack.push(is_func);
            }
            Tok::RParen => {
                if stack.pop().is_none() {
                    return Err("unbalanced parentheses".into());
                }
            }
            Tok::Word(w) if w == "as" => {
                // CTE definition: <name> [ (cols) ] AS [NOT] [MATERIALIZED] (
                let mut j = i + 1;
                while matches!(toks.get(j), Some(Tok::Word(x)) if x == "not" || x == "materialized")
                {
                    j += 1;
                }
                if matches!(toks.get(j), Some(Tok::LParen)) {
                    let mut k = i;
                    if matches!(toks.get(k.wrapping_sub(1)), Some(Tok::RParen)) {
                        // skip a column list
                        let mut depth = 0i32;
                        while k > 0 {
                            k -= 1;
                            match toks[k] {
                                Tok::RParen => depth += 1,
                                Tok::LParen => {
                                    depth -= 1;
                                    if depth == 0 {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    if let Some(name) = k.checked_sub(1).and_then(|p| ident_of(&toks[p])) {
                        ctes.push(name);
                    }
                }
            }
            // `TABLE name` is SQL shorthand for `SELECT * FROM name` (e.g.
            // `… UNION TABLE secret.t`); `table` is reserved, so it can never
            // be an unquoted column name.
            Tok::Word(w)
                if (w == "from" || w == "join" || w == "table") && stack.last() != Some(&true) =>
            {
                let comma_list = w == "from";
                let mut j = i + 1;
                loop {
                    while matches!(toks.get(j), Some(Tok::Word(x)) if x == "only" || x == "lateral")
                    {
                        j += 1;
                    }
                    match toks.get(j) {
                        Some(Tok::LParen) => break, // subquery / VALUES: scanned as we go
                        Some(t @ (Tok::Word(_) | Tok::Quoted(_))) => {
                            let first = ident_of(t).unwrap();
                            let (rel, next) = if matches!(toks.get(j + 1), Some(Tok::Dot)) {
                                match toks.get(j + 2).and_then(ident_of) {
                                    Some(second) => {
                                        if matches!(toks.get(j + 3), Some(Tok::Dot)) {
                                            return Err("three-part names are not supported".into());
                                        }
                                        ((Some(first), second), j + 3)
                                    }
                                    None => return Err("dangling '.' in a relation name".into()),
                                }
                            } else {
                                ((None, first), j + 1)
                            };
                            if matches!(toks.get(next), Some(Tok::LParen)) {
                                // A table function: `from generate_series(…)`.
                                // Forbidden names are caught at the '('.
                                break;
                            }
                            rels.push(rel);
                            if !comma_list {
                                break;
                            }
                            // Skip an alias, then continue on ',' (FROM a, b).
                            let mut k = next;
                            if matches!(toks.get(k), Some(Tok::Word(x)) if x == "as") {
                                k += 1;
                            }
                            if matches!(toks.get(k), Some(Tok::Word(x)) if !is_clause_word(x))
                                || matches!(toks.get(k), Some(Tok::Quoted(_)))
                            {
                                k += 1;
                                // alias column list: a AS x(c1, c2)
                                if matches!(toks.get(k), Some(Tok::LParen)) {
                                    let mut depth = 0;
                                    while k < toks.len() {
                                        match toks[k] {
                                            Tok::LParen => depth += 1,
                                            Tok::RParen => {
                                                depth -= 1;
                                                if depth == 0 {
                                                    k += 1;
                                                    break;
                                                }
                                            }
                                            _ => {}
                                        }
                                        k += 1;
                                    }
                                }
                            }
                            if matches!(toks.get(k), Some(Tok::Comma)) {
                                j = k + 1;
                                continue;
                            }
                            break;
                        }
                        _ => break,
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    if !stack.is_empty() {
        return Err("unbalanced parentheses".into());
    }
    Ok((rels, ctes))
}

fn is_clause_word(w: &str) -> bool {
    matches!(
        w,
        "where"
            | "group"
            | "order"
            | "limit"
            | "offset"
            | "having"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "full"
            | "cross"
            | "natural"
            | "on"
            | "using"
            | "union"
            | "intersect"
            | "except"
            | "window"
            | "fetch"
            | "for"
            | "tablesample"
            | "lateral"
    )
}

/// Check a job's SQL against its scope. `Ok` carries the declared refs it
/// reads (for byte accounting); `Err` a caller-safe reason.
pub fn check_sql(scope: &Scope, sql: &str) -> Result<Vec<String>, String> {
    let (rels, ctes) = extract_relations(sql)?;
    let mut used: Vec<String> = Vec::new();
    for (schema, name) in &rels {
        match schema {
            None => {
                if !ctes.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                    return Err(format!(
                        "relation '{name}' is unqualified — job SQL must name schema.table"
                    ));
                }
            }
            Some(s) if METADATA_SCHEMAS.contains(&s.to_ascii_lowercase().as_str()) => {}
            Some(s) => match scope.covers_table(s, name, Access::Read) {
                Some(r) => {
                    if !used.contains(&r.raw) {
                        used.push(r.raw.clone());
                    }
                }
                None => {
                    return Err(format!(
                        "{}://{s}.{name} is not among this job's declared sources",
                        scope.instance
                    ))
                }
            },
        }
    }
    Ok(used)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::refs::parse;

    fn scope(refs: &[(&str, Access)]) -> Scope {
        Scope {
            hub_id: "home".into(),
            instance: "lumid-data".into(),
            refs: refs
                .iter()
                .map(|(r, a)| ScopedRef {
                    raw: r.to_string(),
                    parsed: parse(r).unwrap(),
                    access: *a,
                })
                .collect(),
        }
    }

    #[test]
    fn classify_routes() {
        let rp = "retrievals";
        assert_eq!(classify("GET", "/catalog/schemas", rp), Need::Allow);
        assert_eq!(
            classify("GET", "/catalog/tables/ukb_demo/phenotype/schema.json", rp),
            Need::Allow
        );
        assert_eq!(
            classify("GET", "/blobs/retrievals/r1/result.jsonl", rp),
            Need::Allow
        );
        assert_eq!(
            classify("GET", "/blobs/lumilake/x/sha256=ab", rp),
            Need::BlobKey("lumilake/x/sha256=ab".into())
        );
        assert_eq!(
            classify("GET", "/me/blobs/ABC", rp),
            Need::MeBlob("abc".into())
        );
        assert_eq!(classify("POST", "/retrieve", rp), Need::Retrieve);
        assert_eq!(classify("POST", "/profile", rp), Need::Profile);
        assert_eq!(
            classify("POST", "/jobs/v1/jobs/j1/events", rp),
            Need::OwnJob("j1".into())
        );
        assert_eq!(
            classify("POST", "/jobs/v1/jobs/j1/token", rp),
            Need::OwnJob("j1".into())
        );
        assert_eq!(
            classify("GET", "/jobs/v1/jobs/j1", rp),
            Need::OwnJob("j1".into())
        );
        assert_eq!(
            classify("POST", "/ingest/exp/results/stream", rp),
            Need::Ingest {
                schema: "exp".into(),
                table: "results".into()
            }
        );
        for (m, p) in [
            ("POST", "/ingest/blob"),
            ("GET", "/catalog/lineage/runs"),
            ("GET", "/blobs"),
            ("GET", "/me/blobs"),
            ("POST", "/me/blobs"),
            ("POST", "/agent/v1"),
            ("POST", "/v1/chat/completions"),
            ("POST", "/admin/ingress/acl"),
            ("GET", "/events/recent"),
            ("POST", "/sync/apply"),
            ("POST", "/jobs/v1/jobs"),
            ("POST", "/jobs/v1/plan"),
            ("GET", "/jobs/v1/jobs"),
            ("POST", "/jobs/v1/sources/s/grants"),
            ("POST", "/jobs/v1/jobs/j/cancel"),
        ] {
            assert!(matches!(classify(m, p, rp), Need::Deny(_)), "{m} {p}");
        }
    }

    #[test]
    fn coverage_of_tables_blobs_and_prefixes() {
        let sha = "ab".repeat(32);
        let s = scope(&[
            ("lumid-data://ukb_demo.phenotype", Access::Read),
            ("lumid-data://reference", Access::Read),
            ("findata://market.ohlc_1d", Access::Read),
            ("lumid-data://exp.results", Access::Write),
            ("data://home/lumilake/frames/", Access::Read),
            (&format!("data://home/blobs/{sha}"), Access::Read),
            ("data://nus/other/", Access::Read),
        ]);
        assert!(s
            .covers_table("ukb_demo", "phenotype", Access::Read)
            .is_some());
        assert!(s
            .covers_table("UKB_DEMO", "Phenotype", Access::Read)
            .is_some());
        assert!(s
            .covers_table("ukb_demo", "genotype", Access::Read)
            .is_none());
        assert!(s
            .covers_table("reference", "anything", Access::Read)
            .is_some()); // whole schema
        assert!(s.covers_table("market", "ohlc_1d", Access::Read).is_none()); // findata is another instance
        assert!(s.covers_table("exp", "results", Access::Write).is_some());
        assert!(s.covers_table("exp", "results", Access::Read).is_some()); // write implies read
        assert!(s
            .covers_table("ukb_demo", "phenotype", Access::Write)
            .is_none());
        assert!(s.covers_key("lumilake/frames/ep1/k.png").is_some());
        assert!(s.covers_key("lumilake/other/x").is_none());
        assert!(s.covers_key("other/x").is_none()); // that prefix is on hub nus
        assert!(s.covers_key("lumilake/frames/../secrets/x").is_none());
        assert!(s.covers_key(&format!("blob/sha256={sha}")).is_some());
        assert!(s.covers_key(&format!("tenants/u-1/sha256={sha}")).is_some());
        assert!(s
            .covers_key(&format!("blob/sha256={}", "cd".repeat(32)))
            .is_none());
        assert!(s.covers_sha(&sha.to_uppercase()).is_some());
    }

    #[test]
    fn sql_extraction_basics() {
        let (r, c) = extract_relations(
            "SELECT a FROM ukb_demo.phenotype p JOIN reference.etf e ON e.id = p.id",
        )
        .unwrap();
        assert_eq!(
            r,
            vec![
                (Some("ukb_demo".into()), "phenotype".into()),
                (Some("reference".into()), "etf".into())
            ]
        );
        assert!(c.is_empty());
        let (r, _) =
            extract_relations("select * from a.b, c.d as x, \"Sch\".\"T\" y where 1=1").unwrap();
        assert_eq!(
            r,
            vec![
                (Some("a".into()), "b".into()),
                (Some("c".into()), "d".into()),
                (Some("Sch".into()), "T".into())
            ]
        );
    }

    #[test]
    fn from_inside_functions_is_not_a_relation() {
        let (r, _) = extract_relations(
            "select extract(year from ts), substring(s from 2 for 3), trim(both ' ' from s) from x.t",
        )
        .unwrap();
        assert_eq!(r, vec![(Some("x".into()), "t".into())]);
    }

    #[test]
    fn subqueries_ctes_and_strings() {
        let (r, c) = extract_relations(
            "with recent as (select * from s.t where note = 'from evil.x'), \
                  agg (n) as materialized (select count(*) from recent) \
             select * from recent, agg where exists (select 1 from s.u) \
               and id in (select id from (select id from s.v) q)",
        )
        .unwrap();
        assert_eq!(c, vec!["recent".to_string(), "agg".to_string()]);
        assert!(r.contains(&(Some("s".into()), "t".into())));
        assert!(r.contains(&(Some("s".into()), "u".into())));
        assert!(r.contains(&(Some("s".into()), "v".into())));
        assert!(!r.iter().any(|(_, n)| n == "x")); // inside a string literal
    }

    #[test]
    fn check_sql_enforces_declared_tables() {
        let s = scope(&[
            ("lumid-data://ukb_demo.phenotype", Access::Read),
            ("lumid-data://reference", Access::Read),
        ]);
        assert_eq!(
            check_sql(&s, "select * from ukb_demo.phenotype limit 5").unwrap(),
            vec!["lumid-data://ukb_demo.phenotype".to_string()]
        );
        assert!(check_sql(&s, "select * from reference.etf_asset_exposure").is_ok());
        assert!(check_sql(
            &s,
            "with x as (select * from ukb_demo.phenotype) select * from x"
        )
        .is_ok());
        assert!(check_sql(&s, "select relname from pg_catalog.pg_class").is_ok());
        let e = check_sql(&s, "select * from provenance.runs").unwrap_err();
        assert!(e.contains("provenance.runs"), "{e}");
        assert!(check_sql(&s, "select * from phenotype")
            .unwrap_err()
            .contains("unqualified"));
        assert!(check_sql(
            &s,
            "select * from ukb_demo.phenotype where id in (select id from ukb_demo.genotype)"
        )
        .is_err());
        assert!(check_sql(&s, "select 1 from ukb_demo.phenotype a, ukb_demo.secret b").is_err());
        // The TABLE shorthand reads a relation without FROM.
        assert!(check_sql(
            &s,
            "select * from ukb_demo.phenotype union all table provenance.runs"
        )
        .is_err());
        assert!(check_sql(&s, "table ukb_demo.phenotype").is_ok());
    }

    #[test]
    fn check_sql_blocks_relation_hiding_functions() {
        let s = scope(&[("lumid-data://ukb_demo.phenotype", Access::Read)]);
        for q in [
            "select query_to_xml('select * from provenance.runs', true, false, '')",
            "select table_to_xml('provenance.runs', true, false, '')",
            "select * from ukb_demo.phenotype, lateral (select pg_catalog.query_to_xml('x', true, false, '')) q",
            "select \"query_to_xml\"('select 1', true, false, '')",
            "select dblink('host=x', 'select 1')",
            "select set_config('role', 'postgres', true)",
        ] {
            assert!(check_sql(&s, q).is_err(), "{q}");
        }
    }

    #[test]
    fn extractor_fails_closed_on_what_it_cannot_read() {
        for q in [
            "select * from a.b.c",
            "select * from a.",
            "select ((1)",
            "select 'unterminated",
            "select $x$ body",
            "select * from tablé",
        ] {
            assert!(extract_relations(q).is_err(), "{q}");
        }
        // Comments and dollar-quoted bodies are not scanned for relations.
        let (r, _) = extract_relations("select 1 /* from secret.t */ -- from secret.u\n from s.ok where x = $q$from secret.v$q$").unwrap();
        assert_eq!(r, vec![(Some("s".into()), "ok".into())]);
    }
}
