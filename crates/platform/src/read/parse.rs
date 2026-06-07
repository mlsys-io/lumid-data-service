//! A bounded read-only SELECT parser: `spec.sql: String` → [`QueryIr`].
//!
//! The read layer is GET-only — no DML, no DDL — which bounds the grammar hard:
//! `SELECT <items> FROM <table> [JOIN …] [WHERE <conjuncts>] [GROUP BY …]
//! [ORDER BY …] [LIMIT …]`. A spec that fits parses cleanly and gains
//! dual-dialect lowering; **anything outside the grammar returns `None`**, and
//! the caller pins that spec to the raw-SQL Postgres path with a `warn!` naming
//! the construct.
//!
//! Deliberately conservative: it parses the *clause skeleton* and the WHERE
//! predicate grammar (comparisons, IN, AND), keeping select/group-by/order-by
//! *items* as verbatim text (the dialects re-emit them, lowering only the
//! `:name` binds). Constructs that don't fit — set operators, subqueries in
//! FROM, CTEs, FILTER, OVER windows, lateral joins, `now() - interval` in
//! comparisons against non-key columns, etc. — fall back. The fallback is the
//! whole point: it is a graceful, opt-in migration, never a silent rewrite.

use super::ir::{
    CmpOp, Expr, Join, JoinKind, Limit, OrderItem, Predicate, QueryIr, SelectItem, SortDir,
    TableRef,
};

/// Outcome of attempting to parse a spec's SQL into an IR.
pub enum ParseOutcome {
    /// The SQL fit the bounded grammar. Boxed so the enum stays small (the IR is
    /// far larger than the `Fallback` string — clippy `large_enum_variant`).
    Ir(Box<QueryIr>),
    /// The SQL used a construct outside the grammar; the named construct is for
    /// the operator-facing `warn!`. The spec stays on the raw-SQL Postgres path.
    Fallback(String),
}

/// Try to parse a read spec's SQL into a [`QueryIr`]. Returns `Fallback(reason)`
/// for anything outside the bounded read-only SELECT grammar.
pub fn parse_select(sql: &str) -> ParseOutcome {
    let norm = normalize_ws(sql);
    let lower = norm.to_ascii_lowercase();

    // Hard bail-outs: constructs we explicitly don't model. Naming them in the
    // fallback reason makes the boot-time warn actionable.
    for (needle, label) in [
        (" union ", "UNION"),
        (" intersect ", "INTERSECT"),
        (" except ", "EXCEPT"),
        ("with ", "CTE (WITH)"),
        (" over (", "window function (OVER)"),
        (" over(", "window function (OVER)"),
        (" filter (", "aggregate FILTER"),
        (" filter(", "aggregate FILTER"),
        ("interval ", "interval arithmetic"),
        ("jsonb_array_elements", "jsonb_array_elements"),
        (" case ", "CASE expression"),
        ("distinct ", "DISTINCT"),
        (" having ", "HAVING"),
    ] {
        if lower.contains(needle) {
            return ParseOutcome::Fallback(label.to_string());
        }
    }
    if !lower.starts_with("select ") {
        return ParseOutcome::Fallback("non-SELECT statement".to_string());
    }

    // Split the statement into top-level clauses by keyword. We operate on the
    // normalized (single-spaced) string; the dialects don't depend on the
    // original whitespace because Postgres lowers the *unmodified* substituted
    // text (not the IR), and ClickHouse re-emits from structure.
    let clauses = match split_clauses(&norm) {
        Some(c) => c,
        None => return ParseOutcome::Fallback("unrecognised clause structure".to_string()),
    };

    // FROM (+ optional alias). A subquery / comma-join in FROM falls back.
    let from = match parse_table_ref(&clauses.from) {
        Some(t) => t,
        None => return ParseOutcome::Fallback("complex FROM (subquery/comma-join)".to_string()),
    };

    // JOINs.
    let mut joins = Vec::new();
    for (kind, rest) in &clauses.joins {
        let (tbl_str, on_str) = match rest.to_ascii_lowercase().find(" on ") {
            Some(pos) => (rest[..pos].trim(), rest[pos + 4..].trim()),
            None => return ParseOutcome::Fallback("JOIN without ON".to_string()),
        };
        let table = match parse_table_ref(tbl_str) {
            Some(t) => t,
            None => return ParseOutcome::Fallback("complex JOIN target".to_string()),
        };
        joins.push(Join { kind: *kind, table, on: on_str.to_string() });
    }

    // SELECT items (verbatim, comma-split at top level).
    let select: Vec<SelectItem> = match split_top_commas(&clauses.select) {
        Some(items) => items
            .into_iter()
            .map(|t| SelectItem { text: t.trim().to_string() })
            .collect(),
        None => return ParseOutcome::Fallback("unbalanced SELECT list".to_string()),
    };
    if select.is_empty() {
        return ParseOutcome::Fallback("empty SELECT list".to_string());
    }

    // WHERE conjuncts.
    let where_ = match &clauses.where_ {
        None => Vec::new(),
        Some(w) => match parse_where(w) {
            Some(preds) => preds,
            None => return ParseOutcome::Fallback("WHERE predicate outside grammar".to_string()),
        },
    };

    // GROUP BY (verbatim items).
    let group_by = match &clauses.group_by {
        None => Vec::new(),
        Some(g) => match split_top_commas(g) {
            Some(items) => items.into_iter().map(|s| s.trim().to_string()).collect(),
            None => return ParseOutcome::Fallback("unbalanced GROUP BY".to_string()),
        },
    };

    // ORDER BY.
    let order_by = match &clauses.order_by {
        None => Vec::new(),
        Some(o) => match parse_order_by(o) {
            Some(items) => items,
            None => return ParseOutcome::Fallback("complex ORDER BY".to_string()),
        },
    };

    // LIMIT.
    let limit = match &clauses.limit {
        None => None,
        Some(l) => match parse_limit(l) {
            Some(lim) => Some(lim),
            None => return ParseOutcome::Fallback("complex LIMIT".to_string()),
        },
    };

    ParseOutcome::Ir(Box::new(QueryIr {
        select,
        from,
        joins,
        where_,
        group_by,
        order_by,
        limit,
        raw_sql: norm,
    }))
}

/// Collapse runs of whitespace (incl. newlines) to single spaces and trim.
fn normalize_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

struct Clauses {
    select: String,
    from: String,
    joins: Vec<(JoinKind, String)>,
    where_: Option<String>,
    group_by: Option<String>,
    order_by: Option<String>,
    limit: Option<String>,
}

/// Split a normalized SELECT statement into its clauses by top-level keyword.
/// Returns `None` if the keyword sequence is unexpected.
fn split_clauses(norm: &str) -> Option<Clauses> {
    // Tokenize into words while tracking parenthesis depth, recording the byte
    // offsets of top-level (depth-0) clause keywords.
    let lower = norm.to_ascii_lowercase();
    let bytes = lower.as_bytes();

    // Find depth-0 keyword positions. We scan for the keywords as whole words.
    let mut markers: Vec<(usize, Marker)> = Vec::new();
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            // Try to match a keyword at a word boundary starting at i.
            if at_word_boundary(bytes, i) {
                for (kw, m) in KEYWORDS {
                    if matches_kw(&lower, i, kw) {
                        markers.push((i, *m));
                        i += kw.len();
                        break;
                    }
                }
            }
        }
        i += 1;
    }

    // The first marker must be SELECT at offset 0.
    if markers.first().map(|(o, m)| (*o, *m)) != Some((0, Marker::Select)) {
        return None;
    }

    // Build clause text spans between markers.
    let mut select = None;
    let mut from = None;
    let mut joins: Vec<(JoinKind, String)> = Vec::new();
    let mut where_ = None;
    let mut group_by = None;
    let mut order_by = None;
    let mut limit = None;

    for idx in 0..markers.len() {
        let (start, marker) = markers[idx];
        let kw_len = marker.kw_len();
        let body_start = start + kw_len;
        let body_end = markers.get(idx + 1).map(|(o, _)| *o).unwrap_or(norm.len());
        if body_start > body_end || body_end > norm.len() {
            return None;
        }
        let body = norm[body_start..body_end].trim().to_string();
        match marker {
            Marker::Select => select = Some(body),
            Marker::From => from = Some(body),
            Marker::Join(k) => joins.push((k, body)),
            Marker::Where => where_ = Some(body),
            Marker::GroupBy => group_by = Some(body),
            Marker::OrderBy => order_by = Some(body),
            Marker::Limit => limit = Some(body),
        }
    }

    Some(Clauses {
        select: select?,
        from: from?,
        joins,
        where_,
        group_by,
        order_by,
        limit,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Marker {
    Select,
    From,
    Join(JoinKind),
    Where,
    GroupBy,
    OrderBy,
    Limit,
}

impl Marker {
    fn kw_len(self) -> usize {
        match self {
            Marker::Select => 6,
            Marker::From => 4,
            Marker::Join(JoinKind::Inner) => 4,
            Marker::Join(JoinKind::Left) => 9,
            Marker::Where => 5,
            Marker::GroupBy => 8,
            Marker::OrderBy => 8,
            Marker::Limit => 5,
        }
    }
}

/// Keyword table, longest-first so `left join` matches before `join`, and
/// `group by` / `order by` match before any prefix.
const KEYWORDS: &[(&str, Marker)] = &[
    ("select", Marker::Select),
    ("from", Marker::From),
    ("left join", Marker::Join(JoinKind::Left)),
    ("inner join", Marker::Join(JoinKind::Inner)),
    ("join", Marker::Join(JoinKind::Inner)),
    ("where", Marker::Where),
    ("group by", Marker::GroupBy),
    ("order by", Marker::OrderBy),
    ("limit", Marker::Limit),
];

fn matches_kw(lower: &str, at: usize, kw: &str) -> bool {
    if !lower[at..].starts_with(kw) {
        return false;
    }
    // Word-boundary after the keyword (space or end). For multi-word keywords
    // the internal space is part of `kw`.
    let after = at + kw.len();
    after >= lower.len() || lower.as_bytes()[after] == b' '
}

fn at_word_boundary(bytes: &[u8], at: usize) -> bool {
    at == 0 || bytes[at - 1] == b' '
}

/// Parse `schema.table [alias]` / `table [alias]`. Returns `None` for anything
/// with parens (subquery) or commas (comma-join) or extra tokens.
fn parse_table_ref(s: &str) -> Option<TableRef> {
    let s = s.trim();
    if s.is_empty() || s.contains('(') || s.contains(',') {
        return None;
    }
    let parts: Vec<&str> = s.split_whitespace().collect();
    let (name, alias) = match parts.len() {
        1 => (parts[0], None),
        2 => (parts[0], Some(parts[1].to_string())),
        // `tbl AS alias`
        3 if parts[1].eq_ignore_ascii_case("as") => (parts[0], Some(parts[2].to_string())),
        _ => return None,
    };
    if !is_dotted_ident(name) {
        return None;
    }
    let (schema, table) = match name.split_once('.') {
        Some((s, t)) => (Some(s.to_string()), t.to_string()),
        None => (None, name.to_string()),
    };
    Some(TableRef { schema, table, alias })
}

fn is_dotted_ident(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|seg| {
            !seg.is_empty()
                && seg.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
                && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

/// Split on top-level commas (depth-0 only). `None` on unbalanced parens.
fn split_top_commas(s: &str) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
            }
            b',' if depth == 0 => {
                out.push(s[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return None;
    }
    out.push(s[start..].to_string());
    Some(out)
}

/// Parse the WHERE body into AND-joined conjuncts. Only AND at the top level is
/// supported; an `OR` at the top level or an un-parseable conjunct falls back.
fn parse_where(s: &str) -> Option<Vec<Predicate>> {
    let conjs = split_top_and(s)?;
    let mut out = Vec::new();
    for c in conjs {
        out.push(parse_predicate(c.trim())?);
    }
    Some(out)
}

/// Split on top-level ` AND ` (case-insensitive, depth-0). `None` if a top-level
/// `OR` appears (we keep the grammar AND-only for the structured path).
fn split_top_and(s: &str) -> Option<Vec<String>> {
    let lower = s.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut depth = 0i32;
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            if lower[i..].starts_with(" or ") {
                return None; // top-level OR → fall back
            }
            if lower[i..].starts_with(" and ") {
                parts.push(s[start..i].to_string());
                i += 5;
                start = i;
                continue;
            }
        }
        i += 1;
    }
    parts.push(s[start..].to_string());
    Some(parts)
}

/// Parse a single comparison/IN predicate. Bounded: `expr <op> expr` or
/// `expr [NOT] IN (a, b, …)`. Anything else returns `None`.
fn parse_predicate(s: &str) -> Option<Predicate> {
    let lower = s.to_ascii_lowercase();
    // IN-list (optionally negated).
    if let Some(pos) = find_top(&lower, " in (").or_else(|| find_top(&lower, " in(")) {
        let lhs = parse_expr(s[..pos].trim())?;
        // Detect NOT directly before IN.
        let (lhs, negated) = strip_trailing_not(lhs, &s[..pos]);
        let open = s[pos..].find('(')? + pos;
        let close = matching_paren(s, open)?;
        let inner = &s[open + 1..close];
        let items = split_top_commas(inner)?
            .into_iter()
            .map(|t| parse_expr(t.trim()))
            .collect::<Option<Vec<_>>>()?;
        return Some(Predicate::InList { expr: lhs, items, negated });
    }

    // Comparison operators, longest-first.
    for (tok, op) in [
        (" >= ", CmpOp::Ge),
        (" <= ", CmpOp::Le),
        (" != ", CmpOp::Ne),
        (" <> ", CmpOp::Ne),
        (" = ", CmpOp::Eq),
        (" > ", CmpOp::Gt),
        (" < ", CmpOp::Lt),
    ] {
        if let Some(pos) = find_top(s, tok) {
            let lhs = parse_expr(s[..pos].trim())?;
            let rhs = parse_expr(s[pos + tok.len()..].trim())?;
            return Some(Predicate::Compare { lhs, op, rhs });
        }
    }
    None
}

fn strip_trailing_not(expr: Expr, lhs_src: &str) -> (Expr, bool) {
    // `parse_expr` of `... not` would have failed earlier; the NOT sits between
    // the lhs expr and IN: handled by callers passing `x NOT` as lhs source.
    let trimmed = lhs_src.trim();
    if let Some(stripped) = trimmed
        .to_ascii_lowercase()
        .strip_suffix(" not")
        .map(|_| &trimmed[..trimmed.len() - 4])
    {
        if let Some(e) = parse_expr(stripped.trim()) {
            return (e, true);
        }
    }
    (expr, false)
}

/// Parse a bounded expression: column, `:bind`, literal, `fn(args)`, or
/// `expr::type` cast. Verbatim-but-structured. Returns `None` only for shapes
/// the grammar can't represent at all (it's permissive — unknown leaf tokens
/// become `Expr::Raw`).
fn parse_expr(s: &str) -> Option<Expr> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Cast: `inner::type` at top level (rightmost top-level `::`).
    if let Some(pos) = find_top(s, "::") {
        let inner = parse_expr(s[..pos].trim())?;
        let ty = crate::read::ir::IrType::parse(s[pos + 2..].trim());
        return Some(Expr::Cast { expr: Box::new(inner), ty });
    }
    // Bind.
    if let Some(name) = s.strip_prefix(':') {
        if is_ident(name) {
            return Some(Expr::Bind(name.to_string()));
        }
    }
    // Function call: `name(...)` spanning the whole token.
    if let Some(open) = s.find('(') {
        if s.ends_with(')') && matching_paren(s, open) == Some(s.len() - 1) {
            let name = s[..open].trim();
            if is_ident(name) {
                let inner = &s[open + 1..s.len() - 1];
                let args = if inner.trim().is_empty() {
                    Vec::new()
                } else {
                    split_top_commas(inner)?
                        .into_iter()
                        .map(|a| parse_expr(a.trim()))
                        .collect::<Option<Vec<_>>>()?
                };
                return Some(Expr::Fn { name: name.to_string(), args });
            }
        }
    }
    // String / numeric / bool / null literal.
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Some(Expr::Lit(s.to_string()));
    }
    if s.parse::<f64>().is_ok()
        || s.eq_ignore_ascii_case("true")
        || s.eq_ignore_ascii_case("false")
        || s.eq_ignore_ascii_case("null")
    {
        return Some(Expr::Lit(s.to_string()));
    }
    // Qualified/unqualified column identifier.
    if is_dotted_ident(s) {
        return Some(Expr::Col(s.to_string()));
    }
    // Anything else: keep verbatim (the dialect re-emits it; PG never uses it).
    Some(Expr::Raw(s.to_string()))
}

fn parse_order_by(s: &str) -> Option<Vec<OrderItem>> {
    let items = split_top_commas(s)?;
    let mut out = Vec::new();
    for it in items {
        let it = it.trim();
        let lower = it.to_ascii_lowercase();
        let (expr, dir) = if let Some(e) = lower.strip_suffix(" desc") {
            (it[..e.len()].trim().to_string(), SortDir::Desc)
        } else if let Some(e) = lower.strip_suffix(" asc") {
            (it[..e.len()].trim().to_string(), SortDir::Asc)
        } else {
            (it.to_string(), SortDir::Asc)
        };
        if expr.is_empty() {
            return None;
        }
        out.push(OrderItem { expr, dir });
    }
    Some(out)
}

fn parse_limit(s: &str) -> Option<Limit> {
    let s = s.trim();
    if let Some(name) = s.strip_prefix(':') {
        if is_ident(name) {
            return Some(Limit::Bind(name.to_string()));
        }
    }
    s.parse::<i64>().ok().map(Limit::Lit)
}

// --- small lexical helpers -------------------------------------------------

fn is_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Find `needle` at parenthesis depth 0 (and outside single-quoted strings).
fn find_top(hay: &str, needle: &str) -> Option<usize> {
    let bytes = hay.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        let b = bytes[i];
        if in_str {
            if b == b'\'' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_str = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && hay[i..].starts_with(needle) {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Given the index of an opening `(`, return the index of its matching `)`.
fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_str = !in_str,
            b'(' if !in_str => depth += 1,
            b')' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}
