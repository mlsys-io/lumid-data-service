//! SQL relation extraction, vendored from jobs::scope (main) for the FinData
//! activity build on the pre-jobs platform base. Keep in step with jobs/scope.rs.

#![allow(dead_code)]

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
