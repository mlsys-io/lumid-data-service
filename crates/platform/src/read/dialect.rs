//! Per-dialect lowering of a read query to executable SQL + ordered binds.
//!
//! The bind layer ([`super::bind::resolve`]) resolves a request to: the spec's
//! SQL **after fragment substitution** (binds still `:name`), the per-name
//! resolved [`BindValue`]s, and (when the spec parsed) a [`QueryIr`]. A
//! [`Dialect`] turns that into a [`BoundSql`] — final SQL string + ordered
//! [`BindValue`] list in placeholder order.
//!
//! * [`PostgresDialect`] lowers the **substituted text** with the exact legacy
//!   `:name → $N` machinery, so its output is byte-equivalent to the pre-IR
//!   `bind::resolve` SQL (the non-negotiable PG invariant). It never needs the
//!   `QueryIr`.
//! * [`ClickHouseDialect`] lowers from the [`QueryIr`] when present (`?`
//!   placeholders, CH casts, `PREWHERE`, opt-in `FINAL`); without an IR it falls
//!   back to translating the PG-lowered placeholders (PR #9 behaviour).

use std::collections::HashMap;

use super::bind::BindValue;
use super::ir::{
    Expr, Join, Limit, OrderItem, Predicate, QueryIr, SelectItem, SortDir, TableRef,
};
use crate::error::ApiError;

/// The result of lowering: executable SQL + the bind values in placeholder order.
pub struct BoundSql {
    pub sql: String,
    pub values: Vec<BindValue>,
}

/// A SQL dialect that lowers a resolved read query to a [`BoundSql`].
pub trait Dialect {
    /// Lower `substituted` (spec SQL with fragments applied, binds as `:name`)
    /// using `values` (name → resolved [`BindValue`]). `ir` is the structured
    /// parse of the *unsubstituted* spec SQL when available — dialects that need
    /// structure (ClickHouse) use it; Postgres ignores it.
    fn lower(
        &self,
        substituted: &str,
        values: &HashMap<String, BindValue>,
        ir: Option<&QueryIr>,
    ) -> Result<BoundSql, ApiError>;
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}
fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The canonical Postgres lowerer — byte-equivalent to the legacy
/// `bind::resolve` `:name → $N` pass (with `::int8`/`::float8` numeric casts).
/// This is the literal pre-IR code path, kept here so the legacy `bind` helper
/// and the IR path share one impl.
pub struct PostgresDialect;

impl PostgresDialect {
    /// Replace `:name` binds with `$N`, pushing each occurrence's value. A `:`
    /// preceded or followed by another `:` (a `::type` cast) is left untouched.
    /// Identical byte-for-byte to the former `bind::lower_binds`.
    pub fn lower_binds(
        sql: &str,
        values: &HashMap<String, BindValue>,
    ) -> Result<BoundSql, ApiError> {
        let bytes = sql.as_bytes();
        let mut out = String::with_capacity(sql.len() + 16);
        let mut ordered: Vec<BindValue> = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b':' {
                let prev_colon = i > 0 && bytes[i - 1] == b':';
                let next_colon = i + 1 < bytes.len() && bytes[i + 1] == b':';
                if !prev_colon && !next_colon && i + 1 < bytes.len() && is_ident_start(bytes[i + 1])
                {
                    let mut j = i + 1;
                    while j < bytes.len() && is_ident(bytes[j]) {
                        j += 1;
                    }
                    let name = &sql[i + 1..j];
                    let v = values.get(name).ok_or_else(|| {
                        ApiError::Internal(anyhow::anyhow!("bind ':{name}' has no resolved value"))
                    })?;
                    ordered.push(v.clone());
                    out.push('$');
                    out.push_str(&ordered.len().to_string());
                    // Cast numeric binds so a single Rust width matches any
                    // column width (i64 vs int4 / numeric): `$N::int8` /
                    // `$N::float8`. Postgres implicitly promotes in comparisons.
                    match v {
                        BindValue::Int(_) => out.push_str("::int8"),
                        BindValue::Float(_) => out.push_str("::float8"),
                        _ => {}
                    }
                    i = j;
                    continue;
                }
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        Ok(BoundSql { sql: out, values: ordered })
    }
}

impl Dialect for PostgresDialect {
    fn lower(
        &self,
        substituted: &str,
        values: &HashMap<String, BindValue>,
        _ir: Option<&QueryIr>,
    ) -> Result<BoundSql, ApiError> {
        // PG lowers the substituted text directly — byte-equivalent to legacy.
        Self::lower_binds(substituted, values)
    }
}

/// ClickHouse lowerer: `?` placeholders, CH type names, `PREWHERE` for leading
/// ORDER-BY-key filters, opt-in `FINAL` for `ReplacingMergeTree` dedup-on-read.
#[derive(Default)]
pub struct ClickHouseDialect {
    /// Emit `FINAL` after the table to force dedup-on-read (ReplacingMergeTree).
    pub final_: bool,
    /// Hoist `WHERE` conjuncts that filter the leading ORDER BY key column into
    /// `PREWHERE`. When empty, no PREWHERE hoist is attempted.
    pub order_key_cols: Vec<String>,
}

impl ClickHouseDialect {
    /// Lower a parsed [`QueryIr`] to ClickHouse SQL with `?` binds.
    fn lower_ir(
        &self,
        ir: &QueryIr,
        values: &HashMap<String, BindValue>,
    ) -> Result<BoundSql, ApiError> {
        let mut sql = String::new();
        let mut ordered: Vec<BindValue> = Vec::new();

        // SELECT
        sql.push_str("SELECT ");
        sql.push_str(
            &ir.select
                .iter()
                .map(|s| self.render_select_item(s, values, &mut ordered))
                .collect::<Result<Vec<_>, _>>()?
                .join(", "),
        );

        // FROM (+ FINAL)
        sql.push_str(" FROM ");
        sql.push_str(&self.render_table(&ir.from));
        if self.final_ {
            sql.push_str(" FINAL");
        }

        // JOINs
        for j in &ir.joins {
            sql.push(' ');
            sql.push_str(&self.render_join(j, values, &mut ordered)?);
        }

        // WHERE / PREWHERE split: hoist conjuncts touching the leading key col.
        let (prewhere, where_) = self.split_prewhere(&ir.where_);
        if !prewhere.is_empty() {
            sql.push_str(" PREWHERE ");
            sql.push_str(&self.render_conjuncts(&prewhere, values, &mut ordered)?);
        }
        if !where_.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.render_conjuncts(&where_, values, &mut ordered)?);
        }

        // GROUP BY
        if !ir.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&ir.group_by.join(", "));
        }

        // ORDER BY
        if !ir.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &ir.order_by
                    .iter()
                    .map(Self::render_order)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        // LIMIT
        if let Some(lim) = &ir.limit {
            sql.push_str(" LIMIT ");
            match lim {
                Limit::Lit(n) => sql.push_str(&n.to_string()),
                Limit::Bind(name) => {
                    let v = values.get(name).ok_or_else(|| {
                        ApiError::Internal(anyhow::anyhow!("bind ':{name}' has no resolved value"))
                    })?;
                    ordered.push(v.clone());
                    sql.push('?');
                }
            }
        }

        Ok(BoundSql { sql, values: ordered })
    }

    fn render_table(&self, t: &TableRef) -> String {
        t.render()
    }

    fn render_select_item(
        &self,
        item: &SelectItem,
        values: &HashMap<String, BindValue>,
        ordered: &mut Vec<BindValue>,
    ) -> Result<String, ApiError> {
        // Select items are verbatim text with possible `:name` binds; lower them
        // to `?` (ClickHouse positional binds) preserving order.
        self.lower_text_binds(&item.text, values, ordered)
    }

    fn render_join(
        &self,
        j: &Join,
        values: &HashMap<String, BindValue>,
        ordered: &mut Vec<BindValue>,
    ) -> Result<String, ApiError> {
        let on = self.lower_text_binds(&j.on, values, ordered)?;
        Ok(format!("{} {} ON {}", j.kind.keyword(), self.render_table(&j.table), on))
    }

    fn render_order(o: &OrderItem) -> String {
        match o.dir {
            SortDir::Asc => format!("{} ASC", o.expr),
            SortDir::Desc => format!("{} DESC", o.expr),
        }
    }

    /// Split the WHERE conjuncts into (prewhere, where) — a conjunct is hoisted to
    /// PREWHERE when it's a comparison whose lhs is the leading ORDER BY key
    /// column (a common CH read optimisation). Everything else stays in WHERE.
    fn split_prewhere(&self, conj: &[Predicate]) -> (Vec<Predicate>, Vec<Predicate>) {
        if self.order_key_cols.is_empty() {
            return (Vec::new(), conj.to_vec());
        }
        let lead = &self.order_key_cols[0];
        let mut pre = Vec::new();
        let mut rest = Vec::new();
        for p in conj {
            if Self::touches_leading_key(p, lead) {
                pre.push(p.clone());
            } else {
                rest.push(p.clone());
            }
        }
        (pre, rest)
    }

    fn touches_leading_key(p: &Predicate, lead: &str) -> bool {
        match p {
            Predicate::Compare { lhs, .. } => Self::expr_is_col(lhs, lead),
            _ => false,
        }
    }

    fn expr_is_col(e: &Expr, col: &str) -> bool {
        match e {
            Expr::Col(c) => {
                // Match the unqualified tail (`d.foo` matches `foo`).
                c == col || c.rsplit('.').next() == Some(col)
            }
            _ => false,
        }
    }

    fn render_conjuncts(
        &self,
        conj: &[Predicate],
        values: &HashMap<String, BindValue>,
        ordered: &mut Vec<BindValue>,
    ) -> Result<String, ApiError> {
        let parts = conj
            .iter()
            .map(|p| self.render_predicate(p, values, ordered))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(parts.join(" AND "))
    }

    fn render_predicate(
        &self,
        p: &Predicate,
        values: &HashMap<String, BindValue>,
        ordered: &mut Vec<BindValue>,
    ) -> Result<String, ApiError> {
        match p {
            Predicate::Compare { lhs, op, rhs } => {
                let l = self.render_expr(lhs, values, ordered)?;
                let r = self.render_expr(rhs, values, ordered)?;
                Ok(format!("{l} {} {r}", op.sql()))
            }
            Predicate::InList { expr, items, negated } => {
                let e = self.render_expr(expr, values, ordered)?;
                let its = items
                    .iter()
                    .map(|i| self.render_expr(i, values, ordered))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                let kw = if *negated { "NOT IN" } else { "IN" };
                Ok(format!("{e} {kw} ({its})"))
            }
            Predicate::And(a, b) => {
                let l = self.render_predicate(a, values, ordered)?;
                let r = self.render_predicate(b, values, ordered)?;
                Ok(format!("({l} AND {r})"))
            }
            Predicate::Or(a, b) => {
                let l = self.render_predicate(a, values, ordered)?;
                let r = self.render_predicate(b, values, ordered)?;
                Ok(format!("({l} OR {r})"))
            }
            Predicate::Not(a) => {
                let inner = self.render_predicate(a, values, ordered)?;
                Ok(format!("NOT ({inner})"))
            }
            Predicate::Raw(t) => self.lower_text_binds(t, values, ordered),
        }
    }

    fn render_expr(
        &self,
        e: &Expr,
        values: &HashMap<String, BindValue>,
        ordered: &mut Vec<BindValue>,
    ) -> Result<String, ApiError> {
        match e {
            Expr::Col(c) => Ok(c.clone()),
            Expr::Lit(l) => Ok(l.clone()),
            Expr::Bind(name) => {
                let v = values.get(name).ok_or_else(|| {
                    ApiError::Internal(anyhow::anyhow!("bind ':{name}' has no resolved value"))
                })?;
                ordered.push(v.clone());
                Ok("?".to_string())
            }
            Expr::Fn { name, args } => {
                let a = args
                    .iter()
                    .map(|x| self.render_expr(x, values, ordered))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(format!("{name}({a})"))
            }
            Expr::Cast { expr, ty } => {
                let inner = self.render_expr(expr, values, ordered)?;
                let ch_ty = ty.ch().ok_or_else(|| {
                    ApiError::Internal(anyhow::anyhow!(
                        "ClickHouse lowerer cannot express cast to {:?}",
                        ty.pg()
                    ))
                })?;
                Ok(format!("CAST({inner} AS {ch_ty})"))
            }
            Expr::Raw(t) => self.lower_text_binds(t, values, ordered),
        }
    }

    /// Lower verbatim text containing `:name` binds to ClickHouse `?` form,
    /// stripping the PG numeric-widening casts (`::int8`/`::float8`) the legacy
    /// path would add — CH binds carry their own type.
    fn lower_text_binds(
        &self,
        text: &str,
        values: &HashMap<String, BindValue>,
        ordered: &mut Vec<BindValue>,
    ) -> Result<String, ApiError> {
        let bytes = text.as_bytes();
        let mut out = String::with_capacity(text.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b':' {
                let prev_colon = i > 0 && bytes[i - 1] == b':';
                let next_colon = i + 1 < bytes.len() && bytes[i + 1] == b':';
                if !prev_colon && !next_colon && i + 1 < bytes.len() && is_ident_start(bytes[i + 1])
                {
                    let mut j = i + 1;
                    while j < bytes.len() && is_ident(bytes[j]) {
                        j += 1;
                    }
                    let name = &text[i + 1..j];
                    let v = values.get(name).ok_or_else(|| {
                        ApiError::Internal(anyhow::anyhow!("bind ':{name}' has no resolved value"))
                    })?;
                    ordered.push(v.clone());
                    out.push('?');
                    i = j;
                    continue;
                }
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        Ok(out)
    }
}

impl Dialect for ClickHouseDialect {
    fn lower(
        &self,
        substituted: &str,
        values: &HashMap<String, BindValue>,
        ir: Option<&QueryIr>,
    ) -> Result<BoundSql, ApiError> {
        match ir {
            // Structured lowering when the spec parsed into an IR.
            Some(ir) => self.lower_ir(ir, values),
            // No IR (raw-SQL fallback spec on a CH table): translate the
            // PG-lowered placeholders to `?` (PR #9 behaviour). This keeps the
            // pre-IR CH read path working for un-parseable specs.
            None => {
                let pg = PostgresDialect::lower_binds(substituted, values)?;
                let ch_sql = pg_placeholders_to_ch(&pg.sql);
                Ok(BoundSql { sql: ch_sql, values: pg.values })
            }
        }
    }
}

/// Translate PG `$N`(+`::int8`/`::float8` cast) placeholders into CH `?` form.
/// Lifted from the PR #9 `clickhouse::pg_placeholders_to_ch` so the fallback
/// path reuses one impl.
pub fn pg_placeholders_to_ch(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            for cast in ["::int8", "::float8"] {
                if sql[j..].starts_with(cast) {
                    j += cast.len();
                    break;
                }
            }
            out.push('?');
            i = j;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Validate that a parsed [`QueryIr`] can be expressed by a given dialect at
/// spec-load (loud, before serving). Returns the offending construct on failure.
/// Currently only `Cast` to an unmappable type can fail for ClickHouse.
pub fn validate_clickhouse(ir: &QueryIr) -> Result<(), String> {
    fn check_expr(e: &Expr) -> Result<(), String> {
        match e {
            Expr::Cast { expr, ty } => {
                if ty.ch().is_none() {
                    return Err(format!("cast to {:?}", ty.pg()));
                }
                check_expr(expr)
            }
            Expr::Fn { args, .. } => {
                for a in args {
                    check_expr(a)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    fn check_pred(p: &Predicate) -> Result<(), String> {
        match p {
            Predicate::Compare { lhs, rhs, .. } => {
                check_expr(lhs)?;
                check_expr(rhs)
            }
            Predicate::InList { expr, items, .. } => {
                check_expr(expr)?;
                for i in items {
                    check_expr(i)?;
                }
                Ok(())
            }
            Predicate::And(a, b) | Predicate::Or(a, b) => {
                check_pred(a)?;
                check_pred(b)
            }
            Predicate::Not(a) => check_pred(a),
            Predicate::Raw(_) => Ok(()),
        }
    }
    for p in &ir.where_ {
        check_pred(p)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::parse::{parse_select, ParseOutcome};
    use chrono::NaiveDate;

    /// The legacy `bind::lower_binds` algorithm, copied verbatim as the GOLDEN
    /// reference. The byte-equivalence pin asserts `PostgresDialect::lower_binds`
    /// reproduces this exactly over the spec corpus — i.e. the IR refactor caused
    /// zero behaviour change for Postgres-backed reads.
    fn golden_lower_binds(sql: &str, values: &HashMap<String, BindValue>) -> (String, usize) {
        fn is_ident_start(b: u8) -> bool {
            b.is_ascii_alphabetic() || b == b'_'
        }
        fn is_ident(b: u8) -> bool {
            b.is_ascii_alphanumeric() || b == b'_'
        }
        let bytes = sql.as_bytes();
        let mut out = String::with_capacity(sql.len() + 16);
        let mut n = 0usize;
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b':' {
                let prev_colon = i > 0 && bytes[i - 1] == b':';
                let next_colon = i + 1 < bytes.len() && bytes[i + 1] == b':';
                if !prev_colon && !next_colon && i + 1 < bytes.len() && is_ident_start(bytes[i + 1])
                {
                    let mut j = i + 1;
                    while j < bytes.len() && is_ident(bytes[j]) {
                        j += 1;
                    }
                    let name = &sql[i + 1..j];
                    let v = values.get(name).expect("resolved");
                    n += 1;
                    out.push('$');
                    out.push_str(&n.to_string());
                    match v {
                        BindValue::Int(_) => out.push_str("::int8"),
                        BindValue::Float(_) => out.push_str("::float8"),
                        _ => {}
                    }
                    i = j;
                    continue;
                }
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        (out, n)
    }

    fn vals(pairs: &[(&str, BindValue)]) -> HashMap<String, BindValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    /// The substituted SQL bodies of the existing spec corpus (lqt.toml +
    /// app.toml), with their resolved bind values. Fragment substitution is a
    /// no-op for these (none use `{{frag}}`), so the substituted text == the
    /// spec `sql`. This is the corpus the PG byte-equivalence is pinned against.
    fn corpus() -> Vec<(&'static str, &'static str, HashMap<String, BindValue>)> {
        vec![
            (
                "obs.risk.decisions",
                "SELECT decision_id, decided_at, instrument_id, intent_id, venue, side, \
                 qty_lots, verdict, reason_code, fired_rule_ids, policy_id, policy_hash, \
                 projected_notional_ticks, mid_staleness_ns, ttr_s, decision_latency_ns \
                 FROM obs.risk_decisions WHERE tenant_id = :tenant \
                 ORDER BY decided_at DESC LIMIT :limit",
                vals(&[
                    ("tenant", BindValue::Text("acme".into())),
                    ("limit", BindValue::Int(100)),
                ]),
            ),
            (
                "obs.risk.rule_stats (interval — PG-only)",
                "SELECT rule_id, count(*) AS fired FROM obs.risk_decisions \
                 WHERE tenant_id = :tenant AND decided_at >= now() - (:hours * interval '1 hour') \
                 GROUP BY rule_id ORDER BY fired DESC",
                vals(&[
                    ("tenant", BindValue::Text("acme".into())),
                    ("hours", BindValue::Int(24)),
                ]),
            ),
            (
                "md.bbo.by_instrument",
                "SELECT tenant_id, instrument_id, venue, bid_price_ticks, bid_size_lots, \
                 ask_price_ticks, ask_size_lots, ts_event_ns, ts_recv_ns FROM md.bbo \
                 WHERE instrument_id = :instrument ORDER BY ts_event_ns DESC LIMIT :limit",
                vals(&[
                    ("instrument", BindValue::Text("BTC-USD".into())),
                    ("limit", BindValue::Int(100)),
                ]),
            ),
            (
                "md.trade.by_instrument",
                "SELECT tenant_id, instrument_id, venue, side, price, size, ts_event_ns \
                 FROM md.trade WHERE instrument_id = :instrument \
                 ORDER BY ts_event_ns DESC LIMIT :limit",
                vals(&[
                    ("instrument", BindValue::Text("ETH-USD".into())),
                    ("limit", BindValue::Int(50)),
                ]),
            ),
            (
                "instrument.classes.by_venue (JOIN)",
                "SELECT c.venue, m.instrument_id, m.class_id \
                 FROM instrument.equivalence_class_member m \
                 JOIN instrument.equivalence_class c \
                 ON c.tenant_id = m.tenant_id AND c.class_id = m.class_id \
                 WHERE c.venue = :venue LIMIT :limit",
                vals(&[
                    ("venue", BindValue::Text("KALSHI".into())),
                    ("limit", BindValue::Int(10000)),
                ]),
            ),
            (
                "events.recent",
                "SELECT id, ts, kind, payload FROM app.events ORDER BY ts DESC LIMIT :limit",
                vals(&[("limit", BindValue::Int(100))]),
            ),
            (
                "float-and-date casts present",
                "SELECT x FROM s.t WHERE a >= :f AND d >= :day LIMIT :n",
                vals(&[
                    ("f", BindValue::Float(1.5)),
                    ("day", BindValue::Date(NaiveDate::from_ymd_opt(2026, 6, 2).unwrap())),
                    ("n", BindValue::Int(5)),
                ]),
            ),
        ]
    }

    /// THE pin: the Postgres lowerer is byte-equivalent to the legacy
    /// `:name → $N` pass over the entire spec corpus. No PG behaviour change.
    #[test]
    fn postgres_lowerer_byte_equivalent_to_legacy() {
        let pg = PostgresDialect;
        for (id, sql, values) in corpus() {
            let (golden, n) = golden_lower_binds(sql, &values);
            let got = pg.lower(sql, &values, None).expect(id);
            assert_eq!(got.sql, golden, "PG byte-equivalence broke for spec '{id}'");
            assert_eq!(got.values.len(), n, "bind count mismatch for spec '{id}'");
        }
    }

    /// And: when a spec parses into an IR, lowering that IR via Postgres is STILL
    /// byte-equivalent to lowering the raw substituted text — the IR is purely a
    /// CH enabler, it never perturbs the PG output (PG ignores the IR).
    #[test]
    fn postgres_output_independent_of_ir_presence() {
        let pg = PostgresDialect;
        for (id, sql, values) in corpus() {
            let ir = match parse_select(sql) {
                ParseOutcome::Ir(ir) => Some(ir),
                ParseOutcome::Fallback(_) => None,
            };
            let with_ir = pg.lower(sql, &values, ir.as_deref()).expect(id);
            let without = pg.lower(sql, &values, None).expect(id);
            assert_eq!(with_ir.sql, without.sql, "IR perturbed PG output for '{id}'");
        }
    }

    #[test]
    fn simple_select_parses_to_ir() {
        let sql = "SELECT a, b FROM s.t WHERE k = :k ORDER BY a DESC LIMIT :n";
        match parse_select(sql) {
            ParseOutcome::Ir(ir) => {
                assert_eq!(ir.select.len(), 2);
                assert_eq!(ir.from.schema.as_deref(), Some("s"));
                assert_eq!(ir.from.table, "t");
                assert_eq!(ir.where_.len(), 1);
                assert_eq!(ir.order_by.len(), 1);
                assert_eq!(ir.order_by[0].dir, SortDir::Desc);
                assert!(matches!(ir.limit, Some(Limit::Bind(_))));
            }
            ParseOutcome::Fallback(r) => panic!("should parse, got fallback: {r}"),
        }
    }

    #[test]
    fn pg_only_constructs_fall_back() {
        // Each of these must fall back (never silently mis-lower on CH).
        for (sql, _why) in [
            (
                "SELECT count(*) FILTER (WHERE v = 'x') AS n FROM s.t WHERE k = :k",
                "FILTER",
            ),
            (
                "SELECT a FROM s.t WHERE ts >= now() - (:h * interval '1 hour')",
                "interval",
            ),
            (
                "SELECT rule_id FROM obs.t, jsonb_array_elements_text(ids) AS rule_id WHERE k = :k",
                "jsonb_array_elements",
            ),
            (
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "CTE",
            ),
            (
                "SELECT n, sum(n) OVER () FROM s.t",
                "window",
            ),
            (
                "SELECT a FROM s.t WHERE k = :k OR j = :j",
                "top-level OR",
            ),
        ] {
            assert!(
                matches!(parse_select(sql), ParseOutcome::Fallback(_)),
                "expected fallback for: {sql}"
            );
        }
    }

    #[test]
    fn clickhouse_lowers_ir_with_question_marks_and_prewhere() {
        let sql = "SELECT tenant_id, instrument_id FROM md.trade \
                   WHERE instrument_id = :instrument ORDER BY ts_event_ns DESC LIMIT :limit";
        let ir = match parse_select(sql) {
            ParseOutcome::Ir(ir) => ir,
            ParseOutcome::Fallback(r) => panic!("should parse: {r}"),
        };
        let values = vals(&[
            ("instrument", BindValue::Text("ETH-USD".into())),
            ("limit", BindValue::Int(50)),
        ]);
        let dialect = ClickHouseDialect {
            final_: false,
            order_key_cols: vec!["ts_event_ns".into()],
        };
        let out = dialect.lower(sql, &values, Some(&*ir)).unwrap();
        // `?` placeholders (not `$N`), and the values come out in left-to-right
        // order (instrument before limit).
        assert!(out.sql.contains('?'), "{}", out.sql);
        assert!(!out.sql.contains('$'), "{}", out.sql);
        assert_eq!(out.values.len(), 2);
        assert!(matches!(out.values[0], BindValue::Text(_)));
        assert!(matches!(out.values[1], BindValue::Int(50)));
        // ts_event_ns is the (only) leading ORDER BY key; the WHERE on
        // instrument_id is NOT the key, so it stays in WHERE (no spurious hoist).
        assert!(out.sql.contains("WHERE instrument_id = ?"), "{}", out.sql);
        assert!(out.sql.contains("ORDER BY ts_event_ns DESC"), "{}", out.sql);
        assert!(out.sql.contains("LIMIT ?"), "{}", out.sql);
    }

    #[test]
    fn clickhouse_prewhere_hoists_leading_key_filter() {
        let sql = "SELECT a FROM s.t WHERE ts = :ts ORDER BY ts DESC LIMIT 10";
        let ir = match parse_select(sql) {
            ParseOutcome::Ir(ir) => ir,
            ParseOutcome::Fallback(r) => panic!("{r}"),
        };
        let values = vals(&[("ts", BindValue::Int(123))]);
        let dialect = ClickHouseDialect { final_: false, order_key_cols: vec!["ts".into()] };
        let out = dialect.lower(sql, &values, Some(&*ir)).unwrap();
        assert!(out.sql.contains("PREWHERE ts = ?"), "{}", out.sql);
    }

    #[test]
    fn clickhouse_final_opt_in() {
        let sql = "SELECT a FROM s.t LIMIT 10";
        let ir = match parse_select(sql) {
            ParseOutcome::Ir(ir) => ir,
            ParseOutcome::Fallback(r) => panic!("{r}"),
        };
        let d = ClickHouseDialect { final_: true, order_key_cols: vec![] };
        let out = d.lower(sql, &HashMap::new(), Some(&*ir)).unwrap();
        assert!(out.sql.contains("FROM s.t FINAL"), "{}", out.sql);
    }

    #[test]
    fn clickhouse_fallback_translates_placeholders_when_no_ir() {
        // No IR ⇒ CH dialect falls back to PG-placeholder translation.
        let values = vals(&[("k", BindValue::Int(5))]);
        let d = ClickHouseDialect::default();
        let out = d.lower("SELECT a FROM s.t WHERE k = :k", &values, None).unwrap();
        assert_eq!(out.sql, "SELECT a FROM s.t WHERE k = ?");
        assert_eq!(out.values.len(), 1);
    }

    #[test]
    fn clickhouse_cast_to_unmappable_type_rejected_by_validator() {
        // `:n::numeric` → IrType::Other("numeric") → CH can't express → validator
        // flags it loud at spec-load.
        let sql = "SELECT a FROM s.t WHERE x = :n::numeric";
        if let ParseOutcome::Ir(ir) = parse_select(sql) {
            let err = validate_clickhouse(&ir);
            assert!(err.is_err(), "expected CH-incompat cast to be flagged");
        }
    }

    #[test]
    fn clickhouse_cast_int8_maps() {
        let sql = "SELECT a FROM s.t WHERE x = :n::int8";
        if let ParseOutcome::Ir(ir) = parse_select(sql) {
            assert!(validate_clickhouse(&ir).is_ok());
            let values = vals(&[("n", BindValue::Int(7))]);
            let out = ClickHouseDialect::default().lower(sql, &values, Some(&*ir)).unwrap();
            assert!(out.sql.contains("CAST(? AS Int64)"), "{}", out.sql);
        } else {
            panic!("should parse");
        }
    }
}
