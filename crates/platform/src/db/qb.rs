//! Tiny runtime query-param builder — DRYs up the `args.append(x);
//! where.append(f"col = ${len}")` idiom from the Python query modules.
//!
//! Params are boxed `+ Send` so they can be held across `.await` (axum handler
//! futures must be Send). `refs()` produces the `&[&(dyn ToSql + Sync)]` slice
//! `tokio_postgres::query` expects.

use tokio_postgres::types::ToSql;

pub struct Qb {
    params: Vec<Box<dyn ToSql + Sync + Send>>,
    pub where_: Vec<String>,
}

impl Qb {
    pub fn new() -> Self {
        Qb { params: Vec::new(), where_: Vec::new() }
    }

    /// Push a bind param, returning its 1-based placeholder index.
    pub fn push<T: ToSql + Sync + Send + 'static>(&mut self, v: T) -> usize {
        self.params.push(Box::new(v));
        self.params.len()
    }

    /// `col <op> $N` predicate for a bound value.
    pub fn cmp<T: ToSql + Sync + Send + 'static>(&mut self, col: &str, op: &str, v: T) {
        let n = self.push(v);
        self.where_.push(format!("{col} {op} ${n}"));
    }

    pub fn eq<T: ToSql + Sync + Send + 'static>(&mut self, col: &str, v: T) {
        self.cmp(col, "=", v);
    }

    /// "WHERE a AND b" or "" when no predicates.
    pub fn where_clause(&self) -> String {
        if self.where_.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.where_.join(" AND "))
        }
    }

    /// "a AND b" (caller supplies the leading WHERE) — for queries that always
    /// have at least one predicate.
    pub fn and_join(&self) -> String {
        self.where_.join(" AND ")
    }

    pub fn refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
    }
}
