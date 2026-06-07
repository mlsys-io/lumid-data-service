//! Backend-neutral query IR for the read layer (`T-READ-IR-001`).
//!
//! A read spec's `sql` is a raw Postgres-dialect string. PR #9 made the
//! *placeholders* backend-neutral (`$N`/`::cast` → `?` + [`BindValue`] binds);
//! this module makes the *dialect* neutral: a bounded read-only SELECT is parsed
//! into a [`QueryIr`] at spec-load and lowered per dialect at execute time, so
//! one `[[read.endpoint]]` runs on Postgres **or** ClickHouse, authored once.
//!
//! # Why the IR retains clause text
//!
//! The non-negotiable invariant is **PG byte-equivalence**: the Postgres lowerer
//! must emit SQL byte-identical to today's [`super::bind::resolve`] output over
//! the existing spec corpus — zero behaviour change for PG-backed reads. A pure
//! structured AST cannot reproduce the hand-authored whitespace/formatting of the
//! corpus, so the IR keeps each clause's **substituted source text** (binds still
//! as `:name`) verbatim. The Postgres lowerer then runs the *exact same*
//! `substitute_fragments` + `lower_binds` machinery the legacy path uses —
//! byte-equivalence is structural, not coincidental.
//!
//! The IR *also* carries a structured view of the clauses it could parse
//! (`select`/`from`/`joins`/`where`/`group_by`/`order_by`/`limit` over the
//! `Expr`/`Predicate` grammar). The ClickHouse lowerer renders from that
//! structure — `?` placeholders, CH type names, `PREWHERE` for leading
//! ORDER-BY-key filters, opt-in `FINAL`. A construct the parser can't represent
//! makes the whole spec **fall back** to the raw-SQL path (Postgres-pinned, with
//! a `warn!` naming the construct) — never a silent mis-execution.
//!
//! `Expr::Bind` reuses PR #9's [`super::bind::BindValue`] at lower time (the IR
//! carries only the slot *name*; the resolved value lives in the per-request bind
//! map).

/// A normalized read-only SELECT, backend-neutral. Built by [`super::parse`] from
/// a spec's `sql` when it fits the bounded grammar; otherwise the spec keeps the
/// raw-SQL fallback (no IR).
#[derive(Debug, Clone)]
pub struct QueryIr {
    /// Projected items, in order. Each carries its verbatim text (for PG) plus a
    /// best-effort structured `Expr` (for CH).
    pub select: Vec<SelectItem>,
    /// `FROM schema.table [alias]` — drives backend resolution (already handled
    /// by the spec's `tables[]`, mirrored here for the lowerers).
    pub from: TableRef,
    /// Zero or more `JOIN`s.
    pub joins: Vec<Join>,
    /// `WHERE` conjuncts (AND-joined). Empty ⇒ no WHERE clause.
    pub where_: Vec<Predicate>,
    /// `GROUP BY` columns/expressions (verbatim text).
    pub group_by: Vec<String>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT` value, if present.
    pub limit: Option<Limit>,
    /// The fully-substituted SQL text (binds still `:name`) — the byte-equivalent
    /// source for the Postgres lowerer. This is the spec's `sql` after fragment
    /// substitution would be applied; the PG lowerer re-derives it per request
    /// because fragments are request-dependent, so this field is only the
    /// *structural* parse witness and is not used for PG lowering directly.
    pub raw_sql: String,
}

/// `schema.table` (+ optional alias) reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
}

impl TableRef {
    /// Render the `schema.table [alias]` form back out (CH lowerer).
    pub fn render(&self) -> String {
        let mut s = match &self.schema {
            Some(sc) => format!("{sc}.{}", self.table),
            None => self.table.clone(),
        };
        if let Some(a) = &self.alias {
            s.push(' ');
            s.push_str(a);
        }
        s
    }
}

/// One `JOIN` clause.
#[derive(Debug, Clone)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    /// The `ON` predicate text, verbatim (binds still `:name`).
    pub on: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

impl JoinKind {
    pub fn keyword(self) -> &'static str {
        match self {
            JoinKind::Inner => "JOIN",
            JoinKind::Left => "LEFT JOIN",
        }
    }
}

/// A projected item: `expr [AS alias]`.
#[derive(Debug, Clone)]
pub struct SelectItem {
    /// Verbatim projection text (binds still `:name`) — used by the CH lowerer.
    pub text: String,
}

/// One `ORDER BY` item: an expression + direction.
#[derive(Debug, Clone)]
pub struct OrderItem {
    /// Verbatim expression text (a column or expression).
    pub expr: String,
    pub dir: SortDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

impl SortDir {
    pub fn keyword(self) -> &'static str {
        match self {
            SortDir::Asc => "ASC",
            SortDir::Desc => "DESC",
        }
    }
}

/// `LIMIT` — either a literal integer or a `:name` bind.
#[derive(Debug, Clone)]
pub enum Limit {
    /// A bound `:name` (lowered to `$N` for PG / `?` for CH).
    Bind(String),
    /// A literal integer.
    Lit(i64),
}

/// A `WHERE`/`ON` predicate (AND-joined at the [`QueryIr`] level). The structured
/// grammar is the bounded read-only shape — anything richer falls back.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// `lhs <op> rhs`.
    Compare { lhs: Expr, op: CmpOp, rhs: Expr },
    /// `expr IN (a, b, …)`.
    InList { expr: Expr, items: Vec<Expr>, negated: bool },
    /// `pred AND pred` / `pred OR pred`.
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    /// `NOT pred`.
    Not(Box<Predicate>),
    /// A predicate we kept verbatim because it parsed as a leaf but isn't one of
    /// the structured comparators (e.g. `x = true`). Carried as raw text for the
    /// CH lowerer; the parser only produces this for shapes it can re-emit safely.
    Raw(String),
}

/// The expression sublanguage shared (in shape) with the LQT governed-policy
/// substrate: arithmetic/comparison/logic over columns, binds, literals,
/// function calls and casts.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A column reference, possibly qualified (`d.tenant_id`). Verbatim text.
    Col(String),
    /// A `:name` bind — reuses PR #9's [`BindValue`] at lower time.
    Bind(String),
    /// A literal (string/number/bool/null), verbatim text as written.
    Lit(String),
    /// `name(args…)` function call.
    Fn { name: String, args: Vec<Expr> },
    /// `expr::type` (PG) / `CAST(expr AS type)`.
    Cast { expr: Box<Expr>, ty: IrType },
    /// An expression we parsed as a leaf token but didn't decompose — verbatim.
    Raw(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn sql(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

/// A backend-neutral type name (for `Cast`). Each dialect renders its own
/// spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IrType {
    Int8,
    Float8,
    Text,
    Bool,
    /// A type the IR doesn't model — kept as the verbatim source spelling. A
    /// dialect that can't express it errors at lower time.
    Other(String),
}

impl IrType {
    /// Parse a cast type spelling into an [`IrType`].
    pub fn parse(s: &str) -> IrType {
        match s.trim().to_ascii_lowercase().as_str() {
            "int8" | "bigint" => IrType::Int8,
            "float8" | "double precision" => IrType::Float8,
            "text" | "varchar" => IrType::Text,
            "bool" | "boolean" => IrType::Bool,
            other => IrType::Other(other.to_string()),
        }
    }

    /// Postgres spelling (e.g. `int8`).
    pub fn pg(&self) -> String {
        match self {
            IrType::Int8 => "int8".into(),
            IrType::Float8 => "float8".into(),
            IrType::Text => "text".into(),
            IrType::Bool => "bool".into(),
            IrType::Other(s) => s.clone(),
        }
    }

    /// ClickHouse spelling, or `None` if the CH dialect can't express it.
    pub fn ch(&self) -> Option<&'static str> {
        match self {
            IrType::Int8 => Some("Int64"),
            IrType::Float8 => Some("Float64"),
            IrType::Text => Some("String"),
            IrType::Bool => Some("Bool"),
            IrType::Other(_) => None,
        }
    }
}
