# Read-layer query IR — compile specs to a backend-neutral IR, lower to PG / CH

**Status:** proposed (design)
**Author:** Yao Lu
**Builds on:** PR #9 (backend-neutral `BindValue` ABI + CH read placeholder translation)
**Related:** LQT `docs/decisions/governed-policy-substrate.md` (the platform-wide "DSL+compile" consolidation; this is its **layer β** consumer)
**Task:** `T-READ-IR-001`

## Problem

The read layer (`crates/platform/src/read/`) is already a mini-DSL: each
`[[read.endpoint]]` is a TOML spec with `:name` binds + `{{fragment}}` selection,
"compiled" by `bind::resolve` into lowered SQL + ordered params. But the spec's
`sql` is a **raw Postgres-dialect string** (`spec.rs`: `pub sql: String`).

PR #9 made the multi-backend write+read path work and added a backend-neutral
bind ABI (`read::bind::BindValue` on `BoundQuery.binds`), so the ClickHouse
backend can now translate the **placeholders** (`$N`/`::int8` → `?`) and bind
values. But it cannot translate **dialect**: PG idioms (`::casts`, `now() -
interval`, `CASE`, window fns, CTEs, array ops) don't map 1:1 to ClickHouse. So a
CH-backed table today still needs a **hand-authored CH-native spec**, and the
`query_rows` doc-comment says exactly that ("authoring CH read specs is
deferred"). One logical read = two SQL strings to maintain, or it 503s.

## Proposal

Compile a read spec to a **backend-neutral query IR**, then lower the IR to the
owning backend's dialect at execute time. One spec → runs on Postgres **or**
ClickHouse, authored once.

### The IR (normalized relational shape)

```
QueryIr {
  select:   [ Col | Expr(aliased) ],
  from:     TableRef,                  // schema.table (drives backend resolution)
  joins:    [ Join { table, on: Predicate } ],
  where:    [ Predicate ],             // AND-joined; fragment-gated predicates included/elided at compile
  group_by: [ Col ],
  order_by: [ (Col, Asc|Desc) ],
  limit:    Bound | Lit,
}
Predicate ::= Expr CmpOp Expr | Expr InList | Predicate And/Or Predicate | Not Predicate
Expr      ::= Col | Bind(slot) | Lit | Fn(name, [Expr]) | Cast(Expr, IrType)
```

`Bind(slot)` reuses PR #9's `BindValue` exactly — the IR carries the same
backend-neutral values; only the lowering differs (`$N` for PG, `?` for CH).
**The `Expr`/`Predicate` grammar is shared with the LQT governed-policy
substrate's expression sublanguage** (same arithmetic/comparison/logic tree) —
that is the "consolidate the compilation, not one per module" win: one
expression front-end, two back-ends (a SQL lowerer here, a bytecode emitter
there).

### Back-ends (lower IR → dialect)

```
trait Dialect { fn lower(&self, ir: &QueryIr, binds: &[BindValue]) -> BoundSql; }
PostgresDialect  // $N placeholders, ::int8 casts, ANSI
ClickHouseDialect // ? placeholders, CH type names, PREWHERE for leading key filters,
                  // FINAL opt-in for ReplacingMergeTree dedup-on-read
```

A `Fn`/`Cast` the target dialect can't express is a **compile-time error on that
backend** (loud, at spec-load), not a runtime 503 — you learn at boot that
`endpoint X uses CASE which the CH lowerer doesn't support`, before serving.

### Authoring — keep the SQL surface, parse it

Operators keep writing SQL in the spec (low friction, familiar). The compiler
**parses** the `sql` into the IR at spec-load (`spec::parse`) using a small
read-only SELECT grammar (the read layer is GET-only — no DML, no DDL, which
bounds the grammar hard). Specs that parse cleanly gain dual-dialect for free;
specs using an un-parseable construct fall back to the **raw-SQL path** (today's
behaviour) and are pinned to Postgres — a graceful, opt-in migration, not a
big-bang rewrite.

## Why this is the right shape (and what it buys)

- **Retires the dialect wall**: the open item from the ClickHouse activation
  ("CH read specs are placeholder-translated only, not SQL-transpiled") closes —
  `md.trade` and friends get one spec that lowers to CH.
- **70% already built** (the Explore audit): typed param specs (`kind`, `type`,
  `transform`, `select`/`present`/`absent`), fragment selection (clean enum
  dispatch, not string concat), and bind lowering with the neutral `BindValue`
  (PR #9) are all in place. The missing piece is the `sql: String` → IR parse +
  the two `Dialect` lowerers.
- **Compile-time backend validation**: spec-load fails loud per backend instead
  of a runtime `Unavailable`.
- **No VM** — this is layer β. It does not need or use `lqt-dsl-vm`; it shares
  only the expression grammar + the compile/version discipline.

## Migration / rollout (`T-READ-IR-001`)

1. Define `QueryIr` + `Predicate`/`Expr` (reuse `BindValue`).
2. `PostgresDialect` lowerer; assert byte-equivalent SQL to today's `bind::resolve`
   output over the existing spec corpus (no behaviour change for PG).
3. SELECT parser: `sql: String` → `QueryIr`; un-parseable specs fall back to the
   raw-SQL path (Postgres-pinned) with a `log::warn!` naming the construct.
4. `ClickHouseDialect` lowerer (`?` binds, CH types, `PREWHERE`, optional `FINAL`).
5. Convert `md.*` read specs to dual-dialect; drop the "author CH-native specs"
   caveat from `query_rows`.

## Non-goals

- Arbitrary SQL transpilation. The grammar is the bounded read-only SELECT shape
  the read layer already serves; anything outside it keeps the raw-SQL fallback.
- Writes (the ingest plane is already backend-agnostic via the `Backend` trait).
