# T-READ-IR-001 — backend-neutral query IR for the read layer

> **Status update (2026-07):** landed. `QueryIr`/`Predicate`/`Expr`, the SELECT
> parser with raw-SQL fallback, and both `PostgresDialect` + `ClickHouseDialect`
> lowerers now live in `crates/platform/src/read/{ir,parse,dialect}.rs`. The
> checklist below is the original spec, kept as a historical record.

**Area:** READ-IR · **Size:** L · **Status:** implemented (was: proposed)
**Design:** `docs/decisions/read-query-ir.md`
**Builds on:** PR #9 (`BindValue` ABI + CH placeholder translation)

## Context

Read specs emit raw Postgres-dialect SQL (`read::spec::EndpointSpec::sql:
String`). PR #9 lets the CH backend translate placeholders but not dialect, so a
CH-backed table needs a hand-authored CH-native spec or 503s. Compile specs to a
backend-neutral IR and lower per dialect so one spec runs on PG or CH.

## Scope

1. **`QueryIr`** — normalized read-only SELECT (`select/from/joins/where/group_by/
   order_by/limit`) with `Predicate`/`Expr` over `Col | Bind(slot) | Lit | Fn |
   Cast`. `Bind` reuses `read::bind::BindValue` verbatim.
2. **`Dialect` trait** + `PostgresDialect` (lower IR → `$N`/`::int8`/ANSI). Pin
   byte-equivalence to today's `bind::resolve` SQL over the existing spec corpus.
3. **SELECT parser** — `sql: String` → `QueryIr` at `spec::parse`. Un-parseable
   constructs fall back to the raw-SQL path (Postgres-pinned) with a `warn!`
   naming the construct. Graceful, opt-in migration.
4. **`ClickHouseDialect`** — `?` binds, CH type names, `PREWHERE` for leading
   ORDER-BY-key filters, opt-in `FINAL` for ReplacingMergeTree dedup-on-read.
5. **Convert `md.*` read specs** to dual-dialect; remove the "author CH-native
   specs" caveat from `clickhouse::query_rows`.

## Definition of done

- [ ] PG lowerer output byte-equivalent to current `bind::resolve` over the spec
      corpus (no behaviour change for Postgres-backed reads).
- [ ] A `md.trade` (CH-backed) read spec authored ONCE lowers + executes on CH
      (closes the placeholder-only limitation from the CH activation).
- [ ] Un-parseable spec → raw-SQL fallback, Postgres-pinned, logged — never a
      silent mis-execution.
- [ ] Per-backend compile-time validation: a spec using a construct the CH
      lowerer can't express fails at spec-load (loud), not at request time.
- [ ] Platform lib tests green; clippy adds no new `-D warnings` (the crate has
      pre-existing debt — don't worsen it).

## Non-goals

- Arbitrary SQL transpilation (grammar is the bounded read-only SELECT shape).
- Writes (ingest is already backend-agnostic via the `Backend` trait).
