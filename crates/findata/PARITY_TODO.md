# Read-layer parity follow-ups

Config engine wired + 92 endpoints config-served. Parity vs `.parity_baseline`
(113 endpoints): **87 byte-identical**, 26 differ. Triage below.

## Benign — time-varying data drift (NOT regressions)
News (`/news/*`), market-movers, dividends/splits calendars, `/technical/:symbol`,
`/macro/treasury-rates`, `/prediction-markets/leaderboard`, `/transcripts/*`,
`/insider/*` (new rows since capture), and `/catalog/*` (run counts / sizes;
catalog isn't migrated anyway). Content moves between captures — shape matches.

## Real spec fixes needed (financial.toml is runtime-loaded — no recompile)

1. **Optional bind in BASE SQL without default → 500 "bind ':x' has no resolved
   value".** Confirmed: `/index/:index_symbol/constituents` (`:as_of`), plus
   `:since`-bearing specs. FIX PATTERN: an optional value bind must live inside
   a `present` fragment (only substituted when the param is supplied); the
   `absent` fragment supplies a no-bind default. E.g. constituents:
   ```
   sql: ... WHERE upper(index_symbol)=upper(:index_symbol){{asof_filter}}
   param as_of (date, optional):
     present: asof_filter = " AND (added_date<=:as_of) AND (removed_date IS NULL OR removed_date>:as_of)"
     absent:  asof_filter = " AND (added_date<=current_date) AND (removed_date IS NULL OR removed_date>current_date)"
   ```
   Audit every spec for a `:name` in base SQL whose param is optional + has no
   default.

2. **"ORDER BY x DESC LIMIT n then reverse" mis-ported to `ORDER BY x ASC LIMIT n`.**
   That returns the EARLIEST n, not the latest n ascending. Confirmed:
   `/market-cap/:symbol/history` (returned 1990 instead of recent). Likely also
   `/prediction-markets/open-interest/*` and `/wallet/*/pnl`. FIX: wrap —
   `SELECT * FROM (SELECT ... ORDER BY date DESC LIMIT :limit) s ORDER BY date ASC`.

3. **Envelope dropped the computed `as_of` top-level field.** `/holders/:symbol/top`
   and `/fund-ownership/:symbol` baseline include `"as_of": <max period>`; config
   envelope omits it. FIX: add `as_of` (the subquery value) — either surface it as
   a row col the envelope hoists, or defer these two to findata-ext.

4. **shape=one 404 message** (`/etf/:symbol/info`): generic `not found` vs the
   original `no ETF info for "AAPL"`. Cosmetic; optionally add a per-spec
   `not_found_message`.

## Verify loop
Edit `financial.toml`, reboot the binary (re-parses), re-run the baseline capture
+ `diff` (see the parity harness). Target: only the benign data-drift set differs.
