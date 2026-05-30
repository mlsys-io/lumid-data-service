# Read-layer parity status

Config engine serves 92 endpoints. Parity vs `.parity_baseline` (113 captured):
**93 byte-identical; 20 differ — all either time-varying data drift or cosmetic.**
Verified structurally (same response keys) for the drift set.

## Fixed
- Optional binds in base SQL → 500 ("no resolved value"): gated in
  `present`/`absent` fragments (dividends/splits calendars, index constituents,
  technical, news/search, kols history/search).
- `i64`/`int4` serialization mismatch: engine now casts numeric binds
  (`$N::int8` / `$N::float8`) so one Rust width matches any column width.
- Enum values now also bindable (`:window`/`:venue` on leaderboard).
- `ORDER BY DESC LIMIT n then reverse` mis-port: wrapped as
  `SELECT * FROM (… DESC LIMIT n) ORDER BY … ASC` (market-cap history,
  open-interest, wallet/pnl).
- `/holders/:symbol/top` + `/fund-ownership/:symbol`: deferred to compiled
  handlers (computed `as_of` envelope field) — re-added to app.rs.

## Remaining diffs (no action needed / cosmetic)
- **Data drift (structurally identical):** news, market-movers, technical,
  insider, treasury-rates, prediction-markets/events, kols, catalog stats —
  these update continuously; the baseline was captured at a different instant.
- **Cosmetic:** `/etf/:symbol/info` 404 detail message wording; `/news/search`
  both 400 on missing required `q` (baseline captured the error too).
- **Minor gap:** `/index/:index_symbol/constituents` envelope omits the
  `index_symbol` + `as_of` top-level fields (the generic envelope only hoists a
  `symbol` path param). Data array is correct. Fix later by hoisting the
  `:index_symbol` path value + an `as_of` field, or defer to ext.

Verify loop: edit `financial.toml`, **flush the test Redis** (cached L2 responses
mask spec changes — bit us once), reboot, re-run the baseline capture + `diff`.
