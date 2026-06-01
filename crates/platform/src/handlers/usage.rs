//! `GET /usage` — API usage dashboard. Port of `api/metrics.py::render_usage`.
//!
//! Reads the `metrics:*` Redis counters that the Python `record()` middleware
//! writes (all-time totals, status/method/endpoint hashes, principals zset,
//! and the minute/hour/day time-bucket keys) and renders the same light-theme
//! HTML page.
//!
//! DEFERRED: the per-request metric *recording* middleware (`metrics.record`)
//! is NOT ported here — this handler only reads + renders. If no Redis is
//! configured, it renders a "metrics unavailable" page.
//!
//! NOTE: the Python page also rendered the realtime hub's per-source stream
//! stats (`hub.realtime_stats()`). This Rust port has no in-process hub, so the
//! "Realtime streams" table renders its empty-state row, matching the
//! no-frames-yet case.

use axum::extract::{Query, State};
use axum::response::Html;
use redis::AsyncCommands;
use serde::Deserialize;

use crate::state::AppState;

const PREFIX: &str = "metrics:";
const SPARK_POINTS: usize = 36;

/// (label, seconds) — None = all-time. Mirrors Python `WINDOWS`.
const WINDOWS: &[(&str, Option<i64>)] = &[
    ("1h", Some(3600)),
    ("6h", Some(21600)),
    ("24h", Some(86400)),
    ("7d", Some(604800)),
    ("30d", Some(2592000)),
    ("all", None),
];

#[derive(Deserialize)]
pub struct UsageParams {
    pub window: Option<String>,
}

/// `GET /usage`. Gated (same as every other data route in the Python app).
pub async fn usage(
    State(st): State<AppState>,
    Query(p): Query<UsageParams>,
) -> Html<String> {
    let Some(mut conn) = st.redis.clone() else {
        return Html(unavailable_page());
    };

    // Resolve window label → seconds (default 24h).
    let win = p
        .window
        .as_deref()
        .map(|w| w.to_lowercase())
        .filter(|w| WINDOWS.iter().any(|(l, _)| *l == w))
        .unwrap_or_else(|| "24h".to_string());
    let win_secs = WINDOWS
        .iter()
        .find(|(l, _)| *l == win)
        .and_then(|(_, s)| *s);

    let m = gather(&mut conn, win_secs).await;

    Html(render(&win, &m))
}

/// Mirrors Python `_gather` — the bundle of counters the page renders.
struct Metrics {
    total: i64,
    bytes_out: i64,
    r429: i64,
    since: String,
    status: Vec<(String, i64)>,
    method: Vec<(String, i64)>,
    endpoints: Vec<(String, i64)>,
    principals: Vec<(String, i64)>,
    n_principals: i64,
    win_count: i64,
    spark: Vec<i64>,
    spark_unit: String,
    win_bytes: Option<i64>,
    last_60m: i64,
    rpm_now: i64,
}

async fn get_i64(conn: &mut redis::aio::MultiplexedConnection, key: &str) -> i64 {
    let v: Option<i64> = conn.get(key).await.ok().flatten();
    v.unwrap_or(0)
}

/// Fire-and-forget per-request recording (called from the auth gate). Writes the
/// global counters the dashboard reads + per-`sub` counters for `/usage/me`.
/// Swallows all errors — metrics must never affect the response.
pub async fn record(
    mut conn: redis::aio::MultiplexedConnection,
    sub: String,
    method: String,
    tmpl: String,
    status: u16,
    bytes: i64,
) {
    let now = chrono::Utc::now();
    let ts = now.timestamp();
    let (minute, hour, day) = (ts / 60, ts / 3600, ts / 86400);
    let cls = match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    };
    let mut p = redis::pipe();
    p.cmd("INCR").arg(format!("{PREFIX}total")).ignore();
    p.cmd("INCRBY").arg(format!("{PREFIX}bytes_out")).arg(bytes).ignore();
    p.cmd("HINCRBY").arg(format!("{PREFIX}status")).arg(cls).arg(1).ignore();
    p.cmd("HINCRBY").arg(format!("{PREFIX}endpoint")).arg(&tmpl).arg(1).ignore();
    p.cmd("HINCRBY").arg(format!("{PREFIX}method")).arg(&method).arg(1).ignore();
    p.cmd("ZINCRBY").arg(format!("{PREFIX}principals")).arg(1).arg(&sub).ignore();
    p.cmd("INCR").arg(format!("{PREFIX}min:{minute}")).ignore();
    p.cmd("EXPIRE").arg(format!("{PREFIX}min:{minute}")).arg(21600).ignore();
    p.cmd("INCR").arg(format!("{PREFIX}hr:{hour}")).ignore();
    p.cmd("EXPIRE").arg(format!("{PREFIX}hr:{hour}")).arg(3024000).ignore();
    p.cmd("INCR").arg(format!("{PREFIX}day:{day}")).ignore();
    p.cmd("EXPIRE").arg(format!("{PREFIX}day:{day}")).arg(34560000).ignore();
    p.cmd("INCRBY").arg(format!("{PREFIX}bytes_hr:{hour}")).arg(bytes).ignore();
    p.cmd("EXPIRE").arg(format!("{PREFIX}bytes_hr:{hour}")).arg(3024000).ignore();
    if status == 429 {
        p.cmd("INCR").arg(format!("{PREFIX}429")).ignore();
    }
    p.cmd("SETNX").arg(format!("{PREFIX}since")).arg(now.to_rfc3339()).ignore();
    // Per-sub (for /usage/me).
    p.cmd("INCR").arg(format!("{PREFIX}sub:{sub}:total")).ignore();
    p.cmd("INCRBY").arg(format!("{PREFIX}sub:{sub}:bytes")).arg(bytes).ignore();
    p.cmd("INCR").arg(format!("{PREFIX}sub:{sub}:hr:{hour}")).ignore();
    p.cmd("EXPIRE").arg(format!("{PREFIX}sub:{sub}:hr:{hour}")).arg(3024000).ignore();
    let _: Result<(), _> = p.query_async(&mut conn).await;
}

/// `GET /usage/me` — the calling identity's own usage (authed). Reads the
/// per-`sub` counters written by `record`.
pub async fn usage_me(
    State(st): State<AppState>,
    axum::Extension(identity): axum::Extension<crate::auth::Identity>,
) -> axum::response::Json<serde_json::Value> {
    let sub = identity.sub.clone();
    let Some(mut conn) = st.redis.clone() else {
        return axum::response::Json(serde_json::json!({"sub": sub, "metrics": "unavailable"}));
    };
    let total = get_i64(&mut conn, &format!("{PREFIX}sub:{sub}:total")).await;
    let bytes = get_i64(&mut conn, &format!("{PREFIX}sub:{sub}:bytes")).await;
    let hour = chrono::Utc::now().timestamp() / 3600;
    let mut last24: Vec<i64> = Vec::with_capacity(24);
    for h in (0..24).rev() {
        last24.push(get_i64(&mut conn, &format!("{PREFIX}sub:{sub}:hr:{}", hour - h)).await);
    }
    axum::response::Json(serde_json::json!({
        "sub": sub,
        "total_calls": total,
        "bytes_out": bytes,
        "calls_last_24h": last24.iter().sum::<i64>(),
        "hourly_last_24h": last24,
    }))
}

async fn gather(conn: &mut redis::aio::MultiplexedConnection, win_secs: Option<i64>) -> Metrics {
    let total = get_i64(conn, &format!("{PREFIX}total")).await;
    let bytes_out = get_i64(conn, &format!("{PREFIX}bytes_out")).await;
    let r429 = get_i64(conn, &format!("{PREFIX}429")).await;
    let since: Option<String> = conn.get(format!("{PREFIX}since")).await.ok().flatten();
    let since = since.unwrap_or_else(|| "—".to_string());

    let status_map: Vec<(String, i64)> = conn
        .hgetall(format!("{PREFIX}status"))
        .await
        .unwrap_or_default();
    let mut status = status_map;
    status.sort_by(|a, b| a.0.cmp(&b.0));

    let method_map: Vec<(String, i64)> = conn
        .hgetall(format!("{PREFIX}method"))
        .await
        .unwrap_or_default();
    let mut method = method_map;
    method.sort_by(|a, b| a.0.cmp(&b.0));

    let ep_map: Vec<(String, i64)> = conn
        .hgetall(format!("{PREFIX}endpoint"))
        .await
        .unwrap_or_default();
    let mut endpoints = ep_map;
    endpoints.sort_by(|a, b| b.1.cmp(&a.1));
    endpoints.truncate(20);

    // Top callers (zset, masked).
    let princ_raw: Vec<(String, i64)> = conn
        .zrevrange_withscores(format!("{PREFIX}principals"), 0, 9)
        .await
        .unwrap_or_default();
    let principals: Vec<(String, i64)> = princ_raw
        .into_iter()
        .map(|(s, c)| (mask(&s), c))
        .collect();
    let n_principals: i64 = conn.zcard(format!("{PREFIX}principals")).await.unwrap_or(0);

    let (win_count, spark, spark_unit, win_bytes) = windowed(conn, win_secs).await;

    // Short-window numbers (last 60 minute buckets).
    let now = now_secs();
    let now_min = now / 60;
    let keys: Vec<String> = (0..60).map(|i| format!("{PREFIX}min:{}", now_min - i)).collect();
    let mc = mget_i64(conn, &keys).await;
    let last_60m: i64 = mc.iter().sum();
    let rpm_now = if mc.len() > 1 { mc[1] } else { 0 };

    Metrics {
        total,
        bytes_out,
        r429,
        since,
        status,
        method,
        endpoints,
        principals,
        n_principals,
        win_count,
        spark,
        spark_unit,
        win_bytes,
        last_60m,
        rpm_now,
    }
}

/// Mirrors Python `_windowed` — count + sparkline + windowed bytes, choosing
/// bucket granularity by window size. Returns (count, spark, unit, bytes).
async fn windowed(
    conn: &mut redis::aio::MultiplexedConnection,
    win_secs: Option<i64>,
) -> (i64, Vec<i64>, String, Option<i64>) {
    let now = now_secs();

    let Some(secs) = win_secs else {
        // all-time: count from metrics:total, sparkline from last 30 day buckets.
        let total = get_i64(conn, &format!("{PREFIX}total")).await;
        let d0 = now / 86400;
        let keys: Vec<String> = (0..30).map(|i| format!("{PREFIX}day:{}", d0 - i)).collect();
        let mut vals = mget_i64(conn, &keys).await;
        vals.reverse();
        let bytes = get_i64(conn, &format!("{PREFIX}bytes_out")).await;
        return (total, vals, "day".to_string(), Some(bytes));
    };

    let (counts, unit, bytes_win): (Vec<i64>, &str, Option<i64>) = if secs <= 21600 {
        // <= 6h -> minute buckets
        let n = secs / 60;
        let m0 = now / 60;
        let keys: Vec<String> = (0..n).map(|i| format!("{PREFIX}min:{}", m0 - i)).collect();
        (mget_i64(conn, &keys).await, "min", None)
    } else if secs <= 2592000 {
        // <= 30d -> hour buckets
        let n = secs / 3600;
        let h0 = now / 3600;
        let keys: Vec<String> = (0..n).map(|i| format!("{PREFIX}hr:{}", h0 - i)).collect();
        let counts = mget_i64(conn, &keys).await;
        let bkeys: Vec<String> = (0..n).map(|i| format!("{PREFIX}bytes_hr:{}", h0 - i)).collect();
        let bytes_win: i64 = mget_i64(conn, &bkeys).await.iter().sum();
        (counts, "hr", Some(bytes_win))
    } else {
        // day buckets
        let n = secs / 86400;
        let d0 = now / 86400;
        let keys: Vec<String> = (0..n).map(|i| format!("{PREFIX}day:{}", d0 - i)).collect();
        (mget_i64(conn, &keys).await, "day", None)
    };

    let total: i64 = counts.iter().sum();
    // Bin newest->oldest down to SPARK_POINTS, then reverse to oldest->newest.
    let bin_sz = (counts.len() / SPARK_POINTS).max(1);
    let mut binned: Vec<i64> = Vec::new();
    let mut i = 0;
    while i < counts.len() {
        let end = (i + bin_sz).min(counts.len());
        binned.push(counts[i..end].iter().sum());
        i += bin_sz;
    }
    binned.truncate(SPARK_POINTS);
    binned.reverse();
    (total, binned, unit.to_string(), bytes_win)
}

async fn mget_i64(conn: &mut redis::aio::MultiplexedConnection, keys: &[String]) -> Vec<i64> {
    if keys.is_empty() {
        return Vec::new();
    }
    // MGET returns an array of Option<String>; missing keys are nil -> 0.
    let vals: Vec<Option<String>> = conn.get(keys).await.unwrap_or_default();
    vals.into_iter()
        .map(|v| v.and_then(|s| s.parse::<i64>().ok()).unwrap_or(0))
        .collect()
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Mirrors Python `_mask`.
fn mask(sub: &str) -> String {
    if sub == "anon" || sub.chars().count() <= 10 {
        return sub.to_string();
    }
    let chars: Vec<char> = sub.chars().collect();
    let head: String = chars[..8].iter().collect();
    let tail: String = chars[chars.len() - 3..].iter().collect();
    format!("{head}…{tail}")
}

/// Mirrors Python `_fmt_bytes`.
fn fmt_bytes(n: i64) -> String {
    let mut v = n as f64;
    for unit in ["B", "KB", "MB", "GB", "TB"] {
        if v < 1024.0 || unit == "TB" {
            return if unit == "B" {
                format!("{} B", n)
            } else {
                format!("{:.1} {}", v, unit)
            };
        }
        v /= 1024.0;
    }
    format!("{:.1} TB", v)
}

/// Mirrors Python `_spark`.
fn spark(vals: &[i64]) -> String {
    if vals.is_empty() {
        return String::new();
    }
    let blocks: Vec<char> = "▁▂▃▄▅▆▇█".chars().collect();
    let mx = (*vals.iter().max().unwrap_or(&0)).max(1) as f64;
    vals.iter()
        .map(|&v| {
            let idx = ((v as f64 / mx) * (blocks.len() - 1) as f64) as usize;
            blocks[idx.min(blocks.len() - 1)]
        })
        .collect()
}

/// Group separators every 3 digits, mirroring Python's `{:,}` format.
fn thousands(n: i64) -> String {
    let neg = n < 0;
    let s = n.abs().to_string();
    let bytes = s.as_bytes();
    let mut out = String::new();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*b as char);
    }
    if neg {
        format!("-{out}")
    } else {
        out
    }
}

fn unavailable_page() -> String {
    r#"<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>API usage</title>
<style>body{margin:0;background:#fbfaf7;color:#0f172a;
font-family:Inter,-apple-system,system-ui,sans-serif;line-height:1.5;}
.wrap{max-width:980px;margin:0 auto;padding:32px 24px 64px;}
h1{font-size:22px;margin:0 0 2px;}.sub{color:#94a3b8;font-size:12.5px;}</style>
</head><body><div class="wrap"><h1>API usage</h1>
<div class="sub">metrics unavailable — no metrics backend configured</div>
</div></body></html>"#
        .to_string()
}

fn now_utc() -> String {
    // Best-effort UTC timestamp without pulling chrono; the page is informational.
    let secs = now_secs();
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Convert epoch days to a Y-M-D via civil-from-days (Howard Hinnant algorithm).
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC", y, mo, d, h, mi, s)
}

fn render(win: &str, m: &Metrics) -> String {
    // Window selector links.
    let selector = WINDOWS
        .iter()
        .map(|(label, _)| {
            if *label == win {
                format!("<b>{label}</b>")
            } else {
                format!("<a href='/usage?window={label}'>{label}</a>")
            }
        })
        .collect::<Vec<_>>()
        .join(" · ");

    let kv_rows = |pairs: &[(String, i64)]| -> String {
        if pairs.is_empty() {
            return "<tr><td colspan=2 class='dim'>—</td></tr>".to_string();
        }
        pairs
            .iter()
            .map(|(k, v)| format!("<tr><td class='k'>{}</td><td class='v'>{}</td></tr>", k, thousands(*v)))
            .collect()
    };

    let ep_rows = if m.endpoints.is_empty() {
        "<tr><td colspan=2 class='dim'>no requests yet</td></tr>".to_string()
    } else {
        kv_rows(&m.endpoints)
    };
    let pr_rows = kv_rows(&m.principals);
    let st_rows = kv_rows(&m.status);
    let mth_rows = kv_rows(&m.method);
    // No in-process hub in the Rust port -> empty-state stream row.
    let stream_rows = "<tr><td colspan=5 class='dim'>no stream frames yet</td></tr>".to_string();

    let win_bytes_str = m
        .win_bytes
        .map(fmt_bytes)
        .unwrap_or_else(|| "—".to_string());

    // Replace the longer `{win_*}` placeholders BEFORE the `{win}` prefix,
    // otherwise `.replace("{win}", ...)` would corrupt `{win_count}` /
    // `{win_bytes}`.
    PAGE_TEMPLATE
        .replace("{now}", &now_utc())
        .replace("{since}", &m.since)
        .replace("{selector}", &selector)
        .replace("{total}", &thousands(m.total))
        .replace("{win_count}", &thousands(m.win_count))
        .replace("{win_bytes}", &win_bytes_str)
        .replace("{win}", win)
        .replace("{bytes_out}", &fmt_bytes(m.bytes_out))
        .replace("{last_60m}", &thousands(m.last_60m))
        .replace("{rpm}", &thousands(m.rpm_now))
        .replace("{r429}", &thousands(m.r429))
        .replace("{n_princ}", &thousands(m.n_principals))
        .replace("{spark_unit}", &m.spark_unit)
        .replace("{spark}", &spark(&m.spark))
        .replace("{ep_rows}", &ep_rows)
        .replace("{pr_rows}", &pr_rows)
        .replace("{st_rows}", &st_rows)
        .replace("{mth_rows}", &mth_rows)
        .replace("{stream_rows}", &stream_rows)
}

/// The `_PAGE` template from `metrics.py`, with the doubled `{{`/`}}` braces
/// (Python `str.format` escapes) un-doubled back to single braces, since this
/// port uses plain `.replace()` for the placeholders.
const PAGE_TEMPLATE: &str = r#"<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>API usage</title>
<meta http-equiv="refresh" content="15">
<style>
:root { --bg:#fbfaf7; --bg2:#fff; --fg:#0f172a; --dim:#64748b; --muted:#94a3b8;
        --accent:#0f766e; --border:rgba(15,23,42,.10); }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--fg);
        font-family:Inter,-apple-system,system-ui,sans-serif; line-height:1.5; }
.wrap { max-width:980px; margin:0 auto; padding:32px 24px 64px; }
h1 { font-size:22px; margin:0 0 2px; letter-spacing:-.01em; }
.sub { color:var(--muted); font-size:12.5px; margin-bottom:24px; }
.cards { display:grid; grid-template-columns:repeat(4,1fr); gap:1px; background:var(--border);
          border:1px solid var(--border); border-radius:10px; overflow:hidden; margin-bottom:8px; }
@media (max-width:720px){ .cards { grid-template-columns:1fr 1fr; } }
.card { background:var(--bg2); padding:18px 16px; }
.card .v { font-size:24px; font-weight:600; color:var(--accent); line-height:1; }
.card .l { font-size:11px; color:var(--muted); margin-top:7px; text-transform:uppercase; letter-spacing:.05em; }
.spark { font-family:ui-monospace,monospace; color:var(--accent); font-size:18px;
          letter-spacing:1px; margin:14px 0 28px; }
.spark .lbl { color:var(--muted); font-size:11px; letter-spacing:.05em; text-transform:uppercase; }
.windows { font-size:13px; margin:0 0 20px; color:var(--muted); }
.windows a { color:var(--accent); text-decoration:none; }
.windows a:hover { text-decoration:underline; }
.windows b { color:var(--fg); }
.grid { display:grid; grid-template-columns:1fr 1fr; gap:24px; }
@media (max-width:720px){ .grid { grid-template-columns:1fr; } }
h2 { font-size:13px; text-transform:uppercase; letter-spacing:.06em; color:var(--dim);
      margin:24px 0 8px; }
table { width:100%; border-collapse:collapse; font-size:13px; }
td { padding:6px 8px 6px 0; border-bottom:1px solid var(--border); }
td.k { font-family:ui-monospace,monospace; color:var(--fg); }
td.v { text-align:right; color:var(--dim); font-variant-numeric:tabular-nums; }
td.dim { color:var(--muted); }
tr:last-child td { border-bottom:none; }
.full { grid-column:1 / -1; }
</style></head><body><div class="wrap">
<h1>API usage</h1>
<div class="sub">kv.run:5000 · since {since} · {now} · auto-refresh 15s</div>
<div class="windows">window: {selector}</div>

<div class="cards">
  <div class="card"><div class="v">{win_count}</div><div class="l">calls · {win}</div></div>
  <div class="card"><div class="v">{win_bytes}</div><div class="l">bytes · {win}</div></div>
  <div class="card"><div class="v">{rpm}</div><div class="l">calls · last min</div></div>
  <div class="card"><div class="v">{r429}</div><div class="l">rate-limit 429s</div></div>
  <div class="card"><div class="v">{total}</div><div class="l">total calls (all-time)</div></div>
  <div class="card"><div class="v">{bytes_out}</div><div class="l">bytes (all-time)</div></div>
  <div class="card"><div class="v">{n_princ}</div><div class="l">distinct callers</div></div>
  <div class="card"><div class="v">{last_60m}</div><div class="l">calls · last 60m</div></div>
</div>
<div class="spark"><span class="lbl">calls/{spark_unit} ({win})&nbsp;</span>{spark}</div>

<div class="grid">
  <div>
    <h2>Top endpoints</h2>
    <table>{ep_rows}</table>
  </div>
  <div>
    <h2>Top callers (masked)</h2>
    <table>{pr_rows}</table>
    <h2>Status classes</h2>
    <table>{st_rows}</table>
    <h2>Methods</h2>
    <table>{mth_rows}</table>
  </div>
  <div class="full">
    <h2>Realtime streams (frames seen / lag ms by source)</h2>
    <table>
      <tr><td class="k" style="color:var(--muted)">source</td><td class="v">frames</td>
          <td class="v">p50</td><td class="v">p95</td><td class="v">p99</td></tr>
      {stream_rows}
    </table>
  </div>
</div>
</div></body></html>"#;
