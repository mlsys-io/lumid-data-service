//! Freshness handler — port of `api/routes/freshness.py`. `freshness` is the
//! JSON view (gated); `status` is the public HTML status board linked from the
//! landing page (health pills + freshness summary).

use axum::extract::State;
use axum::response::Html;
use axum::Json;
use serde_json::{Map, Value};

use crate::error::ApiResult;
use crate::queries;
use crate::state::AppState;

pub async fn freshness(State(st): State<AppState>) -> ApiResult<Json<Map<String, Value>>> {
    Ok(Json(queries::freshness::counts(&st.pool).await?))
}

/// Public HTML status board (`/status`) — a compact port of
/// `freshness.py::status_combined`: DB / Redis / pool health pills plus the
/// endpoint-freshness SLA summary. Best-effort throughout — never 500s.
pub async fn status(State(st): State<AppState>) -> Html<String> {
    // DB health
    let db_ok = match st.pool.get().await {
        Ok(c) => c.query_one("SELECT 1", &[]).await.is_ok(),
        Err(_) => false,
    };
    // Redis health (PING)
    let redis_state = match st.redis.clone() {
        Some(mut c) => {
            let pong: Result<String, _> = redis::cmd("PING").query_async(&mut c).await;
            if pong.is_ok() { "ok" } else { "fail" }
        }
        None => "off",
    };
    // Pool stats
    let ps = st.pool.status();
    let in_use = ps.size.saturating_sub(ps.available);
    // Freshness SLA counts (green/amber/red/gray)
    let g = |m: &Map<String, Value>, k: &str| m.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
    let (green, amber, red, gray) = match queries::freshness::counts(&st.pool).await {
        Ok(m) => (g(&m, "green"), g(&m, "amber"), g(&m, "red"), g(&m, "gray")),
        Err(_) => (0, 0, 0, 0),
    };
    fn pill(name: &str, state: &str, descr: &str) -> String {
        let cls = match state {
            "ok" | "info" | "up" => "ok",
            "degraded" => "warn",
            "off" => "off",
            _ => "bad",
        };
        format!(
            "<div class=row><span class='pill {cls}'>{name}: {state}</span>\
             <span class=dim>{descr}</span></div>"
        )
    }

    // ---- Realtime feed health, MEASURED from last:tick freshness (not login
    // state): a feed is only live if fresh ticks are actually arriving. Group
    // the warm set by asset class; classify each by the freshest tick's age +
    // latency + source. Crypto/forex are 24/7 (no live ticks ⇒ fail unless a
    // standby is delivering); equities only tick during market hours.
    const LIVE_S: i64 = 30; // a tick this fresh ⇒ the feed is flowing
    fn classify(s: &str) -> &'static str {
        const CCY: &[&str] = &[
            "USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "CNY", "HKD", "SGD", "NOK", "SEK",
        ];
        if s.len() == 6 && s.chars().all(|c| c.is_ascii_alphabetic())
            && CCY.contains(&&s[..3]) && CCY.contains(&&s[3..])
        {
            return "forex";
        }
        if s.ends_with("USD") || s.ends_with("USDT") || s.ends_with("USDC") {
            return "crypto";
        }
        "equity"
    }

    // Sample the warm set's last:tick into per-class aggregates.
    struct Agg { total: usize, live: usize, best_age: i64, latency: Option<i64>, source: String }
    let mut feeds: std::collections::BTreeMap<&'static str, Agg> = std::collections::BTreeMap::new();
    if let Some(mut c) = st.redis.clone() {
        let now = chrono::Utc::now();
        for sym in &st.settings.rt_warm_symbols {
            let cls = classify(sym);
            let e = feeds.entry(cls).or_insert(Agg { total: 0, live: 0, best_age: i64::MAX, latency: None, source: String::new() });
            e.total += 1;
            let payload: Option<String> = redis::cmd("HGET")
                .arg(format!("last:tick:{sym}")).arg("payload")
                .query_async(&mut c).await.ok().flatten();
            let v = payload.and_then(|p| serde_json::from_str::<Value>(&p).ok());
            if let Some(ts) = v.as_ref().and_then(|v| v.get("ts")).and_then(|t| t.as_str()) {
                let age = chrono::DateTime::parse_from_rfc3339(ts)
                    .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds())
                    .unwrap_or(i64::MAX);
                if age <= LIVE_S { e.live += 1; }
                if age < e.best_age {
                    e.best_age = age;
                    e.latency = v.as_ref().and_then(|v| v.get("latency_ms")).and_then(|l| l.as_i64());
                    e.source = v.as_ref().and_then(|v| v.get("source")).and_then(|s| s.as_str()).unwrap_or("").to_string();
                }
            }
        }
    }

    // Classify each feed. "fail" only for a 24/7 class (crypto/forex) with no
    // live ticks (no standby delivering). A standby/fallback source (finnhub
    // shadow for crypto/forex, or tier_b polling) ⇒ degraded, not up.
    let mut feeds_html = String::new();
    let mut feed_fail = false;
    for (cls, a) in &feeds {
        let is_247 = *cls == "crypto" || *cls == "forex";
        let (state, detail) = if a.live > 0 {
            let lat = a.latency.map(|l| format!("{l}ms")).unwrap_or_else(|| "?".into());
            let standby = a.source.contains("finnhub") && is_247 || a.source.starts_with("tier_b");
            let how = if standby { "live via standby" } else { "live" };
            let st = if standby { "degraded" } else { "up" };
            (st, format!("{how} · {} · {lat} · {}s ago ({}/{} symbols)", a.source, a.best_age, a.live, a.total))
        } else if is_247 {
            feed_fail = true;
            ("fail", "no live ticks — no standby delivering; /quotes uses last-close fallback".to_string())
        } else {
            ("degraded", "no live ticks (market closed?) — /quotes uses last close".to_string())
        };
        feeds_html.push_str(&pill(cls, state, &detail));
    }

    // Realtime health hash: data feeds (kind=feed, e.g. PM/predexon recorders)
    // and link connections (kind=connection, e.g. the quote WS upstreams).
    let rt = match st.redis.clone() {
        Some(mut c) => crate::realtime::health::read_all(&mut c).await,
        None => Vec::new(),
    };
    // Append measured feed-kind entries to the feeds section, classified by
    // DATA freshness (the recorders refresh their ts on every flush, so an aged
    // ts = stalled feed). A reported "down" is a hard fail.
    let now2 = chrono::Utc::now();
    for h in rt.iter().filter(|h| h.kind == "feed") {
        let (state, detail) = if h.state == "down" {
            feed_fail = true;
            ("fail", format!("{} · as of {}", h.detail, h.ts))
        } else {
            let age = chrono::DateTime::parse_from_rfc3339(&h.ts)
                .map(|t| (now2 - t.with_timezone(&chrono::Utc)).num_seconds())
                .unwrap_or(i64::MAX);
            if age < 60 {
                ("up", format!("{} · {age}s ago", h.detail))
            } else if age < 300 {
                ("degraded", format!("lagging · {} · {age}s ago", h.detail))
            } else {
                feed_fail = true;
                ("fail", format!("stalled · {} · {age}s ago", h.detail))
            }
        };
        feeds_html.push_str(&pill(&h.name, state, &detail));
    }
    if feeds_html.is_empty() {
        feeds_html = "<div class=row><span class='pill off'>feeds: n/a</span>\
            <span class=dim>no warm symbols configured (FINDATA_RT_WARM_SYMBOLS)</span></div>".to_string();
    }

    // Connection diagnostics (raw WS link state) — connection-kind only.
    let conns_html = rt.iter().filter(|h| h.kind == "connection").map(|h| {
        let state = match h.state.as_str() { "up" => "up", "degraded" => "degraded", "down" => "fail", _ => "off" };
        let detail = if h.detail.is_empty() { format!("as of {}", h.ts) } else { format!("{} · as of {}", h.detail, h.ts) };
        pill(&h.name, state, &detail)
    }).collect::<String>();

    // Overall verdict: measured feed failure (a 24/7 feed with no live data)
    // is a genuine DEGRADED — not the FMP login state, which a standby covers.
    let overall_ok = db_ok && redis_state != "fail" && !feed_fail;

    let body = format!(
        "<!doctype html><html><head><meta charset=utf-8>\
<meta name=viewport content='width=device-width,initial-scale=1'>\
<title>status · lumid-data-service</title><style>\
body{{font-family:Inter,system-ui,sans-serif;background:#fbfaf7;color:#1a2b29;margin:0;padding:2.5rem;max-width:760px;margin:0 auto}}\
h1{{font-size:1.4rem;font-weight:600}}h2{{font-size:.95rem;color:#0f766e;margin-top:1.8rem}}\
.row{{display:flex;gap:.7rem;align-items:center;margin:.4rem 0}}\
.pill{{padding:.15rem .6rem;border-radius:999px;font-size:.8rem;font-weight:600}}\
.pill.ok{{background:#d1faf0;color:#0f766e}}.pill.bad{{background:#fde2e1;color:#b42318}}\
.pill.warn{{background:#fdf2d0;color:#92600a}}\
.pill.off{{background:#eceae4;color:#6b6b6b}}.dim{{color:#6b6b6b;font-size:.85rem}}\
.sla{{display:flex;gap:.5rem;margin:.4rem 0}}.box{{padding:.4rem .8rem;border-radius:8px;font-weight:600}}\
.green{{background:#d1faf0;color:#0f766e}}.amber{{background:#fdf2d0;color:#92600a}}\
.red{{background:#fde2e1;color:#b42318}}.gray{{background:#eceae4;color:#6b6b6b}}\
a{{color:#0f766e}}</style></head><body>\
<h1>System status &nbsp;{}</h1>\
<h2>Services</h2>{}{}{}\
<h2>Realtime feeds <span class=dim>(measured from tick freshness)</span></h2>{}\
<h2>Feed connections <span class=dim>(raw link state)</span></h2>{}\
<h2>Endpoint freshness (SLA)</h2>\
<div class=sla><span class='box green'>{} green</span><span class='box amber'>{} amber</span>\
<span class='box red'>{} red</span><span class='box gray'>{} gray</span></div>\
<p class=dim style='margin-top:1.6rem'><a href=/>← home</a> · <a href=/usage>usage</a> · <a href=/freshness>freshness (json)</a></p>\
</body></html>",
        if overall_ok { "<span class='pill ok'>ALL OK</span>" } else { "<span class='pill bad'>DEGRADED</span>" },
        pill("postgres", if db_ok { "ok" } else { "fail" }, "warehouse DB"),
        pill("redis", redis_state, "realtime broker · Tier-C cache"),
        pill("pool", "info", &format!("{in_use} in use / {} idle (size {})", ps.available, ps.size)),
        feeds_html,
        conns_html,
        green, amber, red, gray,
    );
    Html(body)
}
