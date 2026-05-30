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
    // Realtime upstream health (reported into Redis by the WS/poll workers).
    let rt = match st.redis.clone() {
        Some(mut c) => crate::realtime::health::read_all(&mut c).await,
        None => Vec::new(),
    };
    let rt_any_down = rt.iter().any(|h| h.state == "down");
    let overall_ok = db_ok && redis_state != "fail" && !rt_any_down;

    fn pill(name: &str, state: &str, descr: &str) -> String {
        let cls = match state {
            "ok" | "info" => "ok",
            "degraded" => "warn",
            "off" => "off",
            _ => "bad",
        };
        format!(
            "<div class=row><span class='pill {cls}'>{name}: {state}</span>\
             <span class=dim>{descr}</span></div>"
        )
    }

    // Realtime section: one pill per reported upstream. Absent → a hint that
    // no worker has reported yet (e.g. just restarted, or Redis off).
    let realtime_html = if rt.is_empty() {
        "<div class=row><span class='pill off'>realtime: n/a</span>\
         <span class=dim>no upstream has reported health yet</span></div>"
            .to_string()
    } else {
        rt.iter()
            .map(|h| {
                let state = match h.state.as_str() {
                    "up" => "ok",
                    "degraded" => "degraded",
                    "down" => "fail",
                    _ => "off",
                };
                let detail = if h.detail.is_empty() {
                    format!("as of {}", h.ts)
                } else {
                    format!("{} · as of {}", h.detail, h.ts)
                };
                pill(&h.name, state, &detail)
            })
            .collect::<String>()
    };

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
<h2>Realtime feeds</h2>{}\
<h2>Endpoint freshness (SLA)</h2>\
<div class=sla><span class='box green'>{} green</span><span class='box amber'>{} amber</span>\
<span class='box red'>{} red</span><span class='box gray'>{} gray</span></div>\
<p class=dim style='margin-top:1.6rem'><a href=/>← home</a> · <a href=/usage>usage</a> · <a href=/freshness>freshness (json)</a></p>\
</body></html>",
        if overall_ok { "<span class='pill ok'>ALL OK</span>" } else { "<span class='pill bad'>DEGRADED</span>" },
        pill("postgres", if db_ok { "ok" } else { "fail" }, "warehouse DB"),
        pill("redis", redis_state, "realtime broker · Tier-C cache"),
        pill("pool", "info", &format!("{in_use} in use / {} idle (size {})", ps.available, ps.size)),
        realtime_html,
        green, amber, red, gray,
    );
    Html(body)
}
