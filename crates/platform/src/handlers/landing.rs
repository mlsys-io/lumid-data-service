//! Generic platform landing — the domain-free fallback page served at `GET /`
//! when an app contributes no landing of its own.
//!
//! The platform names no domain, so this page is intentionally minimal: it
//! states what the service is (a portable read/write/realtime data platform)
//! and points at the generic discovery surfaces (`/openapi.json`, `/status`,
//! `/catalog/schemas`). Apps override it by passing their own landing routes via
//! `ServeParts.landing` (see `boot::serve`).

use axum::response::Html;
use axum::routing::get;
use axum::Router;

use crate::state::AppState;

/// The generic landing routes used when an app provides none. Just `GET /`.
pub fn default_routes() -> Router<AppState> {
    Router::new().route("/", get(landing))
}

/// `GET /` — generic platform landing. Public.
///
/// Links are RELATIVE (`openapi.json`, not `/openapi.json`): behind an edge
/// that mounts the service under a prefix (lum.id serves it at `/data/`,
/// root-stripped, and 301s `/data` to `/data/`) an absolute link escapes the
/// mount, while a relative one resolves under it — and still resolves at `/`
/// when the service is reached directly.
///
/// `LUMID_VIEWER_URL` (optional, http/https only) adds a tile linking to a
/// browsing UI for this service; the platform names no domain itself.
pub async fn landing() -> Html<String> {
    let viewer = std::env::var("LUMID_VIEWER_URL")
        .ok()
        .filter(|u| u.starts_with("https://") || u.starts_with("http://"))
        .map(|u| viewer_tile(&u))
        .unwrap_or_default();
    Html(GENERIC_LANDING_HTML.replace("<!--VIEWER-->", &viewer))
}

fn viewer_tile(url: &str) -> String {
    let u = url
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    format!(
        r#"<a class="tile" href="{u}">
        <div class="t">Viewer</div>
        <div class="d">Browse schemas and tables, sample rows, and manage data jobs in a UI.</div>
        <div class="e">{u}</div>
      </a>"#
    )
}

const GENERIC_LANDING_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>lumid-data-service</title>
  <meta name="description" content="A portable read / write / realtime data service — config-driven REST reads, a schema-introspecting ingest plane, a Redis pub/sub realtime hub, catalog + lineage, auth, and MCP.">
  <meta name="theme-color" content="#0f766e">
  <style>
    :root { --bg:#fbfaf7; --fg:#0f172a; --fg-dim:#475569; --fg-muted:#94a3b8;
            --accent:#0f766e; --accent-2:#115e59; --border:rgba(15,23,42,0.10);
            --card:#fff; --code-bg:#f5f3ee; }
    * { box-sizing:border-box; }
    html,body { margin:0; padding:0; background:var(--bg); color:var(--fg);
      font-family:Inter,-apple-system,BlinkMacSystemFont,sans-serif; line-height:1.55;
      -webkit-font-smoothing:antialiased; }
    a { color:var(--accent); text-decoration:none; } a:hover { color:var(--accent-2); }
    code { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:0.86em; }
    .wrap { max-width:880px; margin:0 auto; padding:0 28px; }
    header.hero { padding:72px 0 40px; }
    header.hero h1 { font-weight:600; font-size:clamp(34px,5vw,56px); letter-spacing:-0.01em;
      margin:0 0 16px; }
    header.hero .tag { font-size:clamp(16px,1.6vw,18px); color:var(--fg-dim); max-width:640px;
      margin:0 0 28px; }
    header.hero .tag em { color:var(--accent); font-style:normal; font-weight:600; }
    .tiles { display:grid; grid-template-columns:repeat(2,1fr); gap:14px; }
    @media (max-width:620px){ .tiles { grid-template-columns:1fr; } }
    .tile { display:block; padding:18px 20px; background:var(--card); border:1px solid var(--border);
      border-radius:10px; color:var(--fg); transition:all 0.15s ease; }
    a.tile:hover { border-color:var(--accent); transform:translateY(-1px); }
    .tile .t { font-weight:600; font-size:15px; }
    .tile .d { color:var(--fg-dim); font-size:13.5px; margin-top:6px; }
    .tile .e { color:var(--fg-muted); font-size:11.5px; margin-top:9px;
      font-family:ui-monospace,monospace; }
    pre.code { background:var(--code-bg); border:1px solid var(--border); border-radius:8px;
      padding:14px 16px; overflow-x:auto; font-size:13px; line-height:1.6; margin:24px 0 0; }
    section { padding:44px 0; border-top:1px solid var(--border); }
    section h2 { font-weight:600; font-size:22px; margin:0 0 12px; }
    section p { color:var(--fg-dim); max-width:680px; }
    footer { padding:40px 0 56px; border-top:1px solid var(--border); color:var(--fg-muted);
      font-size:13px; }
  </style>
</head>
<body>
<div class="wrap">
  <header class="hero">
    <h1>A portable data service.</h1>
    <p class="tag">
      Config-driven REST reads, a schema-introspecting <em>ingest</em> plane, a
      Redis pub/sub <em>realtime</em> hub, catalog + lineage, auth, and an
      auto-generated <em>MCP</em> surface — in one binary. The dataset on top is
      defined by configuration, not code.
    </p>
  </header>

  <section>
    <h2>Discover</h2>
    <div class="tiles">
      <a class="tile" href="openapi.json">
        <div class="t">OpenAPI</div>
        <div class="d">The full machine-readable spec for every read route.</div>
        <div class="e">GET /openapi.json</div>
      </a>
      <a class="tile" href="catalog/schemas">
        <div class="t">Catalog</div>
        <div class="d">Browse schemas, tables, and per-table profiles. Trace row lineage.</div>
        <div class="e">GET /catalog/schemas • /catalog/lineage/*</div>
      </a>
      <a class="tile" href="status">
        <div class="t">Status</div>
        <div class="d">Live health board — realtime feeds + per-endpoint freshness.</div>
        <div class="e">GET /status • /freshness</div>
      </a>
<!--VIEWER-->
      <a class="tile" href="usage">
        <div class="t">Usage</div>
        <div class="d">The global request dashboard.</div>
        <div class="e">GET /usage • /usage/me</div>
      </a>
    </div>

<pre class="code"># Every data route needs a bearer token; discovery is public.
curl -H "Authorization: Bearer &lt;token&gt;" https://&lt;host&gt;/catalog/schemas</pre>
  </section>

  <section>
    <h2>Jobs</h2>
    <p>
      Run work on FlowMesh, Lumilake or a sandbox <em>where the data is</em>. A job declares
      the sources it reads (<code>findata://</code>, <code>lumid-data://</code>,
      <code>src://</code>, <code>data://&lt;hub&gt;/…</code>, <code>hf://</code>); this service
      checks you may read each one, plans the site and the network route for every source,
      and dispatches as you. The job gets a token scoped to exactly its declared sources, and
      withdrawing a share cuts a running job off within seconds.
    </p>
    <div class="tiles">
      <div class="tile">
        <div class="t">Sources</div>
        <div class="d">Register data you own, share it read or write, see what is shared with you.</div>
        <div class="e">/jobs/v1/sources • /jobs/v1/sources/{id}/grants</div>
      </div>
      <div class="tile">
        <div class="t">Plan</div>
        <div class="d">Dry run: where a job would run and how each source is reached, with reasons.</div>
        <div class="e">POST /jobs/v1/plan</div>
      </div>
      <div class="tile">
        <div class="t">Jobs</div>
        <div class="d">Submit (idempotent), follow status and events, cancel.</div>
        <div class="e">/jobs/v1/jobs • /jobs/v1/jobs/{id}/events</div>
      </div>
      <div class="tile">
        <div class="t">Agents</div>
        <div class="d">The same verbs as MCP tools: jobs_sources, jobs_plan, jobs_submit, jobs_status.</div>
        <div class="e">POST /mcp</div>
      </div>
    </div>
  </section>

  <footer>lumid-data-service · the portable data platform</footer>
</div>
</body>
</html>"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_are_relative_so_a_prefix_mount_keeps_them() {
        for a in [
            "href=\"/openapi",
            "href=\"/catalog",
            "href=\"/status",
            "href=\"/usage",
        ] {
            assert!(
                !GENERIC_LANDING_HTML.contains(a),
                "absolute link {a} escapes a prefix mount"
            );
        }
        assert!(GENERIC_LANDING_HTML.contains("<h2>Jobs</h2>"));
    }

    #[test]
    fn viewer_tile_escapes_its_url() {
        let t = viewer_tile("https://x/y?a=1&b=\"<z>");
        assert!(
            t.contains("href=\"https://x/y?a=1&amp;b=&quot;&lt;z&gt;\""),
            "{t}"
        );
    }
}
