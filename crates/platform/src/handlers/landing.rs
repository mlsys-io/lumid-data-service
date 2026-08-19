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
/// Branding is per-deployment via env (all `LUMID_`-prefixed): a deployment that
/// serves inference sets `SERVICE_NAME`/`SERVICE_HEADING`/`SERVICE_TAGLINE` to
/// present as e.g. "Lumid LLM"; the findata deployment (env unset) renders the
/// original data-platform copy byte-for-byte.
pub async fn landing() -> Html<String> {
    let name = crate::config::env_var("SERVICE_NAME")
        .unwrap_or_else(|| "lumid-data-service".to_string());
    let heading = crate::config::env_var("SERVICE_HEADING")
        .unwrap_or_else(|| "A portable data service.".to_string());
    // An LLM deployment sets SERVICE_TAGLINE (+ enable_llm); render the inference
    // "Discover" surface (models + multimodal examples). Unset → the data-service
    // page, byte-for-byte as before.
    let is_llm = crate::config::env_var("SERVICE_TAGLINE").is_some();
    let tagline =
        crate::config::env_var("SERVICE_TAGLINE").unwrap_or_else(|| DEFAULT_TAGLINE.to_string());
    let discover = if is_llm { LLM_DISCOVER } else { DATA_DISCOVER };
    // The hero tagline carries inline <em> markup, which is invalid inside a
    // meta content attribute — strip tags (and escape quotes) for the <meta>
    // description so the two never drift apart the way they did when the LLM
    // deployment kept advertising the data-service copy to crawlers.
    let tagline_text = strip_tags(&tagline);
    Html(
        GENERIC_LANDING_HTML
            .replace("__SVC_NAME__", &name)
            .replace("__SVC_HEADING__", &heading)
            .replace("__SVC_TAGLINE_TEXT__", &tagline_text)
            .replace("__SVC_TAGLINE__", &tagline)
            .replace("__DISCOVER__", discover),
    )
}

/// Strip inline HTML tags and escape quotes so a hero tagline can be reused as
/// the `<meta name="description">` content. Deliberately minimal — the taglines
/// are our own copy with `<em>` in them, not untrusted input.
fn strip_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            '"' => {
                if !in_tag {
                    out.push_str("&quot;")
                }
            }
            c if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

/// Default hero tagline (the original data-platform copy) used when
/// `LUMID_SERVICE_TAGLINE` is unset.
const DEFAULT_TAGLINE: &str = "Config-driven REST reads, a schema-introspecting <em>ingest</em> \
    plane, a Redis pub/sub <em>realtime</em> hub, catalog + lineage, auth, and an auto-generated \
    <em>MCP</em> surface — in one binary. The dataset on top is defined by configuration, not code.";

const GENERIC_LANDING_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>__SVC_NAME__</title>
  <meta name="description" content="__SVC_TAGLINE_TEXT__">
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
    a.tile { display:block; padding:18px 20px; background:var(--card); border:1px solid var(--border);
      border-radius:10px; color:var(--fg); transition:all 0.15s ease; }
    a.tile:hover { border-color:var(--accent); transform:translateY(-1px); }
    a.tile .t { font-weight:600; font-size:15px; }
    a.tile .d { color:var(--fg-dim); font-size:13.5px; margin-top:6px; }
    a.tile .e { color:var(--fg-muted); font-size:11.5px; margin-top:9px;
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
    <h1>__SVC_HEADING__</h1>
    <p class="tag">__SVC_TAGLINE__</p>
  </header>

  <section>
    <h2>Discover</h2>
__DISCOVER__
  </section>

  <footer>__SVC_NAME__</footer>
</div>
</body>
</html>"##;

/// Data-service "Discover" section (findata deployment — unchanged copy).
const DATA_DISCOVER: &str = r##"    <div class="tiles">
      <a class="tile" href="/openapi.json">
        <div class="t">OpenAPI</div>
        <div class="d">The full machine-readable spec for every read route.</div>
        <div class="e">GET /openapi.json</div>
      </a>
      <a class="tile" href="/catalog/schemas">
        <div class="t">Catalog</div>
        <div class="d">Browse schemas, tables, and per-table profiles. Trace row lineage.</div>
        <div class="e">GET /catalog/schemas • /catalog/lineage/*</div>
      </a>
      <a class="tile" href="/status">
        <div class="t">Status</div>
        <div class="d">Live health board — realtime feeds + per-endpoint freshness.</div>
        <div class="e">GET /status • /freshness</div>
      </a>
      <a class="tile" href="/usage">
        <div class="t">Usage</div>
        <div class="d">The global request dashboard.</div>
        <div class="e">GET /usage • /usage/me</div>
      </a>
    </div>

<pre class="code"># Every data route needs a bearer token; discovery is public.
curl -H "Authorization: Bearer &lt;token&gt;" https://&lt;host&gt;/catalog/schemas</pre>"##;

/// LLM-gateway "Discover" section — models + multimodal usage.
const LLM_DISCOVER: &str = r##"    <div class="tiles">
      <a class="tile" href="/v1/models">
        <div class="t">Models</div>
        <div class="d">Chat + vision, embeddings, image generation, text-to-speech, and the omni agent — one OpenAI/Anthropic-compatible API.</div>
        <div class="e">GET /v1/models</div>
      </a>
      <a class="tile" href="/openapi.json">
        <div class="t">OpenAPI</div>
        <div class="d">The full machine-readable spec for every /v1 route.</div>
        <div class="e">GET /openapi.json</div>
      </a>
      <a class="tile" href="/status">
        <div class="t">Status</div>
        <div class="d">Live health board — backend pool + per-endpoint freshness.</div>
        <div class="e">GET /status</div>
      </a>
      <a class="tile" href="/usage">
        <div class="t">Usage</div>
        <div class="d">Your request dashboard.</div>
        <div class="e">GET /usage • /usage/me</div>
      </a>
    </div>

<pre class="code"># Chat — OpenAI- or Anthropic-compatible. All calls need a bearer token.
# deepseek-v4-flash is the default, served in house on our own GB10 pair.
curl https://&lt;host&gt;/v1/chat/completions -H "Authorization: Bearer &lt;token&gt;" \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"Hello"}]}'

# Omit "model" to get the default. Anthropic-style clients use /v1/messages.
curl https://&lt;host&gt;/v1/messages -H "Authorization: Bearer &lt;token&gt;" \
  -d '{"max_tokens":256,"messages":[{"role":"user","content":"Hello"}]}'

# Reasoning models stream their thinking separately — give max_tokens room, or a
# small budget is spent before any answer and content comes back empty.

# GET /v1/models lists what is served locally. Any other id falls through to
# OpenRouter and is BILLED per token — including a typo or a retired model id,
# which returns 200 rather than 404. Note deepseek-v4-flash (ours, free) and
# deepseek/deepseek-v4-flash-0731 (OpenRouter, metered) are NOT the same route.
curl https://&lt;host&gt;/v1/models -H "Authorization: Bearer &lt;token&gt;"</pre>"##;
