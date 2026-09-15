//! Generic platform landing — the domain-free fallback page served at `GET /`
//! when an app contributes no landing of its own.
//!
//! It is CONFIG-DRIVEN, for a reason worth stating: this one binary ships as
//! several products (the data service, `lumid-llm`, …), and a hardcoded page
//! meant every non-data app described itself as "a portable data service" and
//! advertised discovery routes it does not serve. Measured on lum.id/llm
//! 2026-08-23: the LLM gateway's front page offered Catalog / lineage / ingest
//! tiles, three of which 401 or 404 there, and named neither `/v1/messages` nor
//! a single model — while `LUMID_SERVICE_NAME`, `LUMID_SERVICE_HEADING` and
//! `LUMID_SERVICE_TAGLINE` sat in its manifest read by nothing.
//!
//! So: the copy comes from those env vars, and the tiles come from what the
//! process can ACTUALLY serve — the LLM tiles appear iff an LLM route exists
//! (a pooled backend, a primary, or an OpenRouter roster). Apps can still
//! replace the page wholesale via `ServeParts.landing` (see `boot::serve`).

use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::Router;

use crate::state::AppState;

/// The generic landing routes used when an app provides none. Just `GET /`.
pub fn default_routes() -> Router<AppState> {
    Router::new().route("/", get(landing))
}

/// Minimal HTML-attribute/text escape. The inputs are operator-set env vars,
/// not user input, but they land in both attributes and text nodes and a stray
/// quote would silently break the head.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// One discovery tile.
struct Tile {
    href: &'static str,
    title: &'static str,
    desc: &'static str,
    endpoint: String,
}

fn tile_html(t: &Tile) -> String {
    format!(
        r#"      <a class="tile" href="{href}">
        <div class="t">{title}</div>
        <div class="d">{desc}</div>
        <div class="e">{endpoint}</div>
      </a>
"#,
        href = t.href,
        title = t.title,
        desc = t.desc,
        endpoint = t.endpoint
    )
}

/// `GET /` — generic platform landing. Public.
pub async fn landing(State(st): State<AppState>) -> Html<String> {
    // Does this process actually serve the LLM plane? Asked of live config
    // rather than a compile-time flag, so the page cannot drift from what the
    // binary will answer.
    let llm_live = !st.llm_pool.all.is_empty()
        || !st.settings.llm_backend_url.is_empty()
        || !st.llm_pool.openrouter_url.is_empty();

    let name = crate::config::env_var("SERVICE_NAME").unwrap_or_else(|| {
        if llm_live { "Lumid LLM".into() } else { "lumid-data-service".into() }
    });
    let heading = crate::config::env_var("SERVICE_HEADING").unwrap_or_else(|| {
        if llm_live {
            "One gateway, many models.".into()
        } else {
            "A portable data service.".into()
        }
    });
    let tagline = crate::config::env_var("SERVICE_TAGLINE").unwrap_or_else(|| {
        if llm_live {
            "An OpenAI- and Anthropic-compatible inference gateway. Model-routed \
             across a health-probed, least-loaded backend pool with a circuit \
             breaker, overflowing to <em>OpenRouter</em> only for models an \
             operator configured — unknown ids are refused, never forwarded."
                .into()
        } else {
            "Config-driven REST reads, a schema-introspecting <em>ingest</em> plane, a \
             Redis pub/sub <em>realtime</em> hub, catalog + lineage, auth, and an \
             auto-generated <em>MCP</em> surface — in one binary. The dataset on top is \
             defined by configuration, not code."
                .into()
        }
    });

    let mut tiles: Vec<Tile> = Vec::new();
    tiles.push(Tile {
        href: "/openapi.json",
        title: "OpenAPI",
        desc: "The full machine-readable spec for every route this binary serves.",
        endpoint: "GET /openapi.json".into(),
    });

    if llm_live {
        // Name the real default so the page answers "what do I put in `model`?"
        // — the single most common question this page exists to answer.
        let default_model = if st.settings.llm_default_model.is_empty() {
            "—".to_string()
        } else {
            esc(&st.settings.llm_default_model)
        };
        tiles.push(Tile {
            href: "/v1/models",
            title: "Models",
            desc: "Every model this gateway will actually route — local pool first,                    then the configured OpenRouter roster. Ids listed here are exactly                    the ids accepted in `model`.",
            endpoint: "GET /v1/models".into(),
        });
        tiles.push(Tile {
            href: "/openapi.json",
            title: "Chat",
            desc: "OpenAI-shaped and Anthropic-shaped chat, streaming or buffered.                    SSE keepalives are relayed, so a long prefill never looks idle.",
            endpoint: format!(
                "POST /v1/chat/completions • /v1/messages<br>default model: <code>{default_model}</code>"
            ),
        });
        tiles.push(Tile {
            href: "/status",
            title: "Status",
            desc: "Live backend health — per-backend probe state, in-flight count                    and circuit-breaker position.",
            endpoint: "GET /status".into(),
        });
    } else {
        tiles.push(Tile {
            href: "/catalog/schemas",
            title: "Catalog",
            desc: "Browse schemas, tables, and per-table profiles. Trace row lineage.",
            endpoint: "GET /catalog/schemas • /catalog/lineage/*".into(),
        });
        tiles.push(Tile {
            href: "/status",
            title: "Status",
            desc: "Live health board — realtime feeds + per-endpoint freshness.",
            endpoint: "GET /status • /freshness".into(),
        });
    }
    tiles.push(Tile {
        href: "/usage",
        title: "Usage",
        desc: "The global request dashboard.",
        endpoint: "GET /usage • /usage/me".into(),
    });

    let tiles_html: String = tiles.iter().map(tile_html).collect();

    // Raw strings: this block is shell + JSON + HTML entities, and every one of
    // those wants a backslash or a brace of its own.
    let example = if llm_live {
        let m = if st.settings.llm_default_model.is_empty() {
            "&lt;model&gt;".to_string()
        } else {
            esc(&st.settings.llm_default_model)
        };
        let tmpl = r#"# Chat needs a bearer token; discovery (/openapi.json) is public.
curl -H "Authorization: Bearer &lt;token&gt;" \
  -H "Content-Type: application/json" \
  -d '{"model":"__MODEL__","messages":[{"role":"user","content":"hello"}]}' \
  https://&lt;host&gt;/v1/chat/completions"#;
        tmpl.replace("__MODEL__", &m)
    } else {
        r#"# Every data route needs a bearer token; discovery is public.
curl -H "Authorization: Bearer &lt;token&gt;" https://&lt;host&gt;/catalog/schemas"#
            .to_string()
    };

    let footer = if llm_live {
        format!("{} · model-routed inference over a pooled GPU fleet", esc(&name))
    } else {
        format!("{} · the portable data platform", esc(&name))
    };

    let meta_desc = {
        // Strip inline markup for the meta description.
        let re_less = tagline.replace("<em>", "").replace("</em>", "");
        esc(&re_less.split_whitespace().collect::<Vec<_>>().join(" "))
    };

    Html(
        LANDING_SHELL
            .replace("{{TITLE}}", &esc(&name))
            .replace("{{META_DESC}}", &meta_desc)
            .replace("{{HEADING}}", &esc(&heading))
            // Tagline intentionally NOT escaped: operators author light inline
            // markup (<em>) in it, and it is not user-supplied.
            .replace("{{TAGLINE}}", &tagline)
            .replace("{{TILES}}", &tiles_html)
            .replace("{{EXAMPLE}}", &example)
            .replace("{{FOOTER}}", &footer),
    )
}

const LANDING_SHELL: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{TITLE}}</title>
  <meta name="description" content="{{META_DESC}}">
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
    <h1>{{HEADING}}</h1>
    <p class="tag">{{TAGLINE}}</p>
  </header>

  <section>
    <h2>Discover</h2>
    <div class="tiles">
{{TILES}}    </div>

<pre class="code">{{EXAMPLE}}</pre>
  </section>

  <footer>{{FOOTER}}</footer>
</div>
</body>
</html>"##;

#[cfg(test)]
mod landing_template_tests {
    use super::LANDING_SHELL;

    /// Every `{{PLACEHOLDER}}` in the shell must be one `landing()` actually
    /// substitutes. A forgotten one does not fail to compile -- it ships a
    /// literal `{{TAGLINE}}` to the front page of a product.
    #[test]
    fn every_placeholder_is_substituted() {
        let substituted = [
            "{{TITLE}}",
            "{{META_DESC}}",
            "{{HEADING}}",
            "{{TAGLINE}}",
            "{{TILES}}",
            "{{EXAMPLE}}",
            "{{FOOTER}}",
        ];
        let mut rest = LANDING_SHELL.to_string();
        for p in substituted {
            assert!(
                LANDING_SHELL.contains(p),
                "{p} is substituted by landing() but absent from the shell — dead replace()"
            );
            rest = rest.replace(p, "");
        }
        assert!(
            !rest.contains("{{"),
            "shell has a placeholder landing() never substitutes: {}",
            rest.split("{{").nth(1).unwrap_or("").split("}}").next().unwrap_or("")
        );
    }
}
