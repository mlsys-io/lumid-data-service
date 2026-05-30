//! Static landing surfaces — ports of `api/landing.py` and `api/llm_landing.py`.
//!
//! All three routes are PUBLIC (no auth) in the Python app: `GET /`,
//! `GET /reference` (both serve the main landing page), and `GET /llm`.
//! Handlers return `Html<String>`; the big HTML lives in `const` strings below
//! copied verbatim from the Python `_LANDING_HTML` / `_TEMPLATE`.

use axum::response::Html;

/// Default LLM model id, used to fill the `__MODEL__` placeholder in the
/// `/llm` template. The Python source reads `settings.llm_default_model or
/// _MODEL`; `Settings` here has no such field, so we use the same fallback
/// constant verbatim.
const LLM_MODEL: &str = "cyankiwi/MiniMax-M2.7-AWQ-4bit";

/// `GET /` and `GET /reference` — the main landing page. Public.
pub async fn landing() -> Html<String> {
    Html(LANDING_HTML.to_string())
}

/// `GET /llm` — the LLM serving landing page. Public.
pub async fn llm_landing() -> Html<String> {
    Html(LLM_TEMPLATE.replace("__MODEL__", LLM_MODEL))
}

const LANDING_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>findata</title>
  <meta name="description" content="A read-only HTTP, WebSocket, and MCP interface over a 1.3 TB financial dataset: OHLC, fundamentals, news, KOL tweets, prediction markets, realtime ticks.">
  <meta name="theme-color" content="#fbfaf7">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Cinzel:wght@400;500;600&family=Inter:wght@300;400;500;600;700&display=swap">
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%230f766e'/%3E%3Cpath d='M7 22 L13 14 L18 18 L25 9' stroke='%23ecfdf5' stroke-width='2.5' fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
  <style>
    :root {
      --bg:         #fbfaf7;
      --bg-2:       #ffffff;
      --bg-3:       #f5f3ee;
      --card:       #ffffff;
      --card-hover: #f8fafc;
      --border:     rgba(15, 118, 110, 0.22);
      --border-soft:rgba(15, 23, 42, 0.10);
      --fg:         #0f172a;
      --fg-dim:     #475569;
      --fg-muted:   #94a3b8;
      --accent:     #0f766e;
      --accent-2:   #115e59;
      --accent-3:   #134e4a;
      --red:        #dc2626;
      --yellow:     #d97706;
      --code-bg:    #f5f3ee;
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; background: var(--bg); color: var(--fg);
                 font-family: Inter, -apple-system, BlinkMacSystemFont, sans-serif;
                 font-weight: 400; line-height: 1.55; -webkit-font-smoothing: antialiased;
                 font-feature-settings: "cv11", "ss01"; }
    a { color: var(--accent); text-decoration: none; }
    a:hover { color: var(--accent-2); }
    code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
                  font-size: 0.86em; }

    /* layout */
    .wrap { max-width: 1080px; margin: 0 auto; padding: 0 28px; }

    /* top nav */
    nav.top { padding: 22px 0 0; display: flex; align-items: center; gap: 28px;
              font-size: 14px; }
    nav.top .brand { font-family: Cinzel, Georgia, serif; font-weight: 500;
                     font-size: 18px; letter-spacing: 0.08em; color: var(--fg);
                     display: flex; align-items: center; gap: 10px; }
    nav.top .brand-mark { width: 22px; height: 22px; }
    nav.top a.link { color: var(--fg-dim); font-weight: 400; }
    nav.top a.link:hover { color: var(--fg); }
    nav.top .spacer { flex: 1; }
    nav.top .pill { padding: 6px 12px; border: 1px solid var(--border);
                    border-radius: 999px; color: var(--accent); font-size: 12px;
                    font-weight: 500; letter-spacing: 0.04em; }

    /* hero */
    header.hero { padding: 80px 0 56px; }
    header.hero h1 { font-family: Cinzel, Georgia, serif; font-weight: 500;
                     font-size: clamp(40px, 6vw, 72px); line-height: 1.02;
                     letter-spacing: -0.01em; margin: 0 0 18px;
                     color: var(--fg); }
    header.hero .tagline { font-size: clamp(17px, 1.7vw, 19px); color: var(--fg-dim);
                           max-width: 660px; margin: 0 0 30px; line-height: 1.55; }
    header.hero .tagline em { color: var(--accent); font-style: normal; font-weight: 500; }
    header.hero .cta { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }
    header.hero .btn { display: inline-flex; align-items: center; gap: 6px;
                       padding: 11px 20px; border-radius: 8px; font-weight: 500;
                       font-size: 14px; letter-spacing: 0.01em;
                       border: 1px solid transparent; transition: all 0.15s ease; }
    header.hero .btn.primary { background: var(--accent); color: #fff; }
    header.hero .btn.primary:hover { background: var(--accent-2); }
    header.hero .btn.secondary { background: var(--bg-2); color: var(--fg);
                                 border-color: var(--border-soft); }
    header.hero .btn.secondary:hover { border-color: var(--border); color: var(--accent); }
    header.hero .quickstart { margin-top: 38px; max-width: 660px; }
    header.hero .quickstart .label { font-size: 11px; letter-spacing: 0.12em;
                                     color: var(--fg-muted); text-transform: uppercase;
                                     margin-bottom: 10px; }

    /* code blocks */
    pre.code { background: var(--code-bg); border: 1px solid var(--border-soft);
               border-radius: 8px; padding: 16px 18px; overflow-x: auto;
               font-size: 13px; line-height: 1.65; margin: 0; color: #1e293b; }
    pre.code .c1 { color: #94a3b8; }               /* comment */
    pre.code .k1 { color: var(--accent); font-weight: 500; }   /* curl / verb */
    pre.code .s1 { color: #b45309; }               /* string */
    pre.code .p1 { color: #1d4ed8; }               /* path / endpoint */
    pre.code .v1 { color: #7c3aed; }               /* var */

    /* sections */
    section { padding: 56px 0; border-top: 1px solid var(--border-soft); }
    section h2 { font-family: Cinzel, Georgia, serif; font-weight: 500;
                 font-size: 28px; letter-spacing: -0.005em; margin: 0 0 12px;
                 color: var(--fg); }
    section h2 .num { color: var(--accent); font-size: 18px; margin-right: 12px;
                      font-weight: 500; vertical-align: 4px; letter-spacing: 0.08em; }
    section p.intro { color: var(--fg-dim); max-width: 720px; margin: 0 0 32px;
                      font-size: 15.5px; }

    /* capability tiles */
    .tiles { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
    .tiles a.tile { display: block; padding: 20px 22px; background: var(--card);
                    border: 1px solid var(--border-soft); border-radius: 10px;
                    transition: all 0.15s ease; color: var(--fg); }
    .tiles a.tile:hover { background: var(--card-hover);
                          border-color: var(--border); transform: translateY(-1px); }
    .tiles a.tile .title { font-weight: 600; font-size: 15px; color: var(--fg);
                           display: flex; align-items: center; gap: 8px; }
    .tiles a.tile .title .ic { color: var(--accent); font-size: 13px;
                               font-family: ui-monospace, monospace; }
    .tiles a.tile .desc { color: var(--fg-dim); font-size: 13.5px;
                          margin-top: 8px; line-height: 1.5; }
    .tiles a.tile .ep { color: var(--fg-muted); font-size: 11.5px; margin-top: 10px;
                        font-family: ui-monospace, monospace; }
    @media (max-width: 820px) { .tiles { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 540px) { .tiles { grid-template-columns: 1fr; } }

    /* examples */
    .examples { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .examples .ex { background: var(--bg-2); border: 1px solid var(--border-soft);
                    border-radius: 10px; padding: 18px 20px;
                    box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04); }
    .examples .ex h3 { margin: 0 0 4px; font-size: 14px; color: var(--fg);
                       font-weight: 600; }
    .examples .ex .sub { color: var(--fg-muted); font-size: 12.5px; margin-bottom: 12px; }
    .examples .ex pre { margin: 0; font-size: 12.5px; }
    @media (max-width: 760px) { .examples { grid-template-columns: 1fr; } }

    /* stats strip */
    .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px;
             background: var(--border-soft); border: 1px solid var(--border-soft);
             border-radius: 10px; overflow: hidden; margin: 32px 0 0; }
    .stats .stat { background: var(--bg-2); padding: 22px 18px; }
    @media (max-width: 820px) { .stats { grid-template-columns: 1fr 1fr; } }
    .stats .stat .v { font-family: Cinzel, Georgia, serif; font-size: 28px;
                      color: var(--accent); font-weight: 500; line-height: 1; }
    .stats .stat .l { color: var(--fg-muted); font-size: 11.5px; margin-top: 8px;
                      letter-spacing: 0.06em; text-transform: uppercase; }
    @media (max-width: 760px) { .stats { grid-template-columns: 1fr 1fr; } }

    /* surface table */
    table.surface { width: 100%; border-collapse: collapse; font-size: 14px;
                    margin-top: 4px; }
    table.surface th { text-align: left; padding: 10px 12px 10px 0;
                       color: var(--fg-muted); font-weight: 500;
                       font-size: 11.5px; letter-spacing: 0.08em;
                       text-transform: uppercase;
                       border-bottom: 1px solid var(--border-soft); }
    table.surface td { padding: 12px 12px 12px 0; vertical-align: top;
                       border-bottom: 1px solid var(--border-soft); }
    table.surface td.k { font-family: ui-monospace, monospace; color: var(--accent);
                         font-size: 13px; white-space: nowrap; width: 22%; }
    table.surface td.d { color: var(--fg-dim); }
    table.surface tr:last-child td { border-bottom: none; }

    /* footer */
    footer { padding: 48px 0 56px; border-top: 1px solid var(--border-soft);
             color: var(--fg-muted); font-size: 13px; }
    footer .row { display: flex; flex-wrap: wrap; gap: 24px; align-items: center; }
    footer .row a { color: var(--fg-dim); }
    footer .row a:hover { color: var(--accent); }
    footer .row .spacer { flex: 1; }
    footer .signature { font-family: Cinzel, Georgia, serif; letter-spacing: 0.06em;
                        color: var(--fg-dim); font-size: 12px; }

    /* status dot */
    .dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%;
           background: var(--accent); margin-right: 7px; vertical-align: 1px;
           box-shadow: 0 0 0 3px rgba(52, 211, 153, 0.18); }

    /* subtle ambient gradient */
    body::before { content: ""; position: fixed; inset: 0;
                   background: radial-gradient(ellipse 80% 50% at 50% -10%,
                       rgba(15, 118, 110, 0.06), transparent 60%);
                   pointer-events: none; z-index: -1; }
  </style>
</head>
<body>
<div class="wrap">

  <nav class="top">
    <div class="brand">
      <svg class="brand-mark" viewBox="0 0 32 32" fill="none">
        <rect width="32" height="32" rx="6" fill="#0f766e"/>
        <path d="M7 22 L13 14 L18 18 L25 9" stroke="#ecfdf5" stroke-width="2.5"
              fill="none" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
      findata
    </div>
    <a class="link" href="/reference">Reference</a>
    <a class="link" href="/usage.md">Guide</a>
    <a class="link" href="/llm">LLM</a>
    <a class="link" href="/status">Status</a>
    <a class="link" href="/usage">Usage</a>
    <a class="link" href="/redoc">ReDoc</a>
    <a class="link" href="/docs">Swagger</a>
    <div class="spacer"></div>
    <span class="pill"><span class="dot"></span>kv.run:5000</span>
  </nav>

  <header class="hero">
    <h1>Financial data, one API.</h1>
    <p class="tagline">
      A read-only HTTP, WebSocket, and MCP interface over a <em>1.3 TB</em>
      financial dataset — symbols, OHLC bars, fundamentals, news, ownership,
      regulatory, macro, plus an <em>11.4 M-row</em> KOL tweet archive with
      <em>4.7 M</em> mirrored images. Refreshed hourly, streamed live.
    </p>
    <div class="cta">
      <a class="btn primary" href="/reference">Open reference →</a>
      <a class="btn secondary" href="#quickstart">Quick start</a>
      <a class="btn secondary" href="#realtime">Realtime &amp; KOL</a>
    </div>

    <div class="stats">
      <div class="stat"><div class="v">133</div><div class="l">endpoints</div></div>
      <div class="stat"><div class="v">11.4 M</div><div class="l">KOL tweets</div></div>
      <div class="stat"><div class="v">4.7 M</div><div class="l">tweet images</div></div>
      <div class="stat"><div class="v">17.4 M</div><div class="l">news articles</div></div>
      <div class="stat"><div class="v">7,851</div><div class="l">symbols</div></div>
      <div class="stat"><div class="v">8.2 M+</div><div class="l">prediction markets</div></div>
      <div class="stat"><div class="v">85 M+</div><div class="l">PM trades</div></div>
      <div class="stat"><div class="v">1.3 TB</div><div class="l">total dataset</div></div>
    </div>
  </header>

  <!-- ============== QUICK START ============== -->
  <section id="quickstart">
    <h2><span class="num">01</span>Quick start</h2>
    <p class="intro">
      Every endpoint is a plain GET that returns JSON. <b>A Lumid PAT is
      required</b> &mdash; pass it as <code>Authorization: Bearer &lt;pat&gt;</code>.
      Anonymous requests get <code>401</code>. The authed tier is 100 req/min.
    </p>

    <div class="examples">
      <div class="ex">
        <h3>Latest snapshot for one symbol</h3>
        <div class="sub">Profile, sector, market cap from the warehouse.</div>
<pre class="code"><span class="k1">curl</span> https://kv.run:5000<span class="p1">/symbols/AAPL</span></pre>
      </div>
      <div class="ex">
        <h3>OHLC bars — one liner</h3>
        <div class="sub">Defaults to last 365 d for <code>1d</code>, 7 d for <code>5min</code>, 1 d for <code>1min</code>.</div>
<pre class="code"><span class="k1">curl</span> "https://kv.run:5000<span class="p1">/ohlc/AAPL</span>?interval=1d"</pre>
      </div>
      <div class="ex">
        <h3>Real-time tick snapshot</h3>
        <div class="sub">Last-known tick across the Tier-C cache.</div>
<pre class="code"><span class="k1">curl</span> "https://kv.run:5000<span class="p1">/quotes</span>?symbols=AAPL,BTCUSD"</pre>
      </div>
      <div class="ex">
        <h3>Stream live ticks (WebSocket)</h3>
        <div class="sub">Lumid bearer required. Subscribe by symbol after connect.</div>
<pre class="code"><span class="k1">websocat</span> -H="Authorization: Bearer $TOKEN" \
  wss://kv.run:5000<span class="p1">/ws/quotes</span></pre>
      </div>
      <div class="ex">
        <h3>Server-side screener</h3>
        <div class="sub">Sector / industry / market-cap / exchange / instrument-type.</div>
<pre class="code"><span class="k1">curl</span> "https://kv.run:5000<span class="p1">/screener</span>?sector=<span class="s1">Technology</span>&amp;market_cap_min=<span class="v1">1e12</span>"</pre>
      </div>
      <div class="ex">
        <h3>KOL tweets — archive search</h3>
        <div class="sub">11.4 M tweets, 2010-2026. Full-text + cashtag indexed.</div>
<pre class="code"><span class="k1">curl</span> "https://kv.run:5000<span class="p1">/kols/tweets/search</span>?q=<span class="s1">earnings+beat</span>"</pre>
      </div>
      <div class="ex">
        <h3>News firehose by category</h3>
        <div class="sub">17.4 M articles across multiple wire and CSV feeds.</div>
<pre class="code"><span class="k1">curl</span> "https://kv.run:5000<span class="p1">/news/latest</span>?category=<span class="s1">general</span>&amp;limit=20"</pre>
      </div>
      <div class="ex">
        <h3>Tweet image (locally mirrored)</h3>
        <div class="sub">4.7 M cached images. <code>by-url</code> falls through to pbs.twimg.com if not cached.</div>
<pre class="code"><span class="k1">curl</span> -L "https://kv.run:5000<span class="p1">/kols/media/by-url</span>?u=&lt;twitter-cdn-url&gt;"</pre>
      </div>
    </div>
  </section>

  <!-- ============== SURFACE ============== -->
  <section id="surface">
    <h2><span class="num">02</span>Surface</h2>
    <p class="intro">
      Endpoints are grouped by data domain. Open any group in the
      <a href="/reference">reference</a> for a request builder, response schema,
      and an example payload.
    </p>

    <div class="tiles">

      <a class="tile" href="/reference#tag/Symbols">
        <div class="title"><span class="ic">/</span>Symbols</div>
        <div class="desc">Universe lookup, search, profile, the canonical 7,851-symbol roster.</div>
        <div class="ep">GET /symbols • /universe • /screener</div>
      </a>

      <a class="tile" href="/reference#tag/OHLC">
        <div class="title"><span class="ic">/</span>OHLC</div>
        <div class="desc">1-minute, 5-minute, and dividend-adjusted daily bars across asset classes.</div>
        <div class="ep">GET /ohlc/{symbol}?interval=…</div>
      </a>

      <a class="tile" href="/reference#tag/Realtime">
        <div class="title"><span class="ic">/</span>Realtime</div>
        <div class="desc">WS + SSE tick streams, last-tick snapshot, day range + 50/200-day SMAs, news WS, lag percentiles per source.</div>
        <div class="ep">WSS /ws/quotes • /ws/news • SSE /quotes/stream • /quote-stats/{sym}</div>
      </a>

      <a class="tile" href="/reference#tag/PredictionMarkets">
        <div class="title"><span class="ic">/</span>Prediction Markets</div>
        <div class="desc"><b>Kalshi + Polymarket</b>: 7.8M+ markets (Polymarket back to 2020-10), 84M+ trades, 150M+ L2 orderbook snapshots, OHLC bars across 5 intervals (1m/5m/15m/1h/1d via continuous aggregates). Polymarket 1m/5m candles UNION executed trade prints with orderbook-midprice for ~7-month history; Kalshi candles back to 2021-06. Open-interest history, top holders, wallet leaderboard + PnL/positions/activity, cross-venue equivalents. <b>Live</b>: containerized CLOB-WSS recorder captures 500 active Polymarket markets; consume via <code>SSE /prediction-markets/stream</code>.</div>
        <div class="ep">GET /prediction-markets/markets/search • /trades/{venue}/{id} • SSE /stream</div>
      </a>

      <a class="tile" href="/reference#tag/Fundamentals">
        <div class="title"><span class="ic">/</span>Fundamentals</div>
        <div class="desc">Income, balance, and cash-flow statements. Wide-format latest or historical.</div>
        <div class="ep">GET /fundamentals/{symbol} • /financials/{symbol}/{kind}</div>
      </a>

      <a class="tile" href="/reference#tag/Analysis">
        <div class="title"><span class="ic">/</span>Analysis</div>
        <div class="desc">Ratios, key metrics, point-in-time metrics snapshot (52-week stats, returns, beta), per-statement growth, financial scores, owner earnings, DCF.</div>
        <div class="ep">GET /ratios/{sym} • /key-metrics/{sym} • /metrics-snapshot/{sym} • /dcf/{sym}</div>
      </a>

      <a class="tile" href="/reference#tag/Estimates">
        <div class="title"><span class="ic">/</span>Estimates</div>
        <div class="desc">Analyst price targets, grades (current + history), upgrade-downgrade news.</div>
        <div class="ep">GET /price-target/{sym} • /grades/{sym}</div>
      </a>

      <a class="tile" href="/reference#tag/Investors">
        <div class="title"><span class="ic">/</span>Investors</div>
        <div class="desc">13F top holders, insider transactions, fund ownership, gov / acquisitions.</div>
        <div class="ep">GET /holders/{sym} • /insider/{sym} • /gov-trades/{sym}</div>
      </a>

      <a class="tile" href="/reference#tag/Events">
        <div class="title"><span class="ic">/</span>Events</div>
        <div class="desc">Earnings calendar, transcripts, IPOs, mergers, FDA calendar.</div>
        <div class="ep">GET /earnings/{sym} • /transcripts/{sym} • /ipos</div>
      </a>

      <a class="tile" href="/reference#tag/News">
        <div class="title"><span class="ic">/</span>News</div>
        <div class="desc"><b>17.4 M articles</b> deduped across multiple wire and CSV feeds. Per-symbol, global firehose by category, or PG full-text search.</div>
        <div class="ep">GET /news/latest • /news/search • /news/{symbol}</div>
      </a>

      <a class="tile" href="/reference#tag/KOL">
        <div class="title"><span class="ic">/</span>KOL</div>
        <div class="desc">Curated 78-handle roster + live cashtag SSE. <b>11.4 M-row Postgres archive</b> (2010-2026, full-text + cashtag-indexed) with <b>4.7 M mirrored images</b> served at <code>/kols/media/</code>.</div>
        <div class="ep">GET /kols/tweets/search • /kols/{handle}/tweets/history • /kols/media/by-url</div>
      </a>

      <a class="tile" href="/reference#tag/ETF">
        <div class="title"><span class="ic">/</span>ETF</div>
        <div class="desc">Info, holdings, sector weights, asset-class exposure for 500 ETFs.</div>
        <div class="ep">GET /etf/{sym}/info • /etf/{sym}/holdings</div>
      </a>

      <a class="tile" href="/reference#tag/Macro">
        <div class="title"><span class="ic">/</span>Macro &amp; Regulatory</div>
        <div class="desc">Treasury rates, economic indicators, ESG, patents, lobbying, USA-spending.</div>
        <div class="ep">GET /macro/* • /regulatory/* • /esg/{sym}/*</div>
      </a>

    </div>
  </section>

  <!-- ============== REALTIME ============== -->
  <section id="realtime">
    <h2><span class="num">03</span>Realtime</h2>
    <p class="intro">
      Three streams over the same hub: tick prices, news articles, KOL tweets.
      Both <code>WSS /ws/quotes</code> (bidirectional, subscribe-by-symbol) and
      <code>GET /quotes/stream</code> (SSE) are supported; <code>WSS /ws/news</code>
      is a news-only variant. Authentication via Lumid PAT (or X-API-Key).
    </p>

    <table class="surface">
      <thead>
        <tr><th>Stream</th><th>Transport</th><th>Source tier</th></tr>
      </thead>
      <tbody>
        <tr><td class="k">tick:&lt;sym&gt;</td>
            <td class="d">/ws/quotes • /quotes/stream</td>
            <td class="d">A — WS feed for US equities + crypto / forex. Crypto &amp; forex run a primary + hot-standby WSS pair: the standby auto-covers within seconds if the primary feed goes quiet, so there's no manual gap.</td></tr>
        <tr><td class="k">news:&lt;sym&gt;</td>
            <td class="d">/ws/news</td>
            <td class="d">B — news firehose poller (60 s)</td></tr>
        <tr><td class="k">kol:&lt;sym&gt;</td>
            <td class="d">/kols/tweets/stream (SSE)</td>
            <td class="d">B — tweet provider, filtered by roster</td></tr>
        <tr><td class="k">pm:events</td>
            <td class="d">/prediction-markets/stream (SSE)</td>
            <td class="d">A — 1st-hand Polymarket CLOB WSS (500 active markets)</td></tr>
        <tr><td class="k">last:tick:&lt;sym&gt;</td>
            <td class="d">/quotes?symbols=…</td>
            <td class="d">C — Tier-C Redis snapshot, evictable on memory pressure</td></tr>
      </tbody>
    </table>

    <p class="intro" style="margin-top: 32px">
      Tier and lag are exposed live at <a href="/freshness"><code>GET /freshness</code></a>
      (<code>realtime.by_source.{p50,p95,p99}_ms</code>).
    </p>
  </section>

  <!-- ============== AUTH ============== -->
  <section id="auth">
    <h2><span class="num">04</span>Authentication</h2>
    <p class="intro">
      <b>A Lumid PAT is required on every route</b> &mdash; anonymous requests
      return <code>401</code>. Auth goes through Lumid identity introspection;
      bring a Lumid PAT (<code>lm_pat_live_…</code> / <code>rm_pat_live_…</code>)
      or an RS256 JWT. Authed tier: 100 req/min.
    </p>

<pre class="code"><span class="c1"># Bring your Lumid PAT (lm_pat_live_… / rm_pat_live_…) or JWT.</span>
<span class="k1">curl</span> -H "Authorization: Bearer $LUMID_TOKEN" \
     https://kv.run:5000<span class="p1">/symbols/AAPL</span>

<span class="c1"># Or via X-API-Key header — same effect.</span>
<span class="k1">curl</span> -H "X-API-Key: $LUMID_TOKEN" \
     "https://kv.run:5000<span class="p1">/quotes</span>?symbols=AAPL"</pre>
  </section>

  <!-- ============== MCP ============== -->
  <section id="mcp">
    <h2><span class="num">05</span>MCP — for agents</h2>
    <p class="intro">
      Every query is also a Model Context Protocol tool, so an agent can pull
      OHLC, fundamentals, holdings, or a real-time quote without learning a
      separate HTTP shape.
    </p>

<pre class="code"><span class="c1"># HTTP/SSE transport (remote agents):</span>
<span class="k1">curl</span> https://kv.run:5000<span class="p1">/mcp/sse</span>

<span class="c1"># stdio transport (Claude Code, local):</span>
<span class="k1">docker</span> exec -i finai-api python3 -m api.mcp.stdio</pre>

    <p class="intro" style="margin-top: 24px">
      <b>Read tools:</b> <code>symbols_search</code>, <code>symbol_get</code>,
      <code>universe_list</code>, <code>ohlc</code>, <code>fundamentals_latest</code>,
      <code>fundamentals_history</code>, <code>news_for_symbol</code>,
      <code>holders_top</code>, <code>price_target</code>, <code>freshness</code>,
      <code>get_quote</code>, <code>get_quotes</code>.
      <br/><b>Write &amp; discovery tools:</b> <code>catalog_ingress</code>,
      <code>catalog_table_profile</code>, <code>catalog_table_schema</code>,
      <code>catalog_lineage_run</code>, <code>catalog_lineage_row</code>,
      <code>catalog_sources</code>, <code>ingest_typed</code>, <code>ingest_adapter</code>.
    </p>
  </section>

  <!-- ============== INGRESS ============== -->
  <section id="ingress">
    <h2><span class="num">06</span>Ingress &mdash; write surface</h2>
    <p class="intro">
      The read API is paired with an ingress surface. Partners and agents
      push data with a Lumid PAT (per-role allowlist); the server stamps
      provenance and exposes the lineage chain back at
      <a href="/catalog/lineage/run/{run_id}"><code>/catalog/lineage/*</code></a>.
      One call discovery: <a href="/catalog/ingress"><code>GET /catalog/ingress</code></a>.
    </p>

    <table class="surface">
      <thead>
        <tr><th>Mode</th><th>Endpoint</th><th>When</th></tr>
      </thead>
      <tbody>
        <tr><td class="k">Typed</td>
            <td class="d"><code>POST /ingest/{schema}/{table}</code></td>
            <td class="d">JSON body with records in target-column shape. Idempotent.</td></tr>
        <tr><td class="k">Adapter</td>
            <td class="d"><code>POST /ingest/adapter/{adapter_id}</code></td>
            <td class="d">Upstream-shape records flattened server-side via the registered adapter (69 available).</td></tr>
        <tr><td class="k">Stream</td>
            <td class="d"><code>POST /ingest/{schema}/{table}/stream</code></td>
            <td class="d">NDJSON, chunked transfer; one run row spans the whole stream. Gzip/zstd accepted.</td></tr>
        <tr><td class="k">File</td>
            <td class="d"><code>POST /ingest/{schema}/{table}/file</code></td>
            <td class="d">Multipart upload &mdash; JSON / NDJSON / CSV / TSV / XML / YAML / Parquet / Arrow.</td></tr>
        <tr><td class="k">Blob</td>
            <td class="d"><code>POST /ingest/blob</code></td>
            <td class="d">Images / PDFs / opaque binary. sha256 dedup; bytes land in sibling object storage, metadata in <code>raw.blobs</code>.</td></tr>
        <tr><td class="k">Webhook</td>
            <td class="d"><code>POST /webhook/{webhook_id}</code></td>
            <td class="d">HMAC-SHA256 signed, no PAT. Created via <code>POST /admin/ingress/webhooks</code>. Rate-limited per webhook.</td></tr>
      </tbody>
    </table>

    <p class="intro" style="margin-top: 24px">
      Full guide: <a href="https://github.com/mlsys-io/findata/blob/main/api/INGRESS.md">INGRESS.md</a>.
      MCP tools for AI agents: <code>catalog_ingress</code>, <code>catalog_table_schema</code>,
      <code>ingest_typed</code>, <code>ingest_adapter</code>,
      <code>catalog_lineage_run</code>, <code>catalog_lineage_row</code>,
      <code>catalog_sources</code>.
    </p>
  </section>

  <!-- ============== REFRESH CADENCE ============== -->
  <section id="cadence">
    <h2><span class="num">07</span>Refresh cadence</h2>
    <table class="surface">
      <thead><tr><th>Source</th><th>Cadence</th></tr></thead>
      <tbody>
        <tr><td class="k">Realtime tick stream</td>
            <td class="d">Continuous — WS feed across asset classes (stocks, crypto, forex, indices)</td></tr>
        <tr><td class="k">Prediction-markets trades + orderbook</td>
            <td class="d">Continuous WSS recorder; live trade prints + L2 deltas land in &lt;1 s</td></tr>
        <tr><td class="k">Kalshi trades (incremental)</td>
            <td class="d">Every 30 min</td></tr>
        <tr><td class="k">Non-stock 1-min OHLC</td>
            <td class="d">Every 30 minutes (etf / crypto / forex / index / commodity)</td></tr>
        <tr><td class="k">Stock 1-min OHLC</td>
            <td class="d">Daily at 04:30 UTC (upstream feed lags ~3 days)</td></tr>
        <tr><td class="k">Stock daily-adjusted EOD</td>
            <td class="d">Daily at 05:15 UTC (dividend + non-split adjusted)</td></tr>
        <tr><td class="k">Analyst estimates</td>
            <td class="d">Every 4 hours — grades, price-target, recommendation, ratings</td></tr>
        <tr><td class="k">Fundamentals statements</td>
            <td class="d">Daily at 06:00 UTC — income / balance / cashflow + ratios + key-metrics + scores + earnings + growth + enterprise-values</td></tr>
        <tr><td class="k">Ownership (13F + insider + gov-trades)</td>
            <td class="d">Daily at 07:00 UTC</td></tr>
        <tr><td class="k">Events + macro</td>
            <td class="d">Daily at 07:30 UTC — earnings calendar, IPOs, treasury rates, economic indicators, COT, FDA</td></tr>
        <tr><td class="k">Market metadata</td>
            <td class="d">Daily at 08:00 UTC — dividends, splits, market_cap</td></tr>
        <tr><td class="k">Social + news sentiment</td>
            <td class="d">Daily at 09:00 UTC</td></tr>
        <tr><td class="k">Reference (profile, executives, ETF metadata)</td>
            <td class="d">Weekly Sun 03:00 UTC + ETF holdings refresh</td></tr>
        <tr><td class="k">Regulatory (ESG, SEC filings, patents, lobbying)</td>
            <td class="d">Weekly Sun 04:30 UTC</td></tr>
        <tr><td class="k">News articles (5 sources)</td>
            <td class="d">Every 15 min — company-news, general, stock, crypto, forex, press releases</td></tr>
        <tr><td class="k">KOL tweets</td>
            <td class="d">Hourly per active roster handle (1,347+ handles)</td></tr>
        <tr><td class="k">News firehose → SSE</td>
            <td class="d">Polled every 60 s — event-driven WS fan-out</td></tr>
      </tbody>
    </table>
  </section>

  <!-- ============== CONVENTIONS ============== -->
  <section id="conventions">
    <h2><span class="num">08</span>Conventions</h2>
    <table class="surface">
      <tbody>
        <tr><td class="k">Times</td>
            <td class="d">UTC ISO-8601. Dates are <code>YYYY-MM-DD</code>.</td></tr>
        <tr><td class="k">Symbols</td>
            <td class="d">Upper-cased server-side. Crypto / forex use the upstream shape (e.g. <code>BTCUSD</code>, <code>EURUSD</code>).</td></tr>
        <tr><td class="k">Lineage</td>
            <td class="d">The fields <code>source</code>, <code>source_endpoint</code>, <code>source_run_id</code>, <code>ingest_ts</code>, <code>raw</code> are intentionally stripped from responses. The API picks the canonical provider per surface.</td></tr>
        <tr><td class="k">Errors</td>
            <td class="d">JSON <code>{"detail": "…"}</code> on 4xx / 5xx. 429 carries a <code>Retry-After</code> header.</td></tr>
        <tr><td class="k">Pagination</td>
            <td class="d">Endpoints that return lists accept <code>limit</code> (max 1000) and <code>offset</code>; <code>/ohlc</code> uses a server-side row cap instead.</td></tr>
        <tr><td class="k">Failure mode</td>
            <td class="d">Lumid unreachable while validating a presented token → <strong>503 fail-closed</strong>.</td></tr>
      </tbody>
    </table>
  </section>

  <footer>
    <div class="row">
      <a href="/status">Status</a>
      <a href="/usage">Usage</a>
      <a href="/reference">Reference</a>
      <div class="spacer"></div>
      <span class="signature">findata · mlsys-io</span>
    </div>
  </footer>

</div>
</body>
</html>"##;

const LLM_TEMPLATE: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LLM · kv.run</title>
  <meta name="description" content="OpenAI- and Anthropic-compatible LLM inference with Lumid PAT auth. 192k context, streaming, one model endpoint.">
  <meta name="theme-color" content="#fbfaf7">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Cinzel:wght@400;500;600&family=Inter:wght@300;400;500;600;700&display=swap">
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%230f766e'/%3E%3Cpath d='M7 22 L13 14 L18 18 L25 9' stroke='%23ecfdf5' stroke-width='2.5' fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
  <style>
    :root {
      --bg:#fbfaf7; --bg-2:#ffffff; --bg-3:#f5f3ee; --card:#ffffff; --card-hover:#f8fafc;
      --border:rgba(15,118,110,0.22); --border-soft:rgba(15,23,42,0.10);
      --fg:#0f172a; --fg-dim:#475569; --fg-muted:#94a3b8;
      --accent:#0f766e; --accent-2:#115e59; --red:#dc2626; --yellow:#d97706;
      --code-bg:#f5f3ee;
    }
    * { box-sizing:border-box; }
    html,body { margin:0; padding:0; background:var(--bg); color:var(--fg);
      font-family:Inter,-apple-system,BlinkMacSystemFont,sans-serif; font-weight:400;
      line-height:1.55; -webkit-font-smoothing:antialiased; }
    a { color:var(--accent); text-decoration:none; }
    a:hover { color:var(--accent-2); }
    code,.mono { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:0.86em; }
    .wrap { max-width:1080px; margin:0 auto; padding:0 28px; }
    nav.top { padding:22px 0 0; display:flex; align-items:center; gap:28px; font-size:14px; }
    nav.top .brand { font-family:Cinzel,Georgia,serif; font-weight:500; font-size:18px;
      letter-spacing:0.08em; color:var(--fg); display:flex; align-items:center; gap:10px; }
    nav.top .brand-mark { width:22px; height:22px; }
    nav.top a.link { color:var(--fg-dim); font-weight:400; }
    nav.top a.link:hover { color:var(--fg); }
    nav.top .spacer { flex:1; }
    nav.top .pill { padding:6px 12px; border:1px solid var(--border); border-radius:999px;
      color:var(--accent); font-size:12px; font-weight:500; letter-spacing:0.04em; }
    header.hero { padding:64px 0 44px; }
    header.hero h1 { font-family:Cinzel,Georgia,serif; font-weight:500;
      font-size:clamp(38px,5.5vw,64px); line-height:1.04; letter-spacing:-0.01em;
      margin:0 0 18px; }
    header.hero .tagline { font-size:clamp(16px,1.6vw,18px); color:var(--fg-dim);
      max-width:680px; margin:0 0 28px; }
    header.hero .tagline em { color:var(--accent); font-style:normal; font-weight:500; }
    header.hero .cta { display:flex; flex-wrap:wrap; gap:12px; align-items:center; }
    .btn { display:inline-flex; align-items:center; gap:6px; padding:11px 20px;
      border-radius:8px; font-weight:500; font-size:14px; border:1px solid transparent;
      transition:all 0.15s ease; }
    .btn.primary { background:var(--accent); color:#fff; }
    .btn.primary:hover { background:var(--accent-2); }
    .btn.secondary { background:var(--bg-2); color:var(--fg); border-color:var(--border-soft); }
    .btn.secondary:hover { border-color:var(--border); color:var(--accent); }
    pre.code { background:var(--code-bg); border:1px solid var(--border-soft);
      border-radius:8px; padding:16px 18px; overflow-x:auto; font-size:13px;
      line-height:1.65; margin:0; color:#1e293b; }
    pre.code .c1 { color:#94a3b8; } pre.code .k1 { color:var(--accent); font-weight:500; }
    pre.code .s1 { color:#b45309; } pre.code .p1 { color:#1d4ed8; } pre.code .v1 { color:#7c3aed; }
    section { padding:52px 0; border-top:1px solid var(--border-soft); }
    section h2 { font-family:Cinzel,Georgia,serif; font-weight:500; font-size:26px;
      margin:0 0 12px; }
    section h2 .num { color:var(--accent); font-size:17px; margin-right:12px; vertical-align:3px;
      letter-spacing:0.08em; }
    section p.intro { color:var(--fg-dim); max-width:720px; margin:0 0 28px; font-size:15px; }
    .stats { display:grid; grid-template-columns:repeat(4,1fr); gap:1px; background:var(--border-soft);
      border:1px solid var(--border-soft); border-radius:10px; overflow:hidden; margin:30px 0 0; }
    .stats .stat { background:var(--bg-2); padding:22px 18px; }
    .stats .stat .v { font-family:Cinzel,Georgia,serif; font-size:26px; color:var(--accent);
      font-weight:500; line-height:1; }
    .stats .stat .l { color:var(--fg-muted); font-size:11px; margin-top:8px; letter-spacing:0.06em;
      text-transform:uppercase; }
    @media (max-width:820px){ .stats { grid-template-columns:1fr 1fr; } }
    .examples { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .examples .ex { background:var(--bg-2); border:1px solid var(--border-soft); border-radius:10px;
      padding:18px 20px; box-shadow:0 1px 2px rgba(15,23,42,0.04); }
    .examples .ex h3 { margin:0 0 4px; font-size:14px; font-weight:600; }
    .examples .ex .sub { color:var(--fg-muted); font-size:12.5px; margin-bottom:12px; }
    .examples .ex pre { margin:0; font-size:12px; }
    @media (max-width:760px){ .examples { grid-template-columns:1fr; } }
    table.surface { width:100%; border-collapse:collapse; font-size:14px; margin-top:4px; }
    table.surface th { text-align:left; padding:10px 12px 10px 0; color:var(--fg-muted);
      font-weight:500; font-size:11.5px; letter-spacing:0.08em; text-transform:uppercase;
      border-bottom:1px solid var(--border-soft); }
    table.surface td { padding:12px 12px 12px 0; vertical-align:top;
      border-bottom:1px solid var(--border-soft); }
    table.surface td.k { font-family:ui-monospace,monospace; color:var(--accent); font-size:13px;
      white-space:nowrap; }
    table.surface td.d { color:var(--fg-dim); }
    table.surface tr:last-child td { border-bottom:none; }
    .note { background:var(--bg-3); border:1px solid var(--border-soft); border-left:3px solid var(--yellow);
      border-radius:8px; padding:14px 18px; color:var(--fg-dim); font-size:14px; margin-top:18px; }
    .note b { color:var(--fg); }
    footer { padding:44px 0 56px; border-top:1px solid var(--border-soft); color:var(--fg-muted);
      font-size:13px; }
    footer .row { display:flex; flex-wrap:wrap; gap:24px; align-items:center; }
    footer .row a { color:var(--fg-dim); }
    footer .row .spacer { flex:1; }
    footer .signature { font-family:Cinzel,Georgia,serif; letter-spacing:0.06em; color:var(--fg-dim); font-size:12px; }
    .dot { display:inline-block; width:7px; height:7px; border-radius:50%; background:var(--accent);
      margin-right:7px; vertical-align:1px; }
  </style>
</head>
<body>
<div class="wrap">

  <nav class="top">
    <span class="brand">
      <svg class="brand-mark" viewBox="0 0 32 32"><rect width="32" height="32" rx="6" fill="#0f766e"/><path d="M7 22 L13 14 L18 18 L25 9" stroke="#ecfdf5" stroke-width="2.5" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>
      LLM
    </span>
    <a class="link" href="#endpoints">Endpoints</a>
    <span class="spacer"></span>
    <span class="pill"><span class="dot"></span>kv.run:5000</span>
  </nav>

  <header class="hero">
    <h1>OpenAI &amp; Anthropic<br>compatible inference</h1>
    <p class="tagline">
      Point your existing SDK at this host, use your Lumid PAT as the API key,
      and call <code>chat.completions</code> or <code>messages</code> against a
      <em>192k-context</em> reasoning model. Streaming on both shapes.
      <strong>Auth required — no PAT, no serving.</strong>
    </p>
    <div class="cta">
      <a class="btn primary" href="#token">Get a token →</a>
      <a class="btn secondary" href="#quickstart">Quick start</a>
      <a class="btn secondary" href="#endpoints">Endpoints</a>
    </div>

    <div class="stats">
      <div class="stat"><div class="v">192K</div><div class="l">context window</div></div>
      <div class="stat"><div class="v">~25/s</div><div class="l">output tok/s</div></div>
      <div class="stat"><div class="v">~85/s</div><div class="l">aggregate (8 streams)</div></div>
      <div class="stat"><div class="v">6</div><div class="l">endpoints</div></div>
    </div>
  </header>

  <!-- GET A TOKEN -->
  <section id="token">
    <h2><span class="num">01</span>Get a token</h2>
    <p class="intro">
      Every request authenticates with a Lumid Personal Access Token (PAT).
      It's the only credential you need — the same token is your API key for
      every SDK and curl call below.
    </p>
    <table class="surface">
      <tbody>
        <tr><td class="k">1 · Sign in</td><td class="d">Go to <a href="https://lum.id" target="_blank" rel="noopener">lum.id</a> and sign in (or create an account).</td></tr>
        <tr><td class="k">2 · Open Tokens</td><td class="d">Head to <a href="https://lum.id/dashboard/tokens" target="_blank" rel="noopener">lum.id/dashboard/tokens</a> → <b>Create token</b>. Name it, set an expiry, and copy the value — it's shown once.</td></tr>
        <tr><td class="k">3 · Use it</td><td class="d">Pass it as <code>Authorization: Bearer &lt;pat&gt;</code>, or as the <code>api_key</code> in any OpenAI / Anthropic SDK.</td></tr>
      </tbody>
    </table>
    <div class="note">
      Treat a PAT like a password — it carries your identity and rate-limit tier.
      Store it in an env var, never commit it. Revoke and rotate from the same
      Tokens page if it leaks.
    </div>
  </section>

  <!-- QUICK START -->
  <section id="quickstart">
    <h2><span class="num">02</span>Quick start</h2>
    <p class="intro">
      Model id is <code>__MODEL__</code> — or omit <code>model</code> and the
      server fills in the default.
    </p>
    <div class="examples">
      <div class="ex">
        <h3>OpenAI SDK</h3>
        <div class="sub">pip install openai</div>
        <pre class="code"><span class="k1">from</span> openai <span class="k1">import</span> OpenAI

client = OpenAI(
    base_url=<span class="s1">"https://kv.run:5000/v1"</span>,
    api_key=<span class="s1">"&lt;YOUR_LUMID_PAT&gt;"</span>,
)
r = client.chat.completions.create(
    model=<span class="s1">"__MODEL__"</span>,
    messages=[{<span class="s1">"role"</span>: <span class="s1">"user"</span>,
               <span class="s1">"content"</span>: <span class="s1">"Hello"</span>}],
    max_tokens=<span class="v1">1024</span>,
)
print(r.choices[<span class="v1">0</span>].message.content)</pre>
      </div>
      <div class="ex">
        <h3>Anthropic SDK</h3>
        <div class="sub">pip install anthropic</div>
        <pre class="code"><span class="k1">import</span> anthropic

client = anthropic.Anthropic(
    base_url=<span class="s1">"https://kv.run:5000"</span>,
    api_key=<span class="s1">"&lt;YOUR_LUMID_PAT&gt;"</span>,
)
m = client.messages.create(
    model=<span class="s1">"__MODEL__"</span>,
    messages=[{<span class="s1">"role"</span>: <span class="s1">"user"</span>,
               <span class="s1">"content"</span>: <span class="s1">"Hello"</span>}],
    max_tokens=<span class="v1">1024</span>,
)
print(m.content)</pre>
      </div>
    </div>
    <div class="note">
      <b>Reasoning model.</b> It thinks before answering — on the OpenAI shape the
      thinking lands in a <code>reasoning</code> field (not <code>content</code>),
      on the Anthropic shape it's a <code>thinking</code> block. Set
      <code>max_tokens</code> ≥ a few hundred or the reply may be all reasoning and
      no answer.
    </div>
  </section>

  <!-- ENDPOINTS -->
  <section id="endpoints">
    <h2><span class="num">03</span>Endpoints</h2>
    <p class="intro">All under <code>/v1/</code>. Add <code>"stream": true</code> for SSE
      on the generative endpoints.</p>
    <table class="surface">
      <tbody>
        <tr><td class="k">GET&nbsp;/v1/models</td><td class="d">List the deployed model id(s).</td></tr>
        <tr><td class="k">POST&nbsp;/v1/chat/completions</td><td class="d">OpenAI chat completion. Streaming.</td></tr>
        <tr><td class="k">POST&nbsp;/v1/completions</td><td class="d">OpenAI text completion. Streaming.</td></tr>
        <tr><td class="k">POST&nbsp;/v1/embeddings</td><td class="d">OpenAI embeddings (if the model has an embedding head).</td></tr>
        <tr><td class="k">POST&nbsp;/v1/messages</td><td class="d">Anthropic messages. Streaming.</td></tr>
        <tr><td class="k">POST&nbsp;/v1/messages/count_tokens</td><td class="d">Anthropic input-token count, no inference.</td></tr>
      </tbody>
    </table>
  </section>

  <!-- AUTH + LIMITS -->
  <section id="auth">
    <h2><span class="num">04</span>Auth &amp; limits</h2>
    <table class="surface">
      <tbody>
        <tr><td class="k">Auth</td><td class="d"><b>Required.</b> Present a Lumid PAT as <code>Authorization: Bearer &lt;pat&gt;</code>. Anonymous requests get <code>401</code> — there is no free tier.</td></tr>
        <tr><td class="k">Rate limit</td><td class="d">Per-principal request cap, keyed on your PAT subject — limits are per-identity, not per-IP.</td></tr>
        <tr><td class="k">Streaming</td><td class="d">SSE pass-through. OpenAI <code>stream=True</code> and Anthropic <code>messages.stream()</code> both work unchanged.</td></tr>
        <tr><td class="k">Model field</td><td class="d">Optional — omit it and the default is injected. Pass <code>__MODEL__</code> to be explicit.</td></tr>
        <tr><td class="k">Errors</td><td class="d"><code>504</code> upstream timeout · <code>502</code> backend unreachable · <code>503</code> no backend configured.</td></tr>
      </tbody>
    </table>
  </section>

  <footer>
    <div class="row">
      <span class="signature">LLM · kv.run</span>
      <span class="spacer"></span>
      <a href="#endpoints">Endpoints</a>
    </div>
  </footer>

</div>
</body>
</html>"##;
