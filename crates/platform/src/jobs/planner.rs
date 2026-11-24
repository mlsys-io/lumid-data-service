//! The networking planner: where a job runs, and how it reaches each source.
//!
//! A pure, deterministic function of its inputs — the same inputs always give
//! the same plan, and every choice carries a reason a human or an agent can
//! read. Principle: storage and compute colocate. The job goes to the site
//! holding most of its input bytes; what is not local is reached over the
//! best healthy route; a remote read too large to be sensible is refused with
//! the alternatives spelled out (staging data near compute is a later phase).
//!
//! Size estimates are not all equal, and only trustworthy ones may refuse:
//!
//! | `Size`        | Source of the number                    | Over the remote cap        |
//! |---------------|-----------------------------------------|----------------------------|
//! | `Exact`       | blob sizes                              | refused                    |
//! | `Declared`    | the submitter's own `max_bytes`         | refused                    |
//! | `UpperBound`  | a table's on-disk size (reads filter)   | lan/tailnet only + warning |
//! | `Unknown`     | nothing known                           | lan/tailnet only + warning |
//!
//! The `UpperBound` rule exists because real retrievals filter: lumilake's
//! `ai4finance_10g` tables are 0.3–5.4 GB on disk but are read a window at a
//! time. Models (`hf://`) and live external APIs (`ext://`) never count toward
//! the cap — a model's locality is a worker's cache, an API's is the internet.

use chrono::{DateTime, Utc};
use serde::Serialize;

use super::access::Access;
use super::refs::SourceRef;

/// How much a source would move, and how much to trust the number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "bytes", rename_all = "snake_case")]
pub enum Size {
    Exact(i64),
    Declared(i64),
    UpperBound(i64),
    Unknown,
}

impl Size {
    /// Bytes to use for ranking sites (an upper bound still says where the
    /// bulk lives).
    fn rank_bytes(self) -> i64 {
        match self {
            Size::Exact(n) | Size::Declared(n) | Size::UpperBound(n) => n.max(0),
            Size::Unknown => 0,
        }
    }
}

/// One declared source, resolved to where its data lives.
#[derive(Clone, Debug)]
pub struct SourceNeed {
    pub raw_ref: String,
    pub parsed: SourceRef,
    pub access: Access,
    /// The registry's source id (routes are keyed by it). `None` for models
    /// and external APIs, which need no route.
    pub source_id: Option<String>,
    /// Site the data lives at (its primary location). `None` = unknown.
    pub site: Option<String>,
    pub size: Size,
}

/// A site that could run the job.
#[derive(Clone, Debug)]
pub struct SiteCapacity {
    pub site: String,
    /// Runtimes with at least one healthy worker at this site.
    pub runtimes: Vec<String>,
    /// Whether jobs here can reach the public internet (`ext://` sources).
    /// Home chatbox pods, for one, are egress-locked to lum.id.
    pub ext_egress: bool,
    /// Model refs (`org/model`, lowercase) some worker here has cached.
    pub cached_models: Vec<String>,
}

/// A configured route from a site to a source, with its last known health.
#[derive(Clone, Debug)]
pub struct Route {
    pub route_id: String,
    pub from_site: String,
    pub source_id: String,
    pub kind: String,
    pub url: String,
    pub max_bytes: Option<i64>,
    pub pref: i32,
    pub health: String,
    pub latency_ms: Option<i32>,
    pub health_at: Option<DateTime<Utc>>,
}

/// Knobs. Defaults match the plan of record.
#[derive(Clone, Debug)]
pub struct Policy {
    /// Largest remote data-plane read the planner will route (bytes).
    pub remote_max_bytes: i64,
    /// Tie-break order among equally good sites.
    pub site_order: Vec<String>,
    /// Health older than this counts as `unknown`.
    pub health_stale_after: chrono::Duration,
    pub now: DateTime<Utc>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            remote_max_bytes: 256 * 1024 * 1024,
            site_order: ["home", "office", "cloud", "nus"]
                .map(String::from)
                .to_vec(),
            health_stale_after: chrono::Duration::minutes(5),
            now: Utc::now(),
        }
    }
}

/// Which runtime(s) may run the job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeSel {
    Named(String),
    /// Any of these (in preference order) — `auto` resolves to flowmesh then
    /// sandbox; lumilake jobs must name lumilake (its payload differs).
    Any(Vec<String>),
}

impl RuntimeSel {
    pub fn auto() -> Self {
        RuntimeSel::Any(vec!["flowmesh".into(), "sandbox".into()])
    }
    fn pick(&self, site: &SiteCapacity) -> Option<String> {
        match self {
            RuntimeSel::Named(r) => site.runtimes.contains(r).then(|| r.clone()),
            RuntimeSel::Any(rs) => rs.iter().find(|r| site.runtimes.contains(r)).cloned(),
        }
    }
}

/// A short machine code plus a human sentence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Reason {
    pub code: &'static str,
    pub msg: String,
}

fn reason(code: &'static str, msg: impl Into<String>) -> Reason {
    Reason {
        code,
        msg: msg.into(),
    }
}

/// How one source is reached from the chosen site.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PlannedSource {
    #[serde(rename = "ref")]
    pub raw_ref: String,
    pub access: &'static str,
    pub source_site: Option<String>,
    /// `local`, a route kind (`lan`, `tailnet`, `proxy`, `egress`, `public`),
    /// `model` or `internet`.
    pub via: String,
    pub route_id: Option<String>,
    pub url: Option<String>,
    pub size: Size,
}

/// A placed job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Plan {
    pub site: String,
    pub runtime: String,
    pub sources: Vec<PlannedSource>,
    /// Bytes of data-plane sources at the chosen site (by rank bytes).
    pub local_bytes: i64,
    /// Bytes crossing a route (by rank bytes).
    pub remote_bytes: i64,
    pub reasons: Vec<Reason>,
    pub alternatives: Vec<Alternative>,
}

/// Another site and why it lost (or could not run the job).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Alternative {
    pub site: String,
    pub blocking: Vec<Reason>,
}

/// Result of planning.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PlanOutcome {
    Placed(Plan),
    Refused {
        reasons: Vec<Reason>,
        alternatives: Vec<Alternative>,
    },
}

const KIND_ORDER: &[&str] = &["lan", "tailnet", "proxy", "egress", "public"];

fn kind_rank(kind: &str) -> usize {
    KIND_ORDER
        .iter()
        .position(|k| *k == kind)
        .unwrap_or(KIND_ORDER.len())
}

/// Effective health: `down` stays down at any age; anything not refreshed
/// within `health_stale_after` is `unknown`.
fn effective_health<'a>(r: &'a Route, p: &Policy) -> &'a str {
    if r.health == "down" {
        return "down";
    }
    match r.health_at {
        Some(t) if p.now - t <= p.health_stale_after => r.health.as_str(),
        _ => "unknown",
    }
}

/// Route preference: confirmed health, then configured `pref`, then kind
/// (lan < tailnet < proxy < egress < public), then latency, then id.
fn route_order(a: &Route, b: &Route, p: &Policy) -> std::cmp::Ordering {
    health_rank(effective_health(a, p))
        .cmp(&health_rank(effective_health(b, p)))
        .then(a.pref.cmp(&b.pref))
        .then(kind_rank(&a.kind).cmp(&kind_rank(&b.kind)))
        .then(
            a.latency_ms
                .unwrap_or(i32::MAX)
                .cmp(&b.latency_ms.unwrap_or(i32::MAX)),
        )
        .then(a.route_id.cmp(&b.route_id))
}

fn health_rank(h: &str) -> usize {
    match h {
        "up" => 0,
        "unknown" => 1,
        "degraded" => 2,
        _ => 3,
    }
}

/// Evaluate one candidate site: Ok(sources, local, remote, latency, warnings)
/// or the list of blocking reasons.
#[allow(clippy::type_complexity)]
fn evaluate(
    site: &SiteCapacity,
    needs: &[SourceNeed],
    routes: &[Route],
    p: &Policy,
) -> Result<(Vec<PlannedSource>, i64, i64, i64, Vec<Reason>), Vec<Reason>> {
    let mut planned = Vec::with_capacity(needs.len());
    let mut blocking = Vec::new();
    let mut warnings = Vec::new();
    let (mut local, mut remote, mut latency) = (0i64, 0i64, 0i64);

    for n in needs {
        let base = |via: &str, route: Option<&Route>| PlannedSource {
            raw_ref: n.raw_ref.clone(),
            access: n.access.as_str(),
            source_site: n.site.clone(),
            via: via.to_string(),
            route_id: route.map(|r| r.route_id.clone()),
            url: route.map(|r| r.url.clone()),
            size: n.size,
        };
        match &n.parsed {
            SourceRef::Model { org, model, .. } => {
                let key = format!("{org}/{model}").to_ascii_lowercase();
                if !site.cached_models.contains(&key) {
                    warnings.push(reason(
                        "model_not_cached",
                        format!(
                            "{} is not cached at {}; first run downloads it",
                            n.raw_ref, site.site
                        ),
                    ));
                }
                planned.push(base("model", None));
                continue;
            }
            SourceRef::External { host } => {
                if site.ext_egress {
                    planned.push(base("internet", None));
                } else {
                    blocking.push(reason(
                        "no_internet_egress",
                        format!("jobs at {} cannot reach ext://{host}", site.site),
                    ));
                }
                continue;
            }
            _ => {}
        }
        // Data plane, local: no bytes cross a route, but the runtime still
        // needs an address — hand it the best site-local route (lan first) so
        // a worker never falls back to the public URL for data on its own LAN
        // (97% of home data_retrieval tasks did exactly that).
        if n.site.as_deref() == Some(site.site.as_str()) {
            local += n.size.rank_bytes();
            let mut local_routes: Vec<&Route> = n
                .source_id
                .as_deref()
                .map(|sid| {
                    routes
                        .iter()
                        .filter(|r| r.from_site == site.site && r.source_id == sid)
                        .filter(|r| effective_health(r, p) != "down")
                        .collect()
                })
                .unwrap_or_default();
            local_routes.sort_by(|a, b| route_order(a, b, p));
            planned.push(base("local", local_routes.first().copied()));
            continue;
        }
        let Some(source_id) = n.source_id.as_deref() else {
            blocking.push(reason(
                "source_location_unknown",
                format!("{} has no known location", n.raw_ref),
            ));
            continue;
        };
        // Size gate.
        let (refuse_over_cap, lan_tailnet_only) = match n.size {
            Size::Exact(b) | Size::Declared(b) => (b > p.remote_max_bytes, false),
            Size::UpperBound(b) => (false, b > p.remote_max_bytes),
            Size::Unknown => (false, true),
        };
        if refuse_over_cap {
            blocking.push(reason(
                "source_too_large_remote",
                format!(
                    "{} is {} at {}, over the {} remote-read limit from {}",
                    n.raw_ref,
                    human(n.size.rank_bytes()),
                    n.site.as_deref().unwrap_or("?"),
                    human(p.remote_max_bytes),
                    site.site
                ),
            ));
            continue;
        }
        let est = n.size.rank_bytes();
        let mut candidates: Vec<&Route> = routes
            .iter()
            .filter(|r| r.from_site == site.site && r.source_id == source_id)
            .filter(|r| effective_health(r, p) != "down")
            .filter(|r| r.max_bytes.is_none_or(|m| est <= m))
            .filter(|r| !lan_tailnet_only || matches!(r.kind.as_str(), "lan" | "tailnet"))
            .collect();
        candidates.sort_by(|a, b| route_order(a, b, p));
        match candidates.first() {
            Some(r) => {
                remote += est;
                latency += i64::from(r.latency_ms.unwrap_or(0));
                let h = effective_health(r, p);
                if h != "up" {
                    warnings.push(reason(
                        "route_health_unconfirmed",
                        format!("route {} ({}) health is {h}", r.route_id, r.kind),
                    ));
                }
                if lan_tailnet_only {
                    warnings.push(reason(
                        "remote_size_unbounded",
                        match n.size {
                            Size::UpperBound(b) => format!(
                                "{} may move up to {} over {}; declare max_bytes or a window to tighten",
                                n.raw_ref,
                                human(b),
                                r.kind
                            ),
                            _ => format!("{} size is unknown; reading it over {}", n.raw_ref, r.kind),
                        },
                    ));
                }
                planned.push(base(&r.kind, Some(r)));
            }
            None => blocking.push(reason(
                "route_unavailable",
                format!(
                    "no healthy{} route from {} to {}",
                    if lan_tailnet_only { " lan/tailnet" } else { "" },
                    site.site,
                    n.raw_ref
                ),
            )),
        }
    }
    if blocking.is_empty() {
        Ok((planned, local, remote, latency, warnings))
    } else {
        Err(blocking)
    }
}

fn human(b: i64) -> String {
    const U: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < U.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// Plan a job.
pub fn plan(
    needs: &[SourceNeed],
    sites: &[SiteCapacity],
    routes: &[Route],
    policy: &Policy,
    runtime: &RuntimeSel,
) -> PlanOutcome {
    let order_of = |s: &str| {
        policy
            .site_order
            .iter()
            .position(|x| x == s)
            .unwrap_or(policy.site_order.len())
    };

    let mut not_runnable = Vec::new();
    let mut evaluated = Vec::new();
    for site in sites {
        let Some(rt) = runtime.pick(site) else {
            not_runnable.push(Alternative {
                site: site.site.clone(),
                blocking: vec![reason(
                    "no_worker",
                    format!(
                        "no healthy {} worker at {}",
                        runtime_label(runtime),
                        site.site
                    ),
                )],
            });
            continue;
        };
        evaluated.push((site, rt, evaluate(site, needs, routes, policy)));
    }

    // Rank feasible sites: most local bytes, fewest remote sources, lowest
    // summed route latency, policy order, name.
    let mut feasible: Vec<_> = evaluated
        .iter()
        .filter_map(|(s, rt, r)| r.as_ref().ok().map(|ok| (*s, rt, ok)))
        .collect();
    feasible.sort_by(|(a, _, ra), (b, _, rb)| {
        let remote_n = |r: &(Vec<PlannedSource>, i64, i64, i64, Vec<Reason>)| {
            r.0.iter()
                .filter(|p| !matches!(p.via.as_str(), "local" | "model" | "internet"))
                .count()
        };
        rb.1.cmp(&ra.1)
            .then(remote_n(ra).cmp(&remote_n(rb)))
            .then(ra.3.cmp(&rb.3))
            .then(order_of(&a.site).cmp(&order_of(&b.site)))
            .then(a.site.cmp(&b.site))
    });

    let mut alternatives: Vec<Alternative> = Vec::new();
    for (s, _, r) in &evaluated {
        if let Err(blocking) = r {
            alternatives.push(Alternative {
                site: s.site.clone(),
                blocking: blocking.clone(),
            });
        }
    }
    alternatives.extend(not_runnable);

    let Some((site, rt, (sources, local, remote, _lat, warnings))) = feasible.first().cloned()
    else {
        alternatives.sort_by(|a, b| {
            order_of(&a.site)
                .cmp(&order_of(&b.site))
                .then(a.site.cmp(&b.site))
        });
        let reasons = if sites.is_empty() {
            vec![reason("no_sites", "no compute site is known")]
        } else {
            vec![reason(
                "no_feasible_site",
                "no site can run this job with its sources as declared — see alternatives",
            )]
        };
        return PlanOutcome::Refused {
            reasons,
            alternatives,
        };
    };

    let (local, remote) = (*local, *remote);
    // Runners-up are alternatives too (feasible but ranked lower).
    for (s, _, ok) in feasible.iter().skip(1) {
        alternatives.push(Alternative {
            site: s.site.clone(),
            blocking: vec![reason(
                "ranked_lower",
                format!("{} local / {} remote", human(ok.1), human(ok.2)),
            )],
        });
    }
    alternatives.sort_by(|a, b| {
        order_of(&a.site)
            .cmp(&order_of(&b.site))
            .then(a.site.cmp(&b.site))
    });

    let mut reasons = vec![reason(
        "colocated",
        if remote == 0 {
            format!(
                "all data-plane inputs ({}) are local to {}",
                human(local),
                site.site
            )
        } else {
            format!(
                "{} holds the most input ({} local, {} over routes)",
                site.site,
                human(local),
                human(remote)
            )
        },
    )];
    reasons.extend(warnings.clone());

    PlanOutcome::Placed(Plan {
        site: site.site.clone(),
        runtime: rt.clone(),
        sources: sources.clone(),
        local_bytes: local,
        remote_bytes: remote,
        reasons,
        alternatives,
    })
}

fn runtime_label(r: &RuntimeSel) -> String {
    match r {
        RuntimeSel::Named(n) => n.clone(),
        RuntimeSel::Any(v) => v.join("/"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::refs::parse;

    const MIB: i64 = 1024 * 1024;
    const GIB: i64 = 1024 * MIB;

    fn need(r: &str, src: Option<&str>, site: Option<&str>, size: Size) -> SourceNeed {
        SourceNeed {
            raw_ref: r.into(),
            parsed: parse(r).unwrap(),
            access: Access::Read,
            source_id: src.map(String::from),
            site: site.map(String::from),
            size,
        }
    }
    fn site(name: &str, rts: &[&str]) -> SiteCapacity {
        SiteCapacity {
            site: name.into(),
            runtimes: rts.iter().map(|s| s.to_string()).collect(),
            ext_egress: true,
            cached_models: vec![],
        }
    }
    fn route(id: &str, from: &str, src: &str, kind: &str, health: &str, p: &Policy) -> Route {
        Route {
            route_id: id.into(),
            from_site: from.into(),
            source_id: src.into(),
            kind: kind.into(),
            url: format!("http://{id}"),
            max_bytes: None,
            pref: 100,
            health: health.into(),
            latency_ms: Some(5),
            health_at: Some(p.now),
        }
    }
    fn lumilake() -> RuntimeSel {
        RuntimeSel::Named("lumilake".into())
    }
    fn placed(o: PlanOutcome) -> Plan {
        match o {
            PlanOutcome::Placed(p) => p,
            other => panic!("expected placed, got {other:#?}"),
        }
    }
    fn refused(o: PlanOutcome) -> Vec<Alternative> {
        match o {
            PlanOutcome::Refused { alternatives, .. } => alternatives,
            other => panic!("expected refused, got {other:#?}"),
        }
    }

    #[test]
    fn all_local_runs_where_the_data_is() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://ukb_demo.phenotype",
            Some("lumid-data"),
            Some("home"),
            Size::UpperBound(GIB),
        )];
        let sites = [site("office", &["lumilake"]), site("home", &["lumilake"])];
        let pl = placed(plan(&needs, &sites, &[], &p, &lumilake()));
        assert_eq!(pl.site, "home");
        assert_eq!(pl.sources[0].via, "local");
        assert_eq!(pl.remote_bytes, 0);
        assert_eq!(pl.reasons[0].code, "colocated");
    }

    #[test]
    fn local_sources_get_the_site_local_address_not_the_public_one() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://reference.etf",
            Some("ld"),
            Some("home"),
            Size::Exact(MIB),
        )];
        let routes = [
            route("home-pub", "home", "ld", "public", "up", &p),
            route("home-lan", "home", "ld", "lan", "up", &p),
        ];
        let pl = placed(plan(
            &needs,
            &[site("home", &["flowmesh"])],
            &routes,
            &p,
            &RuntimeSel::auto(),
        ));
        assert_eq!(pl.sources[0].via, "local");
        assert_eq!(pl.sources[0].route_id.as_deref(), Some("home-lan"));
        assert_eq!(pl.remote_bytes, 0);
        // No route configured: still local, the runtime uses its default.
        let pl = placed(plan(
            &needs,
            &[site("home", &["flowmesh"])],
            &[],
            &p,
            &RuntimeSel::auto(),
        ));
        assert_eq!(
            (pl.sources[0].via.as_str(), pl.sources[0].url.as_deref()),
            ("local", None)
        );
    }

    #[test]
    fn the_site_with_the_most_bytes_wins_and_the_rest_rides_a_route() {
        let p = Policy::default();
        let needs = [
            need(
                "nus://ukb",
                Some("nus"),
                Some("nus"),
                Size::UpperBound(150 * GIB),
            ),
            need(
                "lumid-data://reference.etf",
                Some("lumid-data"),
                Some("home"),
                Size::Exact(3 * MIB),
            ),
        ];
        let sites = [site("home", &["flowmesh"]), site("nus", &["flowmesh"])];
        let routes = [route("nus-pub-ld", "nus", "lumid-data", "public", "up", &p)];
        let pl = placed(plan(&needs, &sites, &routes, &p, &RuntimeSel::auto()));
        assert_eq!(pl.site, "nus");
        assert_eq!(pl.sources[1].via, "public");
        assert_eq!(pl.sources[1].route_id.as_deref(), Some("nus-pub-ld"));
        assert_eq!(pl.remote_bytes, 3 * MIB);
        // home is an alternative, blocked: it can't reach 150 GiB at NUS.
        let home = pl.alternatives.iter().find(|a| a.site == "home").unwrap();
        assert!(home.blocking.iter().any(|r| r.code == "route_unavailable"));
    }

    #[test]
    fn hairpin_fix_home_prefers_lan_over_public() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://reference.etf",
            Some("lumid-data"),
            Some("nas"),
            Size::Exact(10 * MIB),
        )];
        let routes = [
            route("home-pub", "home", "lumid-data", "public", "up", &p),
            route("home-lan", "home", "lumid-data", "lan", "up", &p),
            route("home-tail", "home", "lumid-data", "tailnet", "up", &p),
        ];
        let pl = placed(plan(
            &needs,
            &[site("home", &["flowmesh"])],
            &routes,
            &p,
            &RuntimeSel::auto(),
        ));
        assert_eq!(pl.sources[0].via, "lan");
    }

    #[test]
    fn pref_outranks_kind_and_health_outranks_pref() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://r.t",
            Some("ld"),
            Some("x"),
            Size::Exact(MIB),
        )];
        let mut lan = route("lan", "home", "ld", "lan", "up", &p);
        lan.pref = 50;
        let mut tail = route("tail", "home", "ld", "tailnet", "up", &p);
        tail.pref = 10;
        let s = [site("home", &["flowmesh"])];
        assert_eq!(
            placed(plan(
                &needs,
                &s,
                &[lan.clone(), tail.clone()],
                &p,
                &RuntimeSel::auto()
            ))
            .sources[0]
                .via,
            "tailnet"
        );
        tail.health = "degraded".into();
        assert_eq!(
            placed(plan(&needs, &s, &[lan, tail], &p, &RuntimeSel::auto())).sources[0].via,
            "lan"
        );
    }

    #[test]
    fn exact_or_declared_over_the_cap_is_refused_with_alternatives() {
        let p = Policy::default();
        for size in [Size::Exact(GIB), Size::Declared(GIB)] {
            let needs = [need(
                "data://home/frames/",
                Some("frames"),
                Some("home"),
                size,
            )];
            let sites = [site("office", &["lumilake"])];
            let routes = [route("off-tail", "office", "frames", "tailnet", "up", &p)];
            let alts = refused(plan(&needs, &sites, &routes, &p, &lumilake()));
            assert_eq!(
                alts[0].blocking[0].code, "source_too_large_remote",
                "{size:?}"
            );
        }
    }

    #[test]
    fn a_big_upper_bound_is_never_refused_only_kept_to_lan_or_tailnet() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://ai.fact_ohlc_10m",
            Some("ld"),
            Some("home"),
            Size::UpperBound(5 * GIB),
        )];
        let s = [site("office", &["lumilake"])];
        // Public only → refused (unbounded over the internet is not allowed) …
        let alts = refused(plan(
            &needs,
            &s,
            &[route("pub", "office", "ld", "public", "up", &p)],
            &p,
            &lumilake(),
        ));
        assert_eq!(alts[0].blocking[0].code, "route_unavailable");
        // … tailnet → placed with an explicit warning.
        let pl = placed(plan(
            &needs,
            &s,
            &[
                route("pub", "office", "ld", "public", "up", &p),
                route("tail", "office", "ld", "tailnet", "up", &p),
            ],
            &p,
            &lumilake(),
        ));
        assert_eq!(pl.sources[0].via, "tailnet");
        assert!(pl.reasons.iter().any(|r| r.code == "remote_size_unbounded"));
        // A small upper bound may use any route.
        let small = [need(
            "lumid-data://ai.small",
            Some("ld"),
            Some("home"),
            Size::UpperBound(10 * MIB),
        )];
        let pl = placed(plan(
            &small,
            &s,
            &[route("pub", "office", "ld", "public", "up", &p)],
            &p,
            &lumilake(),
        ));
        assert_eq!(pl.sources[0].via, "public");
    }

    #[test]
    fn unknown_size_needs_lan_or_tailnet() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://x.y",
            Some("ld"),
            Some("home"),
            Size::Unknown,
        )];
        let s = [site("nus", &["flowmesh"])];
        let alts = refused(plan(
            &needs,
            &s,
            &[route("pub", "nus", "ld", "public", "up", &p)],
            &p,
            &RuntimeSel::auto(),
        ));
        assert!(alts[0].blocking[0].msg.contains("lan/tailnet"));
    }

    #[test]
    fn down_routes_are_skipped_and_stale_health_is_unknown() {
        let mut p = Policy::default();
        let needs = [need(
            "lumid-data://r.t",
            Some("ld"),
            Some("home"),
            Size::Exact(MIB),
        )];
        let s = [site("office", &["flowmesh"])];
        let down = route("tail", "office", "ld", "tailnet", "down", &p);
        let alts = refused(plan(&needs, &s, std::slice::from_ref(&down), &p, &RuntimeSel::auto()));
        assert_eq!(alts[0].blocking[0].code, "route_unavailable");
        // Down stays down even when its health is old.
        let mut old_down = down.clone();
        old_down.health_at = Some(p.now - chrono::Duration::hours(1));
        assert!(matches!(
            plan(&needs, &s, &[old_down], &p, &RuntimeSel::auto()),
            PlanOutcome::Refused { .. }
        ));
        // Stale "up" is usable but flagged unconfirmed.
        let mut stale = route("pub", "office", "ld", "public", "up", &p);
        stale.health_at = Some(p.now - chrono::Duration::minutes(6));
        let plan1 = placed(plan(&needs, &s, &[stale], &p, &RuntimeSel::auto()));
        assert!(plan1
            .reasons
            .iter()
            .any(|r| r.code == "route_health_unconfirmed"));
        // A fresh healthy route beats a stale one regardless of kind.
        p.now = Utc::now();
        let mut stale_lan = route("lan", "office", "ld", "lan", "up", &p);
        stale_lan.health_at = None;
        let fresh_pub = route("pub", "office", "ld", "public", "up", &p);
        assert_eq!(
            placed(plan(
                &needs,
                &s,
                &[stale_lan, fresh_pub],
                &p,
                &RuntimeSel::auto()
            ))
            .sources[0]
                .via,
            "public"
        );
    }

    #[test]
    fn route_max_bytes_is_respected() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://r.t",
            Some("ld"),
            Some("home"),
            Size::Exact(60 * MIB),
        )];
        let mut capped = route("pub", "office", "ld", "public", "up", &p);
        capped.max_bytes = Some(50 * MIB);
        let alts = refused(plan(
            &needs,
            &[site("office", &["flowmesh"])],
            &[capped],
            &p,
            &RuntimeSel::auto(),
        ));
        assert_eq!(alts[0].blocking[0].code, "route_unavailable");
    }

    #[test]
    fn no_worker_for_the_runtime_is_a_reason_not_a_crash() {
        let p = Policy::default();
        let needs = [need(
            "lumid-data://r.t",
            Some("ld"),
            Some("home"),
            Size::Exact(MIB),
        )];
        let alts = refused(plan(
            &needs,
            &[site("home", &["sandbox"])],
            &[],
            &p,
            &lumilake(),
        ));
        assert_eq!(alts[0].blocking[0].code, "no_worker");
        assert!(matches!(
            plan(&needs, &[], &[], &p, &lumilake()),
            PlanOutcome::Refused { reasons, .. } if reasons[0].code == "no_sites"
        ));
    }

    #[test]
    fn models_and_external_apis_never_count_toward_the_cap() {
        let p = Policy::default();
        let needs = [
            need("hf://Qwen/Qwen3-8B", None, None, Size::Exact(16 * GIB)),
            need("ext://clob.polymarket.com", None, None, Size::Unknown),
            need(
                "lumid-data://r.t",
                Some("ld"),
                Some("office"),
                Size::Exact(MIB),
            ),
        ];
        let mut office = site("office", &["flowmesh"]);
        office.cached_models = vec!["qwen/qwen3-8b".into()];
        let plan1 = placed(plan(
            &needs,
            &[office.clone()],
            &[],
            &p,
            &RuntimeSel::auto(),
        ));
        assert_eq!(plan1.sources[0].via, "model");
        assert_eq!(plan1.sources[1].via, "internet");
        assert_eq!(plan1.remote_bytes, 0);
        assert!(!plan1.reasons.iter().any(|r| r.code == "model_not_cached"));
        // Not cached → a warning, not a refusal.
        office.cached_models.clear();
        assert!(placed(plan(
            &needs,
            &[office.clone()],
            &[],
            &p,
            &RuntimeSel::auto()
        ))
        .reasons
        .iter()
        .any(|r| r.code == "model_not_cached"));
        // No internet egress → that site cannot take an ext:// job.
        office.ext_egress = false;
        let alts = refused(plan(&needs, &[office], &[], &p, &RuntimeSel::auto()));
        assert_eq!(alts[0].blocking[0].code, "no_internet_egress");
    }

    #[test]
    fn ties_break_on_remote_count_then_latency_then_policy_order() {
        let p = Policy::default();
        // No bytes anywhere: both sites read one remote source.
        let needs = [need(
            "lumid-data://r.t",
            Some("ld"),
            Some("nas"),
            Size::Unknown,
        )];
        let mut a = route("a", "office", "ld", "tailnet", "up", &p);
        a.latency_ms = Some(40);
        let mut b = route("b", "home", "ld", "lan", "up", &p);
        b.latency_ms = Some(40);
        let sites = [site("office", &["flowmesh"]), site("home", &["flowmesh"])];
        // Equal latency → policy order (home before office).
        assert_eq!(
            placed(plan(
                &needs,
                &sites,
                &[a.clone(), b.clone()],
                &p,
                &RuntimeSel::auto()
            ))
            .site,
            "home"
        );
        // Lower latency wins over policy order.
        a.latency_ms = Some(3);
        assert_eq!(
            placed(plan(&needs, &sites, &[a, b], &p, &RuntimeSel::auto())).site,
            "office"
        );
    }

    #[test]
    fn same_input_same_plan() {
        let p = Policy::default();
        let needs = [
            need(
                "nus://ukb",
                Some("nus"),
                Some("nus"),
                Size::UpperBound(150 * GIB),
            ),
            need(
                "lumid-data://reference.etf",
                Some("ld"),
                Some("home"),
                Size::Exact(3 * MIB),
            ),
            need("hf://Qwen/Qwen3-8B", None, None, Size::Exact(16 * GIB)),
        ];
        let sites = [
            site("home", &["flowmesh"]),
            site("nus", &["flowmesh"]),
            site("office", &["flowmesh"]),
        ];
        let routes = [
            route("nus-pub", "nus", "ld", "public", "up", &p),
            route("off-tail", "office", "ld", "tailnet", "up", &p),
        ];
        let a = plan(&needs, &sites, &routes, &p, &RuntimeSel::auto());
        for _ in 0..20 {
            assert_eq!(plan(&needs, &sites, &routes, &p, &RuntimeSel::auto()), a);
        }
        let json = serde_json::to_value(&a).unwrap();
        assert_eq!(json["outcome"], "placed");
        assert_eq!(json["site"], "nus");
        assert!(json["sources"][1]["ref"].is_string());
    }

    #[test]
    fn sinks_route_like_sources() {
        let p = Policy::default();
        let mut sink = need(
            "data://home/results/",
            Some("results"),
            Some("home"),
            Size::Declared(5 * MIB),
        );
        sink.access = Access::Write;
        let s = [site("office", &["lumilake"])];
        let plan1 = placed(plan(
            &[sink],
            &s,
            &[route("pub", "office", "results", "public", "up", &p)],
            &p,
            &lumilake(),
        ));
        assert_eq!(plan1.sources[0].access, "write");
        assert_eq!(plan1.sources[0].via, "public");
    }
}
