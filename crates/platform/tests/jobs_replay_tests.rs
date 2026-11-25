//! Offline replay of REAL job history through the planner.
//!
//!   REPLAY_JSONL=/tmp/replay-data/data_retrieval.jsonl \
//!   [REPLAY_SIZES=/tmp/replay-data/sizes.json] \
//!     cargo test -p lumid-platform --test jobs_replay_tests -- --ignored --nocapture
//!
//! Input: one exported FlowMesh `data_retrieval` task per line
//! (`{task_id, worker, mode, url, template, …}`, tokens never exported).
//! Every task in the export ran at the HOME site. Each becomes the sources it
//! would declare (SQL relations → `lumid-data://schema.table`, s3 keys →
//! `data://home/<prefix>/`), planned against the proposed route table.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use lumid_platform::jobs::access::Access;
use lumid_platform::jobs::planner::{
    plan, PlanOutcome, Policy, Route, RuntimeSel, SiteCapacity, Size, SourceNeed,
};
use lumid_platform::jobs::refs::parse;
use lumid_platform::jobs::scope::extract_relations;
use serde_json::Value;

fn routes(p: &Policy) -> Vec<Route> {
    let r = |id: &str, from: &str, src: &str, kind: &str, url: &str, pref: i32| Route {
        route_id: id.into(),
        from_site: from.into(),
        source_id: src.into(),
        kind: kind.into(),
        url: url.into(),
        max_bytes: None,
        pref,
        health: "up".into(),
        latency_ms: Some(5),
        health_at: Some(p.now),
    };
    // The route table proposed for the home hub (sandbox DEFAULT_DATA_SOURCES
    // ported, plus the public gateway everywhere).
    let mut v = Vec::new();
    for src in ["lumid-data", "frames"] {
        v.push(r(
            &format!("home-lan-{src}"),
            "home",
            src,
            "lan",
            "http://192.168.7.155:5101",
            10,
        ));
        v.push(r(
            &format!("home-pub-{src}"),
            "home",
            src,
            "public",
            "https://lum.id/data",
            90,
        ));
        v.push(r(
            &format!("office-tail-{src}"),
            "office",
            src,
            "tailnet",
            "http://100.115.66.10:5101",
            20,
        ));
        v.push(r(
            &format!("office-pub-{src}"),
            "office",
            src,
            "public",
            "https://lum.id/data",
            90,
        ));
        v.push(r(
            &format!("cloud-egress-{src}"),
            "cloud",
            src,
            "egress",
            "http://lumid-data-primary:5101",
            30,
        ));
        v.push(r(
            &format!("nus-pub-{src}"),
            "nus",
            src,
            "public",
            "https://lum.id/data",
            90,
        ));
    }
    v
}

fn sites() -> Vec<SiteCapacity> {
    let s = |name: &str| SiteCapacity {
        site: name.into(),
        runtimes: vec!["flowmesh".into(), "lumilake".into()],
        ext_egress: true,
        cached_models: vec![],
    };
    vec![s("home"), s("office"), s("cloud"), s("nus")]
}

#[test]
#[ignore = "offline replay; set REPLAY_JSONL"]
fn replay_real_data_retrieval_history() {
    let Ok(path) = std::env::var("REPLAY_JSONL") else {
        eprintln!("skipping: set REPLAY_JSONL");
        return;
    };
    let sizes: HashMap<String, i64> = std::env::var("REPLAY_SIZES")
        .ok()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let p = Policy::default();
    let routes = routes(&p);
    // Counterfactual: pretend only these sites have workers.
    let only: Option<Vec<String>> = std::env::var("REPLAY_ONLY_SITES")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.split(',').map(str::to_string).collect());
    let sites: Vec<SiteCapacity> = sites()
        .into_iter()
        .filter(|s| only.as_ref().is_none_or(|o| o.contains(&s.site)))
        .collect();
    let counterfactual = only.is_some();

    let mut n = 0usize;
    let mut placed_at: BTreeMap<String, usize> = BTreeMap::new();
    let mut via: BTreeMap<String, usize> = BTreeMap::new();
    let mut refused: BTreeMap<String, usize> = BTreeMap::new();
    let mut warnings: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut unattributable: BTreeMap<String, usize> = BTreeMap::new();
    let mut tables: BTreeSet<String> = BTreeSet::new();
    let (mut hairpin_before, mut hairpin_after) = (0usize, 0usize);

    for line in std::fs::read_to_string(&path).unwrap().lines() {
        let t: Value = serde_json::from_str(line).unwrap();
        n += 1;
        let public_before = t["url"]
            .as_str()
            .unwrap_or("")
            .starts_with("https://lum.id");
        let mut needs = Vec::new();
        match t["mode"].as_str() {
            Some("sql") => {
                let sql = t["template"].as_str().unwrap_or("");
                match extract_relations(sql) {
                    Ok((rels, ctes)) => {
                        let mut seen = BTreeSet::new();
                        for (schema, name) in rels {
                            match schema {
                                Some(s) if seen.insert(format!("{s}.{name}")) => {
                                    let key = format!("{s}.{name}");
                                    tables.insert(key.clone());
                                    let r = format!("lumid-data://{key}");
                                    needs.push(SourceNeed {
                                        parsed: parse(&r).unwrap(),
                                        raw_ref: r,
                                        access: Access::Read,
                                        source_id: Some("lumid-data".into()),
                                        site: Some("home".into()),
                                        size: sizes
                                            .get(&key)
                                            .map(|b| Size::UpperBound(*b))
                                            .unwrap_or(Size::Unknown),
                                    });
                                }
                                Some(_) => {}
                                None if ctes.iter().any(|c| c.eq_ignore_ascii_case(&name)) => {}
                                None => {
                                    *unattributable
                                        .entry(format!("unqualified:{name}"))
                                        .or_default() += 1
                                }
                            }
                        }
                    }
                    Err(e) => *unattributable.entry(format!("extractor:{e}")).or_default() += 1,
                }
            }
            Some("s3") => {
                let tpl = t["template"].as_str().unwrap_or("");
                let fixed = tpl.split('{').next().unwrap_or("");
                let prefix = &fixed[..fixed.rfind('/').map(|i| i + 1).unwrap_or(0)];
                match parse(&format!("data://home/{prefix}")) {
                    Ok(parsed) => needs.push(SourceNeed {
                        raw_ref: format!("data://home/{prefix}"),
                        parsed,
                        access: Access::Read,
                        source_id: Some("frames".into()),
                        site: Some("home".into()),
                        // One image per task; the history shows per-object reads.
                        size: Size::Declared(2 * 1024 * 1024),
                    }),
                    Err(e) => *unattributable.entry(format!("s3-prefix:{e}")).or_default() += 1,
                }
            }
            other => *unattributable.entry(format!("mode:{other:?}")).or_default() += 1,
        }
        if needs.is_empty() {
            continue;
        }
        match plan(
            &needs,
            &sites,
            &routes,
            &p,
            &RuntimeSel::Named("flowmesh".into()),
        ) {
            PlanOutcome::Placed(pl) => {
                *placed_at.entry(pl.site.clone()).or_default() += 1;
                for s in &pl.sources {
                    let k = format!(
                        "{}{}",
                        s.via,
                        s.route_id
                            .as_deref()
                            .map(|r| format!(" ({r})"))
                            .unwrap_or_default()
                    );
                    *via.entry(k).or_default() += 1;
                }
                for r in &pl.reasons {
                    if r.code != "colocated" {
                        *warnings.entry(r.code).or_default() += 1;
                    }
                }
                let public_after = pl.sources.iter().any(|s| {
                    s.url
                        .as_deref()
                        .is_some_and(|u| u.starts_with("https://lum.id"))
                });
                hairpin_before += public_before as usize;
                hairpin_after += (public_before && public_after) as usize;
            }
            PlanOutcome::Refused { alternatives, .. } => {
                for a in alternatives {
                    for b in a.blocking {
                        *refused.entry(format!("{}@{}", b.code, a.site)).or_default() += 1;
                    }
                }
            }
        }
    }
    println!("tasks: {n}");
    println!("placed at: {placed_at:?}");
    println!("source via: {via:#?}");
    println!("refusals: {refused:?}");
    println!("warnings: {warnings:?}");
    println!("unattributable: {unattributable:?}");
    println!("hairpin (public URL for home data): before={hairpin_before} after={hairpin_after}");
    println!(
        "distinct tables ({}): {}",
        tables.len(),
        tables.iter().cloned().collect::<Vec<_>>().join(",")
    );
    // The invariant the replay exists to check: no home task keeps the hairpin.
    if !counterfactual {
        assert_eq!(
            hairpin_after, 0,
            "some home tasks still route home data via the public gateway"
        );
    }
}
