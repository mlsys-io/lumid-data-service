//! Source references — the one string a job uses to name a data source.
//!
//! A ref always names WHERE the data lives (its instance or hub), because two
//! hubs can hold same-named schemas (both the home NAS and the NUS storage-head
//! call their database `lumilake`; `reference`, `provenance`, … exist on both).
//! A bare `schema.table` is therefore refused.
//!
//! | Ref                                   | Meaning                                         |
//! |---------------------------------------|-------------------------------------------------|
//! | `findata://schema.table`              | table on an estate instance (also `lumid-data`, `lqt`, `nus`) |
//! | `findata://schema`                    | every table in a schema on that instance        |
//! | `data://<hub>/blobs/<sha256>`         | one content-addressed blob held by `<hub>`      |
//! | `data://<hub>/<prefix>/`              | a blob prefix on `<hub>` (also a Lumilake sink) |
//! | `src://<source_id>`                   | a registered (user-owned, shareable) source     |
//! | `src://<owner>/<name>`                | the same, by owner + name                       |
//! | `hf://<org>/<model>[@<rev>]`          | a model; locality = a worker's cache            |
//! | `ext://<host>`                        | a live external API (audit + routing only)      |
//!
//! `ext://` hosts are checked here, not later: loopback, private, link-local
//! and metadata addresses are refused. FlowMesh `api` tasks aimed at
//! `127.0.0.1:2375` (a worker's own Docker daemon) were the entry point of the
//! 2026-10-30 home compromise; a source ref must never be a way back in.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde::Serialize;

/// Estate instances a ref may name directly. Each is a data store with a
/// fixed home; registering a new one is a code change on purpose.
pub const ESTATE_INSTANCES: &[&str] = &["findata", "lumid-data", "lqt", "nus"];

/// A parsed, validated source reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceRef {
    /// `findata://schema.table` / `findata://schema`.
    Table {
        instance: String,
        schema: String,
        table: Option<String>,
    },
    /// `data://<hub>/blobs/<sha256>`.
    Blob { hub: String, sha256: String },
    /// `data://<hub>/<prefix>/`.
    BlobPrefix { hub: String, prefix: String },
    /// `src://<id>`.
    SourceId { source_id: String },
    /// `src://<owner>/<name>`.
    SourceName { owner: String, name: String },
    /// `hf://<org>/<model>[@<rev>]`.
    Model {
        org: String,
        model: String,
        rev: Option<String>,
    },
    /// `ext://<host>`.
    External { host: String },
}

impl SourceRef {
    /// True for kinds whose bytes cross the data plane (count toward the
    /// planner's remote-read cap). Models and external APIs never do.
    pub fn is_data_plane(&self) -> bool {
        !matches!(self, SourceRef::Model { .. } | SourceRef::External { .. })
    }
}

/// Why a ref was refused. The message is safe to return to the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefError(pub String);

impl std::fmt::Display for RefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(msg: impl Into<String>) -> Result<T, RefError> {
    Err(RefError(msg.into()))
}

/// Longest ref accepted at all.
const MAX_REF_LEN: usize = 512;

/// A Postgres-style identifier as used in this platform's catalogs: starts
/// with a letter or `_`, then letters, digits, `_`; at most 63 bytes.
fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && s.len() <= 63
}

/// Hub ids, owner subs and source ids: `[A-Za-z0-9_-]`, 1..=128.
fn is_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Hugging Face org/model names also allow `.`.
fn is_hf_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 96
        && !s.starts_with('.')
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Parse and validate a source reference.
pub fn parse(raw: &str) -> Result<SourceRef, RefError> {
    let r = raw.trim();
    if r.is_empty() {
        return err("empty source ref");
    }
    if r.len() > MAX_REF_LEN {
        return err(format!("source ref longer than {MAX_REF_LEN} bytes"));
    }
    let Some((scheme, rest)) = r.split_once("://") else {
        return err(format!(
            "'{r}' has no scheme — name where the data lives, e.g. findata://schema.table \
             (a bare schema.table is ambiguous across hubs)"
        ));
    };
    match scheme {
        s if ESTATE_INSTANCES.contains(&s) => parse_table(s, rest),
        "data" => parse_data(rest),
        "src" => parse_src(rest),
        "hf" => parse_hf(rest),
        "ext" => parse_ext(rest),
        other => err(format!(
            "unknown source scheme '{other}' (expected one of {}, data, src, hf, ext)",
            ESTATE_INSTANCES.join(", ")
        )),
    }
}

fn parse_table(instance: &str, rest: &str) -> Result<SourceRef, RefError> {
    let (schema, table) = match rest.split_once('.') {
        Some((s, t)) => (s, Some(t)),
        None => (rest, None),
    };
    if !is_ident(schema) {
        return err(format!("invalid schema name '{schema}'"));
    }
    if let Some(t) = table {
        if !is_ident(t) {
            return err(format!("invalid table name '{t}'"));
        }
    }
    Ok(SourceRef::Table {
        instance: instance.to_string(),
        schema: schema.to_string(),
        table: table.map(str::to_string),
    })
}

fn parse_data(rest: &str) -> Result<SourceRef, RefError> {
    let Some((hub, path)) = rest.split_once('/') else {
        return err(
            "data:// refs name a hub: data://<hub>/blobs/<sha256> or data://<hub>/<prefix>/",
        );
    };
    if !is_slug(hub) {
        return err(format!("invalid hub id '{hub}'"));
    }
    if let Some(sha) = path.strip_prefix("blobs/") {
        if !is_sha256_hex(sha) {
            return err("data://<hub>/blobs/<sha256> needs a 64-char hex sha256");
        }
        return Ok(SourceRef::Blob {
            hub: hub.to_string(),
            sha256: sha.to_ascii_lowercase(),
        });
    }
    // A prefix: path segments of slug characters (plus '.', '='), no traversal,
    // no empty or dot-only segments. Trailing '/' is required so a prefix can't
    // be confused with an object key.
    let Some(prefix) = path.strip_suffix('/') else {
        return err("a blob prefix must end with '/' (data://<hub>/<prefix>/)");
    };
    let ok = !prefix.is_empty()
        && prefix.split('/').all(|seg| {
            !seg.is_empty()
                && !seg.chars().all(|c| c == '.')
                && seg
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'='))
        });
    if !ok {
        return err(format!("invalid blob prefix '{prefix}/'"));
    }
    // Private namespaces are never addressable as a prefix: tenant blobs are
    // named by sha256 (and owner-gated), retrieval outputs belong to one run.
    let first = prefix.split('/').next().unwrap_or("");
    if matches!(first, "tenants" | "retrievals") {
        return err(format!(
            "'{first}/' is a private namespace, not a shareable prefix"
        ));
    }
    Ok(SourceRef::BlobPrefix {
        hub: hub.to_string(),
        prefix: format!("{prefix}/"),
    })
}

fn parse_src(rest: &str) -> Result<SourceRef, RefError> {
    match rest.split_once('/') {
        None if is_slug(rest) => Ok(SourceRef::SourceId {
            source_id: rest.to_string(),
        }),
        Some((owner, name)) if is_slug(owner) && is_slug(name) => Ok(SourceRef::SourceName {
            owner: owner.to_string(),
            name: name.to_string(),
        }),
        _ => err(format!(
            "invalid src:// ref 'src://{rest}' (src://<id> or src://<owner>/<name>)"
        )),
    }
}

fn parse_hf(rest: &str) -> Result<SourceRef, RefError> {
    let (path, rev) = match rest.split_once('@') {
        Some((p, r)) => (p, Some(r)),
        None => (rest, None),
    };
    let Some((org, model)) = path.split_once('/') else {
        return err("hf:// refs are hf://<org>/<model>[@<rev>]");
    };
    if !is_hf_name(org) || !is_hf_name(model) {
        return err(format!("invalid model name '{path}'"));
    }
    if let Some(r) = rev {
        if !is_hf_name(r) {
            return err(format!("invalid model revision '{r}'"));
        }
    }
    Ok(SourceRef::Model {
        org: org.to_string(),
        model: model.to_string(),
        rev: rev.map(str::to_string),
    })
}

fn parse_ext(rest: &str) -> Result<SourceRef, RefError> {
    // Host only — no path, port, userinfo, or query. A port would let a ref
    // aim at an arbitrary service; the API's identity is its host.
    let host = rest.trim_end_matches('/').to_ascii_lowercase();
    if host.is_empty() || host.contains(['/', ':', '@', '?', '#', '[', ']']) {
        return err("ext:// refs are a bare host name: ext://api.example.com");
    }
    check_external_host(&host)?;
    Ok(SourceRef::External { host })
}

/// Refuse hosts that point back inside the estate or at a machine itself.
pub fn check_external_host(host: &str) -> Result<(), RefError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_internal_ip(&ip) {
            return err(format!("ext://{host} is an internal address"));
        }
        return Ok(());
    }
    let labels: Vec<&str> = host.split('.').collect();
    let valid = labels.len() >= 2
        && host.len() <= 253
        && labels.iter().all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    if !valid {
        return err(format!("ext://{host} is not a public DNS name"));
    }
    const INTERNAL_SUFFIXES: &[&str] = &[
        "localhost",
        ".local",
        ".internal",
        ".lan",
        ".home.arpa",
        ".svc",
        ".cluster.local",
        ".ts.net",
    ];
    if INTERNAL_SUFFIXES
        .iter()
        .any(|s| host == s.trim_start_matches('.') || host.ends_with(s))
    {
        return err(format!("ext://{host} is an internal name"));
    }
    // A numeric-looking last label (e.g. "1.2.3.4.5") is not a public name.
    if labels
        .last()
        .is_some_and(|l| l.bytes().all(|b| b.is_ascii_digit()))
    {
        return err(format!("ext://{host} is not a public DNS name"));
    }
    Ok(())
}

fn is_internal_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_internal_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_internal_v4(&v4);
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || *v6 == Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254) // AWS metadata v6
        }
    }
}

fn is_internal_v4(ip: &Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local() // includes 169.254.169.254 metadata
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || o[0] == 0
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // CGNAT 100.64/10 — the tailnet
        || (o[0] == 198 && (o[1] & 0xfe) == 18) // benchmarking 198.18/15
        || o[0] >= 240 // reserved
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(r: &str) -> SourceRef {
        parse(r).unwrap_or_else(|e| panic!("{r}: {e}"))
    }
    fn bad(r: &str) -> String {
        parse(r).expect_err(r).0
    }

    #[test]
    fn estate_tables_and_schemas() {
        assert_eq!(
            ok("findata://market.ohlc_1d"),
            SourceRef::Table {
                instance: "findata".into(),
                schema: "market".into(),
                table: Some("ohlc_1d".into())
            }
        );
        assert_eq!(
            ok("nus://ukb"),
            SourceRef::Table {
                instance: "nus".into(),
                schema: "ukb".into(),
                table: None
            }
        );
        assert!(matches!(
            ok("lumid-data://lumilake_experiment_ai4finance_10g.fact_ohlc_10m"),
            SourceRef::Table { .. }
        ));
        assert!(matches!(
            ok("  lqt://market.bbo_v1 "),
            SourceRef::Table { .. }
        ));
    }

    #[test]
    fn bare_and_unknown_schemes_are_refused() {
        assert!(bad("market.ohlc_1d").contains("no scheme"));
        assert!(bad("postgres://market.x").contains("unknown source scheme"));
        assert!(bad("").contains("empty"));
        assert!(bad(&format!("findata://{}", "a".repeat(600))).contains("longer"));
    }

    #[test]
    fn table_identifiers_are_strict() {
        for r in [
            "findata://market.ohlc;drop",
            "findata://1market.x",
            "findata://market.x.y",
            "findata://market.\"x\"",
            "findata://",
            "findata://.x",
        ] {
            bad(r);
        }
    }

    #[test]
    fn data_refs() {
        let sha = "ab".repeat(32);
        assert_eq!(
            ok(&format!("data://home/blobs/{}", sha.to_uppercase())),
            SourceRef::Blob {
                hub: "home".into(),
                sha256: sha.clone()
            }
        );
        assert_eq!(
            ok("data://nus/lumilake-archive/artifacts/"),
            SourceRef::BlobPrefix {
                hub: "nus".into(),
                prefix: "lumilake-archive/artifacts/".into()
            }
        );
        bad("data://home/blobs/abc");
        bad("data://home/x"); // no trailing slash
        bad("data://home/../etc/");
        bad("data://home/a//b/");
        bad("data:///x/");
        assert!(bad("data://home/tenants/u-1/").contains("private"));
        assert!(bad("data://home/retrievals/r/").contains("private"));
    }

    #[test]
    fn src_refs() {
        assert_eq!(
            ok("src://src_01HZ"),
            SourceRef::SourceId {
                source_id: "src_01HZ".into()
            }
        );
        assert_eq!(
            ok("src://u-1/my-panel"),
            SourceRef::SourceName {
                owner: "u-1".into(),
                name: "my-panel".into()
            }
        );
        bad("src://a/b/c");
        bad("src://a b");
    }

    #[test]
    fn hf_refs() {
        assert_eq!(
            ok("hf://Qwen/Qwen3-8B@main"),
            SourceRef::Model {
                org: "Qwen".into(),
                model: "Qwen3-8B".into(),
                rev: Some("main".into())
            }
        );
        assert!(!ok("hf://Qwen/Qwen3.5-9B-AWQ").is_data_plane());
        bad("hf://Qwen");
        bad("hf://../x");
        bad("hf://Qwen/x@a/b");
    }

    #[test]
    fn ext_refs_allow_public_hosts() {
        for h in [
            "clob.polymarket.com",
            "api.binance.com",
            "raw.githubusercontent.com",
            "8.8.8.8",
        ] {
            let r = ok(&format!("ext://{h}"));
            assert!(!r.is_data_plane());
        }
        assert_eq!(
            ok("ext://API.Binance.com/"),
            SourceRef::External {
                host: "api.binance.com".into()
            }
        );
    }

    #[test]
    fn ext_refs_refuse_the_2026_10_30_entry_points_and_friends() {
        for h in [
            "127.0.0.1", // the worker's own docker daemon (127.0.0.1:2375)
            "localhost",
            "0.0.0.0",
            "169.254.169.254", // cloud metadata
            "192.168.7.155",   // home LAN (the NAS)
            "10.42.13.185",    // k3s pod net — the 10-30 exfil host
            "172.19.0.5",      // docker bridge
            "100.115.66.10",   // tailnet
            "::1",
            "fe80::1",
            "fd00::1",
            "::ffff:127.0.0.1",
            "mesh-federator.lumid.svc.cluster.local",
            "lumid-data-primary.lumid.svc",
            "nas.local",
            "x.internal",
            "host.ts.net",
            "single-label",
            "1.2.3.4.5",
        ] {
            let e = bad(&format!("ext://{h}"));
            assert!(
                e.contains("internal") || e.contains("public DNS") || e.contains("bare host"),
                "{h}: {e}"
            );
        }
        // Ports, paths and userinfo are never part of an ext ref.
        bad("ext://api.binance.com:2375");
        bad("ext://api.binance.com/v3/x");
        bad("ext://user@api.binance.com");
        bad("ext://[::1]");
    }

    #[test]
    fn data_plane_classification() {
        assert!(ok("findata://market.x").is_data_plane());
        assert!(ok(&format!("data://home/blobs/{}", "0".repeat(64))).is_data_plane());
        assert!(ok("src://s1").is_data_plane());
        assert!(!ok("ext://api.binance.com").is_data_plane());
    }
}
