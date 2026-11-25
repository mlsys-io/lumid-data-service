//! The driving side: one cycle per peer per `LUMID_SYNC_INTERVAL`, on the hub
//! that lists peers in `LUMID_HUB_SYNC_PEERS` (the entrance hub).
//!
//! Per cycle, in order: push registry → pull registry → push blobs → pull
//! blobs. Each leg pages until its peer reports no more, and stores its
//! cursor only after the page is applied (pushed: the peer ACKed every batch;
//! pulled: every batch applied here). A failed leg records its error in
//! `sync.hub_state` and keeps its cursor, so the next cycle resumes there.
//!
//! Each leg starts `LUMID_HUB_SYNC_OVERLAP_SECS` behind its stored cursor:
//! `updated_at` is a transaction's start time, so a row can commit after the
//! cursor has passed its version. The re-read rows apply as `stale` no-ops.

use std::time::Duration;

use super::blobs::{self, BlobIndexRow, BlobKey, HaveReq, HaveResp, INDEX_HEADER};
use super::registry::{self, Cursor, RegistryAck};
use super::{load_cursor, record, HubPeer, OutboxResponse};
use crate::state::AppState;

/// What one cycle did (also returned to tests).
#[derive(Debug, Default, Clone)]
pub struct CycleReport {
    pub pushed: i64,
    pub pulled: i64,
    pub blobs_pushed: i64,
    pub blobs_pulled: i64,
    pub errors: Vec<String>,
}

/// Keep, per table, the later of the stored and the new position, so a cycle
/// that read only its overlap window never moves a cursor backwards.
pub fn merge_max(saved: &Cursor, next: &Cursor) -> Cursor {
    let mut out = saved.clone();
    for (t, pos) in next {
        match out.get(t) {
            Some(old) if old >= pos => {}
            _ => {
                out.insert(t.clone(), pos.clone());
            }
        }
    }
    out
}

fn short(s: &str) -> String {
    s.chars().take(300).collect()
}

async fn http_err(resp: reqwest::Response) -> String {
    let code = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    format!("HTTP {code}: {}", short(&body))
}

/// Run every leg against one peer. Never panics; errors land in the report
/// and in `sync.hub_state`.
pub async fn run_cycle(st: &AppState, peer: &HubPeer) -> CycleReport {
    let mut rep = CycleReport::default();
    match push_registry(st, peer).await {
        Ok(n) => rep.pushed = n,
        Err(e) => rep.errors.push(format!("registry push: {e}")),
    }
    match pull_registry(st, peer).await {
        Ok(n) => rep.pulled = n,
        Err(e) => rep.errors.push(format!("registry pull: {e}")),
    }
    if blobs::enabled(st) {
        match push_blobs(st, peer).await {
            Ok(n) => rep.blobs_pushed = n,
            Err(e) => rep.errors.push(format!("blobs push: {e}")),
        }
        match pull_blobs(st, peer).await {
            Ok(n) => rep.blobs_pulled = n,
            Err(e) => rep.errors.push(format!("blobs pull: {e}")),
        }
    }
    rep
}

/// Start of a leg: the stored cursor, rewound by the overlap.
async fn start(
    st: &AppState,
    peer: &str,
    scope: &str,
    dir: &str,
) -> Result<(Cursor, Cursor), String> {
    let saved = load_cursor(&st.pool, peer, scope, dir)
        .await
        .map_err(|e| e.to_string())?;
    let saved = registry::parse_cursor(saved.as_deref()).map_err(|e| e.to_string())?;
    let cur = registry::rewind(&saved, st.settings.hub_sync_overlap_s);
    Ok((saved, cur))
}

async fn save(
    st: &AppState,
    peer: &str,
    scope: &str,
    dir: &str,
    saved: &mut Cursor,
    next: &Cursor,
    rows: i64,
) {
    *saved = merge_max(saved, next);
    let enc = registry::encode_cursor(saved);
    record(
        &st.pool,
        peer,
        scope,
        dir,
        Ok(rows),
        Some((&enc, registry::newest(saved))),
    )
    .await;
}

/// Run a leg body, recording its terminal error (if any) after its progress.
async fn finish(
    st: &AppState,
    peer: &str,
    scope: &str,
    dir: &str,
    r: Result<(i64, Option<String>), String>,
) -> Result<i64, String> {
    match r {
        Ok((n, None)) => Ok(n),
        Ok((n, Some(warn))) => {
            // Applied, but some rows were refused: keep the leg visibly failing.
            record(&st.pool, peer, scope, dir, Err(&warn), None).await;
            tracing::warn!(peer, scope, dir, "hub sync: {warn}");
            Ok(n)
        }
        Err(e) => {
            record(&st.pool, peer, scope, dir, Err(&e), None).await;
            Err(e)
        }
    }
}

fn limit(st: &AppState) -> i64 {
    (st.settings.sync_batch_rows as i64).clamp(1, 5000)
}

async fn push_registry(st: &AppState, peer: &HubPeer) -> Result<i64, String> {
    let body = async {
        let (mut saved, mut cur) = start(st, &peer.hub_id, "registry", "push").await?;
        let url = format!("{}/sync/inbox", peer.base_url);
        let mut total = 0i64;
        let mut warn = None;
        loop {
            let (batches, next, more) = registry::collect(st, &cur, limit(st))
                .await
                .map_err(|e| e.to_string())?;
            let mut page = 0i64;
            for b in &batches {
                let resp = st
                    .http
                    .post(&url)
                    .bearer_auth(&peer.token)
                    .json(b)
                    .send()
                    .await
                    .map_err(|e| format!("send: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!("{}: {}", b.table, http_err(resp).await));
                }
                let ack: RegistryAck = resp.json().await.map_err(|e| format!("ack: {e}"))?;
                page += ack.inserted + ack.updated;
                if ack.rejected > 0 {
                    warn = Some(format!(
                        "{}: peer rejected {} row(s): {}",
                        b.table,
                        ack.rejected,
                        ack.first_error.as_deref().unwrap_or("")
                    ));
                }
            }
            total += page;
            save(
                st,
                &peer.hub_id,
                "registry",
                "push",
                &mut saved,
                &next,
                page,
            )
            .await;
            cur = next;
            if !more {
                break;
            }
        }
        Ok((total, warn))
    }
    .await;
    finish(st, &peer.hub_id, "registry", "push", body).await
}

async fn fetch_outbox(
    st: &AppState,
    peer: &HubPeer,
    scope: &str,
    cur: &Cursor,
) -> Result<OutboxResponse, String> {
    let resp = st
        .http
        .get(format!("{}/sync/outbox", peer.base_url))
        .bearer_auth(&peer.token)
        .query(&[
            ("scope", scope.to_string()),
            ("since", registry::encode_cursor(cur)),
            ("limit", limit(st).to_string()),
        ])
        .send()
        .await
        .map_err(|e| format!("send: {e}"))?;
    if !resp.status().is_success() {
        return Err(http_err(resp).await);
    }
    let out: OutboxResponse = resp.json().await.map_err(|e| format!("outbox body: {e}"))?;
    if out.hub != peer.hub_id {
        return Err(format!(
            "outbox answered as hub `{}`, expected `{}`",
            out.hub, peer.hub_id
        ));
    }
    Ok(out)
}

async fn pull_registry(st: &AppState, peer: &HubPeer) -> Result<i64, String> {
    let body = async {
        let (mut saved, mut cur) = start(st, &peer.hub_id, "registry", "pull").await?;
        let mut total = 0i64;
        let mut warn = None;
        loop {
            let out = fetch_outbox(st, peer, "registry", &cur).await?;
            let next = registry::parse_cursor(Some(&out.cursor)).map_err(|e| e.to_string())?;
            let mut page = 0i64;
            for b in &out.batches {
                // The same apply path as the inbox: same ledger, same rule,
                // and the batch must be the peer's own rows.
                let ack = registry::apply_batch(st, &peer.hub_id, b)
                    .await
                    .map_err(|e| format!("{}: {e}", b.table))?;
                page += ack.inserted + ack.updated;
                if ack.rejected > 0 {
                    warn = Some(format!(
                        "{}: rejected {} pulled row(s): {}",
                        b.table,
                        ack.rejected,
                        ack.first_error.as_deref().unwrap_or("")
                    ));
                }
            }
            total += page;
            save(
                st,
                &peer.hub_id,
                "registry",
                "pull",
                &mut saved,
                &next,
                page,
            )
            .await;
            if !out.more || next == cur {
                break;
            }
            cur = next;
        }
        Ok((total, warn))
    }
    .await;
    finish(st, &peer.hub_id, "registry", "pull", body).await
}

async fn push_blobs(st: &AppState, peer: &HubPeer) -> Result<i64, String> {
    let body = async {
        let (mut saved, mut cur) = start(st, &peer.hub_id, "blobs", "push").await?;
        let mut total = 0i64;
        loop {
            let (rows, next, more) = blobs::collect_index(st, &cur, limit(st))
                .await
                .map_err(|e| e.to_string())?;
            let mut page = 0i64;
            if !rows.is_empty() {
                let items: Vec<BlobKey> = rows
                    .iter()
                    .map(|r| BlobKey {
                        owner_sub: r.owner_sub.clone(),
                        blob_sha256: r.blob_sha256.clone(),
                    })
                    .collect();
                let resp = st
                    .http
                    .post(format!("{}/sync/blobs/have", peer.base_url))
                    .bearer_auth(&peer.token)
                    .json(&HaveReq { items })
                    .send()
                    .await
                    .map_err(|e| format!("send: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!("have: {}", http_err(resp).await));
                }
                let have: HaveResp = resp.json().await.map_err(|e| format!("have body: {e}"))?;
                for m in &have.missing {
                    let Some(row) = rows
                        .iter()
                        .find(|r| r.owner_sub == m.owner_sub && r.blob_sha256 == m.blob_sha256)
                    else {
                        continue;
                    };
                    let key =
                        crate::handlers::me_blobs::tenant_key(&row.owner_sub, &row.blob_sha256);
                    let bytes = st
                        .blob_store
                        .get(&object_store::path::Path::from(key.as_str()))
                        .await
                        .map_err(|e| format!("read local blob: {e}"))?
                        .bytes()
                        .await
                        .map_err(|e| format!("read local blob: {e}"))?;
                    let meta = hex::encode(serde_json::to_vec(row).map_err(|e| e.to_string())?);
                    let resp = st
                        .http
                        .put(format!(
                            "{}/sync/blobs/{}/{}",
                            peer.base_url, row.owner_sub, row.blob_sha256
                        ))
                        .bearer_auth(&peer.token)
                        .header(INDEX_HEADER, meta)
                        .header("content-type", "application/octet-stream")
                        .body(bytes)
                        .send()
                        .await
                        .map_err(|e| format!("send: {e}"))?;
                    if !resp.status().is_success() {
                        return Err(format!("put: {}", http_err(resp).await));
                    }
                    page += 1;
                }
            }
            total += page;
            save(st, &peer.hub_id, "blobs", "push", &mut saved, &next, page).await;
            cur = next;
            if !more {
                break;
            }
        }
        Ok((total, None))
    }
    .await;
    finish(st, &peer.hub_id, "blobs", "push", body).await
}

async fn pull_blobs(st: &AppState, peer: &HubPeer) -> Result<i64, String> {
    let body = async {
        let (mut saved, mut cur) = start(st, &peer.hub_id, "blobs", "pull").await?;
        let cap = st.settings.hub_small_max.min(i64::MAX as u64) as i64;
        let mut total = 0i64;
        loop {
            let out = fetch_outbox(st, peer, "blobs", &cur).await?;
            let next = registry::parse_cursor(Some(&out.cursor)).map_err(|e| e.to_string())?;
            let keys: Vec<BlobKey> = out
                .blobs
                .iter()
                .map(|r| BlobKey {
                    owner_sub: r.owner_sub.clone(),
                    blob_sha256: r.blob_sha256.clone(),
                })
                .collect();
            let missing = blobs::missing_of(st, &keys)
                .await
                .map_err(|e| e.to_string())?;
            let mut page = 0i64;
            for m in &missing {
                let Some(row) = out
                    .blobs
                    .iter()
                    .find(|r| r.owner_sub == m.owner_sub && r.blob_sha256 == m.blob_sha256)
                else {
                    continue;
                };
                // Only the peer's own uploads, and never above OUR cap.
                if row.home_hub != peer.hub_id || row.size_bytes > cap {
                    continue;
                }
                let resp = st
                    .http
                    .get(format!(
                        "{}/sync/blobs/{}/{}",
                        peer.base_url, row.owner_sub, row.blob_sha256
                    ))
                    .bearer_auth(&peer.token)
                    .send()
                    .await
                    .map_err(|e| format!("send: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!("get: {}", http_err(resp).await));
                }
                if resp
                    .content_length()
                    .map(|n| n as i64 > cap)
                    .unwrap_or(false)
                {
                    continue;
                }
                let bytes = resp.bytes().await.map_err(|e| format!("body: {e}"))?;
                let row: BlobIndexRow = row.clone();
                if blobs::store(st, &row, bytes)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    page += 1;
                }
            }
            total += page;
            save(st, &peer.hub_id, "blobs", "pull", &mut saved, &next, page).await;
            if !out.more || next == cur {
                break;
            }
            cur = next;
        }
        Ok((total, None))
    }
    .await;
    finish(st, &peer.hub_id, "blobs", "pull", body).await
}

/// The scheduler: a no-op without peers.
pub fn spawn(st: AppState, peers: Vec<HubPeer>) {
    if peers.is_empty() {
        return;
    }
    let every = Duration::from_secs(st.settings.hub_sync_interval_s.max(1));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            for p in &peers {
                let r = run_cycle(&st, p).await;
                if r.errors.is_empty() {
                    if r.pushed + r.pulled + r.blobs_pushed + r.blobs_pulled > 0 {
                        tracing::info!(
                            peer = %p.hub_id,
                            pushed = r.pushed,
                            pulled = r.pulled,
                            blobs_pushed = r.blobs_pushed,
                            blobs_pulled = r.blobs_pulled,
                            "hub sync: cycle"
                        );
                    }
                } else {
                    tracing::warn!(peer = %p.hub_id, errors = ?r.errors, "hub sync: cycle had errors");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn merge_never_moves_a_cursor_back() {
        let mut saved = Cursor::new();
        saved.insert(
            "jobs.sources".into(),
            (ts("2026-11-25T10:00:00Z"), "b".into()),
        );
        let rewound = registry::rewind(&saved, 120);
        assert_eq!(merge_max(&saved, &rewound), saved);
        let mut next = Cursor::new();
        next.insert(
            "jobs.sources".into(),
            (ts("2026-11-25T10:00:00Z"), "c".into()),
        );
        next.insert("jobs.jobs".into(), (ts("2026-11-25T09:00:00Z"), "x".into()));
        let m = merge_max(&saved, &next);
        assert_eq!(m["jobs.sources"].1, "c");
        assert!(m.contains_key("jobs.jobs"));
    }
}
