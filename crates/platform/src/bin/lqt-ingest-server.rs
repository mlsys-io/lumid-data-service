//! lqt-ingest-server — minimal platform-only server for the scope-aware generic
//! `/ingest` sidecar (`lqt-mailbox-ingest`).
//!
//! It composes NO app ext-routes (`ServeParts::default()` → empty `/xpio` etc.):
//! the closed findata-ext `/xpio` surface stays on the merged `lqt` app; this
//! sidecar only needs the platform's `POST /ingest/:schema/:table` behind the
//! scope-aware ACL (`lumid_platform::ingest::acl::scope_grants_write`), which now
//! blesses `lqt:strategy`/`lqt:trading` → `mailbox.lqt_inbox` so an off-box
//! `signal.publish` producer can self-serve (no borrowed universe-refresh cred).
//! Buildable from THIS repo (the full findata-app-bin needs the off-box
//! findata-ext crate; this platform-only bin does not).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lumid_platform::serve(lumid_platform::ServeParts::default()).await
}
