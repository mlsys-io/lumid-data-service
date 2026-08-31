//! Object-store writes that survive a backend without per-object metadata.
//!
//! Every `put` in this crate wants to stamp a Content-Type, because several key
//! shapes (CAS blobs under `sha256=…`, retrieval results under
//! `retrievals/<run_id>/result.<ext>`) are extension-free or use extensions the
//! `GET /blobs/<key>` guesser does not know. Object-store metadata is the only
//! place that type survives.
//!
//! But `Attributes` are an S3-shaped feature. `LocalFileSystem` has nowhere to
//! put them, so its `put_opts` returns `Error::NotImplemented` the moment the
//! attribute set is non-empty — and with no object-store env configured, which
//! is the default, LocalFileSystem is what you get.
//!
//! Call sites propagated that error, so on a local store the write failed
//! outright. Measured 2026-08-31: every `POST /findata/retrieve` returned
//!
//!     internal error: object store write failed: Operation not yet implemented.
//!
//! including `SELECT 1`, because the query had already succeeded and it was the
//! RESULT WRITE that failed. `/health` stayed green throughout — nothing was
//! wrong with the database, the planner or the SQL — so the service looked up
//! while every analytical read was down. Two users reported it independently
//! over two days.
//!
//! Retrying without attributes is the right trade rather than dropping them:
//! on S3 the type is still stamped, and on a local store the object lands and
//! the `GET` handler falls back to its extension guess. A worse content-type on
//! a file that EXISTS beats a 500 on every query.

use std::sync::Arc;

use object_store::{path::Path as ObjPath, Attributes, ObjectStore, PutOptions, PutPayload};

/// Put `body` at `path`, stamping `attrs` when the backend supports them.
///
/// Falls back to a plain `put` — same bytes, same path — when the store reports
/// `NotImplemented`, which is how `LocalFileSystem` answers any non-empty
/// attribute set. Every other error is returned untouched.
pub async fn put_with_optional_attrs(
    store: &Arc<dyn ObjectStore>,
    path: &ObjPath,
    body: bytes::Bytes,
    attrs: Attributes,
) -> object_store::Result<()> {
    match store
        .put_opts(
            path,
            PutPayload::from(body.clone()),
            PutOptions::from(attrs),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotImplemented) => {
            store.put(path, PutPayload::from(body)).await.map(|_| ())
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{local::LocalFileSystem, Attribute};

    // A unique scratch dir without pulling in `tempfile` — adding a
    // dev-dependency would churn Cargo.lock in a repo other sessions share.
    fn scratch(tag: &str) -> std::path::PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!("objstore-test-{tag}-{n}"));
        std::fs::create_dir_all(&d).expect("scratch dir");
        d
    }

    // The exact shape that took `/retrieve` down: a LOCAL store plus a
    // non-empty attribute set. Before the fallback this returned
    // Error::NotImplemented and every analytical query 500'd — including
    // `SELECT 1`, because the failure is in the RESULT WRITE, not the query.
    #[tokio::test]
    async fn local_store_accepts_a_write_that_carries_attributes() {
        let dir = scratch("attrs");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).expect("store"));
        let path = ObjPath::from("retrievals/run-1/result.jsonl");
        let attrs =
            Attributes::from_iter([(Attribute::ContentType, "application/x-ndjson".to_string())]);

        put_with_optional_attrs(
            &store,
            &path,
            bytes::Bytes::from_static(b"{\"ok\":1}\n"),
            attrs,
        )
        .await
        .expect("a local store must not reject a write just because it cannot keep metadata");

        // The bytes must actually be there — a fallback that silently wrote
        // nothing would be worse than the 500 it replaced.
        let got = store.get(&path).await.expect("object must exist");
        let body = got.bytes().await.expect("readable");
        assert_eq!(&body[..], b"{\"ok\":1}\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_attributes_still_write() {
        let dir = scratch("plain");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).expect("store"));
        let path = ObjPath::from("blobs/plain");
        put_with_optional_attrs(
            &store,
            &path,
            bytes::Bytes::from_static(b"x"),
            Attributes::new(),
        )
        .await
        .expect("no attributes is the easy case and must keep working");
        assert_eq!(
            &store.get(&path).await.unwrap().bytes().await.unwrap()[..],
            b"x"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
