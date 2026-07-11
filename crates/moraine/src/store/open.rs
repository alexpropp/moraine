//! Opening a moraine store.
//!
//! Every store is created — and must thereafter be opened — with the
//! tag-byte segment extractor; SlateDB persists the extractor identity
//! and refuses a mismatched open.

use std::sync::Arc;

use object_store::ObjectStore;
use slatedb::Db;

use crate::error::{Error, Result};

/// Open (or create) the store at `path` on `object_store`, configured
/// with the tag-byte segment extractor.
pub(crate) async fn open_store(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Db> {
    Db::builder(path, object_store)
        .with_segment_extractor(Arc::new(crate::store::segment::TagSegmentExtractor))
        .build()
        .await
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use slatedb::{IsolationLevel, config::WriteOptions};

    use super::*;
    use crate::store::key::{self, Key, SysKey};

    fn memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// A commit-shaped transaction spanning several subspaces lands
    /// atomically, and per-subspace prefix scans see exactly their own
    /// segment's keys (multi-segment batches satisfy the antichain rule).
    #[tokio::test]
    async fn multi_subspace_transaction_and_prefix_scans() {
        let db = open_store("test/store", memory_store()).await.unwrap();

        let head = Key::Sys(SysKey::Head).encode();
        let snap = Key::Snap { snapshot_id: 1 }.encode();
        let table = Key::cur(key::EntityKey::Table { table_id: 7 }).encode();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(&head, b"head").unwrap();
        tx.put(&snap, b"snap").unwrap();
        tx.put(&table, b"table").unwrap();
        tx.commit_with_options(&WriteOptions {
            await_durable: true,
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(db.get(&head).await.unwrap().unwrap().as_ref(), b"head");

        // Each subspace scan returns exactly its own keys.
        let mut iter = db
            .scan_prefix(key::subspace_prefix(key::Subspace::Cur), ..)
            .await
            .unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), table.as_slice());
        assert!(iter.next().await.unwrap().is_none());

        let mut iter = db
            .scan_prefix(key::subspace_prefix(key::Subspace::Snap), ..)
            .await
            .unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), snap.as_slice());
        assert!(iter.next().await.unwrap().is_none());

        db.close().await.unwrap();
    }

    /// SlateDB persists the extractor identity: reopening the store
    /// without it is refused rather than silently mis-segmented.
    #[tokio::test]
    async fn reopen_without_extractor_is_refused() {
        let object_store = memory_store();
        let db = open_store("test/store", object_store.clone())
            .await
            .unwrap();
        db.put(&Key::Sys(SysKey::Head).encode(), b"head")
            .await
            .unwrap();
        db.close().await.unwrap();

        let bare = Db::builder("test/store", object_store).build().await;
        assert!(bare.is_err(), "unsegmented reopen must be refused");
    }
}
