//! Opening a moraine store.
//!
//! Every store is created — and must thereafter be opened — with the
//! tag-byte segment extractor; SlateDB persists the extractor identity
//! and refuses a mismatched open.
//!
//! The WAL flush interval bounds per-commit latency: durable commits
//! wait for the next flush, and smaller values mean more frequent
//! object-store PUTs. It comes from `CatalogOptions::flush_interval` and
//! must be nonzero.

use std::{sync::Arc, time::Duration};

use object_store::ObjectStore;
use slatedb::{Db, DbReader, config::Settings};

use crate::{
    error::{Error, Result},
    store::segment::TagSegmentExtractor,
};

/// Store settings for the given flush interval. Zero is refused: it
/// would disable automatic flushing and hang every durable commit.
fn settings(flush_interval: Duration) -> Result<Settings> {
    if flush_interval.is_zero() {
        return Err(Error::Configuration(
            "flush_interval must be nonzero; zero would disable automatic flushing and \
             hang every durable commit"
                .to_string(),
        ));
    }

    Ok(Settings {
        flush_interval: Some(flush_interval),
        ..Default::default()
    })
}

/// Open (or create) the store at `path` on `object_store`, configured
/// with the tag-byte segment extractor and `flush_interval` as the WAL
/// flush cadence.
pub(crate) async fn open_store(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    flush_interval: Duration,
) -> Result<Db> {
    Db::builder(path, object_store)
        .with_settings(settings(flush_interval)?)
        .with_segment_extractor(Arc::new(TagSegmentExtractor))
        .build()
        .await
        .map_err(Error::from)
}

/// Open the store read-only as a [`DbReader`] following the latest manifest,
/// with the same tag-byte segment extractor as the writer. A `DbReader`
/// never opens the writer `Db`, so it never fences a live writer.
pub(crate) async fn open_reader(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<DbReader> {
    DbReader::builder(path, object_store)
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
        let db = open_store("test/store", memory_store(), Duration::from_millis(100))
            .await
            .unwrap();

        let head = Key::Sys(SysKey::Head).encode();
        let snapshot = Key::Snapshot { snapshot_id: 1 }.encode();
        let table = Key::current(key::EntityKey::Table { table_id: 7 }).encode();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(&head, b"head").unwrap();
        tx.put(&snapshot, b"snap").unwrap();
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
            .scan_prefix(key::subspace_prefix(key::Subspace::Current), ..)
            .await
            .unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), table.as_slice());
        assert!(iter.next().await.unwrap().is_none());

        let mut iter = db
            .scan_prefix(key::subspace_prefix(key::Subspace::Snapshot), ..)
            .await
            .unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), snapshot.as_slice());
        assert!(iter.next().await.unwrap().is_none());

        db.close().await.unwrap();
    }

    /// A zero flush interval would disable automatic flushing and hang
    /// every durable commit, so opening with one is refused.
    #[tokio::test]
    async fn zero_flush_interval_is_refused() {
        // `unwrap_err` needs `Db: Debug`, which SlateDB does not provide.
        match open_store("test/store", memory_store(), Duration::ZERO).await {
            Err(Error::Configuration(_)) => {}
            Err(err) => panic!("expected a configuration error, got {err:?}"),
            Ok(_) => panic!("a zero flush interval unexpectedly opened a store"),
        }
    }

    /// An explicit flush interval reaches the SlateDB builder: the store
    /// opens, and a durable commit still lands.
    #[tokio::test]
    async fn explicit_flush_interval_opens_a_working_store() {
        let db = open_store("test/store", memory_store(), Duration::from_millis(1))
            .await
            .unwrap();

        let head = Key::Sys(SysKey::Head).encode();
        db.put(&head, b"head").await.unwrap();
        assert_eq!(db.get(&head).await.unwrap().unwrap().as_ref(), b"head");

        db.close().await.unwrap();
    }

    /// SlateDB persists the extractor identity: reopening the store
    /// without it is refused rather than silently mis-segmented.
    #[tokio::test]
    async fn reopen_without_extractor_is_refused() {
        let object_store = memory_store();
        let db = open_store(
            "test/store",
            object_store.clone(),
            Duration::from_millis(100),
        )
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
