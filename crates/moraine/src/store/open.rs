//! Opening a moraine store.
//!
//! [`StoreBuilder`] opens a store on an object store as a read-write [`Db`]
//! or a read-only [`DbReader`], carrying the shared open configuration.
//! Every store is created — and must thereafter be opened — with the
//! tag-byte segment extractor; SlateDB persists the extractor identity and
//! refuses a mismatched open.

use std::{path::PathBuf, sync::Arc, time::Duration};

use object_store::ObjectStore;
use slatedb::{
    Db, DbReader,
    config::{DbReaderOptions, ObjectStoreCacheOptions, Settings},
};

use crate::{
    error::{Error, Result},
    store::segment::TagSegmentExtractor,
};

/// The default WAL flush cadence when none is configured.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Opens a moraine store on `object_store` — a read-write [`Db`] via
/// [`open_writer`](Self::open_writer) or a read-only [`DbReader`] via
/// [`open_reader`](Self::open_reader) — carrying the shared open
/// configuration: the WAL flush cadence (writer only) and the on-disk cache.
pub(crate) struct StoreBuilder<'a> {
    path: &'a str,
    object_store: Arc<dyn ObjectStore>,
    flush_interval: Duration,
    cache_dir: Option<PathBuf>,
}

impl<'a> StoreBuilder<'a> {
    /// A builder for the store at `path` on `object_store`, with the default
    /// flush cadence and no on-disk cache.
    pub(crate) fn new(path: &'a str, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path,
            object_store,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            cache_dir: None,
        }
    }

    /// Sets the WAL flush cadence. Durable commits wait for the next flush,
    /// so this bounds per-commit latency; smaller values mean more frequent
    /// (on S3, costlier) object-store PUTs. Must be nonzero. Writer only —
    /// a reader never flushes.
    pub(crate) fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Sets the local directory backing SlateDB's on-disk block cache. When
    /// set, warm reads skip repeat object-store GETs and survive process
    /// restarts — worthwhile for remote (`s3://`) stores, redundant for local
    /// ones. `None` (the default) uses only SlateDB's in-memory cache.
    pub(crate) fn cache_dir(mut self, cache_dir: Option<PathBuf>) -> Self {
        self.cache_dir = cache_dir;
        self
    }

    /// Opens (or creates) the store as a read-write [`Db`].
    pub(crate) async fn open_writer(self) -> Result<Db> {
        let settings = self.settings()?;
        Db::builder(self.path, self.object_store)
            .with_settings(settings)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .build()
            .await
            .map_err(Error::from)
    }

    /// Opens the store read-only as a [`DbReader`] following the latest
    /// manifest. A `DbReader` never opens the writer `Db`, so it never fences
    /// a live writer. The flush cadence, if set, is ignored.
    pub(crate) async fn open_reader(self) -> Result<DbReader> {
        let options = DbReaderOptions {
            object_store_cache_options: self.cache_options(),
            ..Default::default()
        };
        DbReader::builder(self.path, self.object_store)
            .with_segment_extractor(Arc::new(TagSegmentExtractor))
            .with_options(options)
            .build()
            .await
            .map_err(Error::from)
    }

    /// SlateDB settings for a writer. A zero flush interval is refused: it
    /// would disable automatic flushing and hang every durable commit.
    fn settings(&self) -> Result<Settings> {
        if self.flush_interval.is_zero() {
            return Err(Error::Configuration(
                "flush_interval must be nonzero; zero would disable automatic flushing and \
                 hang every durable commit"
                    .to_string(),
            ));
        }

        Ok(Settings {
            flush_interval: Some(self.flush_interval),
            object_store_cache_options: self.cache_options(),
            ..Default::default()
        })
    }

    /// SlateDB's on-disk block cache options: a disk-backed cache under
    /// `cache_dir` when set, otherwise none (in-memory cache only). Every
    /// field but `root_folder` stays at SlateDB's defaults (16 GiB, 4 MiB
    /// parts).
    fn cache_options(&self) -> ObjectStoreCacheOptions {
        ObjectStoreCacheOptions {
            root_folder: self.cache_dir.clone(),
            ..Default::default()
        }
    }
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
        let db = StoreBuilder::new("test/store", memory_store())
            .open_writer()
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
        match StoreBuilder::new("test/store", memory_store())
            .flush_interval(Duration::ZERO)
            .open_writer()
            .await
        {
            Err(Error::Configuration(_)) => {}
            Err(err) => panic!("expected a configuration error, got {err:?}"),
            Ok(_) => panic!("a zero flush interval unexpectedly opened a store"),
        }
    }

    /// An explicit flush interval reaches the SlateDB builder: the store
    /// opens, and a durable commit still lands.
    #[tokio::test]
    async fn explicit_flush_interval_opens_a_working_store() {
        let db = StoreBuilder::new("test/store", memory_store())
            .flush_interval(Duration::from_millis(1))
            .open_writer()
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
        let db = StoreBuilder::new("test/store", object_store.clone())
            .open_writer()
            .await
            .unwrap();
        db.put(&Key::Sys(SysKey::Head).encode(), b"head")
            .await
            .unwrap();
        db.close().await.unwrap();

        let bare = Db::builder("test/store", object_store).build().await;
        assert!(bare.is_err(), "unsegmented reopen must be refused");
    }

    /// A configured `cache_dir` reaches SlateDB: a fresh `DbReader`, whose
    /// in-memory cache has never seen the store's blocks, serves committed
    /// data and in doing so populates the on-disk cache directory.
    #[tokio::test]
    async fn cache_dir_backs_the_reader_with_an_on_disk_cache() {
        let object_store = memory_store();
        let cache = std::env::temp_dir().join(format!("moraine-cache-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cache);

        let head = Key::Sys(SysKey::Head).encode();
        let db = StoreBuilder::new("s", object_store.clone())
            .flush_interval(Duration::from_millis(1))
            .cache_dir(Some(cache.clone()))
            .open_writer()
            .await
            .unwrap();
        db.put(&head, b"head").await.unwrap();
        db.close().await.unwrap();

        // The reader is cold: it must GET the store's blocks, which the disk
        // cache records under `cache`.
        let reader = StoreBuilder::new("s", object_store)
            .cache_dir(Some(cache.clone()))
            .open_reader()
            .await
            .unwrap();
        assert_eq!(reader.get(head).await.unwrap().unwrap().as_ref(), b"head");
        reader.close().await.unwrap();

        let populated = std::fs::read_dir(&cache).is_ok_and(|mut entries| entries.next().is_some());
        assert!(populated, "expected an on-disk cache under {cache:?}");
        let _ = std::fs::remove_dir_all(&cache);
    }
}
