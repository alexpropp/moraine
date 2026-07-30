//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use object_store::{ObjectStore, path::Path};
use slatedb::{Db, DbReader, DbTransaction, IsolationLevel};
use tracing::{info, warn};

use crate::{
    catalog::{
        CatalogSnapshot, ColumnId, ColumnOrder, DataFileId, DataFileInfo, FileIndexEntry, IndexDef,
        IndexEntry, IndexId, IndexInfo, IndexState, RowHolder, RowLocation, SnapshotId, TableId,
        projection::ProjectionCache, scoped_read,
    },
    error::{Error, Result},
    store::{
        handle::{ReadHandle, ReadSession},
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, NullOrder, encode_ordered_values,
        },
        inline as store_inline,
        key::{IndexKey, IndexKind, InlineOperation, Key, index_index_prefix, index_kind_prefix},
        open::StoreBuilder,
    },
    transaction::{Transaction, commit, index_maintenance, slot_commit},
};

/// How many entries one staged build step commits. At roughly a kilobyte
/// of write-path memory apiece, a step peaks near a gigabyte.
const BUILD_STEP_ENTRIES: usize = 1_000_000;

/// How many times a staged build re-derives after losing a race before
/// giving up.
const BUILD_DERIVATION_ATTEMPTS: usize = 8;

/// Whether `path` provably holds no objects. A listing that fails answers
/// `false`: this licenses creating a store, so anything short of proof that
/// there is nothing to destroy must deny.
async fn prefix_is_known_empty(object_store: &Arc<dyn ObjectStore>, path: &str) -> bool {
    let prefix: Path = path.split('/').filter(|part| !part.is_empty()).collect();
    let mut listing = object_store.list(Some(&prefix));
    match listing.next().await {
        None => true,
        Some(Ok(object)) => {
            warn!(
                path,
                found = %object.location,
                "refusing to create a store: the prefix already holds objects"
            );
            false
        }
        Some(Err(err)) => {
            warn!(
                path,
                error = %err,
                "refusing to create a store: the prefix could not be listed"
            );
            false
        }
    }
}

/// The per-column orders `orders` asks for, as a definition records them.
/// An empty list means ascending / NULLS LAST throughout.
fn requested_orders(orders: &[ColumnOrder], columns: usize) -> (Vec<Direction>, Vec<NullOrder>) {
    (0..columns)
        .map(|position| {
            orders
                .get(position)
                .map_or((Direction::Ascending, NullOrder::Last), |order| {
                    (order.direction, order.nulls)
                })
        })
        .unzip()
}

/// One materialized head, with what a slot-backed attach adds.
struct HeadRead {
    view: CatalogSnapshot,
    /// The unfolded tail's writes; `None` on a single-topology store.
    tail: Option<moraine_wal::Overlay>,
    /// Set when the view came from a reader the materialization opened for
    /// itself rather than the session's.
    reader: Option<DbReader>,
}

/// A probe's read: the session it scans through and the head it resolves
/// against.
struct ProbeRead {
    session: ReadSession,
    head: HeadRead,
}

impl ProbeRead {
    /// The handle to scan entries through: the reader the head's view came
    /// from, which after a hole retry is not the session's.
    fn handle(&self) -> ReadHandle<'_> {
        match &self.head.reader {
            Some(reader) => ReadHandle::Reader(reader),
            None => self.session.handle(),
        }
    }

    fn view(&self) -> &CatalogSnapshot {
        &self.head.view
    }

    fn tail(&self) -> Option<&moraine_wal::Overlay> {
        self.head.tail.as_ref()
    }

    /// Releases both the session and any reader the head opened.
    async fn finish(self) {
        slot_commit::release_reader(self.head.reader.as_ref()).await;
        self.session.finish();
    }
}

/// Whether a run of build steps finished the index or lost its race.
enum BuildProgress {
    /// A final step flipped the index ready.
    Ready,
    /// A step lost its race; the backfill must be re-derived.
    Conflicted,
}

/// What a maintenance pass should reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceRequest {
    /// Reclaim the entry ranges of indexes that are no longer live —
    /// orphaned by `drop_index`, or by a `drop_table` that ended the
    /// table's indexes with it.
    pub sweep_orphaned_index_entries: bool,
    /// Maximum entries deleted per commit. Each batch is one atomic
    /// write; the pass yields between them so a large reclamation never
    /// holds the writer. Must be nonzero.
    pub batch_size: usize,
}

impl Default for MaintenanceRequest {
    fn default() -> Self {
        Self {
            sweep_orphaned_index_entries: true,
            batch_size: 1024,
        }
    }
}

/// What a maintenance pass reclaimed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// Dead indexes whose entry ranges were reclaimed.
    pub indexes_swept: u64,
    /// Entry keys deleted across those ranges.
    pub index_entries_reclaimed: u64,
}

/// The open store behind a catalog: the read-write `Db` writer, a
/// read-only `DbReader`, or a commit-log-backed attach. A read-only catalog
/// never opens a `Db`, so it never fences a live writer.
enum Store {
    /// The single read-write writer.
    Writer(Db),
    /// A read-only reader following the manifest, shared into read sessions.
    Reader(Arc<DbReader>),
    /// A commit-log-backed attach: a reader plus the slot log. Transitional
    /// — removed once the log is the only topology.
    MultiWriter(MultiWriterStore),
}

/// The store behind a [`Store::MultiWriter`] attach.
// dead_code: `read_only` is read by the fold sprints, landing in a later task.
#[allow(dead_code)]
pub(crate) struct MultiWriterStore {
    pub(crate) reader: Arc<DbReader>,
    pub(crate) slots: moraine_wal::SlotLog,
    /// Retained for fold sprints (reopening the fenced writer) and reads.
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: CatalogOptions,
    pub(crate) read_only: bool,
}

/// Options for opening a catalog.
///
/// # Examples
///
/// ```
/// let options = moraine::CatalogOptions::default();
/// assert_eq!(
///     options.flush_interval,
///     std::time::Duration::from_millis(100)
/// );
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CatalogOptions {
    /// Path prefix of the catalog within the bucket. Empty (the default)
    /// places the catalog at the bucket root; set it when several stores
    /// share a bucket.
    pub path: String,
    /// Whether DuckLake encrypts this catalog's data files. Creation-time
    /// only: recorded as the stored global `encrypted` option when a fresh
    /// store bootstraps, and ignored on an already-initialized store,
    /// where the stored value is authoritative.
    pub encrypted: bool,
    /// How often the store's write-ahead log is flushed to object
    /// storage. Durable commits wait for the next flush, so this bounds
    /// per-commit latency; smaller values mean more frequent (on S3,
    /// costlier) object-store PUTs. Zero flushes continuously (no timer),
    /// so a durable commit waits only on the object-store PUT — the lowest
    /// latency, at the cost of a busy flush loop. Defaults to 100ms.
    pub flush_interval: Duration,
    /// Local directory backing SlateDB's on-disk block cache. When set,
    /// reads are served from a disk-backed cache that survives process
    /// restarts, so warm queries skip repeat object-store GETs — worthwhile
    /// for remote (`s3://`) stores, redundant for local ones. `None` (the
    /// default) uses only SlateDB's in-memory cache.
    pub cache_dir: Option<std::path::PathBuf>,
    /// The lake's data root (DuckLake's `DATA_PATH`). Creation-time only:
    /// recorded as the stored global `data_path` option when a fresh store
    /// bootstraps, so a later open can read it back
    /// ([`CatalogSnapshot::data_path`](crate::CatalogSnapshot::data_path)).
    /// `None` records nothing.
    pub data_path: Option<String>,
    /// Opt into the slot-log topology at store creation. Transitional:
    /// removed once the slot log is the only topology.
    pub multi_writer: bool,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            path: String::new(),
            encrypted: false,
            flush_interval: Duration::from_millis(100),
            cache_dir: None,
            data_path: None,
            multi_writer: false,
        }
    }
}

/// A handle to a moraine catalog: cheap to clone, drives reads and
/// commits. The storage substrate never appears in this API — a catalog
/// lives in a bucket reachable through any [`ObjectStore`].
#[derive(Clone)]
pub struct Catalog {
    store: Arc<Store>,
    // Shared across handle clones: decoded projections folded forward on
    // commit, served without rescanning when their head matches.
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
}

impl std::fmt::Debug for Catalog {
    // `slatedb::Db` carries no `Debug` impl.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog").finish_non_exhaustive()
    }
}

impl Catalog {
    /// Opens (creating and initializing if empty) the catalog in
    /// `object_store` at `options.path`.
    ///
    /// Exactly one process may hold a read-write catalog per store —
    /// opening a second fences the first.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, is mid-migration,
    /// or is stamped with a structural format this binary does not
    /// understand.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// // Bootstrap mints the default `main` schema.
    /// assert_eq!(catalog.snapshot().await?.schemas().len(), 1);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn open(object_store: Arc<dyn ObjectStore>, options: CatalogOptions) -> Result<Self> {
        if options.multi_writer {
            return Self::open_multi_writer(object_store, options).await;
        }

        let store = StoreBuilder::new(&options.path, object_store)
            .flush_interval(options.flush_interval)
            .cache_dir(options.cache_dir.clone());
        let db = commit::open_initialized(
            store,
            options.encrypted,
            options.data_path.as_deref(),
            false,
        )
        .await?;
        info!(
            path = options.path,
            flush_interval_ms = options.flush_interval.as_millis(),
            "opened catalog read-write"
        );
        Ok(Self {
            store: Arc::new(Store::Writer(db)),
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
        })
    }

    /// The `multi_writer: true` open path: attaches directly when the store
    /// is already initialized, else bootstraps it through the writer
    /// (stamping [`commit::FORMAT_MULTI_WRITER`]) and reopens as a reader.
    /// The reader-first order means an already-initialized store is never
    /// fenced by this open.
    async fn open_multi_writer(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<Self> {
        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone());
        let reader = match commit::open_reader_initialized(reader_store).await {
            Ok(Some((reader, format_version))) => {
                commit::validate_mode(format_version, true)?;
                reader
            }
            // The store is readable but carries no format stamp: a bootstrap
            // that did not finish, which `open_initialized` completes
            // idempotently under conflict detection.
            Ok(None) => Self::bootstrap_multi_writer(&object_store, &options).await?,
            // Only a prefix holding no objects licenses a bootstrap. A prefix
            // holding objects whose manifest will not open is a damaged store,
            // and stamping a fresh catalog over it would destroy it.
            Err(err) => {
                if !prefix_is_known_empty(&object_store, &options.path).await {
                    return Err(err);
                }
                Self::bootstrap_multi_writer(&object_store, &options).await?
            }
        };

        info!(path = options.path, "opened catalog multi-writer");
        let slots = moraine_wal::SlotLog::new(object_store.clone(), &options.path);
        Ok(Self {
            store: Arc::new(Store::MultiWriter(MultiWriterStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: false,
            })),
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
        })
    }

    /// Bootstraps an uninitialized store through the writer, fencing any
    /// incumbent old-binary writer, then closes it and reopens read-only.
    async fn bootstrap_multi_writer(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<DbReader> {
        let writer_store = StoreBuilder::new(&options.path, object_store.clone())
            .flush_interval(options.flush_interval)
            .cache_dir(options.cache_dir.clone());
        let db = commit::open_initialized(
            writer_store,
            options.encrypted,
            options.data_path.as_deref(),
            true,
        )
        .await?;
        db.close().await.map_err(Error::from)?;

        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone());
        let (reader, _) = commit::open_reader_initialized(reader_store)
            .await?
            .ok_or_else(|| {
                Error::Corruption(
                    "store still uninitialized immediately after bootstrap".to_string(),
                )
            })?;

        Ok(reader)
    }

    /// Opens the catalog **read-only** in `object_store` at `options.path`,
    /// as a `DbReader` following the latest manifest.
    ///
    /// A read-only catalog never opens the writer `Db`, so it never fences a
    /// live read-write process — any number of read-only catalogs may attach
    /// alongside the one writer. It never bootstraps: opening a
    /// store no writer has initialized is refused. [`commit`](Self::commit)
    /// returns [`Error::Constraint`].
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, is not an initialized
    /// moraine catalog, or is stamped with an unknown structural format.
    pub async fn open_read_only(
        object_store: Arc<dyn ObjectStore>,
        options: CatalogOptions,
    ) -> Result<Self> {
        let store = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone());
        let opened = commit::open_reader_initialized(store).await?;
        let (reader, format_version) = opened.ok_or_else(|| {
            Error::Corruption(
                "store is not an initialized moraine catalog; a read-only attach \
                 needs a writer to have created it first"
                    .to_string(),
            )
        })?;
        info!(path = options.path, "opened catalog read-only");

        // Only a format-4 store rides the slot-log topology.
        let store = if format_version == commit::FORMAT_MULTI_WRITER {
            let slots = moraine_wal::SlotLog::new(object_store.clone(), &options.path);
            Store::MultiWriter(MultiWriterStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: true,
            })
        } else {
            Store::Reader(Arc::new(reader))
        };

        Ok(Self {
            store: Arc::new(store),
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
        })
    }

    /// The maintained-projection state shared by this handle's clones.
    pub(crate) fn projections(&self) -> &Arc<std::sync::RwLock<ProjectionCache>> {
        &self.projections
    }

    /// Whether this catalog maintains served projections: read-write only —
    /// a read-only catalog has no local commits to fold, so its dumps
    /// always scan.
    pub(crate) fn maintains_projections(&self) -> bool {
        matches!(self.store.as_ref(), Store::Writer(_))
    }

    /// The read-write writer, or [`Error::Constraint`] if the catalog was
    /// opened read-only.
    fn writer(&self) -> Result<&Db> {
        match self.store.as_ref() {
            Store::Writer(db) => Ok(db),
            Store::Reader(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
            Store::MultiWriter(_) => Err(Error::Constraint(
                "catalog attached over the slot log; commits are not available yet".to_string(),
            )),
        }
    }

    /// An immutable view of the catalog at the latest committed snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be read.
    pub async fn snapshot(&self) -> Result<CatalogSnapshot> {
        self.view(None).await
    }

    /// An immutable view of the catalog as of `snapshot` (time travel).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if `snapshot` is beyond the head, or
    /// another error if the store cannot be read.
    pub async fn snapshot_at(&self, snapshot: SnapshotId) -> Result<CatalogSnapshot> {
        self.view(Some(snapshot.get())).await
    }

    async fn view(&self, at: Option<u64>) -> Result<CatalogSnapshot> {
        if let Store::MultiWriter(multi) = self.store.as_ref() {
            return match at {
                None => {
                    let head = slot_commit::materialize_slot_head(multi).await?;
                    slot_commit::release_reader(head.reader.as_ref()).await;
                    Ok(head.view)
                }
                Some(snapshot) => slot_commit::materialize_slot_view_at(multi, snapshot).await,
            };
        }

        let session = self.begin_read().await?;
        let view = commit::materialize(session.handle(), at).await;
        session.finish();

        view
    }

    /// One head read: the view, and on a slot-backed attach the byte-level
    /// overlay of the slots no folder has applied — what a probe the projection
    /// does not model must read over the store.
    async fn head_view(&self, handle: ReadHandle<'_>) -> Result<HeadRead> {
        match self.store.as_ref() {
            Store::MultiWriter(multi) => {
                let head = slot_commit::materialize_slot_head(multi).await?;
                Ok(HeadRead {
                    view: head.view,
                    tail: Some(head.overlay),
                    reader: head.reader,
                })
            }
            Store::Writer(_) | Store::Reader(_) => Ok(HeadRead {
                view: commit::materialize(handle, None).await?,
                tail: None,
                reader: None,
            }),
        }
    }

    /// Opens a read session and materializes the head through it, so a probe's
    /// entry scans and the catalog they resolve against are one cut. Released
    /// by [`ProbeRead::finish`].
    async fn begin_probe(&self) -> Result<ProbeRead> {
        let session = self.begin_read().await?;
        match self.head_view(session.handle()).await {
            Ok(head) => Ok(ProbeRead { session, head }),
            Err(err) => {
                session.finish();
                Err(err)
            }
        }
    }

    /// Resolves an equality lookup to the rows currently holding `values`.
    ///
    /// Head-only: the lookup materializes the current head and scans the
    /// `index` subspace under one read session, so the entries and the catalog
    /// they resolve against are one consistent cut. Entries are live-only,
    /// so there is no time-travel variant. Returns candidate
    /// [`RowLocation`]s; the caller applies delete files as any DuckLake
    /// scan does.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a value exceeds the size cap, or a store
    /// error if the scan fails.
    pub async fn index_lookup(
        &self,
        table: TableId,
        index: IndexId,
        values: &[IndexKeyValue],
    ) -> Result<Vec<RowLocation>> {
        let read = self.begin_probe().await?;
        let handle = read.handle();

        let outcome = async {
            let info = read
                .view()
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }
            let key = encode_ordered_values(
                &values.iter().cloned().map(Some).collect::<Vec<_>>(),
                &info.directions,
                &info.nulls,
            )?;
            let row_ids = index_maintenance::lookup_row_ids(
                handle,
                read.tail(),
                index.get(),
                info.unique,
                &key,
            )
            .await?;
            let holders = RowHolders::of(&read.view().data_files_of(table));
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: holders.holder(row_id),
                })
                .collect())
        }
        .await;
        read.finish().await;

        outcome
    }

    /// Resolves a comparison query to the rows whose indexed value falls
    /// between `lower` and `upper` (`<`, `<=`, `>`, `>=`, `BETWEEN`, and their
    /// half-open forms via [`Bound::Unbounded`]). Each bound names the leading
    /// columns' values; equality is the degenerate closed `[v, v]` range.
    ///
    /// Head-only and candidate-returning, exactly like
    /// [`index_lookup`](Self::index_lookup): the scan and the catalog it
    /// resolves against are one consistent cut, and the caller applies delete
    /// files. Results are in the index's stored order, or its exact opposite
    /// when `reverse` is set — the reverse of the materialized result, which
    /// needs no reverse iterator.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] if its staged backfill has not completed,
    /// [`Error::Constraint`] if a bound value exceeds the size cap, or a
    /// store error if the scan fails.
    pub async fn index_range(
        &self,
        table: TableId,
        index: IndexId,
        lower: Bound<Vec<IndexKeyValue>>,
        upper: Bound<Vec<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<RowLocation>> {
        let read = self.begin_probe().await?;
        let handle = read.handle();

        let outcome = async {
            let info = read
                .view()
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }

            let (byte_lower, byte_upper) = encode_range_bounds(&info, index, lower, upper)?;

            // NULL placement is per column, and only the leading column's
            // flag byte bounds the scan's open sides.
            let leading_nulls = info.nulls.first().copied().unwrap_or(NullOrder::Last);

            let mut row_ids = index_maintenance::range_row_ids(
                handle,
                read.tail(),
                index.get(),
                info.unique,
                leading_nulls,
                byte_lower,
                byte_upper,
            )
            .await?;
            // The scan yields the index's declared order; reversing the
            // materialized result serves the exact opposite order.
            if reverse {
                row_ids.reverse();
            }
            let holders = RowHolders::of(&read.view().data_files_of(table));
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: holders.holder(row_id),
                })
                .collect())
        }
        .await;
        read.finish().await;

        outcome
    }

    /// Resolves an `IS NULL` query to the rows whose leading indexed columns
    /// match `prefix` — a leading run of `Some(value)` (equality) and `None`
    /// (`IS NULL`) predicates, e.g. `[None]` for `a IS NULL` or
    /// `[Some(5), None]` for `a = 5 AND b IS NULL`. The prefix must cover the
    /// leading columns contiguously and name at least one `IS NULL`; a gap
    /// (an unconstrained leading column) is not expressible, so a bare
    /// non-leading `IS NULL` is not served — use a scan filter for that.
    ///
    /// Head-only and candidate-returning like
    /// [`index_lookup`](Self::index_lookup).
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist,
    /// [`Error::IndexBuilding`] while its staged backfill runs, or
    /// [`Error::Constraint`] if the prefix is empty, longer than the index,
    /// or names no `IS NULL`.
    pub async fn index_nulls(
        &self,
        table: TableId,
        index: IndexId,
        prefix: Vec<Option<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<RowLocation>> {
        let read = self.begin_probe().await?;
        let handle = read.handle();

        let outcome = async {
            let info = read
                .view()
                .index_by_id(table, index)
                .ok_or_else(|| Error::NotFound(format!("index {index} on table {table}")))?;

            match info.state {
                IndexState::Ready => {}
                IndexState::Building => {
                    return Err(Error::IndexBuilding(format!(
                        "index {index} is still building"
                    )));
                }
                IndexState::Poisoned => {
                    return Err(Error::NotFound(format!("index {index} was poisoned")));
                }
            }

            if prefix.is_empty() || prefix.len() > info.columns.len() {
                return Err(Error::Constraint(format!(
                    "index_nulls: a prefix of {} predicates does not fit the {}-column index \
                     {index}",
                    prefix.len(),
                    info.columns.len()
                )));
            }
            if prefix.iter().all(Option::is_some) {
                return Err(Error::Constraint(
                    "index_nulls: the prefix names no IS NULL; use index_lookup for pure equality"
                        .to_owned(),
                ));
            }

            let key = encode_ordered_values(&prefix, &info.directions, &info.nulls)?;
            let mut row_ids =
                index_maintenance::null_prefix_row_ids(handle, read.tail(), index.get(), &key)
                    .await?;
            if reverse {
                row_ids.reverse();
            }
            let holders = RowHolders::of(&read.view().data_files_of(table));

            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: holders.holder(row_id),
                })
                .collect())
        }
        .await;
        read.finish().await;

        outcome
    }

    /// Opens a read session at the current head — a read-write transaction or
    /// the read-only reader — the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    /// Used by [`crate::ffi_support`]'s raw current+history dumps and inline
    /// scans; every other reader goes through `snapshot`/`snapshot_at`.
    pub(crate) async fn begin_read(&self) -> Result<ReadSession> {
        match self.store.as_ref() {
            Store::Writer(db) => Ok(ReadSession::Tx(
                db.begin(IsolationLevel::Snapshot)
                    .await
                    .map_err(Error::from)?,
            )),
            Store::Reader(reader) => Ok(ReadSession::Reader(reader.clone())),
            Store::MultiWriter(multi) => Ok(ReadSession::Reader(multi.reader.clone())),
        }
    }

    /// Opens a read-write transaction for the staged-row commit path. Fails
    /// with [`Error::Constraint`] on a read-only catalog.
    pub(crate) async fn begin_write_tx(&self) -> Result<DbTransaction> {
        self.writer()?
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)
    }

    /// Derives the index entries for a file the extension path registers, by
    /// scoped-reading it — DuckLake supplies none, so moraine reads them.
    /// The caller resolves each of the index's columns to its physical
    /// position in the file (through the column-mapping rules) and passes
    /// them in the index's column order. The returned entries feed
    /// [`Transaction::register_data_file`] so registration stays covered.
    ///
    /// The file must not carry an embedded row-id column — its rows already
    /// have ids, and re-registering them under a fresh dense range would
    /// fork their identity — so such a file is refused.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if the file cannot be read or a column
    /// type does not match its Parquet type, or [`Error::Constraint`] for a
    /// non-indexable column type or a file carrying an embedded row-id
    /// column.
    pub async fn scoped_file_index_entries(
        &self,
        object_store: Arc<dyn ObjectStore>,
        path: &Path,
        index: IndexId,
        indexed_positions: &[usize],
    ) -> Result<Vec<FileIndexEntry>> {
        let entries = scoped_read::scoped_read_entries(
            object_store,
            path,
            indexed_positions,
            scoped_read::RowIdSource::Ordinal,
            None,
        )
        .await?;
        Ok(entries
            .into_iter()
            .map(|entry| FileIndexEntry {
                index,
                // Ordinal-sourced ids are positions the registration
                // re-maps onto its freshly allocated dense range.
                ordinal: entry.row_id,
                values: entry.values,
            })
            .collect())
    }

    /// Backfills an index over a table's live data by scoped-reading every
    /// live file from `object_store` (the `DATA_PATH` store) and deriving one
    /// entry per row — the extension-path build for a table that already
    /// holds data. The returned entries feed `create_index`'s backfill.
    /// Indexed columns are located by resolving each field id to its physical
    /// position (the file's columns follow the table's column order).
    ///
    /// Row ids resolve per file: the embedded row-id column when the file
    /// carries one (rewrite and flush output), else `row_id_start +
    /// ordinal`. Rows already dead — named by a delete file's positions or an
    /// inline file-delete's row ids — are excluded, so entries stay live-only.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or a column is not live,
    /// [`Error::Constraint`] for a non-indexable type, or
    /// [`Error::Corruption`] if a file cannot be read or names no row-id
    /// source.
    pub async fn scoped_backfill_entries(
        &self,
        object_store: Arc<dyn ObjectStore>,
        data_prefix: &str,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let session = self.begin_read().await?;

        let outcome = async {
            let head = self.head_view(session.handle()).await?;
            slot_commit::release_reader(head.reader.as_ref()).await;
            let snapshot = head.view;
            // `columns_of` is ordered by the column's ordinal, so a column's
            // 0-based index here is its physical position in a file written
            // under this schema — the mapping the scoped read needs. (Ordinals
            // are 1-based in the stored value, so the stored order can't be
            // used directly.)
            let live_columns = snapshot.columns_of(table);
            let positions = columns
                .iter()
                .map(|column| {
                    live_columns
                        .iter()
                        .position(|c| c.id == *column)
                        .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
                })
                .collect::<Result<Vec<_>>>()?;

            let table_prefix = snapshot.table_data_prefix(table)?;
            let resolve = |path: &str, is_relative: bool| {
                let relative = match (is_relative, data_prefix.is_empty()) {
                    (false, _) => path.to_owned(),
                    (true, true) => format!("{table_prefix}{path}"),
                    (true, false) => format!("{data_prefix}/{table_prefix}{path}"),
                };
                object_store::path::Path::from(relative.as_str())
            };

            // Rows already dead when the index is built must not be backfilled
            // (entries are live-only): delete files name positions within their
            // target, inline file-deletes name row ids.
            let mut killed_positions: HashMap<u64, HashSet<u64>> = HashMap::new();
            let mut killed_row_ids: HashMap<u64, HashSet<u64>> = HashMap::new();
            for (data_file_id, row_id, _) in
                store_inline::scan_inline_file_deletes(session.handle(), table.get()).await?
            {
                killed_row_ids
                    .entry(data_file_id)
                    .or_default()
                    .insert(row_id);
            }
            for delete in snapshot.delete_files_of(table) {
                let path = resolve(&delete.path, delete.path_is_relative);
                let positions =
                    scoped_read::delete_file_positions(object_store.as_ref(), &path).await?;
                killed_positions
                    .entry(delete.data_file_id.get())
                    .or_default()
                    .extend(positions);
            }

            let mut entries = Vec::new();
            for file in snapshot.data_files_of(table) {
                let path = resolve(&file.path, file.path_is_relative);
                let scoped = scoped_read::scoped_read_entries(
                    Arc::clone(&object_store),
                    &path,
                    &positions,
                    scoped_read::RowIdSource::Resolve {
                        row_id_start: file.row_id_start,
                    },
                    Some(file.file_size_bytes),
                )
                .await?;
                let dead_positions = killed_positions.get(&file.id.get());
                let dead_row_ids = killed_row_ids.get(&file.id.get());
                entries.extend(
                    scoped
                        .into_iter()
                        .enumerate()
                        .filter_map(|(ordinal, entry)| {
                            let ordinal = u64::try_from(ordinal).unwrap_or(u64::MAX);
                            let dead = dead_positions.is_some_and(|dead| dead.contains(&ordinal))
                                || dead_row_ids.is_some_and(|dead| dead.contains(&entry.row_id));
                            (!dead).then_some(IndexEntry {
                                row_id: entry.row_id,
                                values: entry.values,
                            })
                        }),
                );
            }
            Ok(entries)
        }
        .await;
        session.finish();

        outcome
    }

    /// Creates an index by a staged (multi-commit) build, driving it to
    /// `ready` before returning — for a table whose backfill exceeds what
    /// one commit may stage.
    ///
    /// The definition lands `building` in its own commit; each pass then
    /// derives the table's live entries (external files through
    /// `data_store`, inline rows from the catalog store), orders them by row
    /// id, and commits them in steps of `step_entries`, defaulting to a
    /// million. Writers maintain entries from the first commit forward.
    ///
    /// Interrupting the call leaves the definition `building`: calling again
    /// with the same `def` resumes from the persisted cursor, and
    /// [`Transaction::drop_index`](crate::Transaction::drop_index) abandons
    /// the build. A concurrent write to the table conflicts with a step,
    /// which re-derives at a fresh snapshot rather than staging entries for
    /// rows the winner deleted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if the table already holds a ready
    /// index of this name, or [`Error::Constraint`] if `step_entries` is
    /// zero, the resumed definition differs from `def`, or the rows
    /// duplicate a unique value. A failed build drops its definition.
    pub async fn create_index_staged(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step_entries: Option<usize>,
    ) -> Result<IndexId> {
        let step_entries = step_entries.unwrap_or(BUILD_STEP_ENTRIES);
        if step_entries == 0 {
            return Err(Error::Constraint(
                "a staged build's step size must be at least one entry".to_owned(),
            ));
        }

        let index = self.begin_staged_index(table, def, orders).await?;
        let outcome = self
            .drive_staged_build(table, def, index, data_store, data_prefix, step_entries)
            .await;

        // A build that cannot finish leaves no half-covered index behind.
        // A cleanup that itself fails is logged, never substituted for the
        // failure that caused it.
        if outcome.is_err()
            && let Err(cleanup) = self.commit(|tx| tx.drop_index(index)).await
        {
            warn!(
                index = index.get(),
                error = %cleanup,
                "could not drop the definition of a failed staged build"
            );
        }
        outcome.map(|()| index)
    }

    /// Commits the `building` definition, or adopts the one already there.
    /// A ready definition of the same name belongs to a finished index.
    async fn begin_staged_index(
        &self,
        table: TableId,
        def: &IndexDef,
        orders: &[ColumnOrder],
    ) -> Result<IndexId> {
        if let Some(existing) = self.snapshot().await?.index_by_name(table, &def.name) {
            return match existing.state {
                IndexState::Ready => Err(Error::AlreadyExists(format!(
                    "index {} on table {table}",
                    def.name
                ))),
                IndexState::Building | IndexState::Poisoned => {
                    // Resuming adopts the stored definition, whose entries
                    // are encoded under its own orders.
                    let (directions, nulls) = requested_orders(orders, def.columns.len());
                    if existing.columns != def.columns
                        || existing.unique != def.unique
                        || existing.directions != directions
                        || existing.nulls != nulls
                    {
                        return Err(Error::Constraint(format!(
                            "index {} on table {table} is already building over a different \
                             definition; drop it to rebuild",
                            def.name
                        )));
                    }
                    Ok(existing.id)
                }
            };
        }

        let index = std::cell::Cell::new(None);
        self.commit(|tx| {
            let id = tx.create_index_staged_ordered(table, def, orders)?;
            index.set(Some(id));
            Ok(())
        })
        .await?;

        index
            .get()
            .ok_or_else(|| Error::Corruption("staged create returned no index id".to_owned()))
    }

    /// Derives the live backfill and commits it in bounded steps until the
    /// index is ready, re-deriving at a fresh snapshot after a lost race.
    async fn drive_staged_build(
        &self,
        table: TableId,
        def: &IndexDef,
        index: IndexId,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: &str,
        step_entries: usize,
    ) -> Result<()> {
        for _ in 0..BUILD_DERIVATION_ATTEMPTS {
            let mut entries = match &data_store {
                Some(store) => {
                    self.scoped_backfill_entries(
                        Arc::clone(store),
                        data_prefix,
                        table,
                        &def.columns,
                    )
                    .await?
                }
                None => Vec::new(),
            };
            entries.extend(self.inline_backfill_entries(table, &def.columns).await?);
            // One watermark can describe the covered set only in row-id
            // order, which per-row-id rewrite files would otherwise break.
            entries.sort_unstable_by_key(|entry| entry.row_id);

            if let BuildProgress::Ready = self
                .commit_build_steps(table, index, &entries, step_entries)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::CommitConflict(format!(
            "staged build of index {index} lost its race {BUILD_DERIVATION_ATTEMPTS} times; \
             the table is under concurrent write"
        )))
    }

    /// Commits `entries` above the persisted cursor in steps, the last one
    /// flipping the index ready.
    async fn commit_build_steps(
        &self,
        table: TableId,
        index: IndexId,
        entries: &[IndexEntry],
        step_entries: usize,
    ) -> Result<BuildProgress> {
        loop {
            let cursor = self.staged_build_cursor(table, index).await?;
            // The cursor is the highest row id covered; absent means none
            // is, so row id 0 is still pending.
            let pending = match cursor {
                Some(covered) => entries.partition_point(|entry| entry.row_id <= covered),
                None => 0,
            };
            let remaining = &entries[pending..];
            let step = &remaining[..remaining.len().min(step_entries)];
            let is_final = step.len() == remaining.len();

            match self
                .commit(|tx| tx.build_index_step(index, step, is_final).map(|_| ()))
                .await
            {
                Ok(_) => {
                    if is_final {
                        return Ok(BuildProgress::Ready);
                    }
                }
                Err(Error::CommitConflict(_)) => return Ok(BuildProgress::Conflicted),
                Err(other) => return Err(other),
            }
        }
    }

    /// The staged build's persisted watermark. An index that is no longer
    /// building is refused.
    async fn staged_build_cursor(&self, table: TableId, index: IndexId) -> Result<Option<u64>> {
        let info = self
            .snapshot()
            .await?
            .indexes_of(table)
            .into_iter()
            .find(|info| info.id == index)
            .ok_or_else(|| Error::NotFound(format!("index {index}")))?;

        match info.state {
            IndexState::Building => Ok(info.build_cursor),
            IndexState::Ready => Err(Error::Constraint(format!(
                "index {index} finished building under this build"
            ))),
            IndexState::Poisoned => Err(Error::Constraint(format!(
                "index {index} was poisoned by a duplicate value"
            ))),
        }
    }

    /// Backfill entries for a table's live **inline** rows, by scanning its
    /// inline chunks — the counterpart to [`Self::scoped_backfill_entries`]
    /// for rows moraine holds in the store rather than external files.
    /// Tombstoned (inline-deleted) rows are excluded; a NULL indexed value
    /// yields a `None`, so `IS NULL` finds the row. Reads the catalog store,
    /// so it needs no data object store.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if a column is not live, or [`Error::Corruption`]
    /// if a chunk names no recorded schema or cannot be decoded.
    pub async fn inline_backfill_entries(
        &self,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let session = self.begin_read().await?;

        let outcome = async {
            let head = self.head_view(session.handle()).await?;
            slot_commit::release_reader(head.reader.as_ref()).await;
            let snapshot = head.view;
            let live_columns = snapshot.columns_of(table);
            let positions = columns
                .iter()
                .map(|column| {
                    live_columns
                        .iter()
                        .position(|c| c.id == *column)
                        .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
                })
                .collect::<Result<Vec<_>>>()?;

            // Rows tombstoned out of their chunk by an inline delete are dead
            // and must not be indexed.
            let dead: std::collections::HashSet<u64> =
                store_inline::scan_inline_inline_deletes(session.handle(), table.get())
                    .await?
                    .into_iter()
                    .map(|(row_id, _)| row_id)
                    .collect();

            let mut entries = Vec::new();
            for (op, chunk) in
                store_inline::scan_inline_chunks(session.handle(), table.get()).await?
            {
                let InlineOperation::Insert { schema_version, .. } = op else {
                    continue;
                };
                let schema =
                    store_inline::read_inline_schema(session.handle(), table.get(), schema_version)
                        .await?
                        .ok_or_else(|| {
                            Error::Corruption(format!(
                                "no inline schema for table {table} version {schema_version}"
                            ))
                        })?;
                let scoped = scoped_read::inline_batch_entries(
                    &schema.arrow_schema,
                    &chunk.body,
                    &positions,
                    chunk.row_id_start,
                )?;
                entries.extend(
                    scoped
                        .into_iter()
                        .filter(|entry| !dead.contains(&entry.row_id))
                        .map(|entry| IndexEntry {
                            row_id: entry.row_id,
                            values: entry.values,
                        }),
                );
            }
            Ok(entries)
        }
        .await;
        session.finish();

        outcome
    }

    /// Deletes up to `limit` orphaned entries of a dropped index, in one
    /// bounded batch outside the commit protocol (entries are not catalog
    /// entities, and the dropping commit's batch must stay bounded). Returns
    /// the number deleted; a host loops until it returns 0. Index ids are
    /// never reused, so a concurrent create cannot collide with a sweep.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the index is still live (reclaiming
    /// a live index's entries would corrupt it), or a store error.
    pub async fn reclaim_index_entries(&self, index: IndexId, limit: usize) -> Result<usize> {
        let head = self.snapshot().await?;
        if head
            .indexes
            .values()
            .any(|per_table| per_table.contains_key(&index.get()))
        {
            return Err(Error::Constraint(format!(
                "index {index} is still live; drop it before reclaiming its entries"
            )));
        }

        let tx = self.begin_write_tx().await?;
        let deleted = index_maintenance::reclaim_entries(&tx, index.get(), limit).await?;
        tx.commit_with_options(&commit::durable())
            .await
            .map_err(Error::from)?;

        Ok(deleted)
    }

    /// Runs one maintenance pass, reclaiming what only moraine knows is
    /// dead, and reports what it did.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only catalog,
    /// [`Error::Configuration`] for a zero `batch_size`, or a store error.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, MaintenanceRequest};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// // A fresh catalog has nothing to reclaim.
    /// let report = catalog.maintain(MaintenanceRequest::default()).await?;
    /// assert_eq!(report.index_entries_reclaimed, 0);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn maintain(&self, request: MaintenanceRequest) -> Result<MaintenanceReport> {
        // Refuse before doing anything, including before the
        // nothing-to-do shortcut: a pass that reclaims nothing is still a
        // pass, and answering it differently on a read-only catalog would
        // make the outcome depend on the request rather than the handle.
        self.writer()?;
        if request.batch_size == 0 {
            return Err(Error::Configuration(
                "batch_size must be nonzero; zero would reclaim nothing and never terminate"
                    .to_string(),
            ));
        }

        let mut report = MaintenanceReport::default();
        if !request.sweep_orphaned_index_entries {
            return Ok(report);
        }

        // Index ids come from the monotonic catalog-id counter and are
        // never reused, so an id absent from this view can never become
        // live again: deciding liveness once, here, is sound for the
        // whole pass however long it runs.
        let live: HashSet<u64> = self
            .snapshot()
            .await?
            .indexes
            .values()
            .flat_map(|per_table| per_table.keys().copied())
            .collect();

        for kind in [IndexKind::Unique, IndexKind::Multi] {
            let mut from = 0u64;
            while let Some(index_id) = self.first_index_id_from(kind, from).await? {
                if !live.contains(&index_id) {
                    let reclaimed = self
                        .reclaim_dead_range(kind, index_id, request.batch_size)
                        .await?;
                    if reclaimed > 0 {
                        report.indexes_swept += 1;
                        report.index_entries_reclaimed += reclaimed;
                    }
                }
                // Seek past this index rather than walking its entries.
                match index_id.checked_add(1) {
                    Some(next) => from = next,
                    None => break,
                }
            }
        }

        Ok(report)
    }

    /// The lowest index id at or after `from` holding an entry of `kind`,
    /// or `None` past the last one. One seek per distinct index present —
    /// the scan stops at the first key rather than walking the range.
    pub(crate) async fn first_index_id_from(
        &self,
        kind: IndexKind,
        from: u64,
    ) -> Result<Option<u64>> {
        let kind_prefix = index_kind_prefix(kind);
        let start = index_index_prefix(kind, from);
        // `scan_prefix` takes its bounds as a suffix of the prefix.
        let suffix = start[kind_prefix.len()..].to_vec();

        let session = self.begin_read().await?;
        let first = session
            .handle()
            .scan_prefix(kind_prefix, suffix..)
            .await
            .map_err(Error::from)?
            .next()
            .await
            .map_err(Error::from)?;
        session.finish();

        let Some(entry) = first else {
            return Ok(None);
        };
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Unique { index_id, .. } | IndexKey::Multi { index_id, .. }) => {
                Ok(Some(index_id))
            }
            other => Err(Error::Corruption(format!(
                "key in the index subspace decoded as {other:?}"
            ))),
        }
    }

    /// Deletes every entry of one dead index, `batch_size` per commit,
    /// returning the total. The caller has already established that the
    /// index is not live.
    async fn reclaim_dead_range(
        &self,
        kind: IndexKind,
        index_id: u64,
        batch_size: usize,
    ) -> Result<u64> {
        let mut total = 0u64;
        // Each batch resumes where the last one stopped. Restarting at
        // the range's beginning would make every batch step over the
        // tombstones its predecessors left, which is quadratic in the
        // size of the range.
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let tx = self.begin_write_tx().await?;
            let (deleted, last) = index_maintenance::reclaim_entries_from(
                &tx,
                kind,
                index_id,
                batch_size,
                cursor.as_deref(),
            )
            .await?;
            if deleted == 0 {
                tx.rollback();
                return Ok(total);
            }
            tx.commit_with_options(&commit::durable())
                .await
                .map_err(Error::from)?;
            total += deleted as u64;
            cursor = last;
        }
    }

    /// Closes the catalog, flushing background work.
    ///
    /// A [`Catalog`] is cheaply cloneable, and all clones share one
    /// underlying store handle: closing through any clone shuts that
    /// store down for every clone, so subsequent operations on any of
    /// them — this one included — fail.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails to close cleanly.
    pub async fn close(&self) -> Result<()> {
        match self.store.as_ref() {
            Store::Writer(db) => db.close().await.map_err(Error::from),
            Store::Reader(reader) => reader.close().await.map_err(Error::from),
            Store::MultiWriter(multi) => multi.reader.close().await.map_err(Error::from),
        }
    }

    /// Commits catalog mutations atomically, producing one new snapshot.
    ///
    /// The closure stages mutations on the [`Transaction`]; reads on the
    /// `Transaction` observe its own staged state. It may be re-run against
    /// fresh state after a lost race with a concurrent commit, so it must
    /// be pure: no I/O, no effects other than the `Transaction` calls. A
    /// closure that stages nothing commits nothing and returns the
    /// unchanged head snapshot id.
    ///
    /// # Errors
    ///
    /// Returns whatever error the closure returns (the commit is
    /// aborted), or an error from the underlying store. Returns
    /// [`Error::CommitConflict`] when a concurrent commit truly conflicts
    /// — it touched the same tables or the schema list. Returns
    /// [`Error::RetryBudgetExhausted`] when the bounded internal retry
    /// budget runs out before a benign race resolves; unlike a conflict,
    /// that is terminal, and the caller re-drives the work itself —
    /// usually as smaller commits.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, ColumnDef};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// # let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// let snapshot = catalog
    ///     .commit(|tx| {
    ///         let sales = tx.create_schema("sales")?;
    ///         tx.create_table(
    ///             sales,
    ///             "orders",
    ///             &[ColumnDef {
    ///                 name: "id".into(),
    ///                 column_type: "BIGINT".into(),
    ///                 nulls_allowed: false,
    ///                 default_value: None,
    ///             }],
    ///         )?;
    ///         Ok(())
    ///     })
    ///     .await?;
    /// // `main` plus the newly created `sales` schema.
    /// assert_eq!(catalog.snapshot_at(snapshot).await?.schemas().len(), 2);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn commit<F>(&self, f: F) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        commit::commit_cycle(self.writer()?, &f, &self.projections).await
    }
}

/// Maps a value window onto the byte bounds one index scan answers, refusing
/// the shapes no single scan can.
///
/// Each bound names a leading run of the index's columns. The last named is
/// the range column; its direction decides whether value order runs with or
/// against byte order.
fn encode_range_bounds(
    info: &IndexInfo,
    index: IndexId,
    lower: Bound<Vec<IndexKeyValue>>,
    upper: Bound<Vec<IndexKeyValue>>,
) -> Result<(Bound<CanonicalKey>, Bound<CanonicalKey>)> {
    // How many columns a bound names; `None` when it names none at all.
    let named_len = |bound: &Bound<Vec<IndexKeyValue>>| match bound {
        Bound::Included(values) | Bound::Excluded(values) => Some(values.len()),
        Bound::Unbounded => None,
    };
    let (lower_len, upper_len) = (named_len(&lower), named_len(&upper));

    // A bound naming no column describes no window: its encoding is the empty
    // key, whose extension bound spans the whole index.
    if lower_len == Some(0) || upper_len == Some(0) {
        return Err(Error::Constraint(format!(
            "index_range: a bound of index {index} must name at least one column"
        )));
    }

    // A bound naming more columns than the index has would encode components
    // no stored key carries, silently returning the wrong rows.
    let widest = lower_len.unwrap_or(0).max(upper_len.unwrap_or(0));
    if widest > info.columns.len() {
        return Err(Error::Constraint(format!(
            "index_range: a bound of {widest} values does not fit the {}-column index {index}",
            info.columns.len()
        )));
    }

    // A tuple window needs one byte range. Columns sharing a direction give
    // one; mixed directions do not, so both bounds must name the same leading
    // values and differ only in the last.
    let range_column = widest.saturating_sub(1);
    let mixed_directions = info
        .directions
        .iter()
        .take(widest)
        .any(|direction| Some(direction) != info.directions.first());
    let pinned_prefix = match (&lower, &upper) {
        (
            Bound::Included(low) | Bound::Excluded(low),
            Bound::Included(high) | Bound::Excluded(high),
        ) => low.len() == high.len() && low[..range_column] == high[..range_column],
        _ => false,
    };
    if mixed_directions && !pinned_prefix {
        return Err(Error::Constraint(format!(
            "index_range: index {index} names columns of differing sort directions, so both \
             bounds must pin the same leading values and compare on the last column"
        )));
    }

    let encode_bound = |bound: Bound<Vec<IndexKeyValue>>| -> Result<Bound<_>> {
        let encode = |values: Vec<IndexKeyValue>| {
            encode_ordered_values(
                &values.into_iter().map(Some).collect::<Vec<_>>(),
                &info.directions,
                &info.nulls,
            )
        };
        Ok(match bound {
            Bound::Included(values) => Bound::Included(encode(values)?),
            Bound::Excluded(values) => Bound::Excluded(encode(values)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    };

    // A descending range column reverses value order, so the value-lower
    // bound is the byte-upper bound and vice versa.
    let descending = info.directions.get(range_column).copied() == Some(Direction::Descending);
    let (byte_lower, byte_upper) = if descending {
        (upper, lower)
    } else {
        (lower, upper)
    };
    Ok((encode_bound(byte_lower)?, encode_bound(byte_upper)?))
}

/// A table's dense row-id ranges, ordered for lookup. A query resolves every
/// hit against the same table, so the ranges are built once and searched per
/// row rather than rescanned; an index range can return far more rows than a
/// table has files.
struct RowHolders {
    /// `(start, end, file)` per file carrying a dense range, sorted by start
    /// and disjoint — each row id belongs to at most one file.
    ranges: Vec<(u64, u64, DataFileId)>,
}

impl RowHolders {
    fn of(files: &[DataFileInfo]) -> Self {
        let mut ranges: Vec<_> = files
            .iter()
            .filter_map(|file| {
                let start = file.row_id_start?;
                Some((start, start.saturating_add(file.record_count), file.id))
            })
            .collect();
        ranges.sort_unstable();
        Self { ranges }
    }

    /// The file whose range holds `row_id`, else `Inline` — an inlined row,
    /// or one in a file carrying explicit per-row ids rather than a range.
    fn holder(&self, row_id: u64) -> RowHolder {
        // Only the last range starting at or below `row_id` can contain it.
        let above = self.ranges.partition_point(|(start, ..)| *start <= row_id);
        match self.ranges[..above].last() {
            Some(&(_, end, file)) if row_id < end => RowHolder::DataFile(file),
            _ => RowHolder::Inline,
        }
    }
}
