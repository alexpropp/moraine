//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use object_store::{ObjectStore, path::Path};
use slatedb::{Db, DbReader, IsolationLevel};
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
    transaction::{Transaction, commit, folder, index_maintenance, slot_commit},
};

/// How many entries one staged build step commits. At roughly a kilobyte
/// of write-path memory apiece, a step peaks near a gigabyte.
const BUILD_STEP_ENTRIES: usize = 1_000_000;

/// How many times a staged build re-derives after losing a race before
/// giving up.
const BUILD_DERIVATION_ATTEMPTS: usize = 8;

/// How many times a read-write open re-probes after a racing migration fences
/// its own migration write. Each retry re-reads a store one competitor has by
/// then converted, so a small bound suffices.
const MIGRATION_ATTEMPTS: usize = 8;

/// A probe reader's stored structural format, as a read-write open classifies
/// it.
enum FormatClass {
    /// No format stamp: an empty or unfinished store, to bootstrap.
    Empty,
    /// The slot-log format this binary attaches directly.
    SlotLog,
    /// A legacy format 1–3 store, to migrate to the slot-log format.
    Legacy,
    /// A format newer than this binary understands, to refuse.
    TooNew(u64),
}

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

/// Encodes a staged build's entry batch into store-ready staged entries under
/// the index's declared orders, marked building so a duplicate poisons the
/// definition rather than failing the commit.
fn build_staged_entries(
    info: &IndexInfo,
    index: IndexId,
    batch: &[IndexEntry],
) -> Result<Vec<crate::transaction::index_maintenance::StagedIndexEntry>> {
    batch
        .iter()
        .map(|entry| {
            let has_null = entry.values.iter().any(Option::is_none);
            let key = encode_ordered_values(&entry.values, &info.directions, &info.nulls)?;
            Ok(crate::transaction::index_maintenance::StagedIndexEntry {
                index_id: index.get(),
                unique: info.unique && !has_null,
                key,
                row_id: entry.row_id,
                delete: false,
                building: true,
            })
        })
        .collect()
}

/// Derives index-backfill entries for a table's live inline rows, scanning the
/// inline chunk bodies through `handle` overlaid with `overlay` — the unfolded
/// tail on a slot-backed store — so rows in slots no folder has applied are
/// covered. Inline-tombstoned rows are excluded.
async fn inline_backfill_from(
    handle: ReadHandle<'_>,
    overlay: Option<&moraine_wal::Overlay>,
    snapshot: &CatalogSnapshot,
    table: TableId,
    columns: &[ColumnId],
) -> Result<Vec<IndexEntry>> {
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

    // Rows tombstoned out of their chunk by an inline delete are dead and must
    // not be indexed.
    let dead: HashSet<u64> = store_inline::scan_inline_inline_deletes(handle, overlay, table.get())
        .await?
        .into_iter()
        .map(|(row_id, _)| row_id)
        .collect();

    let mut entries = Vec::new();
    for (op, chunk) in store_inline::scan_inline_chunks(handle, overlay, table.get()).await? {
        let InlineOperation::Insert { schema_version, .. } = op else {
            continue;
        };
        let schema = store_inline::read_inline_schema(handle, overlay, table.get(), schema_version)
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

/// A raw dump's read: the scan handle it reads through and, on a slot-backed
/// attach, the unfolded tail to overlay so a winner no folder has applied yet
/// is not missed. Released by [`finish`](DumpRead::finish).
pub(crate) enum DumpRead {
    /// A pinned slot-log head: scans read through `reader` (or the one the
    /// head opened past a truncation) overlaid with the tail.
    Slots {
        reader: Arc<DbReader>,
        head: Box<crate::transaction::slot_commit::SlotHead>,
    },
}

impl DumpRead {
    /// The handle raw scans read through.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        let Self::Slots { reader, head } = self;
        head.handle(ReadHandle::Reader(reader))
    }

    /// The unfolded tail to overlay, `Some` for the overlay-accepting scan
    /// functions this feeds.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn overlay(&self) -> Option<&moraine_wal::Overlay> {
        let Self::Slots { head, .. } = self;
        Some(&head.overlay)
    }

    /// The head snapshot id, `Some` for the projection callers that gate on a
    /// known head.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn head_id(&self) -> Option<u64> {
        let Self::Slots { head, .. } = self;
        Some(head.view.snapshot.snapshot_id)
    }

    /// Releases the reader a hole retry opened.
    pub(crate) async fn finish(self) {
        let Self::Slots { head, .. } = self;
        slot_commit::release_reader(head.reader.as_ref()).await;
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

/// The open store behind a catalog. Every attach — read-write or read-only —
/// builds [`Store::Slots`].
enum Store {
    /// The slot-log-backed store: a reader following the folded store plus the
    /// slot log its commits ride. Boxed to keep the enum's footprint down.
    Slots(Box<SlotStore>),
}

/// The slot-log-backed store: a reader following the folded store plus the slot
/// log its commits ride. A read-only attach never opens the fenced writer, so
/// it never fences a live writer; a read-write attach opens it only for
/// folder-role work.
pub(crate) struct SlotStore {
    pub(crate) reader: Arc<DbReader>,
    pub(crate) slots: moraine_wal::SlotLog,
    /// Retained for reopening the fenced writer (folder role) and fresh
    /// readers (past a truncation).
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: CatalogOptions,
    /// Whether this attach may write: `false` refuses commits and folder-role
    /// work, so a read-only attach never opens the writer.
    pub(crate) read_only: bool,
    /// Serializes and coalesces this process's slot commits. Shared by every
    /// clone of the handle.
    pub(crate) coalescer: slot_commit::CommitCoalescer,
}

/// Options for opening a catalog.
///
/// # Examples
///
/// ```
/// let options = moraine::CatalogOptions::default();
/// assert_eq!(options.refresh_interval, std::time::Duration::from_secs(10));
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
    /// How long a commit may wait to be batched with others of this process
    /// before racing the log. Zero (the default) still coalesces whatever is
    /// already queued into one envelope — it only declines to *wait* — so an
    /// uncontended commit adds no latency. A non-zero window trades latency
    /// for fewer object-store PUTs under load, the write-side cost axis.
    pub commit_batch_window: Duration,
    /// How often a reader polls the object store for a fresher folded view.
    /// A reader lags the head by up to this interval, replaying that many more
    /// slots per materialization; a shorter interval trades more manifest
    /// reads for less fold lag, the read-side cost axis. Defaults to 10s.
    pub refresh_interval: Duration,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            path: String::new(),
            encrypted: false,
            cache_dir: None,
            data_path: None,
            commit_batch_window: Duration::ZERO,
            refresh_interval: Duration::from_secs(10),
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
        let reader = Self::open_read_write_reader(&object_store, &options).await?;
        info!(
            path = options.path,
            refresh_interval_ms = options.refresh_interval.as_millis(),
            "opened catalog read-write"
        );

        let slots = moraine_wal::SlotLog::new(object_store.clone(), &options.path);
        let coalescer = slot_commit::CommitCoalescer::new(options.commit_batch_window);
        Ok(Self {
            store: Arc::new(Store::Slots(Box::new(SlotStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: false,
                coalescer,
            }))),
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
        })
    }

    /// Resolves a read-write attach to the reader it follows: bootstraps an
    /// empty store at [`commit::FORMAT_MULTI_WRITER`], attaches a store already
    /// there, migrates a legacy format 1–3 store in one atomic batch, and
    /// refuses a format newer than this binary understands. A migration whose
    /// write a racing migration fenced re-probes and finds the store converted.
    async fn open_read_write_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<DbReader> {
        for _ in 0..MIGRATION_ATTEMPTS {
            let Some(reader) = Self::open_probe_reader(object_store, options).await? else {
                return Self::bootstrap_slot_reader(object_store, options).await;
            };
            match Self::classify_format(&reader).await? {
                FormatClass::SlotLog => return Ok(reader),
                FormatClass::Empty => {
                    reader.close().await.map_err(Error::from)?;
                    return Self::bootstrap_slot_reader(object_store, options).await;
                }
                FormatClass::TooNew(version) => {
                    reader.close().await.map_err(Error::from)?;
                    return Err(Error::Configuration(format!(
                        "store format {version} is newer than this binary understands \
                         (up to {}); upgrade moraine to attach it",
                        commit::MAX_FORMAT_VERSION
                    )));
                }
                FormatClass::Legacy => {
                    reader.close().await.map_err(Error::from)?;
                    let writer_store = StoreBuilder::new(&options.path, object_store.clone())
                        .cache_dir(options.cache_dir.clone());
                    match commit::migrate_to_slot_log(writer_store).await {
                        // Converted, or a racing migration converted it first:
                        // fall through to re-probe, which finds the slot format.
                        Ok(()) | Err(Error::Fenced(_)) => {}
                        Err(err) => return Err(err),
                    }
                }
            }
        }
        Err(Error::Fenced(
            "a legacy store's migration was fenced repeatedly and never converged; \
             retry the attach"
                .to_string(),
        ))
    }

    /// Opens a probe reader over the store: `Some(reader)` when the prefix is
    /// readable, `None` when it is a known-empty prefix that licenses a
    /// bootstrap. A prefix holding objects whose manifest will not open is a
    /// damaged store, and the error propagates rather than stamping over it.
    async fn open_probe_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<Option<DbReader>> {
        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .refresh_interval(options.refresh_interval)
            .cache_dir(options.cache_dir.clone());
        match reader_store.open_reader().await {
            Ok(reader) => Ok(Some(reader)),
            Err(err) => {
                if prefix_is_known_empty(object_store, &options.path).await {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Classifies a probe reader's stored structural format.
    async fn classify_format(reader: &DbReader) -> Result<FormatClass> {
        let handle = ReadHandle::Reader(reader);
        if crate::store::read::read_migration(handle).await?.is_some() {
            return Err(Error::Corruption(
                "store is mid-migration; refusing to open".to_string(),
            ));
        }
        match crate::store::read::read_format(handle).await? {
            None => Ok(FormatClass::Empty),
            Some(format) if format.format_version == commit::FORMAT_MULTI_WRITER => {
                Ok(FormatClass::SlotLog)
            }
            Some(format) if format.format_version > commit::MAX_FORMAT_VERSION => {
                Ok(FormatClass::TooNew(format.format_version))
            }
            Some(_) => Ok(FormatClass::Legacy),
        }
    }

    /// Bootstraps an empty store at the slot-log format through the writer,
    /// fencing any incumbent old-binary writer, then closes it and reopens
    /// read-only.
    async fn bootstrap_slot_reader(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
    ) -> Result<DbReader> {
        let writer_store = StoreBuilder::new(&options.path, object_store.clone())
            .cache_dir(options.cache_dir.clone());
        let db = commit::open_initialized(
            writer_store,
            options.encrypted,
            options.data_path.as_deref(),
        )
        .await?;
        db.close().await.map_err(Error::from)?;

        let reader_store = StoreBuilder::new(&options.path, object_store.clone())
            .refresh_interval(options.refresh_interval)
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
            .refresh_interval(options.refresh_interval)
            .cache_dir(options.cache_dir.clone());
        // A store with no manifest fails to open at all; the failure propagates
        // (a read-only attach never bootstraps).
        let reader = store.open_reader().await?;
        match Self::classify_format(&reader).await {
            // Format 1–4 all serve read-only.
            Ok(FormatClass::SlotLog | FormatClass::Legacy) => {}
            Ok(FormatClass::Empty) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(Error::Corruption(
                    "store is not an initialized moraine catalog; a read-only attach \
                     needs a writer to have created it first"
                        .to_string(),
                ));
            }
            // A too-new format is a compatibility problem, not corruption — the
            // same kind the read-write path refuses it with.
            Ok(FormatClass::TooNew(version)) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(Error::Configuration(format!(
                    "store format {version} is newer than this binary understands \
                     (up to {}); upgrade moraine to attach it",
                    commit::MAX_FORMAT_VERSION
                )));
            }
            Err(err) => {
                slot_commit::release_reader(Some(&reader)).await;
                return Err(err);
            }
        }
        info!(path = options.path, "opened catalog read-only");

        // Every readable store serves read-only through the slot topology: a
        // legacy format 1–3 store never migrates (a read-only attach writes
        // nothing), and its absent fold cursor reads as 0 with an empty tail,
        // so it is a slot store with no slots.
        let slots = moraine_wal::SlotLog::new(object_store.clone(), &options.path);
        let coalescer = slot_commit::CommitCoalescer::new(options.commit_batch_window);
        Ok(Self {
            store: Arc::new(Store::Slots(Box::new(SlotStore {
                reader: Arc::new(reader),
                slots,
                object_store,
                options,
                read_only: true,
                coalescer,
            }))),
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
        })
    }

    /// The maintained-projection state shared by this handle's clones.
    pub(crate) fn projections(&self) -> &Arc<std::sync::RwLock<ProjectionCache>> {
        &self.projections
    }

    /// Whether this catalog maintains served projections. The slot topology
    /// folds no local commits into a served head view, so dumps always scan.
    #[allow(clippy::unused_self)]
    pub(crate) fn maintains_projections(&self) -> bool {
        false
    }

    /// Whether direct-store writes are available: a read-write slot-backed
    /// attach (through the folder role). A read-only attach refuses.
    fn ensure_writable(&self) -> Result<()> {
        match self.store.as_ref() {
            Store::Slots(store) if !store.read_only => Ok(()),
            Store::Slots(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
    }

    /// Runs `body` against a writable `Db` for a direct-store maintenance
    /// write: a read-write attach opens a fenced folder session for it — the
    /// one process allowed to write the store directly — and a read-only attach
    /// refuses.
    async fn with_writer<T, F>(&self, body: F) -> Result<T>
    where
        F: AsyncFnOnce(&Db) -> Result<T>,
    {
        match self.store.as_ref() {
            Store::Slots(store) if !store.read_only => folder::with_folder(store, body).await,
            Store::Slots(_) => Err(Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
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
        let Store::Slots(store) = self.store.as_ref();
        match at {
            None => {
                let head = slot_commit::materialize_slot_head(store).await?;
                slot_commit::release_reader(head.reader.as_ref()).await;
                Ok(head.view)
            }
            Some(snapshot) => slot_commit::materialize_slot_view_at(store, snapshot).await,
        }
    }

    /// One head read: the view, and on a slot-backed attach the byte-level
    /// overlay of the slots no folder has applied — what a probe the projection
    /// does not model must read over the store.
    async fn head_view(&self) -> Result<HeadRead> {
        let Store::Slots(store) = self.store.as_ref();
        // A fresh reader: entry scans read folder-written entries — index
        // backfills above all — straight from the store, not the tail.
        let head = slot_commit::materialize_slot_head_fresh(store).await?;
        Ok(HeadRead {
            view: head.view,
            tail: Some(head.overlay),
            reader: head.reader,
        })
    }

    /// Opens a read session and materializes the head through it, so a probe's
    /// entry scans and the catalog they resolve against are one cut. Released
    /// by [`ProbeRead::finish`].
    async fn begin_probe(&self) -> Result<ProbeRead> {
        let session = self.begin_read();
        match self.head_view().await {
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

    /// Opens a read session at the current head — the read-only reader shared
    /// with the catalog — the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    /// Used by [`crate::ffi_support`]'s raw current+history dumps and inline
    /// scans; every other reader goes through `snapshot`/`snapshot_at`.
    pub(crate) fn begin_read(&self) -> ReadSession {
        let Store::Slots(store) = self.store.as_ref();
        ReadSession::Reader(store.reader.clone())
    }

    /// Opens a read for the raw current+history dumps: a single read session
    /// on a single-topology store, or the pinned slot-log head (reader plus
    /// unfolded tail) on a slot-backed attach, so a dump taken mid-transaction
    /// reflects a winner no folder has applied yet.
    pub(crate) async fn begin_dump(&self) -> Result<DumpRead> {
        let Store::Slots(store) = self.store.as_ref();
        let head = slot_commit::materialize_slot_head(store).await?;
        Ok(DumpRead::Slots {
            reader: store.reader.clone(),
            head: Box::new(head),
        })
    }

    /// Test-only: every stored `(key, value)` under `prefix`, read through a
    /// fresh slot head — the folded store overlaid with the unfolded tail — so
    /// both folder-written entries and slot-committed ones are seen.
    #[cfg(test)]
    pub(crate) async fn scan_prefix_overlaid(&self, prefix: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Store::Slots(store) = self.store.as_ref();
        let head = slot_commit::materialize_slot_head_fresh(store)
            .await
            .unwrap();
        let handle = head.handle(ReadHandle::Reader(&store.reader));
        let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            std::collections::BTreeMap::new();
        let mut iter = handle.scan_prefix(prefix.clone(), ..).await.unwrap();
        while let Some(entry) = iter.next().await.unwrap() {
            merged.insert(entry.key.to_vec(), entry.value.to_vec());
        }
        for (key, value) in head.overlay.prefixed(&prefix) {
            match value {
                Some(bytes) => {
                    merged.insert(key.to_vec(), bytes.to_vec());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        slot_commit::release_reader(head.reader.as_ref()).await;
        merged.into_iter().collect()
    }

    /// Test-only: writes `entries` straight into the folded store under the
    /// folder role — the state a completed fold would leave, so the folder-role
    /// sweep can be exercised before a fold implementation lands.
    #[cfg(test)]
    pub(crate) async fn seed_folded_writes(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        self.with_writer(async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            for (key, value) in &entries {
                tx.put(key.clone(), value.clone()).map_err(Error::from)?;
            }
            tx.commit_with_options(&commit::durable())
                .await
                .map_err(Error::from)?;
            Ok(())
        })
        .await
        .unwrap();
    }

    /// Opens a staged-row transaction: materializes the head and pins it, so
    /// the staged batch races one slot at commit. A read-only attach returns
    /// [`Error::Constraint`].
    pub(crate) async fn begin_staged(
        &self,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: String,
    ) -> Result<crate::transaction::staged::StagedTransaction> {
        use crate::transaction::staged::StagedTransaction;

        let Store::Slots(store) = self.store.as_ref();
        if store.read_only {
            return Err(Error::Constraint(
                "catalog attached read-only; writes are unavailable".to_string(),
            ));
        }
        let head = slot_commit::materialize_slot_head(store).await?;
        Ok(StagedTransaction::begin_slots(
            head,
            store.reader.clone(),
            store.slots.clone(),
            data_store,
            data_prefix,
        ))
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
        let session = self.begin_read();

        let outcome = async {
            let head = self.head_view().await?;
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
                store_inline::scan_inline_file_deletes(session.handle(), None, table.get()).await?
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
        if let Store::Slots(store) = self.store.as_ref()
            && store.read_only
        {
            return Err(Error::Constraint(
                "catalog attached read-only; writes are unavailable".to_string(),
            ));
        }

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
    /// flipping the index ready. A single-writer store stages each step's
    /// entries in the commit itself; a slot-backed store writes them straight
    /// into the store under the folder role and rides only the cursor advance
    /// and the ready/poison flip through the log, so a million-row backfill
    /// never rides one envelope.
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

            let Store::Slots(store) = self.store.as_ref();
            let committed = self
                .commit_build_step_slots(store, table, index, step, is_final)
                .await;
            match committed {
                Ok(()) => {
                    if is_final {
                        return Ok(BuildProgress::Ready);
                    }
                }
                Err(Error::CommitConflict(_)) => return Ok(BuildProgress::Conflicted),
                Err(other) => return Err(other),
            }
        }
    }

    /// Commits one staged build step over the slot log: the entry batch lands
    /// in the store directly under the folder role, then a slot commit advances
    /// the build cursor and, on the final step, flips the index ready. A
    /// duplicate the batch discovers poisons the definition through a slot and
    /// fails the build.
    async fn commit_build_step_slots(
        &self,
        store: &SlotStore,
        table: TableId,
        index: IndexId,
        step: &[IndexEntry],
        is_final: bool,
    ) -> Result<()> {
        let info = self
            .snapshot()
            .await?
            .indexes_of(table)
            .into_iter()
            .find(|info| info.id == index)
            .ok_or_else(|| Error::NotFound(format!("index {index}")))?;

        if self.write_build_entries(store, &info, index, step).await? {
            self.commit(|tx| tx.poison_index_build(index)).await?;
            return Err(Error::Constraint(format!(
                "index {index} was poisoned by a duplicate value during its staged build"
            )));
        }

        // The highest row id this step covered advances the cursor; an empty
        // final step (no rows) still needs its flip, so the cursor holds.
        let covered = step.last().map(|entry| entry.row_id);
        self.commit(|tx| tx.advance_index_build(index, covered, is_final).map(|_| ()))
            .await
            .map(|_| ())
    }

    /// Writes a staged build's entry batch straight into the store under the
    /// folder role — the single direct writer — enforcing uniqueness against
    /// the folded store overlaid with the unfolded tail. Returns whether a
    /// duplicate poisoned the build.
    async fn write_build_entries(
        &self,
        store: &SlotStore,
        info: &IndexInfo,
        index: IndexId,
        batch: &[IndexEntry],
    ) -> Result<bool> {
        if batch.is_empty() {
            return Ok(false);
        }
        let staged = build_staged_entries(info, index, batch)?;
        let head = slot_commit::materialize_slot_head(store).await?;
        let overlay = head.overlay;
        let outcome = folder::with_folder(store, async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let probe = index_maintenance::ProbeHandle::Overlaid {
                store: ReadHandle::Tx(&tx),
                overlay: &overlay,
            };
            let (poisoned, writes) = index_maintenance::plan_index_entries(probe, &staged).await?;
            commit::stage_writes(&tx, &writes)?;
            tx.commit_with_options(&commit::durable())
                .await
                .map_err(Error::from)?;
            Ok(!poisoned.is_empty())
        })
        .await;
        slot_commit::release_reader(head.reader.as_ref()).await;
        outcome
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
        let session = self.begin_read();
        let head = match self.head_view().await {
            Ok(head) => head,
            Err(err) => {
                session.finish();
                return Err(err);
            }
        };

        // Scan the inline chunk bodies through the head's reader (fresh on a
        // slot-backed store) overlaid with the unfolded tail, so inline rows
        // committed to slots no folder has applied are backfilled rather than
        // silently skipped — an index built over them would otherwise flip
        // ready covering nothing.
        let scan_handle = head
            .reader
            .as_ref()
            .map_or(session.handle(), ReadHandle::Reader);
        let outcome =
            inline_backfill_from(scan_handle, head.tail.as_ref(), &head.view, table, columns).await;

        slot_commit::release_reader(head.reader.as_ref()).await;
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

        self.with_writer(async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let deleted = index_maintenance::reclaim_entries(&tx, index.get(), limit).await?;
            tx.commit_with_options(&commit::durable())
                .await
                .map_err(Error::from)?;
            Ok(deleted)
        })
        .await
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
        self.ensure_writable()?;
        if request.batch_size == 0 {
            return Err(Error::Configuration(
                "batch_size must be nonzero; zero would reclaim nothing and never terminate"
                    .to_string(),
            ));
        }

        if !request.sweep_orphaned_index_entries {
            return Ok(MaintenanceReport::default());
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

        // The entry deletions are derived-state upkeep in the index subspace,
        // never replayed into a view, so they run under the folder role: the
        // single direct writer of a slot-backed store.
        self.with_writer(async |db| {
            let mut report = MaintenanceReport::default();
            for kind in [IndexKind::Unique, IndexKind::Multi] {
                let mut from = 0u64;
                while let Some(index_id) = self.first_index_id_from(kind, from).await? {
                    if !live.contains(&index_id) {
                        let reclaimed = self
                            .reclaim_dead_range(db, kind, index_id, request.batch_size)
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
        })
        .await
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

        let session = self.begin_read();
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
    /// index is not live, and holds the writer the batches commit through.
    async fn reclaim_dead_range(
        &self,
        db: &Db,
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
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
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
        let Store::Slots(store) = self.store.as_ref();
        store.reader.close().await.map_err(Error::from)
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
        let Store::Slots(store) = self.store.as_ref();
        store.coalescer.commit(store, &f).await
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
