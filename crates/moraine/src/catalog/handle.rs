//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

use std::{sync::Arc, time::Duration};

use object_store::{ObjectStore, path::Path};
use slatedb::{Db, DbReader, DbTransaction, IsolationLevel};

use crate::{
    catalog::{
        CatalogSnapshot, ColumnId, FileIndexEntry, IndexEntry, IndexId, IndexState, RowHolder,
        RowLocation, SnapshotId, TableId, projection::ProjectionCache, scoped_read,
    },
    error::{Error, Result},
    store::{
        handle::{ReadHandle, ReadSession},
        index_encoding::IndexKeyValue,
        key::{IdxKind, idx_index_prefix},
        open::StoreBuilder,
    },
    transaction::{Transaction, commit, index_maintenance},
};

/// The open store behind a catalog: the read-write `Db` writer, or a
/// read-only `DbReader`. A read-only catalog never opens a `Db`, so it never
/// fences a live writer.
enum Store {
    /// The single read-write writer.
    Writer(Db),
    /// A read-only reader following the manifest, shared into read sessions.
    Reader(Arc<DbReader>),
}

/// Options for opening a catalog.
///
/// # Examples
///
/// ```
/// let options = moraine::CatalogOptions::default();
/// assert_eq!(options.flush_interval, std::time::Duration::from_millis(100));
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
    /// costlier) object-store PUTs. Must be nonzero; defaults to 100ms.
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
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            path: String::new(),
            encrypted: false,
            flush_interval: Duration::from_millis(100),
            cache_dir: None,
            data_path: None,
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
        let store = StoreBuilder::new(&options.path, object_store)
            .flush_interval(options.flush_interval)
            .cache_dir(options.cache_dir.clone());
        let db = commit::open_initialized(store, options.encrypted, options.data_path.as_deref())
            .await?;
        Ok(Self {
            store: Arc::new(Store::Writer(db)),
            projections: Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
        })
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
        let store =
            StoreBuilder::new(&options.path, object_store).cache_dir(options.cache_dir.clone());
        let reader = commit::open_reader_initialized(store).await?;
        Ok(Self {
            store: Arc::new(Store::Reader(Arc::new(reader))),
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
        let session = self.begin_read().await?;
        let view = commit::materialize(session.handle(), at).await;
        session.finish();

        view
    }

    /// Resolves an equality lookup to the rows currently holding `values`.
    ///
    /// Head-only: the lookup materializes the current head and scans the
    /// `idx` subspace under one read session, so the entries and the catalog
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
        let session = self.begin_read().await?;
        let handle = session.handle();

        let outcome = async {
            let view = commit::materialize(handle, None).await?;
            let info = view
                .indexes_of(table)
                .into_iter()
                .find(|info| info.id == index)
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
            let key = crate::store::index_encoding::encode_key(values)?;
            let row_ids =
                index_maintenance::lookup_row_ids(handle, index.get(), info.unique, &key).await?;
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: resolve_row_holder(&view, table, row_id),
                })
                .collect())
        }
        .await;
        session.finish();

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
    /// v1 covers new dense-range files (`row_id_start + ordinal`); reading a
    /// rewrite file's embedded row-id column is a follow-up.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] if the file cannot be read or a column
    /// type does not match its Parquet type, or [`Error::Constraint`] for a
    /// non-indexable column type.
    pub async fn scoped_file_index_entries(
        &self,
        object_store: Arc<dyn ObjectStore>,
        path: &Path,
        index: IndexId,
        indexed_positions: &[usize],
    ) -> Result<Vec<FileIndexEntry>> {
        let entries =
            scoped_read::scoped_read_entries(object_store, path, indexed_positions, None, 0, None)
                .await?;
        Ok(entries
            .into_iter()
            .map(|entry| FileIndexEntry {
                index,
                // No row-id column and `row_id_start = 0`, so the derived
                // row id is the ordinal the registration re-maps.
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
    /// v1 covers dense-range files (`row_id_start + ordinal`); a file that
    /// carries explicit per-row ids (compaction output) is refused.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or a column is not live,
    /// [`Error::Constraint`] for a per-row-id file or a non-indexable type,
    /// or [`Error::Corruption`] if a file cannot be read.
    pub async fn scoped_backfill_entries(
        &self,
        object_store: Arc<dyn ObjectStore>,
        data_prefix: &str,
        table: TableId,
        columns: &[ColumnId],
    ) -> Result<Vec<IndexEntry>> {
        let snapshot = self.snapshot().await?;
        // `columns_of` is ordered by the column's ordinal, so a column's
        // 0-based index here is its physical position in a file written under
        // this schema — the mapping the scoped read needs. (Ordinals are
        // 1-based in the stored value, so the stored order can't be used
        // directly.)
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

        // A relative data-file path is relative to the table's data directory
        // (`<schema path><table path>`), itself relative to DATA_PATH.
        let table_value = snapshot
            .tables
            .get(&table.get())
            .ok_or_else(|| Error::NotFound(format!("table {table}")))?;
        let schema_value = snapshot
            .schemas
            .get(&table_value.schema_id)
            .ok_or_else(|| {
                Error::Corruption(format!("table {table} references a missing schema"))
            })?;
        let table_prefix = format!("{}{}", schema_value.path, table_value.path);

        let mut entries = Vec::new();
        for file in snapshot.data_files_of(table) {
            let row_id_start = file.row_id_start.ok_or_else(|| {
                Error::Constraint(format!(
                    "data file {} carries per-row ids; scoped backfill of rewrite files is a follow-up",
                    file.id
                ))
            })?;
            let relative = match (file.path_is_relative, data_prefix.is_empty()) {
                (false, _) => file.path.clone(),
                (true, true) => format!("{table_prefix}{}", file.path),
                (true, false) => format!("{data_prefix}/{table_prefix}{}", file.path),
            };
            let path = object_store::path::Path::from(relative.as_str());
            let scoped = scoped_read::scoped_read_entries(
                Arc::clone(&object_store),
                &path,
                &positions,
                None,
                row_id_start,
                Some(file.file_size_bytes),
            )
            .await?;
            entries.extend(scoped.into_iter().map(|entry| IndexEntry {
                row_id: entry.row_id,
                values: entry.values,
            }));
        }
        Ok(entries)
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
        let mut deleted = 0;
        // An index is exclusively one kind, so only one prefix holds entries;
        // scanning both is harmless.
        for kind in [IdxKind::Unique, IdxKind::Multi] {
            if deleted >= limit {
                break;
            }
            let mut iter = ReadHandle::Tx(&tx)
                .scan_prefix(idx_index_prefix(kind, index.get()), ..)
                .await
                .map_err(Error::from)?;
            while deleted < limit {
                match iter.next().await.map_err(Error::from)? {
                    Some(entry) => {
                        tx.delete(entry.key).map_err(Error::from)?;
                        deleted += 1;
                    }
                    None => break,
                }
            }
        }
        tx.commit_with_options(&commit::durable())
            .await
            .map_err(Error::from)?;

        Ok(deleted)
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
    /// — it touched the same tables or the schema list — or when the
    /// bounded internal retry budget is exhausted before a benign race
    /// resolves.
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

/// Resolves a row id to its current holder against a materialized view: the
/// data file whose live dense row-id range contains it, else `Inline`
/// (an inlined row, or a file that carries explicit per-row ids rather than
/// a dense range).
fn resolve_row_holder(view: &CatalogSnapshot, table: TableId, row_id: u64) -> RowHolder {
    for file in view.data_files_of(table) {
        if let Some(start) = file.row_id_start
            && row_id >= start
            && row_id < start.saturating_add(file.record_count)
        {
            return RowHolder::DataFile(file.id);
        }
    }

    RowHolder::Inline
}
