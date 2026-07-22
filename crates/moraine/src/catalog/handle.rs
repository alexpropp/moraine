//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use object_store::{ObjectStore, path::Path};
use slatedb::{Db, DbReader, DbTransaction, IsolationLevel};

use crate::{
    catalog::{
        CatalogSnapshot, ColumnId, DataFileInfo, FileIndexEntry, IndexEntry, IndexId, IndexState,
        RowHolder, RowLocation, SnapshotId, TableId, projection::ProjectionCache, scoped_read,
    },
    error::{Error, Result},
    store::{
        handle::ReadSession,
        index_encoding::{IndexKeyValue, encode_ordered_values},
        inline as store_inline,
        key::InlineOperation,
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
            let row_ids =
                index_maintenance::lookup_row_ids(handle, index.get(), info.unique, &key).await?;
            let files = view.data_files_of(table);
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: resolve_row_holder(&files, row_id),
                })
                .collect())
        }
        .await;
        session.finish();

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
    /// [`Error::NotFound`] if the index does not exist, [`Error::IndexBuilding`]
    /// if its staged backfill has not completed, [`Error::Constraint`] if a
    /// bound value exceeds the size cap, or a store error if the scan fails.
    pub async fn index_range(
        &self,
        table: TableId,
        index: IndexId,
        lower: Bound<Vec<IndexKeyValue>>,
        upper: Bound<Vec<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<RowLocation>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let outcome = async {
            let view = commit::materialize(handle, None).await?;
            let info = view
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

            let bound_len = |bound: &Bound<Vec<IndexKeyValue>>| match bound {
                Bound::Included(values) | Bound::Excluded(values) => values.len(),
                Bound::Unbounded => 0,
            };
            // A bound naming more columns than the index has would encode
            // components no stored key carries, silently returning the wrong
            // rows; refuse it instead.
            let widest = bound_len(&lower).max(bound_len(&upper));
            if widest > info.columns.len() {
                return Err(Error::Constraint(format!(
                    "index_range: a bound of {widest} values does not fit the {}-column index \
                     {index}",
                    info.columns.len()
                )));
            }
            // The range column is the last one the bounds name (leading
            // columns are pinned to equality); its direction decides whether
            // value order runs with or against the index's byte order.
            let range_column = widest.saturating_sub(1);
            let descending = info.directions.get(range_column).copied()
                == Some(crate::store::index_encoding::Direction::Descending);

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

            // A descending column's byte order reverses value order, so the
            // value-lower bound is the byte-upper bound and vice versa.
            let (byte_lower, byte_upper) = if descending {
                (encode_bound(upper)?, encode_bound(lower)?)
            } else {
                (encode_bound(lower)?, encode_bound(upper)?)
            };

            let mut row_ids = index_maintenance::range_row_ids(
                handle,
                index.get(),
                info.unique,
                byte_lower,
                byte_upper,
            )
            .await?;
            // The scan yields the index's declared order; reversing the
            // materialized result serves the exact opposite order.
            if reverse {
                row_ids.reverse();
            }
            let files = view.data_files_of(table);
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: resolve_row_holder(&files, row_id),
                })
                .collect())
        }
        .await;
        session.finish();

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
    /// Head-only and candidate-returning like [`index_lookup`](Self::index_lookup).
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the index does not exist, [`Error::IndexBuilding`]
    /// while its staged backfill runs, or [`Error::Constraint`] if the prefix
    /// is empty, longer than the index, or names no `IS NULL`.
    pub async fn index_nulls(
        &self,
        table: TableId,
        index: IndexId,
        prefix: Vec<Option<IndexKeyValue>>,
        reverse: bool,
    ) -> Result<Vec<RowLocation>> {
        let session = self.begin_read().await?;
        let handle = session.handle();

        let outcome = async {
            let view = commit::materialize(handle, None).await?;
            let info = view
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
                index_maintenance::null_prefix_row_ids(handle, index.get(), &key).await?;
            if reverse {
                row_ids.reverse();
            }
            let files = view.data_files_of(table);
            Ok(row_ids
                .into_iter()
                .map(|row_id| RowLocation {
                    row_id,
                    holder: resolve_row_holder(&files, row_id),
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
            let snapshot = commit::materialize(session.handle(), None).await?;
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
            let snapshot = commit::materialize(session.handle(), None).await?;
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

/// Resolves a row id to its current holder among a table's data files: the
/// file whose live dense row-id range contains it, else `Inline`
/// (an inlined row, or a file that carries explicit per-row ids rather than
/// a dense range).
fn resolve_row_holder(files: &[DataFileInfo], row_id: u64) -> RowHolder {
    for file in files {
        if let Some(start) = file.row_id_start
            && row_id >= start
            && row_id < start.saturating_add(file.record_count)
        {
            return RowHolder::DataFile(file.id);
        }
    }

    RowHolder::Inline
}
