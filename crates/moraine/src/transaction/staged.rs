//! The staged-row commit path: DuckLake authors rows over the ABI instead
//! of driving [`crate::transaction::Transaction`]'s verbs, and this module
//! turns the accumulated rows into one atomic store write.
//!
//! Three rules bound this path: every value DuckLake supplies is stored
//! **verbatim** except one interpreted convention (an `UPDATE` setting
//! `end_snapshot` becomes current-delete + history-write); one commit is one
//! atomic batch, reusing [`super::commit::diff_writes`]; and a lost race at
//! commit is **never retried** — DuckLake authored the ids and counters in
//! the batch. The loser's error always carries the substring `conflict`
//! (via [`Error::CommitConflict`]'s `Display`), the wire contract
//! DuckLake's retry loop scans for.
//!
//! Translation applies every staged row to a cloned working
//! [`CatalogSnapshot`], then diffs it against the unmodified base exactly
//! as a verb-path commit diffs its closure's output. An `UPDATE ... SET
//! end_snapshot` row's value is validated, not trusted: DuckLake always
//! sets it to this commit's own new snapshot id — the value `diff_writes`
//! stamps on its own — so a mismatch is drift caught loudly.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use moraine_wal::{CommitOutcome, Envelope, Overlay, SlotLog};
use object_store::ObjectStore;
use slatedb::{DbReader, DbTransaction};
use tracing::debug;

use crate::{
    catalog::{
        CatalogSnapshot, ColumnInfo, IndexInfo, SnapshotId, TableId,
        inline::{InlineScanKind, materialize_inline_rows},
        projection::{ProjectionCache, fold_committed_batch, invalidate_head_view},
        scoped_read::{self, ScopedReadEntry},
    },
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        index_encoding::encode_ordered_values,
        inline as store_inline,
        key::{EntityKey, InlineKey, InlineOperation, Key, SysKey},
        proto, read, value,
    },
    transaction::{
        commit::{self, StagedWrite},
        index_maintenance::{ProbeHandle, StagedIndexEntry, plan_index_entries},
        slot_commit::{SlotHead, commit_from, release_reader},
    },
};

mod apply;
mod decode;
mod index_upkeep;
mod inline;
#[cfg(test)]
mod tests;

use apply::{ChildRows, apply_op, build_snapshot_value, collect_child_rows, is_inline_op};
use decode::Cursor;
use index_upkeep::stage_index_maintenance;
use inline::translate_inline;

/// Which `ducklake_*` table a staged row targets. `Snapshot`,
/// `SnapshotChanges`, and `SchemaVersions` all fold into one moraine
/// `snapshot` record; every other kind maps to one `EntityKey` variant or one
/// of the three unversioned statistics kinds.
// The discriminants are the staged-write wire protocol (the ABI's
// `table_kind` values): declaration order is load-bearing, pinned by
// `ALL` and its order test. Insert new kinds at the end only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TableKind {
    /// `ducklake_snapshot`.
    Snapshot,
    /// `ducklake_snapshot_changes`.
    SnapshotChanges,
    /// `ducklake_schema`.
    Schema,
    /// `ducklake_table`.
    Table,
    /// `ducklake_view`.
    View,
    /// `ducklake_column`.
    Column,
    /// `ducklake_data_file`.
    DataFile,
    /// `ducklake_delete_file`.
    DeleteFile,
    /// `ducklake_table_stats`.
    TableStats,
    /// `ducklake_table_column_stats`.
    TableColumnStats,
    /// `ducklake_file_column_stats`.
    FileColumnStats,
    /// `ducklake_schema_versions`: per-table schema-change history, one
    /// `(begin_snapshot, schema_version, table_id)` row per
    /// created-or-schema-altered table per commit. The first two values
    /// are always the committing snapshot's own id and `schema_version`,
    /// so the table-id set is the only new information — folded into the
    /// snapshot record's `schema_changed_table_ids`, with both redundant values
    /// validated against the `ducklake_snapshot` row in the same batch.
    SchemaVersions,
    /// `ducklake_partition_info`.
    PartitionInfo,
    /// `ducklake_partition_column` — folded into its spec's record.
    PartitionColumn,
    /// `ducklake_file_partition_value` — folded into its file's record.
    FilePartitionValue,
    /// `ducklake_sort_info`.
    SortInfo,
    /// `ducklake_sort_expression` — folded into its spec's record.
    SortExpression,
    /// `ducklake_tag` — an entry in its object's container record.
    Tag,
    /// `ducklake_column_tag` — an entry embedded in its column's record.
    ColumnTag,
    /// `ducklake_files_scheduled_for_deletion` — the physical-deletion
    /// schedule, keyed by the scheduled file's id.
    FilesScheduledForDeletion,
    /// `ducklake_macro`.
    Macro,
    /// `ducklake_macro_impl` — folded into its macro's record.
    MacroImpl,
    /// `ducklake_macro_parameters` — folded into its macro's record.
    MacroParameters,
    /// `ducklake_column_mapping`.
    ColumnMapping,
    /// `ducklake_name_mapping` — folded into its mapping's record.
    NameMapping,
}

impl TableKind {
    /// Every kind, in wire-discriminant order — the decode table for the
    /// ABI's `table_kind` values. A new variant added anywhere but the
    /// end fails the order test pinning `ALL[i] as i32 == i`.
    pub const ALL: [Self; 25] = [
        Self::Snapshot,
        Self::SnapshotChanges,
        Self::Schema,
        Self::Table,
        Self::View,
        Self::Column,
        Self::DataFile,
        Self::DeleteFile,
        Self::TableStats,
        Self::TableColumnStats,
        Self::FileColumnStats,
        Self::SchemaVersions,
        Self::PartitionInfo,
        Self::PartitionColumn,
        Self::FilePartitionValue,
        Self::SortInfo,
        Self::SortExpression,
        Self::Tag,
        Self::ColumnTag,
        Self::FilesScheduledForDeletion,
        Self::Macro,
        Self::MacroImpl,
        Self::MacroParameters,
        Self::ColumnMapping,
        Self::NameMapping,
    ];
}

impl TryFrom<i32> for TableKind {
    type Error = i32;

    /// Decodes a wire discriminant; the unrecognized value comes back as
    /// the error.
    fn try_from(value: i32) -> std::result::Result<Self, i32> {
        usize::try_from(value)
            .ok()
            .and_then(|index| Self::ALL.get(index).copied())
            .ok_or(value)
    }
}

/// One column value in a staged row, typed to the small set of primitive
/// kinds every `ducklake_*` column uses. Decoding into the right proto
/// shape happens here, table-kind by table-kind.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    /// SQL `NULL`.
    Null,
    /// An unsigned integer column (every id, counter, and count).
    U64(u64),
    /// A signed integer column (currently only `snapshot_time`, a
    /// `TIMESTAMPTZ` carried as microseconds since the epoch).
    I64(i64),
    /// A boolean column.
    Bool(bool),
    /// A text column (also used for `UUID`, carried as its text form).
    Str(String),
}

/// One staged row mutation, DuckLake-authored. `cells` are positional, in
/// the exact `ducklake_*` column order pinned in
/// `crates/moraine-duckdb/cpp/metadata_tables.cpp`.
#[derive(Debug, Clone)]
pub enum RowOperation {
    /// A new row: becomes a live `current` record (versioned kinds) or
    /// overwrites the current record in place (unversioned statistics
    /// kinds).
    Insert {
        /// The row's table.
        table: TableKind,
        /// The row's column values, in table order.
        cells: Vec<Cell>,
    },
    /// A row removed with no history mirror: the unversioned statistics
    /// kinds and the deletion schedule, plus the hard prunes maintenance
    /// issues — snapshot records, dead entity versions (`current` or
    /// `history`, named by their `end_snapshot`), and dead tag entries.
    Delete {
        /// The row's table.
        table: TableKind,
        /// The removed row's key columns, in table order (id columns
        /// only — see [`Self::Insert`]'s `cells` for the full row shape).
        cells: Vec<Cell>,
    },
    /// An `UPDATE ... SET end_snapshot = <v>` row: the one lifecycle
    /// convention this path interprets — ends the live version (moves it
    /// to `history`). Defined only for the six versioned kinds.
    UpdateSetEnd {
        /// The row's table.
        table: TableKind,
        /// The ended row's key columns, in table order, followed by the
        /// new `end_snapshot` value.
        cells: Vec<Cell>,
    },
    /// An `UPDATE ... SET begin_snapshot = <v>` row: rebases a data
    /// file's visibility window in place. DuckLake issues it only during
    /// a delete-rewrite, against the replacement file the same
    /// transaction just inserted — any other target is a shape error.
    UpdateSetBegin {
        /// The row's table (only `ducklake_data_file`).
        table: TableKind,
        /// The rebased row's key columns, followed by the new
        /// `begin_snapshot` value.
        cells: Vec<Cell>,
    },
    /// `inline/schema`: the Arrow IPC schema for one `(table_id,
    /// schema_version)`, written once at inline-table creation, stored
    /// verbatim.
    InlineSchema {
        /// Owning table.
        table_id: u64,
        /// Schema version the layout is pinned to.
        schema_version: u64,
        /// The Arrow IPC schema message, verbatim.
        arrow_schema: Vec<u8>,
    },
    /// `inline/insert`: one Arrow record-batch chunk of inlined rows.
    /// `chunk_seq` is not carried here — translation allocates it, so
    /// several `InlineInsert`s staged in one commit against the same
    /// `(table_id, schema_version, begin_snapshot)` land as sequential
    /// chunks in stage order.
    InlineInsert {
        /// Owning table.
        table_id: u64,
        /// Schema version the chunk was written under.
        schema_version: u64,
        /// Commit snapshot of the insert, DuckLake-authored verbatim.
        begin_snapshot: u64,
        /// The chunk's first row id; later rows are dense from here.
        row_id_start: u64,
        /// Row count carried by `arrow_body`.
        row_count: u64,
        /// The user-column cells, encoded as one Arrow IPC record-batch
        /// body (opaque bytes to this layer).
        arrow_body: Vec<u8>,
    },
    /// `inline/inline_delete`: tombstones one inlined-insert row.
    InlineInlineDelete {
        /// Owning table.
        table_id: u64,
        /// The tombstoned row.
        row_id: u64,
        /// The commit snapshot the row ends at.
        end_snapshot: u64,
    },
    /// `inline/file_delete`: an inlined delete against a Parquet-file row.
    InlineFileDelete {
        /// Owning table.
        table_id: u64,
        /// Targeted data file.
        data_file_id: u64,
        /// Deleted row.
        row_id: u64,
        /// The commit snapshot the delete takes effect at.
        begin_snapshot: u64,
    },
    /// Removes every `inline/insert` chunk begun at or before
    /// `flush_snapshot` for `(table_id, schema_version)`, plus the
    /// `inline/inline_delete` tombstones those chunks' rows consumed — the
    /// flushed data survives only as the backdated `ducklake_data_file`
    /// DuckLake registers through the ordinary file path.
    InlineFlushDelete {
        /// Owning table.
        table_id: u64,
        /// Schema version being flushed.
        schema_version: u64,
        /// Chunks begun at or before this snapshot are flushed.
        flush_snapshot: u64,
    },
    /// Removes every `inline/*` record for `table_id`: schema, chunks,
    /// and tombstones.
    InlineDrop {
        /// The dropped table.
        table_id: u64,
    },
    /// Removes only the `inline/schema` record for one `(table_id,
    /// schema_version)` — the superseded-schema-version cleanup a flush
    /// issues once its chunks are gone, leaving any other schema
    /// version's `inline/*` records (a newer version accumulating
    /// concurrently) untouched. Distinct from [`Self::InlineDrop`], which
    /// is table-wide (the whole-table `DROP TABLE` cascade).
    InlineSchemaDrop {
        /// Owning table.
        table_id: u64,
        /// The schema version deregistered.
        schema_version: u64,
    },
}

/// A malformed staged row: wrong cell count or a cell of the wrong kind
/// for its column. Never produced by a correct shim translation; this
/// path fails loudly rather than guessing.
fn corrupt_row(table: TableKind, detail: impl std::fmt::Display) -> Error {
    Error::Corruption(format!("staged row for {table:?}: {detail}"))
}

/// What a staged transaction reads through and commits against: the
/// single-writer transaction, or a pinned multi-writer head.
pub(crate) enum StagedBacking {
    /// The one read-write transaction; reads see its own staged writes and a
    /// successful commit lands directly.
    Tx(DbTransaction),
    /// A pinned slot-log head: reads resolve over the reader overlaid with the
    /// unfolded tail, and a commit races one slot at `head.next_sequence`.
    /// Boxed: the head carries a full catalog view, which would otherwise
    /// dominate every future holding a staged transaction.
    Slots {
        head: Box<SlotHead>,
        reader: Arc<DbReader>,
        slots: SlotLog,
    },
}

impl StagedBacking {
    /// The handle every scan and point read routes through: the writer's
    /// transaction, or the reader the pinned head materialized from.
    fn scan_handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Tx(db_tx) => ReadHandle::Tx(db_tx),
            Self::Slots { head, reader, .. } => head.handle(ReadHandle::Reader(reader)),
        }
    }

    /// The uniqueness-probe handle: the store alone for the writer, the store
    /// overlaid with the unfolded tail for a slot-backed attach.
    fn probe(&self) -> ProbeHandle<'_> {
        match self {
            Self::Tx(db_tx) => ProbeHandle::Store(ReadHandle::Tx(db_tx)),
            Self::Slots { head, reader, .. } => ProbeHandle::Overlaid {
                store: head.handle(ReadHandle::Reader(reader)),
                overlay: &head.overlay,
            },
        }
    }

    /// The unfolded tail to overlay on raw `inline/*` scans (which are not
    /// modeled by the catalog view): `Some` for a slot-backed attach, `None`
    /// for the writer, whose transaction reads its own state.
    fn scan_overlay(&self) -> Option<&Overlay> {
        match self {
            Self::Tx(_) => None,
            Self::Slots { head, .. } => Some(&head.overlay),
        }
    }
}

/// A staged-row transaction: one commit backing opened by `begin`
/// (crate-internal; [`crate::ffi_support::staged::staged_begin`] is the entry
/// point outside this crate), accumulating [`RowOperation`]s via
/// [`stage`](Self::stage) until [`commit`](Self::commit) translates and
/// lands them all in one atomic batch, or [`rollback`](Self::rollback)
/// discards them.
pub struct StagedTransaction {
    backing: StagedBacking,
    ops: Vec<RowOperation>,
    projections: Arc<std::sync::RwLock<ProjectionCache>>,
    /// The `DATA_PATH` object store and its bucket-relative prefix, present
    /// when the attach supplied `META_DATA_PATH`. Index maintenance
    /// scoped-reads registered data files through it; absent it is skipped.
    data_store: Option<Arc<dyn ObjectStore>>,
    data_prefix: String,
}

impl StagedTransaction {
    /// Opens a fresh single-writer transaction at the current head. Nothing is
    /// staged yet; [`stage`](Self::stage) accumulates rows in memory only. A
    /// successful commit folds its batch into `projections` (a catalog's
    /// shared maintained-projection state).
    pub(crate) fn begin(
        db_tx: DbTransaction,
        projections: Arc<std::sync::RwLock<ProjectionCache>>,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: String,
    ) -> Self {
        Self::with_backing(
            StagedBacking::Tx(db_tx),
            projections,
            data_store,
            data_prefix,
        )
    }

    /// Opens a fresh transaction over a pinned slot-log head; a successful
    /// commit races one slot rather than landing a writer transaction.
    pub(crate) fn begin_slots(
        head: SlotHead,
        reader: Arc<DbReader>,
        slots: SlotLog,
        projections: Arc<std::sync::RwLock<ProjectionCache>>,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: String,
    ) -> Self {
        Self::with_backing(
            StagedBacking::Slots {
                head: Box::new(head),
                reader,
                slots,
            },
            projections,
            data_store,
            data_prefix,
        )
    }

    fn with_backing(
        backing: StagedBacking,
        projections: Arc<std::sync::RwLock<ProjectionCache>>,
        data_store: Option<Arc<dyn ObjectStore>>,
        data_prefix: String,
    ) -> Self {
        Self {
            backing,
            ops: Vec::new(),
            projections,
            data_store,
            data_prefix,
        }
    }

    /// As [`begin`](Self::begin), but with a throwaway, never-served
    /// projection state and no `DATA_PATH` store — for tests that drive a
    /// `StagedTransaction` directly without a `Catalog`.
    #[cfg(test)]
    pub(crate) fn begin_detached(db_tx: DbTransaction) -> Self {
        Self::begin(
            db_tx,
            Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
            None,
            String::new(),
        )
    }

    /// As [`begin_detached`](Self::begin_detached), but reading registered
    /// files from `data_store` — for tests that exercise the file paths.
    #[cfg(test)]
    pub(crate) fn begin_detached_with_store(
        db_tx: DbTransaction,
        data_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::begin(
            db_tx,
            Arc::new(std::sync::RwLock::new(ProjectionCache::empty())),
            Some(data_store),
            String::new(),
        )
    }

    /// Accumulates one row mutation. Nothing touches the store until
    /// [`commit`](Self::commit).
    pub fn stage(&mut self, op: RowOperation) {
        self.ops.push(op);
    }

    /// Discards every staged row without writing anything.
    pub fn rollback(self) {
        if let StagedBacking::Tx(db_tx) = self.backing {
            db_tx.rollback();
        }
    }

    /// Snapshot records as this transaction sees them: the committed
    /// rows at its read point, minus the snapshot deletes staged so far.
    /// The expiry cascade re-reads `ducklake_snapshot` after staging its
    /// deletes (its dead-row rule is `NOT EXISTS` over the survivors), so
    /// the projection must observe the transaction's own writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails or a staged snapshot-delete row
    /// is malformed.
    pub async fn visible_snapshots(&self) -> Result<Vec<proto::SnapshotValue>> {
        let committed = match &self.backing {
            StagedBacking::Tx(db_tx) => read::scan_snapshots(ReadHandle::Tx(db_tx)).await?,
            StagedBacking::Slots { head, reader, .. } => {
                read::scan_snapshots_overlaid(
                    head.handle(ReadHandle::Reader(reader)),
                    &head.overlay,
                )
                .await?
            }
        };

        let mut deleted = std::collections::BTreeSet::new();
        for op in &self.ops {
            if let RowOperation::Delete {
                table: TableKind::Snapshot,
                cells,
            } = op
            {
                let mut c = Cursor::new(TableKind::Snapshot, cells);
                deleted.insert(c.u64()?);
                c.finish()?;
            }
        }

        Ok(committed
            .into_iter()
            .filter(|s| !deleted.contains(&s.snapshot_id))
            .collect())
    }

    /// Translates every staged row and lands them in one atomic batch.
    ///
    /// A commit with a `ducklake_snapshot` insert mints that snapshot and
    /// advances head. A commit **without** one is a maintenance commit —
    /// snapshot expiry / file cleanup, which DuckLake runs without
    /// minting a snapshot — and lands head-preserving: reclamation
    /// deletes only, no new snapshot record, `sys/head` untouched.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] or [`Error::Corruption`] if the
    /// staged rows are malformed, mutate entities without the required
    /// `ducklake_snapshot` / `ducklake_snapshot_changes` pair, or expire
    /// the head snapshot. Returns [`Error::CommitConflict`] — **never
    /// retried internally** — if a concurrent commit advanced the head
    /// first; the store is left unchanged by the loser.
    pub async fn commit(self) -> Result<SnapshotId> {
        let started = std::time::Instant::now();
        let Self {
            backing,
            ops,
            projections,
            data_store,
            data_prefix,
        } = self;
        let staged_rows = ops.len();

        let assembled = assemble(
            &backing,
            &ops,
            &projections,
            data_store.as_ref(),
            &data_prefix,
        )
        .await;

        match backing {
            StagedBacking::Tx(db_tx) => {
                commit_tx(db_tx, assembled, &projections, staged_rows, started).await
            }
            StagedBacking::Slots { head, slots, .. } => {
                commit_slots(*head, slots, assembled, staged_rows, started).await
            }
        }
    }
}

/// Everything one staged commit assembles, independent of the backing it
/// lands through.
struct Assembly {
    /// The snapshot id a successful commit reports: the minted id, or the
    /// unchanged head for a maintenance commit.
    result_id: u64,
    /// The full batch to land: entity diff, index entries, inline writes, and
    /// (for a minting commit) the snapshot record and head advance.
    writes: Vec<StagedWrite>,
    mints_snapshot: bool,
    /// The pre-commit head view, for folding a landed head-advancing batch
    /// into the single-writer projection cache.
    base: Arc<CatalogSnapshot>,
    /// The transaction id stamped into the minted snapshot and carried by the
    /// slot envelope; `None` on the single-writer path.
    transaction_id: Option<[u8; 16]>,
    /// The commit's classification string, for the slot envelope a lost race
    /// judges against; empty for a maintenance commit.
    changes_made: String,
}

/// Translates every staged row into the atomic batch both backings land.
/// Reads route through the backing: the writer's transaction, or the pinned
/// slot-log head overlaid with its unfolded tail.
async fn assemble(
    backing: &StagedBacking,
    ops: &[RowOperation],
    projections: &std::sync::RwLock<ProjectionCache>,
    data_store: Option<&Arc<dyn ObjectStore>>,
    data_prefix: &str,
) -> Result<Assembly> {
    let handle = backing.scan_handle();
    let overlay = backing.scan_overlay();
    let base: Arc<CatalogSnapshot> = match backing {
        StagedBacking::Tx(db_tx) => commit::head_view_for(db_tx, projections).await?,
        StagedBacking::Slots { head, .. } => Arc::new(head.view.clone()),
    };
    let base_ref: &CatalogSnapshot = &base;

    // A slot-backed commit stamps a fresh transaction id so a lost race can be
    // resolved by identity and a landed snapshot survives folding for the
    // dedup scan; the single-writer path stamps none.
    let transaction_id = match backing {
        StagedBacking::Tx(_) => None,
        StagedBacking::Slots { .. } => Some(uuid::Uuid::new_v4().into_bytes()),
    };

    // Read before any write is staged: `InlineFlushDelete`/`InlineDrop` name a
    // table, not keys, and resolve against the pre-commit state exactly like
    // `base`.
    let inline_writes = translate_inline(handle, overlay, ops).await?;

    let mints_snapshot = ops.iter().any(|op| {
        matches!(
            op,
            RowOperation::Insert {
                table: TableKind::Snapshot,
                ..
            }
        )
    });

    // Maintain equality-index entries for any data file this commit registered
    // on an indexed table, by scoped-reading it from `DATA_PATH`. Gated: a
    // no-op unless a live index covers the file's table. Planned before the
    // translation so a poisoned definition rides the writes it produces, and
    // returned as writes rather than staged so the slot envelope carries them.
    let (poisoned, index_writes) = stage_index_maintenance(
        handle,
        overlay,
        backing.probe(),
        base_ref,
        ops,
        data_store,
        data_prefix,
    )
    .await?;

    let (result_id, mut writes, changes_made) = if mints_snapshot {
        let (new_id, mut writes, mut snap) = translate(base_ref, ops, &poisoned)?;
        snap.transaction_id = transaction_id.map(|id| id.to_vec());
        let changes_made = snap.changes_made.clone();
        writes.push((
            Key::Snapshot {
                snapshot_id: new_id,
            }
            .encode(),
            Some(value::encode_value(&snap)),
        ));
        writes.push((
            Key::Sys(SysKey::Head).encode(),
            Some(value::encode_value(&proto::HeadValue {
                snapshot_id: new_id,
            })),
        ));
        (new_id, writes, changes_made)
    } else {
        let writes = translate_maintenance(base_ref, ops)?;
        (base_ref.snapshot.snapshot_id, writes, String::new())
    };

    writes.extend(index_writes);
    writes.extend(inline_writes);

    Ok(Assembly {
        result_id,
        writes,
        mints_snapshot,
        base,
        transaction_id,
        changes_made,
    })
}

/// Lands an assembled batch through the single-writer transaction, folding a
/// success into the shared projection state. A lost write-write race surfaces
/// as [`Error::CommitConflict`], never retried here.
async fn commit_tx(
    db_tx: DbTransaction,
    assembled: Result<Assembly>,
    projections: &std::sync::RwLock<ProjectionCache>,
    staged_rows: usize,
    started: std::time::Instant,
) -> Result<SnapshotId> {
    let assembly = match assembled {
        Ok(assembly) => assembly,
        Err(err) => {
            db_tx.rollback();
            return Err(err);
        }
    };

    if let Err(err) = commit::stage_writes(&db_tx, &assembly.writes) {
        db_tx.rollback();
        return Err(err);
    }
    // A head-preserving (maintenance) commit reuses the head id; drop the
    // cache before the write is visible so no concurrent attempt reads a stale
    // view that still matches by id.
    if !assembly.mints_snapshot {
        invalidate_head_view(projections);
    }
    match db_tx.commit_with_options(&commit::durable()).await {
        Ok(_) => {
            fold_committed_batch(projections, &assembly.writes, assembly.result_id);
            if assembly.mints_snapshot {
                commit::refresh_head_view(projections, assembly.base.as_ref(), &assembly.writes);
            }
            staged_landed(assembly.result_id, staged_rows, started);
            Ok(SnapshotId::new(assembly.result_id))
        }
        Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
            Err(staged_lost_race(assembly.result_id, staged_rows))
        }
        Err(err) => Err(err.into()),
    }
}

/// Lands an assembled batch through one slot at `head.next_sequence` — a
/// single attempt, deliberately not `drive_commit`: DuckLake authored the
/// ids, so a lost race cannot be rebased and re-run, only surfaced. An
/// ambiguous put still resolves inside `commit_slot` by transaction-id
/// read-back; only *rebasing onto a winner* is forbidden here.
///
/// One consequence of the single attempt: unlike the verb path's
/// `drive_commit`, this does not back off and retry a transient slot
/// contention. A `commit_slot` that returns `Transport` — including the
/// "reported taken but reads absent" read-back on a healthy but contended log
/// — surfaces to DuckLake as [`Error::SlotLog`], terminal by design (it carries
/// none of DuckLake's retry substrings). DuckLake re-drives the whole
/// transaction, since it cannot re-derive its authored ids against a new head.
async fn commit_slots(
    head: SlotHead,
    slots: SlotLog,
    assembled: Result<Assembly>,
    staged_rows: usize,
    started: std::time::Instant,
) -> Result<SnapshotId> {
    let outcome = match assembled {
        Ok(assembly) => {
            let transaction_id = assembly
                .transaction_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().into_bytes());
            let validated_head = head.view.snapshot.snapshot_id;
            let envelope = Envelope {
                commits: vec![commit_from(
                    transaction_id,
                    validated_head,
                    assembly.changes_made,
                    &assembly.writes,
                )],
            };
            match slots.commit_slot(head.next_sequence, &envelope).await {
                Ok(CommitOutcome::Won) => {
                    staged_landed(assembly.result_id, staged_rows, started);
                    Ok(SnapshotId::new(assembly.result_id))
                }
                Ok(CommitOutcome::Lost(_)) => {
                    Err(staged_lost_race(assembly.result_id, staged_rows))
                }
                Err(err) => Err(err.into()),
            }
        }
        Err(err) => Err(err),
    };

    release_reader(head.reader.as_ref()).await;
    outcome
}

/// One landed staged commit's summary event.
fn staged_landed(result_id: u64, staged_rows: usize, started: std::time::Instant) {
    debug!(
        snapshot = result_id,
        staged_rows,
        elapsed_ms = started.elapsed().as_millis(),
        "staged commit landed"
    );
}

/// The lost-race error for a staged commit, logged as it is built:
/// DuckLake's own loop re-drives the loser, so the log line is the only
/// visible trace of the race.
fn staged_lost_race(result_id: u64, staged_rows: usize) -> Error {
    debug!(
        attempted_snapshot = result_id,
        staged_rows, "staged commit lost a write-write race; DuckLake re-drives"
    );
    Error::CommitConflict(format!(
        "a concurrent commit changed state this one read or wrote \
         (attempted snapshot {result_id}); staged-row commits are never \
         retried internally"
    ))
}

/// Applies every op onto a clone of `base`, then diffs the two exactly as
/// a verb-path commit diffs its closure's output.
fn translate(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
    poisoned: &[u64],
) -> Result<(u64, Vec<commit::StagedWrite>, proto::SnapshotValue)> {
    let snapshot = build_snapshot_value(ops)?;
    let new_id = snapshot.snapshot_id;

    // Ends and deletes apply before inserts, independent of DuckLake's
    // emit order: a rename ends the old version and inserts a new one
    // under the same id, and the insert must win their shared `current` key —
    // an end applied afterward would delete the id and erase the new row.
    // Begin-rebases apply last: their target is the row an insert in this
    // same commit created. Inline ops are skipped here entirely —
    // `commit` translates them separately via `translate_inline`, since
    // `CatalogSnapshot` has no notion of inlined rows to diff.
    let mut state = base.clone();
    let mut children = collect_child_rows(ops)?;
    let mut direct = Vec::new();
    for op in ops {
        if !is_inline_op(op)
            && !matches!(
                op,
                RowOperation::Insert { .. } | RowOperation::UpdateSetBegin { .. }
            )
        {
            apply_op(base, &mut state, op, new_id, &mut children, &mut direct)?;
        }
    }
    for op in ops {
        if matches!(op, RowOperation::Insert { .. }) {
            apply_op(base, &mut state, op, new_id, &mut children, &mut direct)?;
        }
    }
    for op in ops {
        if matches!(op, RowOperation::UpdateSetBegin { .. }) {
            apply_op(base, &mut state, op, new_id, &mut children, &mut direct)?;
        }
    }

    if !children.partition_columns.is_empty() {
        return Err(corrupt_row(
            TableKind::PartitionColumn,
            "partition_column rows without a matching partition_info insert in this commit",
        ));
    }
    if !children.sort_expressions.is_empty() {
        return Err(corrupt_row(
            TableKind::SortExpression,
            "sort_expression rows without a matching sort_info insert in this commit",
        ));
    }
    if !children.file_partition_values.is_empty() {
        return Err(corrupt_row(
            TableKind::FilePartitionValue,
            "file_partition_value rows without a matching data_file insert in this commit",
        ));
    }
    if !children.macro_implementations.is_empty() {
        return Err(corrupt_row(
            TableKind::MacroImpl,
            "macro_impl rows without a matching macro insert in this commit",
        ));
    }
    if !children.macro_parameters.is_empty() {
        return Err(corrupt_row(
            TableKind::MacroParameters,
            "macro_parameters rows without a matching macro_impl in this commit",
        ));
    }
    if !children.name_mappings.is_empty() {
        return Err(corrupt_row(
            TableKind::NameMapping,
            "name_mapping rows without a matching column_mapping insert in this commit",
        ));
    }

    // DuckLake authors column ids itself, so its inserts advance no
    // counter; float each table's field-id counter above every live id so
    // a later verb-path `add_column` can never re-allocate one.
    for (table_id, columns) in &state.columns {
        let Some(max_id) = columns.keys().max() else {
            continue;
        };
        if let Some(table) = state.tables.get_mut(table_id)
            && table.next_column_id <= *max_id
        {
            table.next_column_id = max_id + 1;
        }
    }

    crate::transaction::index_maintenance::apply_poison(&mut state, poisoned);

    let mut writes = commit::diff_writes(base, &state, new_id);
    writes.extend(direct);
    Ok((new_id, writes, snapshot))
}

/// Translates a head-preserving maintenance commit: snapshot expiry and
/// file cleanup arrive with no `ducklake_snapshot` insert (DuckLake mints
/// no snapshot for them), so nothing advances head and no snapshot record
/// is written. Only reclamation-shaped operations are legal — raw
/// deletes, schedule inserts, and the inline drops a dead table's cleanup
/// issues; any entity insert or lifecycle update without a snapshot row
/// is a constraint violation (DuckLake always mints a snapshot for real
/// catalog changes).
fn translate_maintenance(
    base: &CatalogSnapshot,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let head = base.snapshot.snapshot_id;

    let mut state = base.clone();
    let mut children = ChildRows::default();
    let mut direct = Vec::new();
    for op in ops {
        let allowed = matches!(
            op,
            RowOperation::Delete { .. }
                | RowOperation::Insert {
                    table: TableKind::FilesScheduledForDeletion,
                    ..
                }
        ) || is_inline_op(op);
        if !allowed {
            return Err(Error::Constraint(
                "a staged commit without a ducklake_snapshot insert may only reclaim state \
                 (maintenance deletes and deletion-schedule inserts)"
                    .to_string(),
            ));
        }
        apply_op(base, &mut state, op, head, &mut children, &mut direct)?;
    }

    let mut writes = commit::diff_writes(base, &state, head);
    writes.extend(direct);
    Ok(writes)
}
