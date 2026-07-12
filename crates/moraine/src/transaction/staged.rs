//! The staged-row commit path: DuckLake authors rows over the ABI instead
//! of driving [`crate::transaction::Transaction`]'s verbs, and this module
//! turns the accumulated rows into one atomic store write.
//!
//! Three rules bound this path (the extension surface's staged-row
//! contract): every value DuckLake supplies is stored **verbatim** except
//! one interpreted convention (an `UPDATE` setting `end_snapshot` becomes
//! cur-delete + hist-write); one commit is one atomic batch, reusing the
//! same [`super::commit::diff_writes`] transition logic the verb path
//! already proves; and a lost race at commit is **never retried** — DuckLake
//! authored the ids and counters in the batch, so re-deriving them here
//! would be silent surgery on another system's data. The loser's error
//! always carries the substring `conflict` (via [`Error::CommitConflict`]'s
//! `Display`), matching the wire contract DuckLake's own retry loop scans
//! for.
//!
//! Translation reuses [`super::commit::materialize`] and
//! [`super::commit::diff_writes`] rather than re-deriving cur/hist
//! transitions: every staged row is applied to a cloned working
//! [`CatalogSnapshot`], then diffed against the unmodified base exactly as
//! a verb-path commit diffs its closure's output. This is also why an
//! `UPDATE ... SET end_snapshot` row's value is validated (not just
//! trusted): DuckLake always sets it to this same commit's own new
//! snapshot id (the `{SNAPSHOT_ID}` template substituted into every
//! staged-row SQL statement in one batch — pinned from
//! `DuckLakeMetadataManager::SubstituteSnapshotPlaceholders`), which is
//! exactly the value `diff_writes` stamps on its own; asserting the two
//! agree catches drift loudly instead of silently diverging from a future
//! DuckLake version's behavior.

use slatedb::DbTransaction;

use crate::{
    catalog::{CatalogSnapshot, SnapshotId},
    error::{Error, Result},
    store::{
        key::{EntityKey, Key, SysKey},
        proto, value,
    },
    transaction::commit,
};

/// Which `ducklake_*` table a staged row targets. `Snapshot`,
/// `SnapshotChanges`, and `SchemaVersions` all fold into one moraine
/// `snap` record; every other kind maps to one `EntityKey` variant or one
/// of the three unversioned statistics kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// `ducklake_schema_versions`: DuckLake's per-table schema-change
    /// history, one `(begin_snapshot, schema_version, table_id)` row per
    /// created-or-schema-altered table per commit. The first two values
    /// are always the committing snapshot's own id and `schema_version`
    /// (`DuckLakeMetadataManager::InsertNewSchema` writes
    /// `commit_snapshot`'s values verbatim), so the table-id set is the
    /// only new information — folded into the snap record's
    /// `schema_changed_table_ids`, with both redundant values validated
    /// against the `ducklake_snapshot` row in the same batch.
    SchemaVersions,
}

/// One column value in a staged row, typed to the small set of primitive
/// kinds every `ducklake_*` column uses. The ABI boundary carries values
/// this loosely typed (rather than one C struct per table) precisely so
/// rows "flow to the ABI, never interpreted in C++" — decoding into the
/// right proto shape happens here, table-kind by table-kind.
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
pub enum RowOp {
    /// A new row: becomes a live `cur` record (versioned kinds) or
    /// overwrites the current record in place (unversioned statistics
    /// kinds).
    Insert {
        /// The row's table.
        table: TableKind,
        /// The row's column values, in table order.
        cells: Vec<Cell>,
    },
    /// A row removed with no history mirror. Defined only for the three
    /// unversioned statistics kinds — DuckLake never issues a raw
    /// `DELETE` against a versioned kind, always the `UPDATE` convention
    /// below.
    Delete {
        /// The row's table.
        table: TableKind,
        /// The removed row's key columns, in table order (id columns
        /// only — see [`Self::Insert`]'s `cells` for the full row shape).
        cells: Vec<Cell>,
    },
    /// An `UPDATE ... SET end_snapshot = <v>` row: the one lifecycle
    /// convention this path interprets — ends the live version (moves it
    /// to `hist`). Defined only for the six versioned kinds.
    UpdateSetEnd {
        /// The row's table.
        table: TableKind,
        /// The ended row's key columns, in table order, followed by the
        /// new `end_snapshot` value.
        cells: Vec<Cell>,
    },
}

/// A malformed staged row: wrong cell count or a cell of the wrong kind
/// for its column. Never produced by a correct shim translation — this is
/// the "fail loudly on data this binary does not understand" path, not an
/// expected runtime outcome.
fn corrupt_row(table: TableKind, detail: impl std::fmt::Display) -> Error {
    Error::Corruption(format!("staged row for {table:?}: {detail}"))
}

/// A positional reader over one staged row's cells, typed cell-by-cell
/// against the column each `ducklake_*` table declares.
struct Cursor<'a> {
    table: TableKind,
    items: std::slice::Iter<'a, Cell>,
}

impl<'a> Cursor<'a> {
    fn new(table: TableKind, cells: &'a [Cell]) -> Self {
        Self {
            table,
            items: cells.iter(),
        }
    }

    fn next(&mut self) -> Result<&'a Cell> {
        self.items
            .next()
            .ok_or_else(|| corrupt_row(self.table, "too few cells"))
    }

    fn u64(&mut self) -> Result<u64> {
        match self.next()? {
            Cell::U64(v) => Ok(*v),
            other => Err(corrupt_row(
                self.table,
                format!("expected u64, got {other:?}"),
            )),
        }
    }

    fn opt_u64(&mut self) -> Result<Option<u64>> {
        match self.next()? {
            Cell::Null => Ok(None),
            Cell::U64(v) => Ok(Some(*v)),
            other => Err(corrupt_row(
                self.table,
                format!("expected optional u64, got {other:?}"),
            )),
        }
    }

    fn i64(&mut self) -> Result<i64> {
        match self.next()? {
            Cell::I64(v) => Ok(*v),
            other => Err(corrupt_row(
                self.table,
                format!("expected i64, got {other:?}"),
            )),
        }
    }

    fn bool(&mut self) -> Result<bool> {
        match self.next()? {
            Cell::Bool(v) => Ok(*v),
            other => Err(corrupt_row(
                self.table,
                format!("expected bool, got {other:?}"),
            )),
        }
    }

    fn opt_bool(&mut self) -> Result<Option<bool>> {
        match self.next()? {
            Cell::Null => Ok(None),
            Cell::Bool(v) => Ok(Some(*v)),
            other => Err(corrupt_row(
                self.table,
                format!("expected optional bool, got {other:?}"),
            )),
        }
    }

    fn string(&mut self) -> Result<String> {
        match self.next()? {
            Cell::Str(v) => Ok(v.clone()),
            other => Err(corrupt_row(
                self.table,
                format!("expected string, got {other:?}"),
            )),
        }
    }

    fn opt_string(&mut self) -> Result<Option<String>> {
        match self.next()? {
            Cell::Null => Ok(None),
            Cell::Str(v) => Ok(Some(v.clone())),
            other => Err(corrupt_row(
                self.table,
                format!("expected optional string, got {other:?}"),
            )),
        }
    }

    /// Confirms every cell was consumed — a row longer than its table's
    /// column list is as much a shape mismatch as one too short.
    fn finish(mut self) -> Result<()> {
        if self.items.next().is_some() {
            Err(corrupt_row(self.table, "too many cells"))
        } else {
            Ok(())
        }
    }
}

fn decode_schema(cells: &[Cell]) -> Result<proto::SchemaValue> {
    let mut c = Cursor::new(TableKind::Schema, cells);
    let value = proto::SchemaValue {
        schema_id: c.u64()?,
        schema_uuid: c.string()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        schema_name: c.string()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
    };
    c.finish()?;
    Ok(value)
}

/// `ducklake_table`'s row shape, minus `next_column_id` — moraine-internal
/// bookkeeping DuckLake never authors (see [`table_value`]).
struct TableCells {
    table_id: u64,
    table_uuid: String,
    begin_snapshot: u64,
    end_snapshot: Option<u64>,
    schema_id: u64,
    table_name: String,
    path: String,
    path_is_relative: bool,
}

fn decode_table(cells: &[Cell]) -> Result<TableCells> {
    let mut c = Cursor::new(TableKind::Table, cells);
    let value = TableCells {
        table_id: c.u64()?,
        table_uuid: c.string()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        schema_id: c.u64()?,
        table_name: c.string()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
    };
    c.finish()?;
    Ok(value)
}

/// Builds the full `TableValue`: `next_column_id` is moraine-internal
/// per-table field-id bookkeeping (RFC-amendment counter, not a DuckLake
/// column — the dump ABI does not even serve it back, see
/// `MoraineTableRow`'s doc comment), read by nothing on the staged-row
/// path today. Carried forward from the table's prior version in `base`
/// when one exists (floor of 1 for a brand-new id) rather than derived
/// from this batch's column inserts: a DuckLake-authored table is never
/// later mutated through the verb path's `add_column` in this slice, so
/// the exact value is inert data, not a correctness-bearing one.
fn table_value(base: &CatalogSnapshot, cells: TableCells) -> proto::TableValue {
    let next_column_id = base
        .tables
        .get(&cells.table_id)
        .map_or(1, |t| t.next_column_id);
    proto::TableValue {
        table_id: cells.table_id,
        table_uuid: cells.table_uuid,
        begin_snapshot: cells.begin_snapshot,
        end_snapshot: cells.end_snapshot,
        schema_id: cells.schema_id,
        table_name: cells.table_name,
        path: cells.path,
        path_is_relative: cells.path_is_relative,
        next_column_id,
    }
}

fn decode_view(cells: &[Cell]) -> Result<proto::ViewValue> {
    let mut c = Cursor::new(TableKind::View, cells);
    let value = proto::ViewValue {
        view_id: c.u64()?,
        view_uuid: c.string()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        schema_id: c.u64()?,
        view_name: c.string()?,
        dialect: c.string()?,
        sql: c.string()?,
        column_aliases: c.opt_string()?,
    };
    c.finish()?;
    Ok(value)
}

fn decode_column(cells: &[Cell]) -> Result<proto::ColumnValue> {
    let mut c = Cursor::new(TableKind::Column, cells);
    let value = proto::ColumnValue {
        column_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        table_id: c.u64()?,
        column_order: c.u64()?,
        column_name: c.string()?,
        column_type: c.string()?,
        initial_default: c.opt_string()?,
        default_value: c.opt_string()?,
        nulls_allowed: c.bool()?,
        parent_column: c.opt_u64()?,
        default_value_type: c.opt_string()?,
        default_value_dialect: c.opt_string()?,
        tags: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

fn decode_data_file(cells: &[Cell]) -> Result<proto::DataFileValue> {
    let mut c = Cursor::new(TableKind::DataFile, cells);
    let value = proto::DataFileValue {
        data_file_id: c.u64()?,
        table_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        file_order: c.opt_u64()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
        file_format: c.string()?,
        record_count: c.u64()?,
        file_size_bytes: c.u64()?,
        footer_size: c.u64()?,
        row_id_start: c.u64()?,
        partition_id: c.opt_u64()?,
        encryption_key: c.opt_string()?,
        mapping_id: c.opt_u64()?,
        partial_max: c.opt_u64()?,
        partition_values: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

fn decode_delete_file(cells: &[Cell]) -> Result<proto::DeleteFileValue> {
    let mut c = Cursor::new(TableKind::DeleteFile, cells);
    let value = proto::DeleteFileValue {
        delete_file_id: c.u64()?,
        table_id: c.u64()?,
        begin_snapshot: c.u64()?,
        end_snapshot: c.opt_u64()?,
        data_file_id: c.u64()?,
        path: c.string()?,
        path_is_relative: c.bool()?,
        format: c.string()?,
        delete_count: c.u64()?,
        file_size_bytes: c.u64()?,
        footer_size: c.u64()?,
        encryption_key: c.opt_string()?,
        partial_max: c.opt_u64()?,
    };
    c.finish()?;
    Ok(value)
}

fn decode_table_stats(cells: &[Cell]) -> Result<proto::TableStatsValue> {
    let mut c = Cursor::new(TableKind::TableStats, cells);
    let value = proto::TableStatsValue {
        table_id: c.u64()?,
        record_count: c.u64()?,
        next_row_id: c.u64()?,
        file_size_bytes: c.u64()?,
    };
    c.finish()?;
    Ok(value)
}

fn decode_table_column_stats(cells: &[Cell]) -> Result<proto::TableColumnStatsValue> {
    let mut c = Cursor::new(TableKind::TableColumnStats, cells);
    let value = proto::TableColumnStatsValue {
        table_id: c.u64()?,
        column_id: c.u64()?,
        contains_null: c.opt_bool()?,
        contains_nan: c.opt_bool()?,
        min_value: c.opt_string()?,
        max_value: c.opt_string()?,
        extra_stats: c.opt_string()?,
    };
    c.finish()?;
    Ok(value)
}

fn decode_file_column_stats(cells: &[Cell]) -> Result<proto::FileColumnStatsValue> {
    let mut c = Cursor::new(TableKind::FileColumnStats, cells);
    let value = proto::FileColumnStatsValue {
        data_file_id: c.u64()?,
        table_id: c.u64()?,
        column_id: c.u64()?,
        column_size_bytes: c.u64()?,
        value_count: c.u64()?,
        null_count: c.u64()?,
        min_value: c.opt_string()?,
        max_value: c.opt_string()?,
        contains_nan: c.opt_bool()?,
        extra_stats: c.opt_string()?,
        variant_stats: Vec::new(),
    };
    c.finish()?;
    Ok(value)
}

/// Decodes an `UPDATE ... SET end_snapshot` row into the ended entity's
/// key and the new `end_snapshot` value. Defined only for the six
/// versioned kinds.
fn decode_end(table: TableKind, cells: &[Cell]) -> Result<(EntityKey, u64)> {
    let mut c = Cursor::new(table, cells);
    let key = match table {
        TableKind::Schema => EntityKey::Schema {
            schema_id: c.u64()?,
        },
        TableKind::Table => EntityKey::Table { table_id: c.u64()? },
        TableKind::View => EntityKey::View { view_id: c.u64()? },
        TableKind::Column => EntityKey::Column {
            table_id: c.u64()?,
            column_id: c.u64()?,
        },
        TableKind::DataFile => EntityKey::File {
            table_id: c.u64()?,
            data_file_id: c.u64()?,
        },
        TableKind::DeleteFile => EntityKey::DeleteFile {
            table_id: c.u64()?,
            delete_file_id: c.u64()?,
        },
        TableKind::Snapshot
        | TableKind::SnapshotChanges
        | TableKind::SchemaVersions
        | TableKind::TableStats
        | TableKind::TableColumnStats
        | TableKind::FileColumnStats => {
            return Err(Error::Constraint(format!(
                "update_set_end is not defined for {table:?}"
            )));
        }
    };
    let end_snapshot = c.u64()?;
    c.finish()?;
    Ok((key, end_snapshot))
}

/// Decodes a raw-delete row's key. Defined only for the three unversioned
/// statistics kinds.
enum StatsKey {
    Table(u64),
    Column(u64, u64),
    FileColumn(u64, u64, u64),
}

fn decode_delete_key(table: TableKind, cells: &[Cell]) -> Result<StatsKey> {
    let mut c = Cursor::new(table, cells);
    let key = match table {
        TableKind::TableStats => StatsKey::Table(c.u64()?),
        TableKind::TableColumnStats => StatsKey::Column(c.u64()?, c.u64()?),
        TableKind::FileColumnStats => {
            let data_file_id = c.u64()?;
            let table_id = c.u64()?;
            let column_id = c.u64()?;
            StatsKey::FileColumn(table_id, data_file_id, column_id)
        }
        _ => {
            return Err(Error::Constraint(format!(
                "delete is not defined for {table:?}"
            )));
        }
    };
    c.finish()?;
    Ok(key)
}

/// Applies one staged row to the working snapshot. `new_id` is this
/// commit's own new snapshot id — the only value an `UpdateSetEnd` row's
/// `end_snapshot` cell is ever allowed to carry (see the module doc).
fn apply_op(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    op: &RowOp,
    new_id: u64,
) -> Result<()> {
    match op {
        RowOp::Insert { table, cells } => apply_insert(base, state, *table, cells),
        RowOp::UpdateSetEnd { table, cells } => apply_update_set_end(state, *table, cells, new_id),
        RowOp::Delete { table, cells } => apply_delete(state, *table, cells),
    }
}

fn apply_insert(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
) -> Result<()> {
    match table {
        // Folded into the snapshot record separately; not an entity mutation.
        TableKind::Snapshot | TableKind::SnapshotChanges | TableKind::SchemaVersions => {}
        TableKind::Schema => state.put_schema(decode_schema(cells)?),
        TableKind::Table => state.put_table(table_value(base, decode_table(cells)?)),
        TableKind::View => state.put_view(decode_view(cells)?),
        TableKind::Column => state.put_column(decode_column(cells)?),
        TableKind::DataFile => state.put_data_file(decode_data_file(cells)?),
        TableKind::DeleteFile => state.put_delete_file(decode_delete_file(cells)?),
        TableKind::TableStats => state.put_table_stats(decode_table_stats(cells)?),
        TableKind::TableColumnStats => {
            state.put_table_column_stats(decode_table_column_stats(cells)?);
        }
        TableKind::FileColumnStats => state.put_file_column_stats(decode_file_column_stats(cells)?),
    }
    Ok(())
}

fn apply_update_set_end(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    new_id: u64,
) -> Result<()> {
    let (key, end_snapshot) = decode_end(table, cells)?;
    if end_snapshot != new_id {
        return Err(corrupt_row(
            table,
            format!(
                "end_snapshot {end_snapshot} does not match this commit's snapshot id {new_id}"
            ),
        ));
    }
    // End only the one row DuckLake named — never a cascade. DuckLake
    // authors every row change explicitly, so ending a table version
    // leaves its columns, files, and stats exactly as DuckLake left them
    // (a rename ends the table row but keeps its columns live). The
    // verb-path `delete_*` helpers cascade and would end those siblings,
    // corrupting the row-faithful projection DuckLake reads back.
    match key {
        EntityKey::Schema { schema_id } => {
            state.schemas.remove(&schema_id);
        }
        EntityKey::Table { table_id } => {
            state.tables.remove(&table_id);
        }
        EntityKey::View { view_id } => {
            state.views.remove(&view_id);
        }
        EntityKey::Column {
            table_id,
            column_id,
        } => {
            if let Some(columns) = state.columns.get_mut(&table_id) {
                columns.remove(&column_id);
            }
        }
        EntityKey::File {
            table_id,
            data_file_id,
        } => {
            if let Some(files) = state.data_files.get_mut(&table_id) {
                files.remove(&data_file_id);
            }
        }
        EntityKey::DeleteFile {
            table_id,
            delete_file_id,
        } => {
            if let Some(files) = state.delete_files.get_mut(&table_id) {
                files.remove(&delete_file_id);
            }
        }
        // decode_end only ever returns the six keys matched above.
        _ => return Err(corrupt_row(table, "unreachable entity key")),
    }
    Ok(())
}

fn apply_delete(state: &mut CatalogSnapshot, table: TableKind, cells: &[Cell]) -> Result<()> {
    match decode_delete_key(table, cells)? {
        StatsKey::Table(table_id) => {
            state.table_stats.remove(&table_id);
        }
        StatsKey::Column(table_id, column_id) => {
            if let Some(cols) = state.table_column_stats.get_mut(&table_id) {
                cols.remove(&column_id);
            }
        }
        StatsKey::FileColumn(table_id, data_file_id, column_id) => {
            if let Some(cols) = state.file_column_stats.get_mut(&table_id) {
                cols.remove(&(data_file_id, column_id));
            }
        }
    }
    Ok(())
}

/// The two DuckLake always inserts as one pair (`InitializeDuckLake`'s
/// bootstrap SQL, and every ordinary commit's `WriteNewSnapshotSql` /
/// `WriteSnapshotChangesSql`) — required, since `ducklake_metadata` writes
/// (the only DuckLake mutation outside the snapshot protocol) are deferred
/// this slice.
fn find_snapshot_rows(ops: &[RowOp]) -> Result<(&[Cell], &[Cell])> {
    let mut snapshot = None;
    let mut changes = None;
    for op in ops {
        if let RowOp::Insert { table, cells } = op {
            match table {
                TableKind::Snapshot => snapshot = Some(cells.as_slice()),
                TableKind::SnapshotChanges => changes = Some(cells.as_slice()),
                _ => {}
            }
        }
    }
    let snapshot = snapshot.ok_or_else(|| {
        Error::Constraint("staged commit requires a ducklake_snapshot insert".to_string())
    })?;
    let changes = changes.ok_or_else(|| {
        Error::Constraint("staged commit requires a ducklake_snapshot_changes insert".to_string())
    })?;
    Ok((snapshot, changes))
}

fn build_snapshot_value(ops: &[RowOp], next_deletion_id: u64) -> Result<proto::SnapshotValue> {
    let (snapshot_cells, changes_cells) = find_snapshot_rows(ops)?;

    let mut s = Cursor::new(TableKind::Snapshot, snapshot_cells);
    let snapshot_id = s.u64()?;
    let snapshot_time_micros = s.i64()?;
    let schema_version = s.u64()?;
    let next_catalog_id = s.u64()?;
    let next_file_id = s.u64()?;
    s.finish()?;

    let mut c = Cursor::new(TableKind::SnapshotChanges, changes_cells);
    let changes_snapshot_id = c.u64()?;
    let changes_made = c.string()?;
    let author = c.opt_string()?;
    let commit_message = c.opt_string()?;
    let commit_extra_info = c.opt_string()?;
    c.finish()?;

    if changes_snapshot_id != snapshot_id {
        return Err(corrupt_row(
            TableKind::SnapshotChanges,
            format!(
                "snapshot_id {changes_snapshot_id} does not match ducklake_snapshot's {snapshot_id}"
            ),
        ));
    }

    // `ducklake_schema_versions` rows fold in as the table-id set; the two
    // redundant columns are validated rather than trusted, for the same
    // reason an `UpdateSetEnd` row's `end_snapshot` is (see the module
    // doc): DuckLake always writes this commit's own snapshot values here,
    // so a mismatch is drift to catch loudly, not data to store.
    let mut schema_changed_table_ids = Vec::new();
    for op in ops {
        if let RowOp::Insert {
            table: TableKind::SchemaVersions,
            cells,
        } = op
        {
            let mut v = Cursor::new(TableKind::SchemaVersions, cells);
            let begin_snapshot = v.u64()?;
            let row_schema_version = v.u64()?;
            let table_id = v.u64()?;
            v.finish()?;
            if begin_snapshot != snapshot_id || row_schema_version != schema_version {
                return Err(corrupt_row(
                    TableKind::SchemaVersions,
                    format!(
                        "(begin_snapshot {begin_snapshot}, schema_version {row_schema_version}) \
                         does not match ducklake_snapshot's ({snapshot_id}, {schema_version})"
                    ),
                ));
            }
            schema_changed_table_ids.push(table_id);
        }
    }

    Ok(proto::SnapshotValue {
        snapshot_id,
        snapshot_time_micros,
        schema_version,
        next_catalog_id,
        next_file_id,
        next_deletion_id,
        changes_made,
        author,
        commit_message,
        commit_extra_info,
        schema_changed_table_ids,
    })
}

/// A staged-row transaction: one SlateDB transaction opened by
/// `begin` (crate-internal; [`crate::ffi_support::staged::staged_begin`]
/// is the entry point outside this crate), accumulating [`RowOp`]s via
/// [`stage`](Self::stage) until [`commit`](Self::commit) translates and
/// lands them all in one atomic batch, or [`rollback`](Self::rollback)
/// discards them.
pub struct StagedTransaction {
    db_tx: DbTransaction,
    ops: Vec<RowOp>,
}

impl StagedTransaction {
    /// Opens a fresh transaction at the current head. Nothing is staged
    /// yet; [`stage`](Self::stage) accumulates rows in memory only.
    pub(crate) fn begin(db_tx: DbTransaction) -> Self {
        Self {
            db_tx,
            ops: Vec::new(),
        }
    }

    /// Accumulates one row mutation. Nothing touches the store until
    /// [`commit`](Self::commit).
    pub fn stage(&mut self, op: RowOp) {
        self.ops.push(op);
    }

    /// Discards every staged row without writing anything.
    pub fn rollback(self) {
        self.db_tx.rollback();
    }

    /// Translates every staged row and lands them in one atomic batch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] or [`Error::Corruption`] if the
    /// staged rows are malformed or omit the required `ducklake_snapshot`
    /// / `ducklake_snapshot_changes` pair. Returns
    /// [`Error::CommitConflict`] — **never retried internally** — if a
    /// concurrent commit advanced the head first; the store is left
    /// unchanged by the loser.
    pub async fn commit(self) -> Result<SnapshotId> {
        let Self { db_tx, ops } = self;

        let base = match commit::materialize(&db_tx, None).await {
            Ok(base) => base,
            Err(err) => {
                db_tx.rollback();
                return Err(err);
            }
        };

        match translate(&base, &ops) {
            Ok((new_id, mut writes, snap)) => {
                writes.push((
                    Key::Snap {
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
                if let Err(err) = commit::stage_writes(&db_tx, writes) {
                    db_tx.rollback();
                    return Err(err);
                }
                match db_tx.commit_with_options(&commit::durable()).await {
                    Ok(_) => Ok(SnapshotId::new(new_id)),
                    Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
                        Err(Error::CommitConflict(format!(
                            "concurrent commit advanced the head past snapshot {new_id}; \
                             staged-row commits are never retried internally"
                        )))
                    }
                    Err(err) => Err(err.into()),
                }
            }
            Err(err) => {
                db_tx.rollback();
                Err(err)
            }
        }
    }
}

/// Applies every op onto a clone of `base`, then diffs the two exactly as
/// a verb-path commit diffs its closure's output.
fn translate(
    base: &CatalogSnapshot,
    ops: &[RowOp],
) -> Result<(u64, Vec<commit::StagedWrite>, proto::SnapshotValue)> {
    let snap = build_snapshot_value(ops, base.snap.next_deletion_id)?;
    let new_id = snap.snapshot_id;

    // Ends and deletes apply before inserts, independent of DuckLake's
    // emit order: a rename ends the old version and inserts a new one
    // under the same id, and the insert must win their shared `cur` key —
    // an end applied afterward would delete the id and erase the new row.
    let mut state = base.clone();
    for op in ops {
        if !matches!(op, RowOp::Insert { .. }) {
            apply_op(base, &mut state, op, new_id)?;
        }
    }
    for op in ops {
        if matches!(op, RowOp::Insert { .. }) {
            apply_op(base, &mut state, op, new_id)?;
        }
    }

    let writes = commit::diff_writes(base, &state, new_id);
    Ok((new_id, writes, snap))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::catalog::{Catalog, CatalogOptions};

    fn schema_row(id: u64, name: &str, begin: u64) -> Vec<Cell> {
        vec![
            Cell::U64(id),
            Cell::Str(format!("uuid-{id}")),
            Cell::U64(begin),
            Cell::Null,
            Cell::Str(name.to_string()),
            Cell::Str(format!("{name}/")),
            Cell::Bool(true),
        ]
    }

    fn table_row(id: u64, schema_id: u64, name: &str, begin: u64, end: Option<u64>) -> Vec<Cell> {
        vec![
            Cell::U64(id),
            Cell::Str(format!("uuid-t{id}")),
            Cell::U64(begin),
            end.map_or(Cell::Null, Cell::U64),
            Cell::U64(schema_id),
            Cell::Str(name.to_string()),
            Cell::Str(format!("{name}/")),
            Cell::Bool(true),
        ]
    }

    fn column_row(table_id: u64, column_id: u64, name: &str, order: u64) -> Vec<Cell> {
        vec![
            Cell::U64(column_id),
            Cell::U64(0),
            Cell::Null,
            Cell::U64(table_id),
            Cell::U64(order),
            Cell::Str(name.to_string()),
            Cell::Str("BIGINT".to_string()),
            Cell::Null,
            Cell::Null,
            Cell::Bool(true),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ]
    }

    fn snapshot_row(id: u64, schema_version: u64, next_catalog_id: u64) -> Vec<Cell> {
        vec![
            Cell::U64(id),
            Cell::I64(1),
            Cell::U64(schema_version),
            Cell::U64(next_catalog_id),
            Cell::U64(0),
        ]
    }

    fn snapshot_changes_row(id: u64, changes_made: &str) -> Vec<Cell> {
        vec![
            Cell::U64(id),
            Cell::Str(changes_made.to_string()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ]
    }

    async fn open() -> Catalog {
        Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap()
    }

    /// A DuckLake-shaped snapshot bump plus table create: table `t` (id
    /// 1, schema 0 = bootstrap's `main`) with one column, staged and
    /// committed as one batch, then verified through the ordinary
    /// snapshot read (the same view the dump ABI serves).
    #[tokio::test]
    async fn stages_table_create_and_snapshot_bump() {
        let catalog = open().await;
        let db_tx = catalog.begin_read_tx().await.unwrap();
        let mut txn = StagedTransaction::begin(db_tx);

        txn.stage(RowOp::Insert {
            table: TableKind::Table,
            cells: table_row(1, 0, "t", 1, None),
        });
        txn.stage(RowOp::Insert {
            table: TableKind::Column,
            cells: column_row(1, 1, "a", 0),
        });
        txn.stage(RowOp::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        txn.stage(RowOp::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
        });

        let id = txn.commit().await.unwrap();
        assert_eq!(id.get(), 1);

        let snap = catalog.snapshot().await.unwrap();
        let tables = snap.tables_in(crate::catalog::SchemaId::new(0));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "t");
        let cols = snap.columns_of(tables[0].id);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "a");
    }

    /// An `UPDATE ... SET end_snapshot` row ends a live table version:
    /// the old row moves to `hist`, the new one lands in `cur`, exactly
    /// the lifecycle convention this path interprets.
    #[tokio::test]
    async fn update_set_end_moves_the_old_version_to_hist() {
        let catalog = open().await;

        // Seed schema `s` (id 1) and table `t` (id 1) via a plain insert.
        let db_tx1 = catalog.begin_read_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOp::Insert {
            table: TableKind::Schema,
            cells: schema_row(1, "s", 1),
        });
        setup.stage(RowOp::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_old", 1, None),
        });
        setup.stage(RowOp::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        setup.stage(RowOp::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_schema:"s""#),
        });
        setup.commit().await.unwrap();

        // Rename: end the old table version, insert the renamed one.
        let db_tx2 = catalog.begin_read_tx().await.unwrap();
        let mut rename = StagedTransaction::begin(db_tx2);
        rename.stage(RowOp::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        });
        rename.stage(RowOp::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_new", 2, None),
        });
        rename.stage(RowOp::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 1, 2),
        });
        rename.stage(RowOp::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, "altered_table:1"),
        });
        rename.commit().await.unwrap();

        let head = catalog.snapshot().await.unwrap();
        assert_eq!(
            head.tables_in(crate::catalog::SchemaId::new(1))[0].name,
            "t_new"
        );

        let past = catalog
            .snapshot_at(crate::catalog::SnapshotId::new(1))
            .await
            .unwrap();
        assert_eq!(
            past.tables_in(crate::catalog::SchemaId::new(1))[0].name,
            "t_old"
        );
    }

    /// A rename staged in DuckLake's live order — the new version's
    /// insert *before* the old version's end — keeps the new version
    /// live. Translation applies ends before inserts, so the shared `cur`
    /// key resolves to the insert regardless of stage order.
    #[tokio::test]
    async fn rename_survives_insert_before_end_order() {
        let catalog = open().await;

        let db_tx1 = catalog.begin_read_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOp::Insert {
            table: TableKind::Schema,
            cells: schema_row(1, "s", 1),
        });
        setup.stage(RowOp::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_old", 1, None),
        });
        setup.stage(RowOp::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        setup.stage(RowOp::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_schema:"s""#),
        });
        setup.commit().await.unwrap();

        // Insert the renamed version first, then end the old one — the
        // reverse of the safe order, matching what DuckLake emits.
        let db_tx2 = catalog.begin_read_tx().await.unwrap();
        let mut rename = StagedTransaction::begin(db_tx2);
        rename.stage(RowOp::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_new", 2, None),
        });
        rename.stage(RowOp::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        });
        rename.stage(RowOp::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 1, 2),
        });
        rename.stage(RowOp::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, "altered_table:1"),
        });
        rename.commit().await.unwrap();

        let head = catalog.snapshot().await.unwrap();
        let live = head.tables_in(crate::catalog::SchemaId::new(1));
        assert_eq!(live.len(), 1, "exactly one live table after rename");
        assert_eq!(live[0].name, "t_new");
    }

    /// A lost race at commit is never retried: the loser's error carries
    /// the literal substring `conflict`, and the store is left exactly as
    /// the winner left it.
    #[tokio::test]
    async fn lost_race_is_not_retried_and_carries_conflict_text() {
        let catalog = open().await;

        let tx_a = catalog.begin_read_tx().await.unwrap();
        let tx_b = catalog.begin_read_tx().await.unwrap();
        let mut a = StagedTransaction::begin(tx_a);
        let mut b = StagedTransaction::begin(tx_b);

        for (txn, name) in [(&mut a, "a"), (&mut b, "b")] {
            txn.stage(RowOp::Insert {
                table: TableKind::Schema,
                cells: schema_row(1, name, 1),
            });
            txn.stage(RowOp::Insert {
                table: TableKind::Snapshot,
                cells: snapshot_row(1, 1, 2),
            });
            txn.stage(RowOp::Insert {
                table: TableKind::SnapshotChanges,
                cells: snapshot_changes_row(1, format!(r#"created_schema:"{name}""#).as_str()),
            });
        }

        a.commit().await.unwrap();
        let err = b.commit().await.unwrap_err();
        assert!(
            err.to_string().contains("conflict"),
            "error must carry the literal substring `conflict`: {err}"
        );

        // The store reflects only the winner: schema `a`, not `b`.
        let head = catalog.snapshot().await.unwrap();
        assert!(head.schema_by_name("a").is_some());
        assert!(head.schema_by_name("b").is_none());
    }

    /// A malformed staged row (wrong cell count) fails loudly as
    /// `Corruption` rather than panicking or silently truncating.
    #[tokio::test]
    async fn malformed_row_is_corruption_not_a_panic() {
        let catalog = open().await;
        let db_tx = catalog.begin_read_tx().await.unwrap();
        let mut txn = StagedTransaction::begin(db_tx);
        txn.stage(RowOp::Insert {
            table: TableKind::Schema,
            cells: vec![Cell::U64(1)], // far too few cells
        });
        txn.stage(RowOp::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        txn.stage(RowOp::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, ""),
        });
        let err = txn.commit().await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }
}
