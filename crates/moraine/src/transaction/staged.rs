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

use std::collections::HashMap;

use slatedb::DbTransaction;

use crate::{
    catalog::{
        CatalogSnapshot, SnapshotId,
        inline::{InlineScanKind, materialize_inline_rows},
    },
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        inline as store_inline,
        key::{EntityKey, InlineKey, InlineOperation, Key, SysKey},
        proto, value,
    },
    transaction::commit,
};

/// Which `ducklake_*` table a staged row targets. `Snapshot`,
/// `SnapshotChanges`, and `SchemaVersions` all fold into one moraine
/// `snapshot` record; every other kind maps to one `EntityKey` variant or one
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
    /// `ducklake_schema_versions`: per-table schema-change history, one
    /// `(begin_snapshot, schema_version, table_id)` row per
    /// created-or-schema-altered table per commit. The first two values
    /// are always the committing snapshot's own id and `schema_version`,
    /// so the table-id set is the only new information — folded into the
    /// snapshot record's `schema_changed_table_ids`, with both redundant values
    /// validated against the `ducklake_snapshot` row in the same batch.
    SchemaVersions,
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
    /// to `history`). Defined only for the six versioned kinds.
    UpdateSetEnd {
        /// The row's table.
        table: TableKind,
        /// The ended row's key columns, in table order, followed by the
        /// new `end_snapshot` value.
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
/// per-table field-id bookkeeping, not a DuckLake column. Carried forward
/// from the table's prior version in `base` when one exists (floor of 1
/// for a brand-new id).
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
    op: &RowOperation,
    new_id: u64,
) -> Result<()> {
    match op {
        RowOperation::Insert { table, cells } => apply_insert(base, state, *table, cells),
        RowOperation::UpdateSetEnd { table, cells } => {
            apply_update_set_end(state, *table, cells, new_id)
        }
        RowOperation::Delete { table, cells } => apply_delete(state, *table, cells),
        // Inline ops never reach here — `translate` routes them to
        // `translate_inline`. Kept only for match exhaustiveness.
        RowOperation::InlineSchema { .. }
        | RowOperation::InlineInsert { .. }
        | RowOperation::InlineInlineDelete { .. }
        | RowOperation::InlineFileDelete { .. }
        | RowOperation::InlineFlushDelete { .. }
        | RowOperation::InlineDrop { .. }
        | RowOperation::InlineSchemaDrop { .. } => Ok(()),
    }
}

/// Whether `op` is one of the seven inline variants — routed to
/// `translate_inline`, never to [`apply_op`]'s `CatalogSnapshot` diff.
fn is_inline_op(op: &RowOperation) -> bool {
    matches!(
        op,
        RowOperation::InlineSchema { .. }
            | RowOperation::InlineInsert { .. }
            | RowOperation::InlineInlineDelete { .. }
            | RowOperation::InlineFileDelete { .. }
            | RowOperation::InlineFlushDelete { .. }
            | RowOperation::InlineDrop { .. }
            | RowOperation::InlineSchemaDrop { .. }
    )
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
    // authors every row change explicitly (a rename ends the table row but
    // keeps its columns live); the verb-path `delete_*` helpers would
    // cascade and end those siblings.
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

/// The `ducklake_snapshot` and `ducklake_snapshot_changes` rows DuckLake
/// always inserts as one pair; both are required for a staged commit.
fn find_snapshot_rows(ops: &[RowOperation]) -> Result<(&[Cell], &[Cell])> {
    let mut snapshot = None;
    let mut changes = None;
    for op in ops {
        if let RowOperation::Insert { table, cells } = op {
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

fn build_snapshot_value(
    ops: &[RowOperation],
    next_deletion_id: u64,
) -> Result<proto::SnapshotValue> {
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
    // redundant columns are validated against this commit's own snapshot
    // values rather than trusted, so a mismatch is drift caught loudly.
    let schema_changed_table_ids = ops
        .iter()
        .filter_map(|op| match op {
            RowOperation::Insert {
                table: TableKind::SchemaVersions,
                cells,
            } => Some(cells),
            _ => None,
        })
        .map(|cells| {
            let mut cursor = Cursor::new(TableKind::SchemaVersions, cells);
            let begin_snapshot = cursor.u64()?;
            let row_schema_version = cursor.u64()?;
            let table_id = cursor.u64()?;
            cursor.finish()?;
            if begin_snapshot != snapshot_id || row_schema_version != schema_version {
                return Err(corrupt_row(
                    TableKind::SchemaVersions,
                    format!(
                        "(begin_snapshot {begin_snapshot}, schema_version {row_schema_version}) \
                         does not match ducklake_snapshot's ({snapshot_id}, {schema_version})"
                    ),
                ));
            }
            Ok(table_id)
        })
        .collect::<Result<Vec<_>>>()?;

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
/// is the entry point outside this crate), accumulating [`RowOperation`]s via
/// [`stage`](Self::stage) until [`commit`](Self::commit) translates and
/// lands them all in one atomic batch, or [`rollback`](Self::rollback)
/// discards them.
pub struct StagedTransaction {
    db_tx: DbTransaction,
    ops: Vec<RowOperation>,
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
    pub fn stage(&mut self, op: RowOperation) {
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

        let base = match commit::materialize(ReadHandle::Txn(&db_tx), None).await {
            Ok(base) => base,
            Err(err) => {
                db_tx.rollback();
                return Err(err);
            }
        };

        // Read before any write in this commit is staged: `InlineFlushDelete`
        // /`InlineDrop` name a table, not keys, and resolve against
        // `db_tx`'s current state exactly like `base` above.
        let inline_writes = match translate_inline(&db_tx, &ops).await {
            Ok(writes) => writes,
            Err(err) => {
                db_tx.rollback();
                return Err(err);
            }
        };

        match translate(&base, &ops) {
            Ok((new_id, mut writes, snap)) => {
                writes.extend(inline_writes);
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
    ops: &[RowOperation],
) -> Result<(u64, Vec<commit::StagedWrite>, proto::SnapshotValue)> {
    let snapshot = build_snapshot_value(ops, base.snapshot.next_deletion_id)?;
    let new_id = snapshot.snapshot_id;

    // Ends and deletes apply before inserts, independent of DuckLake's
    // emit order: a rename ends the old version and inserts a new one
    // under the same id, and the insert must win their shared `current` key —
    // an end applied afterward would delete the id and erase the new row.
    // Inline ops are skipped here entirely — `commit` translates them
    // separately via `translate_inline`, since `CatalogSnapshot` has no
    // notion of inlined rows to diff.
    let mut state = base.clone();
    for op in ops {
        if !is_inline_op(op) && !matches!(op, RowOperation::Insert { .. }) {
            apply_op(base, &mut state, op, new_id)?;
        }
    }
    for op in ops {
        if matches!(op, RowOperation::Insert { .. }) {
            apply_op(base, &mut state, op, new_id)?;
        }
    }

    let writes = commit::diff_writes(base, &state, new_id);
    Ok((new_id, writes, snapshot))
}

/// Allocates `inline/insert` chunk sequence numbers within one commit: the
/// first [`RowOp::InlineInsert`] staged for a given `(table_id,
/// schema_version, begin_snapshot)` gets `chunk_seq` `0`, the next `1`,
/// and so on — disambiguating multiple chunks the same commit stages
/// against the same key prefix.
#[derive(Default)]
struct ChunkSeqAllocator(HashMap<(u64, u64, u64), u64>);

impl ChunkSeqAllocator {
    fn next(&mut self, table_id: u64, schema_version: u64, begin_snapshot: u64) -> u64 {
        let seq = self
            .0
            .entry((table_id, schema_version, begin_snapshot))
            .or_insert(0);
        let allocated = *seq;
        *seq += 1;
        allocated
    }
}

/// Removes every `inline/insert` chunk begun at or before `flush_snapshot`
/// for `(table_id, schema_version)`, plus the `inline/inline_delete` tombstones
/// those chunks' rows consumed. Reads `db_tx`'s current (pre-commit)
/// inline records — the flush op only ever names the table and snapshot,
/// never the keys to remove.
async fn translate_inline_flush_delete(
    db_tx: &DbTransaction,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Txn(db_tx), table_id).await?;
    let inline_deletes =
        store_inline::scan_inline_inline_deletes(ReadHandle::Txn(db_tx), table_id).await?;

    let scoped: Vec<(InlineOperation, proto::InlineChunkValue)> = chunks
        .into_iter()
        .filter(
            |(op, _)| matches!(op, InlineOperation::Insert { schema_version: v, .. } if *v == schema_version),
        )
        .collect();

    for (op, _) in &scoped {
        if let InlineOperation::Insert { begin_snapshot, .. } = op {
            if *begin_snapshot <= flush_snapshot {
                writes.push((Key::Inline(InlineKey::Live(*op)).encode(), None));
            }
        }
    }

    // The rows the flushed chunks carried, including already-tombstoned
    // ones (`ForFlush`) — their `inline/inline_delete` records become orphaned
    // once the owning chunk is gone above, and must go with it.
    let rows = materialize_inline_rows(&scoped, &inline_deletes);
    for row in InlineScanKind::ForFlush.select(&rows, flush_snapshot, 0) {
        if row.end_snapshot.is_some() {
            writes.push((
                Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                    table_id,
                    row_id: row.row_id,
                }))
                .encode(),
                None,
            ));
        }
    }

    Ok(())
}

/// Removes every `inline/*` record for `table_id`: schema, chunks, and
/// tombstones, read from `db_tx`'s current (pre-commit) state.
async fn translate_inline_drop(
    db_tx: &DbTransaction,
    table_id: u64,
    writes: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    for (op, _) in store_inline::scan_inline_chunks(ReadHandle::Txn(db_tx), table_id).await? {
        writes.push((Key::Inline(InlineKey::Live(op)).encode(), None));
    }
    for (row_id, _) in
        store_inline::scan_inline_inline_deletes(ReadHandle::Txn(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
                table_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (data_file_id, row_id, _) in
        store_inline::scan_inline_file_deletes(ReadHandle::Txn(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
                table_id,
                data_file_id,
                row_id,
            }))
            .encode(),
            None,
        ));
    }
    for (schema_version, _) in
        store_inline::scan_inline_schemas(ReadHandle::Txn(db_tx), table_id).await?
    {
        writes.push((
            Key::Inline(InlineKey::Schema {
                table_id,
                schema_version,
            })
            .encode(),
            None,
        ));
    }
    Ok(())
}

/// Translates every staged inline op into direct `inline/*` key writes —
/// a separate pass from [`translate`], since inline records live outside
/// `CatalogSnapshot`'s entity model and are never diffed. `db_tx` is read
/// (for `InlineFlushDelete`/`InlineDrop`, which name a table rather than
/// the keys to remove) at its pre-commit state, before any of this
/// commit's own writes are staged onto it.
fn inline_schema_write(
    table_id: u64,
    schema_version: u64,
    arrow_schema: &[u8],
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Schema {
            table_id,
            schema_version,
        })
        .encode(),
        Some(value::encode_value(&proto::InlineSchemaValue {
            arrow_schema: arrow_schema.to_vec(),
        })),
    )
}

#[allow(clippy::too_many_arguments)]
fn inline_insert_write(
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    chunk_seq: u64,
    row_id_start: u64,
    row_count: u64,
    arrow_body: &[u8],
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::Insert {
            table_id,
            schema_version,
            begin_snapshot,
            chunk_seq,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineChunkValue {
            body: arrow_body.to_vec(),
            row_id_start,
            row_count,
            data_file_id: None,
        })),
    )
}

fn inline_inline_delete_write(
    table_id: u64,
    row_id: u64,
    end_snapshot: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::InlineDelete {
            table_id,
            row_id,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineInlineDeleteValue {
            end_snapshot,
        })),
    )
}

fn inline_file_delete_write(
    table_id: u64,
    data_file_id: u64,
    row_id: u64,
    begin_snapshot: u64,
) -> commit::StagedWrite {
    (
        Key::Inline(InlineKey::Live(InlineOperation::FileDelete {
            table_id,
            data_file_id,
            row_id,
        }))
        .encode(),
        Some(value::encode_value(&proto::InlineFileDeleteValue {
            begin_snapshot,
        })),
    )
}

async fn translate_inline(
    db_tx: &DbTransaction,
    ops: &[RowOperation],
) -> Result<Vec<commit::StagedWrite>> {
    let mut writes = Vec::new();
    let mut chunk_seqs = ChunkSeqAllocator::default();

    for op in ops {
        match op {
            RowOperation::InlineSchema {
                table_id,
                schema_version,
                arrow_schema,
            } => writes.push(inline_schema_write(
                *table_id,
                *schema_version,
                arrow_schema,
            )),
            RowOperation::InlineInsert {
                table_id,
                schema_version,
                begin_snapshot,
                row_id_start,
                row_count,
                arrow_body,
            } => {
                let chunk_seq = chunk_seqs.next(*table_id, *schema_version, *begin_snapshot);
                writes.push(inline_insert_write(
                    *table_id,
                    *schema_version,
                    *begin_snapshot,
                    chunk_seq,
                    *row_id_start,
                    *row_count,
                    arrow_body,
                ));
            }
            RowOperation::InlineInlineDelete {
                table_id,
                row_id,
                end_snapshot,
            } => writes.push(inline_inline_delete_write(
                *table_id,
                *row_id,
                *end_snapshot,
            )),
            RowOperation::InlineFileDelete {
                table_id,
                data_file_id,
                row_id,
                begin_snapshot,
            } => writes.push(inline_file_delete_write(
                *table_id,
                *data_file_id,
                *row_id,
                *begin_snapshot,
            )),
            RowOperation::InlineFlushDelete {
                table_id,
                schema_version,
                flush_snapshot,
            } => {
                translate_inline_flush_delete(
                    db_tx,
                    *table_id,
                    *schema_version,
                    *flush_snapshot,
                    &mut writes,
                )
                .await?;
            }
            RowOperation::InlineDrop { table_id } => {
                translate_inline_drop(db_tx, *table_id, &mut writes).await?;
            }
            RowOperation::InlineSchemaDrop {
                table_id,
                schema_version,
            } => {
                writes.push((
                    Key::Inline(InlineKey::Schema {
                        table_id: *table_id,
                        schema_version: *schema_version,
                    })
                    .encode(),
                    None,
                ));
            }
            RowOperation::Insert { .. }
            | RowOperation::Delete { .. }
            | RowOperation::UpdateSetEnd { .. } => {}
        }
    }

    Ok(writes)
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
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut txn = StagedTransaction::begin(db_tx);

        txn.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 0, "t", 1, None),
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::Column,
            cells: column_row(1, 1, "a", 0),
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
        });

        let id = txn.commit().await.unwrap();
        assert_eq!(id.get(), 1);

        let snapshot = catalog.snapshot().await.unwrap();
        let tables = snapshot.tables_in(crate::catalog::SchemaId::new(0));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "t");
        let cols = snapshot.columns_of(tables[0].id);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "a");
    }

    /// DuckLake-authored data-file and delete-file rows carry
    /// `encryption_key` through commit and back out of the snapshot read
    /// verbatim — the faithful-conduit guarantee for key material.
    #[tokio::test]
    async fn encryption_keys_round_trip_through_staged_rows() {
        let catalog = open().await;
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin(db_tx);

        tx.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 0, "t", 1, None),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Column,
            cells: column_row(1, 1, "a", 0),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::DataFile,
            cells: vec![
                Cell::U64(1),                     // data_file_id
                Cell::U64(1),                     // table_id
                Cell::U64(1),                     // begin_snapshot
                Cell::Null,                       // end_snapshot
                Cell::Null,                       // file_order
                Cell::Str("data.parquet".into()), // path
                Cell::Bool(true),                 // path_is_relative
                Cell::Str("parquet".into()),      // file_format
                Cell::U64(10),                    // record_count
                Cell::U64(1024),                  // file_size_bytes
                Cell::U64(64),                    // footer_size
                Cell::U64(0),                     // row_id_start
                Cell::Null,                       // partition_id
                Cell::Str("ZGF0YS1rZXk=".into()), // encryption_key
                Cell::Null,                       // mapping_id
                Cell::Null,                       // partial_max
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::DeleteFile,
            cells: vec![
                Cell::U64(2),                         // delete_file_id
                Cell::U64(1),                         // table_id
                Cell::U64(1),                         // begin_snapshot
                Cell::Null,                           // end_snapshot
                Cell::U64(1),                         // data_file_id
                Cell::Str("delete.parquet".into()),   // path
                Cell::Bool(true),                     // path_is_relative
                Cell::Str("parquet".into()),          // format
                Cell::U64(2),                         // delete_count
                Cell::U64(128),                       // file_size_bytes
                Cell::U64(32),                        // footer_size
                Cell::Str("ZGVsZXRlLWtleQ==".into()), // encryption_key
                Cell::Null,                           // partial_max
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
        });
        tx.commit().await.unwrap();

        let head = catalog.snapshot().await.unwrap();
        let table = head.tables_in(crate::catalog::SchemaId::new(0))[0].id;
        assert_eq!(
            head.data_files_of(table)[0].encryption_key.as_deref(),
            Some("ZGF0YS1rZXk=")
        );
        assert_eq!(
            head.delete_files_of(table)[0].encryption_key.as_deref(),
            Some("ZGVsZXRlLWtleQ==")
        );
    }

    /// An `UPDATE ... SET end_snapshot` row ends a live table version:
    /// the old row moves to `history`, the new one lands in `current`, exactly
    /// the lifecycle convention this path interprets.
    #[tokio::test]
    async fn update_set_end_moves_the_old_version_to_history() {
        let catalog = open().await;

        // Seed schema `s` (id 1) and table `t` (id 1) via a plain insert.
        let db_tx1 = catalog.begin_write_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: schema_row(1, "s", 1),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_old", 1, None),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_schema:"s""#),
        });
        setup.commit().await.unwrap();

        // Rename: end the old table version, insert the renamed one.
        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut rename = StagedTransaction::begin(db_tx2);
        rename.stage(RowOperation::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        });
        rename.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_new", 2, None),
        });
        rename.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 1, 2),
        });
        rename.stage(RowOperation::Insert {
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
    /// live. Translation applies ends before inserts, so the shared `current`
    /// key resolves to the insert regardless of stage order.
    #[tokio::test]
    async fn rename_survives_insert_before_end_order() {
        let catalog = open().await;

        let db_tx1 = catalog.begin_write_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: schema_row(1, "s", 1),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_old", 1, None),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, r#"created_schema:"s""#),
        });
        setup.commit().await.unwrap();

        // Insert the renamed version first, then end the old one — the
        // reverse of the safe order, matching what DuckLake emits.
        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut rename = StagedTransaction::begin(db_tx2);
        rename.stage(RowOperation::Insert {
            table: TableKind::Table,
            cells: table_row(1, 1, "t_new", 2, None),
        });
        rename.stage(RowOperation::UpdateSetEnd {
            table: TableKind::Table,
            cells: vec![Cell::U64(1), Cell::U64(2)],
        });
        rename.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 1, 2),
        });
        rename.stage(RowOperation::Insert {
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

        let tx_a = catalog.begin_write_tx().await.unwrap();
        let tx_b = catalog.begin_write_tx().await.unwrap();
        let mut a = StagedTransaction::begin(tx_a);
        let mut b = StagedTransaction::begin(tx_b);

        for (txn, name) in [(&mut a, "a"), (&mut b, "b")] {
            txn.stage(RowOperation::Insert {
                table: TableKind::Schema,
                cells: schema_row(1, name, 1),
            });
            txn.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: snapshot_row(1, 1, 2),
            });
            txn.stage(RowOperation::Insert {
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
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut txn = StagedTransaction::begin(db_tx);
        txn.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: vec![Cell::U64(1)], // far too few cells
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, ""),
        });
        let err = txn.commit().await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    /// Stages an inline schema plus two inserts against the same
    /// `(table_id, schema_version, begin_snapshot)` in one commit: the
    /// chunks land with sequential `chunk_seq` (stage order), and the
    /// schema is readable back verbatim.
    #[tokio::test]
    async fn stages_inline_schema_and_sequential_inserts() {
        let catalog = open().await;
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut txn = StagedTransaction::begin(db_tx);

        txn.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: b"schema".to_vec(),
        });
        txn.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk-a".to_vec(),
        });
        txn.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 2,
            row_count: 1,
            arrow_body: b"chunk-b".to_vec(),
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 0, 1),
        });
        txn.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, "inlined_insert:1"),
        });
        txn.commit().await.unwrap();

        let tx = catalog.begin_write_tx().await.unwrap();
        let chunks = store_inline::scan_inline_chunks(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks[0].0,
            InlineOperation::Insert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: 1,
                chunk_seq: 0,
            }
        );
        assert_eq!(chunks[0].1.body, b"chunk-a");
        assert_eq!(chunks[0].1.row_id_start, 0);
        assert_eq!(chunks[0].1.row_count, 2);
        assert_eq!(
            chunks[1].0,
            InlineOperation::Insert {
                table_id: 1,
                schema_version: 0,
                begin_snapshot: 1,
                chunk_seq: 1,
            }
        );
        assert_eq!(chunks[1].1.body, b"chunk-b");

        let schemas = store_inline::scan_inline_schemas(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        assert_eq!(
            schemas,
            vec![(
                0,
                proto::InlineSchemaValue {
                    arrow_schema: b"schema".to_vec(),
                }
            )]
        );
        tx.rollback();
    }

    /// An `InlineIdel` tombstones a row: the row is absent from a
    /// `Table`-kind materialization at or after its `end_snapshot`.
    #[tokio::test]
    async fn stages_inline_idel_and_row_disappears_from_table_scan_after_it() {
        let catalog = open().await;

        let db_tx1 = catalog.begin_write_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk".to_vec(),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 0, 1),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, "inlined_insert:1"),
        });
        setup.commit().await.unwrap();

        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut inline_delete = StagedTransaction::begin(db_tx2);
        inline_delete.stage(RowOperation::InlineInlineDelete {
            table_id: 1,
            row_id: 0,
            end_snapshot: 2,
        });
        inline_delete.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 0, 1),
        });
        inline_delete.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, "inlined_delete:1"),
        });
        inline_delete.commit().await.unwrap();

        let tx = catalog.begin_write_tx().await.unwrap();
        let chunks = store_inline::scan_inline_chunks(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        let inline_deletes = store_inline::scan_inline_inline_deletes(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        tx.rollback();
        assert_eq!(
            inline_deletes,
            vec![(0, proto::InlineInlineDeleteValue { end_snapshot: 2 })]
        );

        let rows = materialize_inline_rows(&chunks, &inline_deletes);
        assert_eq!(
            InlineScanKind::Table
                .select(&rows, 1, 0)
                .iter()
                .map(|r| r.row_id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            InlineScanKind::Table
                .select(&rows, 2, 0)
                .iter()
                .map(|r| r.row_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    /// `InlineFlushDelete` removes chunks begun at or before the flush
    /// snapshot for the named schema version, plus the `inline/inline_delete`
    /// tombstones those chunks' rows consumed — a later schema version's
    /// chunk (begun after the flush point) survives untouched.
    #[tokio::test]
    async fn stages_inline_flush_delete_removes_flushed_chunks_and_their_idels() {
        let catalog = open().await;

        let db_tx1 = catalog.begin_write_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 2,
            arrow_body: b"chunk".to_vec(),
        });
        setup.stage(RowOperation::InlineInlineDelete {
            table_id: 1,
            row_id: 0,
            end_snapshot: 1,
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 0, 1),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, "inlined_insert:1"),
        });
        setup.commit().await.unwrap();

        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut flush = StagedTransaction::begin(db_tx2);
        flush.stage(RowOperation::InlineFlushDelete {
            table_id: 1,
            schema_version: 0,
            flush_snapshot: 1,
        });
        flush.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 0, 1),
        });
        flush.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, "flushed_inlined_data:1"),
        });
        flush.commit().await.unwrap();

        let tx = catalog.begin_write_tx().await.unwrap();
        let chunks = store_inline::scan_inline_chunks(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        let inline_deletes = store_inline::scan_inline_inline_deletes(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        tx.rollback();
        assert!(chunks.is_empty(), "flushed chunk must be gone: {chunks:?}");
        assert!(
            inline_deletes.is_empty(),
            "consumed inline_delete must be gone: {inline_deletes:?}"
        );
    }

    /// `InlineDrop` removes every `inline/*` record for the table:
    /// schema, chunks, and tombstones.
    #[tokio::test]
    async fn stages_inline_drop_removes_every_record_for_the_table() {
        let catalog = open().await;

        let db_tx1 = catalog.begin_write_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: b"schema".to_vec(),
        });
        setup.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 1,
            arrow_body: b"chunk".to_vec(),
        });
        setup.stage(RowOperation::InlineFileDelete {
            table_id: 1,
            data_file_id: 9,
            row_id: 5,
            begin_snapshot: 1,
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 0, 1),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, "inlined_insert:1"),
        });
        setup.commit().await.unwrap();

        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut drop_txn = StagedTransaction::begin(db_tx2);
        drop_txn.stage(RowOperation::InlineDrop { table_id: 1 });
        drop_txn.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 0, 1),
        });
        drop_txn.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, r#"dropped_table:"main"."t""#),
        });
        drop_txn.commit().await.unwrap();

        let tx = catalog.begin_write_tx().await.unwrap();
        let chunks = store_inline::scan_inline_chunks(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        let file_deletes = store_inline::scan_inline_file_deletes(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        let schemas = store_inline::scan_inline_schemas(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        tx.rollback();
        assert!(chunks.is_empty());
        assert!(file_deletes.is_empty());
        assert!(schemas.is_empty());
    }

    /// `InlineSchemaDrop` removes only the named schema version's
    /// `inline/schema` record, leaving a different schema version's
    /// record (and its chunks) untouched — the scoped cleanup a
    /// superseded-inlined-table flush needs, as opposed to `InlineDrop`'s
    /// whole-table sweep.
    #[tokio::test]
    async fn stages_inline_schema_drop_removes_only_the_named_schema_version() {
        let catalog = open().await;

        let db_tx1 = catalog.begin_write_tx().await.unwrap();
        let mut setup = StagedTransaction::begin(db_tx1);
        setup.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: b"schema-v0".to_vec(),
        });
        setup.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 1,
            arrow_schema: b"schema-v1".to_vec(),
        });
        setup.stage(RowOperation::InlineInsert {
            table_id: 1,
            schema_version: 1,
            begin_snapshot: 1,
            row_id_start: 0,
            row_count: 1,
            arrow_body: b"chunk".to_vec(),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 0, 1),
        });
        setup.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, "inlined_insert:1"),
        });
        setup.commit().await.unwrap();

        let db_tx2 = catalog.begin_write_tx().await.unwrap();
        let mut drop_txn = StagedTransaction::begin(db_tx2);
        drop_txn.stage(RowOperation::InlineSchemaDrop {
            table_id: 1,
            schema_version: 0,
        });
        drop_txn.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(2, 0, 1),
        });
        drop_txn.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(2, "flushed_inlined_data:1"),
        });
        drop_txn.commit().await.unwrap();

        let tx = catalog.begin_write_tx().await.unwrap();
        let schemas = store_inline::scan_inline_schemas(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        let chunks = store_inline::scan_inline_chunks(ReadHandle::Txn(&tx), 1)
            .await
            .unwrap();
        tx.rollback();
        assert_eq!(
            schemas,
            vec![(
                1,
                proto::InlineSchemaValue {
                    arrow_schema: b"schema-v1".to_vec()
                }
            )]
        );
        assert_eq!(chunks.len(), 1, "schema_version 1's chunk must survive");
    }
}
