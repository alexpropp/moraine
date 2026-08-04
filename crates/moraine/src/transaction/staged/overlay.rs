//! Read-your-writes for the `dump_*` projections: the committed records a
//! staged transaction can see, with its own uncommitted rows applied over
//! them.
//!
//! A staged row is stored untranslated — DuckLake's own cells, carrying the
//! snapshot ids DuckLake itself authored — so what the transaction will
//! *see* is fully determined the moment the row is staged, without waiting
//! for commit-time translation. That is what makes an overlay possible at
//! all: it decodes each staged row with the same decoders translation uses
//! and applies it to the scanned records, in stage order.
//!
//! The overlay is deliberately not translation. It never validates, never
//! diffs, and never touches the store: a row translation would refuse still
//! shows up here, and fails loudly at commit where it belongs.

use super::{
    Cell, EntityKey, Error, Result, RowOperation, TableKind,
    decode::{
        Cursor, StatsKey, decode_column, decode_column_mapping, decode_data_file,
        decode_delete_file, decode_delete_key, decode_end, decode_file_column_stats,
        decode_gc_file_row, decode_hard_delete, decode_macro, decode_metadata,
        decode_partition_info, decode_schema, decode_sort_info, decode_table,
        decode_table_column_stats, decode_table_stats, decode_tag_row, decode_view,
    },
    proto,
};

/// One versioned kind's overlay vocabulary: how a record names itself, how
/// its lifecycle moves, and how a staged row becomes one.
pub(super) trait Versioned: Sized {
    /// The staged-row kind whose rows apply to this record type.
    const KIND: TableKind;

    /// The entity a delete or a lifecycle update names.
    fn key(&self) -> EntityKey;

    /// `None` for the live version, `Some` for a history one.
    fn end_snapshot(&self) -> Option<u64>;

    fn set_end_snapshot(&mut self, end: u64);

    /// Rebases the live version's `begin_snapshot`. Defaulted to a no-op:
    /// an `UPDATE ... SET begin_snapshot` is refused at decode for every
    /// kind but `ducklake_data_file`, so no other implementation is ever
    /// reached.
    fn set_begin_snapshot(&mut self, begin: u64) {
        let _ = begin;
    }

    /// Builds a record from one staged insert's cells, given the records
    /// already in hand for the fields DuckLake does not supply.
    fn decode_row(cells: &[Cell], committed: &[Self]) -> Result<Self>;
}

/// `rows` with the transaction's staged rows for `T::KIND` applied, in
/// stage order.
///
/// Inserts append; a hard delete drops exactly the version it names
/// (`end_snapshot` `NULL` for the live one, a value for that history one);
/// an `UPDATE ... SET end_snapshot` ends the live version; an
/// `UPDATE ... SET begin_snapshot` rebases it. Stage order is load-bearing:
/// a transaction that inserts a row and then ends it must see both, in that
/// order.
pub(super) fn overlay_versioned<T: Versioned>(
    ops: &[RowOperation],
    mut rows: Vec<T>,
) -> Result<Vec<T>> {
    for op in ops {
        match op {
            RowOperation::Insert { table, cells } if *table == T::KIND => {
                let row = T::decode_row(cells, &rows)?;
                rows.push(row);
            }
            RowOperation::Delete { table, cells } if *table == T::KIND => {
                let (key, end_snapshot) = decode_hard_delete(*table, cells)?;
                rows.retain(|row| !(row.key() == key && row.end_snapshot() == end_snapshot));
            }
            RowOperation::UpdateSetEnd { table, cells } if *table == T::KIND => {
                let (key, end_snapshot) = decode_end(*table, cells)?;
                for row in &mut rows {
                    if row.key() == key && row.end_snapshot().is_none() {
                        row.set_end_snapshot(end_snapshot);
                    }
                }
            }
            RowOperation::UpdateSetBegin { table, cells } if *table == T::KIND => {
                let (key, begin_snapshot) = decode_begin(*table, cells)?;
                for row in &mut rows {
                    if row.key() == key && row.end_snapshot().is_none() {
                        row.set_begin_snapshot(begin_snapshot);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(rows)
}

/// The `(entity, new begin_snapshot)` an `UPDATE ... SET begin_snapshot`
/// names. DuckLake issues one only against `ducklake_data_file`, during a
/// delete-rewrite.
fn decode_begin(table: TableKind, cells: &[Cell]) -> Result<(EntityKey, u64)> {
    if table != TableKind::DataFile {
        return Err(Error::Constraint(format!(
            "update_set_begin is not defined for {table:?}"
        )));
    }
    let mut c = Cursor::new(table, cells);
    let key = EntityKey::File {
        table_id: c.u64()?,
        data_file_id: c.u64()?,
    };
    let begin_snapshot = c.u64()?;
    c.finish()?;
    Ok((key, begin_snapshot))
}

/// One unversioned kind's overlay vocabulary. These rows carry no
/// lifecycle: they are overwritten in place and removed outright, so the
/// whole vocabulary is a key.
pub(super) trait Unversioned: Sized {
    /// The staged-row kind whose rows apply to this record type.
    const KIND: TableKind;

    /// What identifies a row for overwrite and for removal.
    type Key: PartialEq;

    fn key(&self) -> Self::Key;

    fn decode_row(cells: &[Cell]) -> Result<Self>;

    /// The key a delete row names — its key columns only, so this is not
    /// [`Self::decode_row`] followed by [`Self::key`].
    fn decode_key(cells: &[Cell]) -> Result<Self::Key>;
}

/// `rows` with the transaction's staged rows for `T::KIND` applied, in
/// stage order: an insert overwrites the row it keys and otherwise appends,
/// a delete removes it.
pub(super) fn overlay_unversioned<T: Unversioned>(
    ops: &[RowOperation],
    mut rows: Vec<T>,
) -> Result<Vec<T>> {
    for op in ops {
        match op {
            RowOperation::Insert { table, cells } if *table == T::KIND => {
                let row = T::decode_row(cells)?;
                let key = row.key();
                rows.retain(|existing| existing.key() != key);
                rows.push(row);
            }
            RowOperation::Delete { table, cells } if *table == T::KIND => {
                let key = T::decode_key(cells)?;
                rows.retain(|existing| existing.key() != key);
            }
            _ => {}
        }
    }
    Ok(rows)
}

impl Versioned for proto::DataFileValue {
    const KIND: TableKind = TableKind::DataFile;

    fn key(&self) -> EntityKey {
        EntityKey::File {
            table_id: self.table_id,
            data_file_id: self.data_file_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn set_begin_snapshot(&mut self, begin: u64) {
        self.begin_snapshot = begin;
    }

    /// Partition values ride a child table of their own, so a staged file
    /// carries none here — the `ducklake_data_file` projection does not
    /// report them either.
    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_data_file(cells)
    }
}

impl Versioned for proto::DeleteFileValue {
    const KIND: TableKind = TableKind::DeleteFile;

    fn key(&self) -> EntityKey {
        EntityKey::DeleteFile {
            table_id: self.table_id,
            delete_file_id: self.delete_file_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_delete_file(cells)
    }
}

impl Versioned for proto::ColumnValue {
    const KIND: TableKind = TableKind::Column;

    fn key(&self) -> EntityKey {
        EntityKey::Column {
            table_id: self.table_id,
            column_id: self.column_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_column(cells)
    }
}

impl Versioned for proto::TableValue {
    const KIND: TableKind = TableKind::Table;

    fn key(&self) -> EntityKey {
        EntityKey::Table {
            table_id: self.table_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    /// `next_column_id` is moraine's own counter, not a DuckLake column, so
    /// a staged row carries no value for it: it is inherited from whatever
    /// the table already had, and starts at 1 for a table this transaction
    /// is creating. It reaches no projection either way — translation
    /// recomputes it at commit.
    fn decode_row(cells: &[Cell], committed: &[Self]) -> Result<Self> {
        let cells = decode_table(cells)?;
        let next_column_id = committed
            .iter()
            .filter(|row| row.table_id == cells.table_id)
            .map(|row| row.next_column_id)
            .max()
            .unwrap_or(1);
        Ok(proto::TableValue {
            table_id: cells.table_id,
            table_uuid: cells.table_uuid,
            begin_snapshot: cells.begin_snapshot,
            end_snapshot: cells.end_snapshot,
            schema_id: cells.schema_id,
            table_name: cells.table_name,
            path: cells.path,
            path_is_relative: cells.path_is_relative,
            next_column_id,
        })
    }
}

impl Unversioned for proto::FileColumnStatsValue {
    const KIND: TableKind = TableKind::FileColumnStats;

    type Key = (u64, u64, u64);

    fn key(&self) -> Self::Key {
        (self.table_id, self.data_file_id, self.column_id)
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_file_column_stats(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_delete_key(Self::KIND, cells)? {
            StatsKey::FileColumn(table_id, data_file_id, column_id) => {
                Ok((table_id, data_file_id, column_id))
            }
            _ => Err(stats_key_mismatch(Self::KIND)),
        }
    }
}

impl Unversioned for proto::GcFileValue {
    const KIND: TableKind = TableKind::FilesScheduledForDeletion;

    type Key = u64;

    fn key(&self) -> Self::Key {
        self.data_file_id
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_gc_file_row(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        let mut c = Cursor::new(Self::KIND, cells);
        let data_file_id = c.u64()?;
        c.finish()?;
        Ok(data_file_id)
    }
}

impl Versioned for proto::SchemaValue {
    const KIND: TableKind = TableKind::Schema;

    fn key(&self) -> EntityKey {
        EntityKey::Schema {
            schema_id: self.schema_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_schema(cells)
    }
}

impl Versioned for proto::ViewValue {
    const KIND: TableKind = TableKind::View;

    fn key(&self) -> EntityKey {
        EntityKey::View {
            view_id: self.view_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_view(cells)
    }
}

impl Versioned for proto::PartitionValue {
    const KIND: TableKind = TableKind::PartitionInfo;

    fn key(&self) -> EntityKey {
        EntityKey::Partition {
            table_id: self.table_id,
            partition_id: self.partition_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    /// The spec's partition columns ride a child table of their own, so a
    /// staged spec carries none here; the `ducklake_partition_column`
    /// projection folds them back in from the staged child rows.
    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_partition_info(cells)
    }
}

impl Versioned for proto::SortValue {
    const KIND: TableKind = TableKind::SortInfo;

    fn key(&self) -> EntityKey {
        EntityKey::Sort {
            table_id: self.table_id,
            sort_id: self.sort_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_sort_info(cells)
    }
}

impl Versioned for proto::MacroValue {
    const KIND: TableKind = TableKind::Macro;

    fn key(&self) -> EntityKey {
        EntityKey::Macro {
            macro_id: self.macro_id,
        }
    }

    fn end_snapshot(&self) -> Option<u64> {
        self.end_snapshot
    }

    fn set_end_snapshot(&mut self, end: u64) {
        self.end_snapshot = Some(end);
    }

    fn decode_row(cells: &[Cell], _committed: &[Self]) -> Result<Self> {
        decode_macro(cells)
    }
}

impl Unversioned for proto::TableStatsValue {
    const KIND: TableKind = TableKind::TableStats;

    type Key = u64;

    fn key(&self) -> Self::Key {
        self.table_id
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_table_stats(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_delete_key(Self::KIND, cells)? {
            StatsKey::Table(table_id) => Ok(table_id),
            _ => Err(stats_key_mismatch(Self::KIND)),
        }
    }
}

impl Unversioned for proto::TableColumnStatsValue {
    const KIND: TableKind = TableKind::TableColumnStats;

    type Key = (u64, u64);

    fn key(&self) -> Self::Key {
        (self.table_id, self.column_id)
    }

    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_table_column_stats(cells)
    }

    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_delete_key(Self::KIND, cells)? {
            StatsKey::Column(table_id, column_id) => Ok((table_id, column_id)),
            _ => Err(stats_key_mismatch(Self::KIND)),
        }
    }
}

impl Unversioned for proto::MappingValue {
    const KIND: TableKind = TableKind::ColumnMapping;

    type Key = u64;

    fn key(&self) -> Self::Key {
        self.mapping_id
    }

    /// A mapping's name-mapping rows ride a child table of their own, so a
    /// staged mapping carries none here; the `ducklake_name_mapping`
    /// projection folds them back in from the staged child rows.
    fn decode_row(cells: &[Cell]) -> Result<Self> {
        decode_column_mapping(cells)
    }

    /// Mappings are unversioned but their delete is a hard one, keyed like
    /// the versioned kinds' rather than like the statistics kinds'.
    fn decode_key(cells: &[Cell]) -> Result<Self::Key> {
        match decode_hard_delete(Self::KIND, cells)?.0 {
            EntityKey::Mapping { mapping_id, .. } => Ok(mapping_id),
            other => Err(Error::Constraint(format!(
                "column mapping delete decoded as {other:?}"
            ))),
        }
    }
}

/// A statistics delete that decoded as another kind's key — unreachable
/// while [`decode_delete_key`] switches on the same kind the caller asked
/// for, and a loud error rather than a silent mismatch if that changes.
fn stats_key_mismatch(kind: TableKind) -> Error {
    Error::Constraint(format!("{kind:?} delete decoded as another statistics key"))
}

/// `containers` with the transaction's staged `ducklake_tag` rows applied.
///
/// A tag row is an *entry* inside its object's container record, not a
/// record of its own, so this folds rather than appends: an insert adds an
/// entry (minting the container if the object has none yet), an
/// `UPDATE ... SET end_snapshot` ends the live entry with that key, and a
/// delete removes the entry it names, taking an emptied container with it.
///
/// Naming an entry that is not there is a shape error translation refuses
/// at commit; the overlay leaves the view unchanged rather than deciding
/// that here.
pub(super) fn overlay_tag_containers(
    ops: &[RowOperation],
    mut containers: Vec<proto::TagValue>,
) -> Result<Vec<proto::TagValue>> {
    for op in ops {
        match op {
            RowOperation::Insert { table, cells } if *table == TableKind::Tag => {
                let (object_id, entry) = decode_tag_row(cells)?;
                match containers.iter_mut().find(|c| c.object_id == object_id) {
                    Some(container) => container.entries.push(entry),
                    None => containers.push(proto::TagValue {
                        object_id,
                        entries: vec![entry],
                    }),
                }
            }
            RowOperation::UpdateSetEnd { table, cells } if *table == TableKind::Tag => {
                let mut cursor = Cursor::new(TableKind::Tag, cells);
                let object_id = cursor.u64()?;
                let key = cursor.string()?;
                let end_snapshot = cursor.u64()?;
                cursor.finish()?;
                if let Some(container) = containers.iter_mut().find(|c| c.object_id == object_id)
                    && let Some(entry) = container
                        .entries
                        .iter_mut()
                        .find(|e| e.key == key && e.end_snapshot.is_none())
                {
                    entry.end_snapshot = Some(end_snapshot);
                }
            }
            RowOperation::Delete { table, cells } if *table == TableKind::Tag => {
                let mut cursor = Cursor::new(TableKind::Tag, cells);
                let object_id = cursor.u64()?;
                let key = cursor.string()?;
                let begin_snapshot = cursor.u64()?;
                cursor.finish()?;
                if let Some(container) = containers.iter_mut().find(|c| c.object_id == object_id) {
                    container
                        .entries
                        .retain(|e| !(e.key == key && e.begin_snapshot == begin_snapshot));
                }
                containers.retain(|c| !c.entries.is_empty());
            }
            _ => {}
        }
    }

    Ok(containers)
}

/// `scopes` with the transaction's staged `ducklake_metadata` rows applied.
///
/// Options live outside the snapshot protocol — last write wins, no
/// lifecycle — so an insert sets its key in the scope's record (minting the
/// scope if it is new) and a delete removes it, taking an emptied scope with
/// it. A delete of a key already gone is a no-op, matching translation: two
/// writers agreeing a key is absent is not a disagreement about state.
pub(super) fn overlay_option_scopes(
    ops: &[RowOperation],
    mut scopes: Vec<(u64, u64, proto::OptionScopeValue)>,
) -> Result<Vec<(u64, u64, proto::OptionScopeValue)>> {
    for op in ops {
        let (components, key, value) = match op {
            RowOperation::Insert { table, cells } if *table == TableKind::Metadata => {
                let (components, key, value) = decode_metadata(cells)?;
                (components, key, Some(value))
            }
            RowOperation::Delete { table, cells } if *table == TableKind::Metadata => {
                let (components, key, _) = decode_metadata(cells)?;
                (components, key, None)
            }
            _ => continue,
        };

        let at = scopes
            .iter()
            .position(|(scope_kind, scope_id, _)| (*scope_kind, *scope_id) == components);
        match (at, value) {
            (Some(at), Some(value)) => {
                scopes[at].2.options.insert(key, value);
            }
            (Some(at), None) => {
                scopes[at].2.options.remove(&key);
            }
            (None, Some(value)) => scopes.push((
                components.0,
                components.1,
                proto::OptionScopeValue {
                    options: std::collections::HashMap::from([(key, value)]),
                },
            )),
            (None, None) => {}
        }
        scopes.retain(|(.., value)| !value.options.is_empty());
    }

    Ok(scopes)
}
