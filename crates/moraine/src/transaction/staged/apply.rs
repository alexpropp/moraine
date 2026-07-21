//! Snapshot application: replaying decoded row operations onto the
//! working `CatalogSnapshot`, and assembling the commit's snapshot
//! record.

use super::{
    CatalogSnapshot, Cell, EntityKey, Error, HashMap, Key, Result, RowOperation, TableKind, commit,
    corrupt_row,
    decode::{
        Cursor, StatsKey, decode_column, decode_column_mapping, decode_column_tag_row,
        decode_data_file, decode_delete_file, decode_delete_key, decode_end,
        decode_file_column_stats, decode_file_partition_value, decode_gc_file_row,
        decode_hard_delete, decode_macro, decode_macro_impl, decode_macro_parameter,
        decode_name_mapping, decode_partition_column, decode_partition_info, decode_schema,
        decode_sort_expression, decode_sort_info, decode_table, decode_table_column_stats,
        decode_table_stats, decode_tag_row, decode_view, table_value,
    },
    proto,
};

/// Child-table rows collected before the insert pass: each is folded
/// into its parent record when the parent's insert applies, and a
/// leftover after the pass means a child row named a parent this commit
/// never inserted — a shape error.
#[derive(Default)]
pub(super) struct ChildRows {
    pub(super) partition_columns: HashMap<u64, Vec<proto::PartitionColumn>>,
    pub(super) sort_expressions: HashMap<u64, Vec<proto::SortExpression>>,
    pub(super) file_partition_values: HashMap<(u64, u64), Vec<proto::FilePartitionValue>>,
    pub(super) macro_implementations: HashMap<u64, Vec<proto::MacroImplementation>>,
    pub(super) macro_parameters: HashMap<(u64, u64), Vec<proto::MacroParameter>>,
    pub(super) name_mappings: HashMap<u64, Vec<proto::NameMapping>>,
}

pub(super) fn collect_child_rows(ops: &[RowOperation]) -> Result<ChildRows> {
    let mut children = ChildRows::default();
    for op in ops {
        if let RowOperation::Insert { table, cells } = op {
            match table {
                TableKind::PartitionColumn => {
                    let (partition_id, column) = decode_partition_column(cells)?;
                    children
                        .partition_columns
                        .entry(partition_id)
                        .or_default()
                        .push(column);
                }
                TableKind::SortExpression => {
                    let (sort_id, expression) = decode_sort_expression(cells)?;
                    children
                        .sort_expressions
                        .entry(sort_id)
                        .or_default()
                        .push(expression);
                }
                TableKind::FilePartitionValue => {
                    let (file, value) = decode_file_partition_value(cells)?;
                    children
                        .file_partition_values
                        .entry(file)
                        .or_default()
                        .push(value);
                }
                TableKind::MacroImpl => {
                    let (macro_id, implementation) = decode_macro_impl(cells)?;
                    children
                        .macro_implementations
                        .entry(macro_id)
                        .or_default()
                        .push(implementation);
                }
                TableKind::MacroParameters => {
                    let (key, parameter) = decode_macro_parameter(cells)?;
                    children
                        .macro_parameters
                        .entry(key)
                        .or_default()
                        .push(parameter);
                }
                TableKind::NameMapping => {
                    let (mapping_id, row) = decode_name_mapping(cells)?;
                    children
                        .name_mappings
                        .entry(mapping_id)
                        .or_default()
                        .push(row);
                }
                _ => {}
            }
        }
    }
    for columns in children.partition_columns.values_mut() {
        columns.sort_by_key(|c| c.partition_key_index);
    }
    for expressions in children.sort_expressions.values_mut() {
        expressions.sort_by_key(|e| e.sort_key_index);
    }
    for values in children.file_partition_values.values_mut() {
        values.sort_by_key(|v| v.partition_key_index);
    }
    for implementations in children.macro_implementations.values_mut() {
        implementations.sort_by_key(|i| i.impl_id);
    }
    for parameters in children.macro_parameters.values_mut() {
        parameters.sort_by_key(|p| p.column_id);
    }
    for rows in children.name_mappings.values_mut() {
        rows.sort_by_key(|r| r.column_id);
    }

    Ok(children)
}

/// Applies one staged row to the working snapshot. `new_id` is this
/// commit's own new snapshot id — the only value an `UpdateSetEnd` row's
/// `end_snapshot` cell is ever allowed to carry (see the module doc).
pub(super) fn apply_op(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    op: &RowOperation,
    new_id: u64,
    children: &mut ChildRows,
    direct: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    match op {
        RowOperation::Insert { table, cells } => apply_insert(base, state, *table, cells, children),
        RowOperation::UpdateSetEnd { table, cells } => {
            apply_update_set_end(state, *table, cells, new_id)
        }
        RowOperation::UpdateSetBegin { table, cells } => {
            apply_update_set_begin(base, state, *table, cells, new_id)
        }
        RowOperation::Delete { table, cells } => apply_delete(state, *table, cells, direct),
        // Inline ops contribute no snapshot diff — their writes come from
        // `translate_inline` — so both `translate` and
        // `translate_maintenance` pass them through here as no-ops.
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
pub(super) fn is_inline_op(op: &RowOperation) -> bool {
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

/// Folds a `ducklake_macro` insert with its collected impl and parameter
/// rows into one record: at least one impl, ordinals contiguous from
/// zero, one `macro_type` across the macro.
pub(super) fn apply_macro_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    children: &mut ChildRows,
) -> Result<()> {
    let mut value = decode_macro(cells)?;
    let mut implementations = children
        .macro_implementations
        .remove(&value.macro_id)
        .unwrap_or_default();
    if implementations.is_empty() {
        return Err(corrupt_row(
            TableKind::Macro,
            "a macro insert requires at least one macro_impl row in the same commit",
        ));
    }
    for (index, implementation) in implementations.iter().enumerate() {
        if implementation.impl_id != index as u64 {
            return Err(corrupt_row(
                TableKind::MacroImpl,
                "impl_id values must be contiguous from zero",
            ));
        }
        if implementation.macro_type != implementations[0].macro_type {
            return Err(corrupt_row(
                TableKind::MacroImpl,
                "all implementations of one macro must share a type",
            ));
        }
    }
    for implementation in &mut implementations {
        let parameters = children
            .macro_parameters
            .remove(&(value.macro_id, implementation.impl_id))
            .unwrap_or_default();
        for (index, parameter) in parameters.iter().enumerate() {
            if parameter.column_id != index as u64 {
                return Err(corrupt_row(
                    TableKind::MacroParameters,
                    "column_id values must be contiguous from zero",
                ));
            }
        }
        implementation.parameters = parameters;
    }
    value.implementations = implementations;
    state.put_macro(value);

    Ok(())
}

/// Folds a `ducklake_column_mapping` insert with its collected
/// name-mapping rows into one record: at least one row, unique ordinals,
/// parents preceding children, and a mapping id never written before.
pub(super) fn apply_mapping_insert(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    children: &mut ChildRows,
) -> Result<()> {
    let mut value = decode_column_mapping(cells)?;
    let rows = children
        .name_mappings
        .remove(&value.mapping_id)
        .unwrap_or_default();
    if rows.is_empty() {
        return Err(corrupt_row(
            TableKind::ColumnMapping,
            "a column_mapping insert requires name_mapping rows in the same commit",
        ));
    }
    for (index, row) in rows.iter().enumerate() {
        if index > 0 && rows[index - 1].column_id == row.column_id {
            return Err(corrupt_row(
                TableKind::NameMapping,
                "column_id values must be unique within a mapping",
            ));
        }
        if row
            .parent_column
            .is_some_and(|parent| parent >= row.column_id)
        {
            return Err(corrupt_row(
                TableKind::NameMapping,
                "parent_column must reference an earlier column_id",
            ));
        }
    }
    if state
        .mappings
        .get(&value.table_id)
        .is_some_and(|per_table| per_table.contains_key(&value.mapping_id))
    {
        return Err(corrupt_row(
            TableKind::ColumnMapping,
            "mapping_id already exists for this table",
        ));
    }
    value.name_mappings = rows;
    state.put_mapping(value);

    Ok(())
}

pub(super) fn apply_insert(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    children: &mut ChildRows,
) -> Result<()> {
    match table {
        // Snapshot rows fold into the snapshot record separately; child
        // rows fold into their parent records via `collect_child_rows`.
        // Neither is an entity mutation of its own.
        TableKind::Snapshot
        | TableKind::SnapshotChanges
        | TableKind::SchemaVersions
        | TableKind::PartitionColumn
        | TableKind::SortExpression
        | TableKind::FilePartitionValue
        | TableKind::MacroImpl
        | TableKind::MacroParameters
        | TableKind::NameMapping => {}
        TableKind::Schema => state.put_schema(decode_schema(cells)?),
        TableKind::Table => state.put_table(table_value(base, decode_table(cells)?)),
        TableKind::View => state.put_view(decode_view(cells)?),
        TableKind::Column => {
            let mut value = decode_column(cells)?;
            // Column tags outlive column versions (DuckLake keys them by
            // (table_id, column_id) with their own lifecycle), so a new
            // version carries the prior version's entries forward —
            // DuckLake never re-authors tag rows on a column alter.
            if let Some(prior) = base
                .columns
                .get(&value.table_id)
                .and_then(|cols| cols.get(&value.column_id))
            {
                value.tags.clone_from(&prior.tags);
            }
            state.put_column(value);
        }
        TableKind::DataFile => {
            let mut value = decode_data_file(cells)?;
            value.partition_values = children
                .file_partition_values
                .remove(&(value.table_id, value.data_file_id))
                .unwrap_or_default();
            state.put_data_file(value);
        }
        TableKind::DeleteFile => state.put_delete_file(decode_delete_file(cells)?),
        TableKind::PartitionInfo => {
            let mut value = decode_partition_info(cells)?;
            value.columns = children
                .partition_columns
                .remove(&value.partition_id)
                .unwrap_or_default();
            state.put_partition(value);
        }
        TableKind::SortInfo => {
            let mut value = decode_sort_info(cells)?;
            value.expressions = children
                .sort_expressions
                .remove(&value.sort_id)
                .unwrap_or_default();
            state.put_sort(value);
        }
        TableKind::Macro => apply_macro_insert(state, cells, children)?,
        TableKind::ColumnMapping => apply_mapping_insert(state, cells, children)?,
        TableKind::TableStats => state.put_table_stats(decode_table_stats(cells)?),
        TableKind::TableColumnStats => {
            state.put_table_column_stats(decode_table_column_stats(cells)?);
        }
        TableKind::FileColumnStats => state.put_file_column_stats(decode_file_column_stats(cells)?),
        TableKind::FilesScheduledForDeletion => {
            state.put_gc_file(decode_gc_file_row(cells)?);
        }
        TableKind::Tag => {
            let (object_id, entry) = decode_tag_row(cells)?;
            state
                .tags
                .entry(object_id)
                .or_insert_with(|| proto::TagValue {
                    object_id,
                    entries: Vec::new(),
                })
                .entries
                .push(entry);
        }
        TableKind::ColumnTag => {
            let ((table_id, column_id), tag) = decode_column_tag_row(cells)?;
            let Some(column) = state
                .columns
                .get_mut(&table_id)
                .and_then(|cols| cols.get_mut(&column_id))
            else {
                return Err(corrupt_row(
                    table,
                    format!("column tag names an absent column ({table_id}, {column_id})"),
                ));
            };
            column.tags.push(tag);
        }
    }
    Ok(())
}

/// Ends the one live entry (`end_snapshot IS NULL`) for `key` in
/// `entries`, in place. `false` means no live entry matched — the caller
/// reports the shape error (DuckLake's UPDATE only names rows it read).
pub(super) fn end_live_entry<E>(
    entries: &mut [E],
    is_match: impl Fn(&E) -> bool,
    set_end: impl Fn(&mut E),
) -> bool {
    for entry in entries.iter_mut() {
        if is_match(entry) {
            set_end(entry);
            return true;
        }
    }
    false
}

/// Rejects a set-end cell whose `end_snapshot` is not this commit's own
/// snapshot id.
pub(super) fn check_end_snapshot(table: TableKind, end_snapshot: u64, new_id: u64) -> Result<()> {
    if end_snapshot == new_id {
        Ok(())
    } else {
        Err(corrupt_row(
            table,
            format!(
                "end_snapshot {end_snapshot} does not match this commit's snapshot id {new_id}"
            ),
        ))
    }
}

/// Ends a `ducklake_tag` row: the live entry named by `(object_id, key)`
/// gets its `end_snapshot` set, in place — containers never move to
/// history.
pub(super) fn apply_tag_set_end(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    new_id: u64,
) -> Result<()> {
    let mut c = Cursor::new(TableKind::Tag, cells);
    let object_id = c.u64()?;
    let key = c.string()?;
    let end_snapshot = c.u64()?;
    c.finish()?;
    check_end_snapshot(TableKind::Tag, end_snapshot, new_id)?;

    let ended = state.tags.get_mut(&object_id).is_some_and(|container| {
        end_live_entry(
            &mut container.entries,
            |e| e.key == key && e.end_snapshot.is_none(),
            |e| e.end_snapshot = Some(new_id),
        )
    });
    if ended {
        Ok(())
    } else {
        Err(corrupt_row(
            TableKind::Tag,
            format!("no live tag entry ({object_id}, {key:?}) to end"),
        ))
    }
}

/// Ends a `ducklake_column_tag` row: the live entry named by
/// `(table_id, column_id, key)` on the current column record.
pub(super) fn apply_column_tag_set_end(
    state: &mut CatalogSnapshot,
    cells: &[Cell],
    new_id: u64,
) -> Result<()> {
    let mut c = Cursor::new(TableKind::ColumnTag, cells);
    let table_id = c.u64()?;
    let column_id = c.u64()?;
    let key = c.string()?;
    let end_snapshot = c.u64()?;
    c.finish()?;
    check_end_snapshot(TableKind::ColumnTag, end_snapshot, new_id)?;

    let ended = state
        .columns
        .get_mut(&table_id)
        .and_then(|cols| cols.get_mut(&column_id))
        .is_some_and(|column| {
            end_live_entry(
                &mut column.tags,
                |t| t.key == key && t.end_snapshot.is_none(),
                |t| t.end_snapshot = Some(new_id),
            )
        });
    if ended {
        Ok(())
    } else {
        Err(corrupt_row(
            TableKind::ColumnTag,
            format!("no live column tag ({table_id}, {column_id}, {key:?}) to end"),
        ))
    }
}

pub(super) fn apply_update_set_end(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    new_id: u64,
) -> Result<()> {
    // Tag entries are embedded, not entity records of their own: ending
    // one rewrites its container in place rather than moving a key to
    // history.
    match table {
        TableKind::Tag => return apply_tag_set_end(state, cells, new_id),
        TableKind::ColumnTag => return apply_column_tag_set_end(state, cells, new_id),
        _ => {}
    }

    let (key, end_snapshot) = decode_end(table, cells)?;
    check_end_snapshot(table, end_snapshot, new_id)?;
    // End only the one row DuckLake named — never a cascade. DuckLake
    // authors every row change explicitly (a rename ends the table row but
    // keeps its columns live); the verb-path `delete_*` helpers would
    // cascade and end those siblings.
    let ended = match key {
        EntityKey::Schema { schema_id } => state.schemas.remove(&schema_id).is_some(),
        EntityKey::Table { table_id } => state.tables.remove(&table_id).is_some(),
        EntityKey::View { view_id } => state.views.remove(&view_id).is_some(),
        EntityKey::Column {
            table_id,
            column_id,
        } => state
            .columns
            .get_mut(&table_id)
            .is_some_and(|columns| columns.remove(&column_id).is_some()),
        EntityKey::File {
            table_id,
            data_file_id,
        } => state
            .data_files
            .get_mut(&table_id)
            .is_some_and(|files| files.remove(&data_file_id).is_some()),
        EntityKey::DeleteFile {
            table_id,
            delete_file_id,
        } => state
            .delete_files
            .get_mut(&table_id)
            .is_some_and(|files| files.remove(&delete_file_id).is_some()),
        EntityKey::Partition {
            table_id,
            partition_id,
        } => {
            let live = state
                .partitions
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&partition_id));
            state.delete_partition(table_id, partition_id);
            live
        }
        EntityKey::Sort { table_id, sort_id } => {
            let live = state
                .sorts
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&sort_id));
            state.delete_sort(table_id, sort_id);
            live
        }
        EntityKey::Macro { macro_id } => state.macros.remove(&macro_id).is_some(),
        // decode_end only ever returns the keys matched above.
        _ => return Err(corrupt_row(table, "unreachable entity key")),
    };
    // DuckLake's UPDATE only names rows it read; ending an absent row is
    // drift and must fail loudly, never pass as a no-op.
    if !ended {
        return Err(corrupt_row(
            table,
            format!("no live row to end for {key:?}"),
        ));
    }

    Ok(())
}

/// Rebases a data file's `begin_snapshot` in place. The target must have
/// been inserted by this same transaction (absent from `base`): DuckLake
/// only rebases the replacement file a delete-rewrite just created, and
/// rebasing a pre-existing row would rewrite committed visibility.
pub(super) fn apply_update_set_begin(
    base: &CatalogSnapshot,
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    new_id: u64,
) -> Result<()> {
    if table != TableKind::DataFile {
        return Err(Error::Constraint(format!(
            "update_set_begin is not defined for {table:?}"
        )));
    }
    let mut c = Cursor::new(table, cells);
    let table_id = c.u64()?;
    let data_file_id = c.u64()?;
    let begin_snapshot = c.u64()?;
    c.finish()?;
    if begin_snapshot != new_id {
        return Err(corrupt_row(
            table,
            format!(
                "begin_snapshot {begin_snapshot} does not match this commit's snapshot id {new_id}"
            ),
        ));
    }
    if base
        .data_files
        .get(&table_id)
        .is_some_and(|files| files.contains_key(&data_file_id))
    {
        return Err(corrupt_row(
            table,
            format!("file ({table_id}, {data_file_id}) predates this commit and cannot be rebased"),
        ));
    }

    let Some(file) = state
        .data_files
        .get_mut(&table_id)
        .and_then(|files| files.get_mut(&data_file_id))
    else {
        return Err(corrupt_row(
            table,
            format!("no data file ({table_id}, {data_file_id}) to rebase"),
        ));
    };
    file.begin_snapshot = begin_snapshot;
    Ok(())
}

/// A raw `DELETE` row. Three shapes share the op: unversioned records
/// (statistics, the deletion schedule) leave the working state and their
/// removal reaches the store through the diff; versioned rows and
/// snapshot records are pruned with direct key deletes (`history` keys
/// exist only in the store, never in the working state); embedded rows
/// (tag entries, spec columns) rewrite or ride their parent.
pub(super) fn apply_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
    direct: &mut Vec<commit::StagedWrite>,
) -> Result<()> {
    match table {
        TableKind::TableStats | TableKind::TableColumnStats | TableKind::FileColumnStats => {
            apply_stats_delete(state, table, cells)
        }
        TableKind::FilesScheduledForDeletion => {
            let mut c = Cursor::new(table, cells);
            let data_file_id = c.u64()?;
            c.finish()?;
            if state.gc_files.remove(&data_file_id).is_none() {
                return Err(corrupt_row(
                    table,
                    format!("no scheduled deletion for file {data_file_id}"),
                ));
            }
            Ok(())
        }
        // The merged snapshot record dies with the `ducklake_snapshot`
        // delete; the paired `ducklake_snapshot_changes` delete names the
        // same id and stages nothing.
        TableKind::Snapshot => {
            let mut c = Cursor::new(table, cells);
            let snapshot_id = c.u64()?;
            c.finish()?;
            if snapshot_id == state.snapshot.snapshot_id {
                return Err(Error::Constraint(format!(
                    "snapshot {snapshot_id} is the head and cannot be expired"
                )));
            }
            direct.push((Key::Snapshot { snapshot_id }.encode(), None));
            Ok(())
        }
        TableKind::SnapshotChanges => {
            let mut c = Cursor::new(table, cells);
            let _snapshot_id = c.u64()?;
            c.finish()?;
            Ok(())
        }
        // Mappings are unversioned create-only records with no history
        // mirror: cleanup is a direct `current` key delete. The working
        // state keeps its (now equal-to-base) entry, which the create-only
        // diff no-ops on.
        TableKind::ColumnMapping => {
            let mut c = Cursor::new(table, cells);
            let mapping_id = c.u64()?;
            let table_id = c.u64()?;
            c.finish()?;
            direct.push((
                Key::current(EntityKey::Mapping {
                    table_id,
                    mapping_id,
                })
                .encode(),
                None,
            ));
            Ok(())
        }
        TableKind::Schema
        | TableKind::Table
        | TableKind::View
        | TableKind::Column
        | TableKind::DataFile
        | TableKind::DeleteFile
        | TableKind::PartitionInfo
        | TableKind::SortInfo
        | TableKind::Macro => {
            let (entity, end_snapshot) = decode_hard_delete(table, cells)?;
            let key = match end_snapshot {
                Some(end) => Key::history(entity, end),
                None => Key::current(entity),
            };
            // The direct delete is the whole store mutation. The working
            // state is deliberately left alone: removing a live row from
            // it would make the diff stage an end-transition — minting a
            // history mirror DuckLake never authored — on top of this
            // delete. The cascade never re-touches a row it deleted, so
            // the stale working-state entry is unread.
            direct.push((key.encode(), None));
            Ok(())
        }
        TableKind::Tag => apply_tag_delete(state, cells),
        // Embedded rows ride their parent: the cascade deletes them only
        // alongside the parent record (a dead table's columns, a dead
        // file's partition values), so with the parent already pruned
        // there is nothing left to rewrite. A column-tag entry on a
        // still-current column is the one live case (its column survives
        // the entry's death) and rewrites the column in place.
        TableKind::ColumnTag => {
            let mut c = Cursor::new(table, cells);
            let table_id = c.u64()?;
            let column_id = c.u64()?;
            let key = c.string()?;
            let begin_snapshot = c.u64()?;
            c.finish()?;
            if let Some(column) = state
                .columns
                .get_mut(&table_id)
                .and_then(|cols| cols.get_mut(&column_id))
            {
                column
                    .tags
                    .retain(|t| !(t.key == key && t.begin_snapshot == begin_snapshot));
            }
            Ok(())
        }
        TableKind::PartitionColumn
        | TableKind::SortExpression
        | TableKind::FilePartitionValue
        | TableKind::MacroImpl
        | TableKind::MacroParameters
        | TableKind::NameMapping => apply_embedded_delete(state, table, cells),
        TableKind::SchemaVersions => {
            // Schema-version rows fold into snapshot records; the rows a
            // dead-table cleanup deletes are visible only through
            // snapshots the same transaction deletes, so there is nothing
            // separate to remove.
            let mut c = Cursor::new(table, cells);
            let _begin_snapshot = c.u64()?;
            let _schema_version = c.u64()?;
            let _table_id = c.u64()?;
            c.finish()?;
            Ok(())
        }
    }
}

/// Removes a dead `ducklake_tag` entry from its container; a container
/// left empty is removed outright (the key must not linger).
pub(super) fn apply_tag_delete(state: &mut CatalogSnapshot, cells: &[Cell]) -> Result<()> {
    let mut c = Cursor::new(TableKind::Tag, cells);
    let object_id = c.u64()?;
    let key = c.string()?;
    let begin_snapshot = c.u64()?;
    c.finish()?;

    let removed = state.tags.get_mut(&object_id).is_some_and(|container| {
        let before = container.entries.len();
        container
            .entries
            .retain(|e| !(e.key == key && e.begin_snapshot == begin_snapshot));
        container.entries.len() < before
    });
    if !removed {
        return Err(corrupt_row(
            TableKind::Tag,
            format!("no tag entry ({object_id}, {key:?}, {begin_snapshot}) to delete"),
        ));
    }
    if state
        .tags
        .get(&object_id)
        .is_some_and(|container| container.entries.is_empty())
    {
        state.tags.remove(&object_id);
    }
    Ok(())
}

/// A spec-column / partition-value delete: only ever issued alongside its
/// parent record's own deletion, so a still-current parent means the
/// cascade named a row this model says cannot die alone.
pub(super) fn apply_embedded_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
) -> Result<()> {
    let mut c = Cursor::new(table, cells);
    let parent_is_current = match table {
        TableKind::PartitionColumn => {
            let partition_id = c.u64()?;
            let table_id = c.u64()?;
            state
                .partitions
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&partition_id))
        }
        TableKind::SortExpression => {
            let sort_id = c.u64()?;
            let table_id = c.u64()?;
            state
                .sorts
                .get(&table_id)
                .is_some_and(|specs| specs.contains_key(&sort_id))
        }
        TableKind::FilePartitionValue => {
            let data_file_id = c.u64()?;
            let table_id = c.u64()?;
            state
                .data_files
                .get(&table_id)
                .is_some_and(|files| files.contains_key(&data_file_id))
        }
        TableKind::MacroImpl | TableKind::MacroParameters => {
            let macro_id = c.u64()?;
            state.macros.contains_key(&macro_id)
        }
        TableKind::NameMapping => {
            let mapping_id = c.u64()?;
            state
                .mappings
                .values()
                .any(|per_table| per_table.contains_key(&mapping_id))
        }
        _ => return Err(corrupt_row(table, "not an embedded kind")),
    };
    // Remaining identity cells vary by kind and are not needed: the row
    // dies with its parent.
    if parent_is_current {
        return Err(corrupt_row(
            table,
            "embedded row deleted while its parent record is still live",
        ));
    }
    Ok(())
}

/// A stats delete naming an absent row is a no-op, unlike every other
/// delete path (where a miss means drift and fails loudly): DuckLake's
/// drop cleanup issues bulk `DELETE ... WHERE table_id IN (...)` against
/// the stats tables without reading them first, so zero matches is a
/// legitimate outcome, exactly as it is in SQL.
pub(super) fn apply_stats_delete(
    state: &mut CatalogSnapshot,
    table: TableKind,
    cells: &[Cell],
) -> Result<()> {
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
pub(super) fn find_snapshot_rows(ops: &[RowOperation]) -> Result<(&[Cell], &[Cell])> {
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

pub(super) fn build_snapshot_value(ops: &[RowOperation]) -> Result<proto::SnapshotValue> {
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
        changes_made,
        author,
        commit_message,
        commit_extra_info,
        schema_changed_table_ids,
    })
}
