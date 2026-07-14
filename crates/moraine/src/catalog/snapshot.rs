//! An immutable, materialized catalog view. Built once from a consistent
//! store scan; every accessor is an in-memory lookup afterwards.

use std::collections::{BTreeMap, HashMap};

use crate::{
    catalog::types::{
        ColumnId, ColumnInfo, ColumnStats, DataFileId, DataFileInfo, DeleteFileId, DeleteFileInfo,
        MacroId, MacroImplementationDef, MacroInfo, MacroParameterDef, MappingId, MappingInfo,
        NameMappingDef, OptionScope, ScheduledDeletion, SchemaId, SchemaInfo, SnapshotId,
        SnapshotInfo, TableId, TableInfo, TableStats, TagEntry, ViewId, ViewInfo,
    },
    store::{
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, GcFileValue,
            MacroValue, MappingValue, OptionScopeValue, PartitionValue, SchemaValue, SnapshotValue,
            SortValue, TableColumnStatsValue, TableStatsValue, TableValue, TagValue, ViewValue,
        },
        read::EntityRecord,
    },
};

/// An immutable catalog view at one snapshot.
///
/// Reads issue no store I/O after the view is built — a `CatalogSnapshot`
/// is a value, not a cursor. The default value is an empty view at
/// snapshot 0.
#[derive(Debug, Clone, Default)]
pub struct CatalogSnapshot {
    pub(crate) snapshot: SnapshotValue,
    pub(crate) schemas: BTreeMap<u64, SchemaValue>,
    pub(crate) tables: BTreeMap<u64, TableValue>,
    pub(crate) views: BTreeMap<u64, ViewValue>,
    pub(crate) macros: BTreeMap<u64, MacroValue>,
    pub(crate) columns: BTreeMap<u64, BTreeMap<u64, ColumnValue>>,
    pub(crate) schema_names: HashMap<String, u64>,
    pub(crate) table_names: HashMap<(u64, String), u64>,
    pub(crate) view_names: HashMap<(u64, String), u64>,
    pub(crate) macro_names: HashMap<(u64, String), u64>,
    pub(crate) data_files: BTreeMap<u64, BTreeMap<u64, DataFileValue>>,
    pub(crate) delete_files: BTreeMap<u64, BTreeMap<u64, DeleteFileValue>>,
    pub(crate) partitions: BTreeMap<u64, BTreeMap<u64, PartitionValue>>,
    pub(crate) sorts: BTreeMap<u64, BTreeMap<u64, SortValue>>,
    pub(crate) mappings: BTreeMap<u64, BTreeMap<u64, MappingValue>>,
    pub(crate) table_stats: BTreeMap<u64, TableStatsValue>,
    pub(crate) table_column_stats: BTreeMap<u64, BTreeMap<u64, TableColumnStatsValue>>,
    pub(crate) file_column_stats: BTreeMap<u64, BTreeMap<(u64, u64), FileColumnStatsValue>>,
    pub(crate) options: BTreeMap<(u64, u64), OptionScopeValue>,
    pub(crate) tags: BTreeMap<u64, TagValue>,
    pub(crate) gc_files: BTreeMap<u64, GcFileValue>,
}

impl CatalogSnapshot {
    /// Builds the view. With `at: None` every `current` record is live and
    /// `history` is unused (allocation reads the table's persisted counter,
    /// not history); with `at: Some(s)` a record is included iff
    /// `begin_snapshot <= s` and it had not ended by `s` (`current` records
    /// never have; `history` records carry their end).
    pub(crate) fn build(
        snapshot: SnapshotValue,
        current: Vec<EntityRecord>,
        history: Vec<EntityRecord>,
        at: Option<u64>,
    ) -> Self {
        let mut view = Self {
            snapshot,
            ..Self::default()
        };

        let included = |begin: u64, end: Option<u64>| match at {
            None => end.is_none(),
            Some(s) => begin <= s && end.is_none_or(|e| e > s),
        };
        for record in current.into_iter().chain(history) {
            match record {
                EntityRecord::Schema(s) if included(s.begin_snapshot, s.end_snapshot) => {
                    view.put_schema(s);
                }
                EntityRecord::Table(t) if included(t.begin_snapshot, t.end_snapshot) => {
                    view.put_table(t);
                }
                EntityRecord::Column(c) if included(c.begin_snapshot, c.end_snapshot) => {
                    view.put_column(c);
                }
                EntityRecord::File(f) if included(f.begin_snapshot, f.end_snapshot) => {
                    view.put_data_file(f);
                }
                EntityRecord::DeleteFile(d) if included(d.begin_snapshot, d.end_snapshot) => {
                    view.put_delete_file(d);
                }
                EntityRecord::View(v) if included(v.begin_snapshot, v.end_snapshot) => {
                    view.put_view(v);
                }
                EntityRecord::Partition(p) if included(p.begin_snapshot, p.end_snapshot) => {
                    view.put_partition(p);
                }
                EntityRecord::Sort(s) if included(s.begin_snapshot, s.end_snapshot) => {
                    view.put_sort(s);
                }
                EntityRecord::Macro(m) if included(m.begin_snapshot, m.end_snapshot) => {
                    view.put_macro(m);
                }
                // Mappings are unversioned and immutable: included at any
                // time-travel target, since a historical file read still
                // resolves through its mapping.
                EntityRecord::Mapping(m) => {
                    view.put_mapping(m);
                }
                EntityRecord::FileColumnStats(fcs) => {
                    view.put_file_column_stats(fcs);
                }
                EntityRecord::TableStats(ts) => {
                    view.put_table_stats(ts);
                }
                EntityRecord::TableColumnStats(tcs) => {
                    view.put_table_column_stats(tcs);
                }
                EntityRecord::Option {
                    scope_kind,
                    scope_id,
                    value,
                } => {
                    view.set_option_record((scope_kind, scope_id), value);
                }
                // Tag containers are unversioned; their embedded entries
                // carry begin/end individually and are filtered at read.
                EntityRecord::Tag(t) => {
                    view.put_tag(t);
                }
                // Deletion-schedule rows are live bookkeeping with no
                // temporal lifecycle.
                EntityRecord::GcFile(g) => {
                    view.put_gc_file(g);
                }
                EntityRecord::Schema(_)
                | EntityRecord::Table(_)
                | EntityRecord::Column(_)
                | EntityRecord::File(_)
                | EntityRecord::DeleteFile(_)
                | EntityRecord::View(_)
                | EntityRecord::Partition(_)
                | EntityRecord::Sort(_)
                | EntityRecord::Macro(_) => {
                    // Filtered out by version range
                }
            }
        }
        view
    }

    /// Snapshot identity and metadata of this view.
    #[must_use]
    pub fn current_snapshot(&self) -> SnapshotInfo {
        SnapshotInfo {
            id: SnapshotId::new(self.snapshot.snapshot_id),
            time_micros: self.snapshot.snapshot_time_micros,
            schema_version: self.snapshot.schema_version,
        }
    }

    /// All live schemas, ordered by id.
    #[must_use]
    pub fn schemas(&self) -> Vec<SchemaInfo> {
        self.schemas.values().map(schema_info).collect()
    }

    /// One schema by name.
    #[must_use]
    pub fn schema_by_name(&self, name: &str) -> Option<SchemaInfo> {
        self.schema_names
            .get(name)
            .and_then(|id| self.schemas.get(id))
            .map(schema_info)
    }

    /// One schema by id.
    #[must_use]
    pub fn schema_by_id(&self, id: SchemaId) -> Option<SchemaInfo> {
        self.schemas.get(&id.get()).map(schema_info)
    }

    /// The live tables of a schema, ordered by id.
    #[must_use]
    pub fn tables_in(&self, schema: SchemaId) -> Vec<TableInfo> {
        self.tables
            .values()
            .filter(|t| t.schema_id == schema.get())
            .map(table_info)
            .collect()
    }

    /// One table by name within a schema.
    #[must_use]
    pub fn table_by_name(&self, schema: SchemaId, name: &str) -> Option<TableInfo> {
        self.table_names
            .get(&(schema.get(), name.to_owned()))
            .and_then(|id| self.tables.get(id))
            .map(table_info)
    }

    /// One table by id.
    #[must_use]
    pub fn table_by_id(&self, id: TableId) -> Option<TableInfo> {
        self.tables.get(&id.get()).map(table_info)
    }

    /// The live columns of a table, ordered by position.
    #[must_use]
    pub fn columns_of(&self, table: TableId) -> Vec<ColumnInfo> {
        let Some(columns) = self.columns.get(&table.get()) else {
            return Vec::new();
        };

        let mut infos: Vec<ColumnInfo> = columns
            .values()
            .map(|c| ColumnInfo {
                id: ColumnId::new(c.column_id),
                name: c.column_name.clone(),
                column_type: c.column_type.clone(),
                nulls_allowed: c.nulls_allowed,
                default_value: c.default_value.clone(),
                position: c.column_order,
                parent_column: c.parent_column.map(ColumnId::new),
            })
            .collect();
        infos.sort_by_key(|c| c.position);
        infos
    }

    /// The schema's live views, ordered by id.
    #[must_use]
    pub fn views_in(&self, schema: SchemaId) -> Vec<ViewInfo> {
        self.views
            .values()
            .filter(|v| v.schema_id == schema.get())
            .map(view_info)
            .collect()
    }

    /// One view by name within a schema.
    #[must_use]
    pub fn view_by_name(&self, schema: SchemaId, name: &str) -> Option<ViewInfo> {
        self.view_names
            .get(&(schema.get(), name.to_owned()))
            .and_then(|id| self.views.get(id))
            .map(view_info)
    }

    /// One view by id.
    #[must_use]
    pub fn view_by_id(&self, id: ViewId) -> Option<ViewInfo> {
        self.views.get(&id.get()).map(view_info)
    }

    /// The live macros of a schema.
    #[must_use]
    pub fn macros_in(&self, schema: SchemaId) -> Vec<MacroInfo> {
        self.macros
            .values()
            .filter(|m| m.schema_id == schema.get())
            .map(macro_info)
            .collect()
    }

    /// One macro by name within a schema. Macros have their own name
    /// namespace, separate from tables and views.
    #[must_use]
    pub fn macro_by_name(&self, schema: SchemaId, name: &str) -> Option<MacroInfo> {
        self.macro_names
            .get(&(schema.get(), name.to_owned()))
            .and_then(|id| self.macros.get(id))
            .map(macro_info)
    }

    /// One macro by id.
    #[must_use]
    pub fn macro_by_id(&self, id: MacroId) -> Option<MacroInfo> {
        self.macros.get(&id.get()).map(macro_info)
    }

    /// A table's column mappings in `mapping_id` order. Mappings are
    /// unversioned: any time-travel view serves the full set.
    #[must_use]
    pub fn mappings_of(&self, table: TableId) -> Vec<MappingInfo> {
        self.mappings
            .get(&table.get())
            .map(|per_table| per_table.values().map(mapping_info).collect())
            .unwrap_or_default()
    }

    /// A resolved option value: table falls back to its schema, then
    /// global; schema falls back to global. Options are unversioned: a
    /// time-traveled snapshot resolves current values.
    #[must_use]
    pub fn option(&self, scope: OptionScope, key: &str) -> Option<String> {
        let mut candidates = vec![scope.key_components()];
        match scope {
            OptionScope::Table(t) => {
                if let Some(table) = self.tables.get(&t.get()) {
                    candidates
                        .push(OptionScope::Schema(SchemaId::new(table.schema_id)).key_components());
                }
                candidates.push(OptionScope::Global.key_components());
            }
            OptionScope::Schema(_) => candidates.push(OptionScope::Global.key_components()),
            OptionScope::Global => {}
        }
        candidates.into_iter().find_map(|c| {
            self.options
                .get(&c)
                .and_then(|v| v.options.get(key).cloned())
        })
    }

    /// Every tag row on `object_id` (a schema/table/view id), ended
    /// entries included — each entry carries its own begin/end, so
    /// callers filter by lifecycle where it matters.
    #[must_use]
    pub fn tags_of(&self, object_id: u64) -> Vec<TagEntry> {
        self.tags
            .get(&object_id)
            .map(|container| {
                container
                    .entries
                    .iter()
                    .map(|e| TagEntry {
                        begin_snapshot: e.begin_snapshot,
                        end_snapshot: e.end_snapshot,
                        key: e.key.clone(),
                        value: e.value.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn put_tag(&mut self, value: TagValue) {
        self.tags.insert(value.object_id, value);
    }

    /// Files scheduled for physical deletion, ascending by file id.
    #[must_use]
    pub fn scheduled_deletions(&self) -> Vec<ScheduledDeletion> {
        self.gc_files
            .values()
            .map(|g| ScheduledDeletion {
                data_file_id: g.data_file_id,
                path: g.path.clone(),
                path_is_relative: g.path_is_relative,
                schedule_start_micros: g.schedule_start_micros,
            })
            .collect()
    }

    pub(crate) fn put_gc_file(&mut self, value: GcFileValue) {
        self.gc_files.insert(value.data_file_id, value);
    }

    pub(crate) fn put_schema(&mut self, value: SchemaValue) {
        if let Some(old) = self.schemas.get(&value.schema_id) {
            self.schema_names.remove(&old.schema_name);
        }
        self.schema_names
            .insert(value.schema_name.clone(), value.schema_id);
        self.schemas.insert(value.schema_id, value);
    }

    pub(crate) fn delete_schema(&mut self, schema_id: u64) {
        if let Some(old) = self.schemas.remove(&schema_id) {
            self.schema_names.remove(&old.schema_name);
        }
        self.remove_option_record(OptionScope::Schema(SchemaId::new(schema_id)).key_components());
    }

    pub(crate) fn put_table(&mut self, value: TableValue) {
        if let Some(old) = self.tables.get(&value.table_id) {
            self.table_names
                .remove(&(old.schema_id, old.table_name.clone()));
        }
        self.table_names
            .insert((value.schema_id, value.table_name.clone()), value.table_id);
        self.tables.insert(value.table_id, value);
    }

    pub(crate) fn delete_table(&mut self, table_id: u64) {
        if let Some(old) = self.tables.remove(&table_id) {
            self.table_names
                .remove(&(old.schema_id, old.table_name.clone()));
        }
        self.columns.remove(&table_id);
        self.data_files.remove(&table_id);
        self.delete_files.remove(&table_id);
        self.partitions.remove(&table_id);
        self.sorts.remove(&table_id);
        self.table_stats.remove(&table_id);
        self.table_column_stats.remove(&table_id);
        self.remove_option_record(OptionScope::Table(TableId::new(table_id)).key_components());
        // file_column_stats is kept: per-file stats outlive the file's
        // live version until its history is garbage-collected.
    }

    pub(crate) fn put_view(&mut self, value: ViewValue) {
        if let Some(old) = self.views.get(&value.view_id) {
            self.view_names
                .remove(&(old.schema_id, old.view_name.clone()));
        }
        self.view_names
            .insert((value.schema_id, value.view_name.clone()), value.view_id);
        self.views.insert(value.view_id, value);
    }

    pub(crate) fn delete_view(&mut self, view_id: u64) {
        if let Some(old) = self.views.remove(&view_id) {
            self.view_names
                .remove(&(old.schema_id, old.view_name.clone()));
        }
    }

    pub(crate) fn put_mapping(&mut self, value: MappingValue) {
        self.mappings
            .entry(value.table_id)
            .or_default()
            .insert(value.mapping_id, value);
    }

    pub(crate) fn put_macro(&mut self, value: MacroValue) {
        if let Some(old) = self.macros.get(&value.macro_id) {
            self.macro_names
                .remove(&(old.schema_id, old.macro_name.clone()));
        }
        self.macro_names
            .insert((value.schema_id, value.macro_name.clone()), value.macro_id);
        self.macros.insert(value.macro_id, value);
    }
    pub(crate) fn delete_macro(&mut self, macro_id: u64) {
        if let Some(old) = self.macros.remove(&macro_id) {
            self.macro_names
                .remove(&(old.schema_id, old.macro_name.clone()));
        }
    }

    pub(crate) fn put_column(&mut self, value: ColumnValue) {
        self.columns
            .entry(value.table_id)
            .or_default()
            .insert(value.column_id, value);
    }

    pub(crate) fn delete_column(&mut self, table_id: u64, column_id: u64) {
        if let Some(columns) = self.columns.get_mut(&table_id) {
            columns.remove(&column_id);
        }
        // Column stats describe current state and die with the column.
        if let Some(cols) = self.table_column_stats.get_mut(&table_id) {
            cols.remove(&column_id);
        }
    }

    /// The table's data files live at this view's snapshot, ordered by id.
    #[must_use]
    pub fn data_files_of(&self, table: TableId) -> Vec<DataFileInfo> {
        let Some(files) = self.data_files.get(&table.get()) else {
            return Vec::new();
        };

        files.values().map(data_file_info).collect()
    }

    /// The table's delete files live at this view's snapshot, ordered by id.
    #[must_use]
    pub fn delete_files_of(&self, table: TableId) -> Vec<DeleteFileInfo> {
        let Some(files) = self.delete_files.get(&table.get()) else {
            return Vec::new();
        };

        files.values().map(delete_file_info).collect()
    }

    /// Statistics for a table. Unversioned: a time-travel view serves
    /// the current statistics.
    #[must_use]
    pub fn table_stats(&self, table: TableId) -> Option<TableStats> {
        self.table_stats
            .get(&table.get())
            .map(table_stats_from_proto)
    }

    /// Statistics for a column. Unversioned: a time-travel view serves
    /// the current statistics.
    #[must_use]
    pub fn column_stats(&self, table: TableId, column: ColumnId) -> Option<ColumnStats> {
        self.table_column_stats
            .get(&table.get())
            .and_then(|cols| cols.get(&column.get()))
            .map(column_stats_from_proto)
    }

    pub(crate) fn put_data_file(&mut self, value: DataFileValue) {
        self.data_files
            .entry(value.table_id)
            .or_default()
            .insert(value.data_file_id, value);
    }

    pub(crate) fn delete_data_file(&mut self, table_id: u64, data_file_id: u64) {
        if let Some(files) = self.data_files.get_mut(&table_id) {
            files.remove(&data_file_id);
        }
    }

    pub(crate) fn put_partition(&mut self, value: PartitionValue) {
        self.partitions
            .entry(value.table_id)
            .or_default()
            .insert(value.partition_id, value);
    }

    pub(crate) fn delete_partition(&mut self, table_id: u64, partition_id: u64) {
        if let Some(specs) = self.partitions.get_mut(&table_id) {
            specs.remove(&partition_id);
        }
    }

    pub(crate) fn put_sort(&mut self, value: SortValue) {
        self.sorts
            .entry(value.table_id)
            .or_default()
            .insert(value.sort_id, value);
    }

    pub(crate) fn delete_sort(&mut self, table_id: u64, sort_id: u64) {
        if let Some(specs) = self.sorts.get_mut(&table_id) {
            specs.remove(&sort_id);
        }
    }

    pub(crate) fn put_delete_file(&mut self, value: DeleteFileValue) {
        self.delete_files
            .entry(value.table_id)
            .or_default()
            .insert(value.delete_file_id, value);
    }

    pub(crate) fn delete_delete_file(&mut self, table_id: u64, delete_file_id: u64) {
        if let Some(files) = self.delete_files.get_mut(&table_id) {
            files.remove(&delete_file_id);
        }
    }

    pub(crate) fn put_table_stats(&mut self, value: TableStatsValue) {
        self.table_stats.insert(value.table_id, value);
    }

    pub(crate) fn put_table_column_stats(&mut self, value: TableColumnStatsValue) {
        self.table_column_stats
            .entry(value.table_id)
            .or_default()
            .insert(value.column_id, value);
    }

    pub(crate) fn put_file_column_stats(&mut self, value: FileColumnStatsValue) {
        self.file_column_stats
            .entry(value.table_id)
            .or_default()
            .insert((value.data_file_id, value.column_id), value);
    }

    pub(crate) fn set_option_record(&mut self, components: (u64, u64), value: OptionScopeValue) {
        self.options.insert(components, value);
    }

    pub(crate) fn remove_option_record(&mut self, components: (u64, u64)) {
        self.options.remove(&components);
    }
}

fn schema_info(value: &SchemaValue) -> SchemaInfo {
    SchemaInfo {
        id: SchemaId::new(value.schema_id),
        name: value.schema_name.clone(),
    }
}

fn table_info(value: &TableValue) -> TableInfo {
    TableInfo {
        id: TableId::new(value.table_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.table_name.clone(),
    }
}

fn view_info(value: &ViewValue) -> ViewInfo {
    ViewInfo {
        id: ViewId::new(value.view_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.view_name.clone(),
        dialect: value.dialect.clone(),
        sql: value.sql.clone(),
    }
}

fn mapping_info(value: &MappingValue) -> MappingInfo {
    MappingInfo {
        id: MappingId::new(value.mapping_id),
        table_id: TableId::new(value.table_id),
        map_type: value.map_type.clone(),
        name_mappings: value
            .name_mappings
            .iter()
            .map(|row| NameMappingDef {
                column_id: row.column_id,
                source_name: row.source_name.clone(),
                target_field_id: row.target_field_id,
                parent_column: row.parent_column,
                is_partition: row.is_partition,
            })
            .collect(),
    }
}

fn macro_info(value: &MacroValue) -> MacroInfo {
    MacroInfo {
        id: MacroId::new(value.macro_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.macro_name.clone(),
        implementations: value
            .implementations
            .iter()
            .map(|implementation| MacroImplementationDef {
                dialect: implementation.dialect.clone(),
                sql: implementation.sql.clone(),
                macro_type: implementation.macro_type.clone(),
                parameters: implementation
                    .parameters
                    .iter()
                    .map(|parameter| MacroParameterDef {
                        name: parameter.parameter_name.clone(),
                        parameter_type: parameter.parameter_type.clone(),
                        default_value: parameter.default_value.clone(),
                        default_value_type: parameter.default_value_type.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn data_file_info(value: &DataFileValue) -> DataFileInfo {
    DataFileInfo {
        id: DataFileId::new(value.data_file_id),
        path: value.path.clone(),
        path_is_relative: value.path_is_relative,
        file_format: value.file_format.clone(),
        record_count: value.record_count,
        file_size_bytes: value.file_size_bytes,
        footer_size: value.footer_size,
        row_id_start: value.row_id_start,
        encryption_key: value.encryption_key.clone(),
    }
}

fn delete_file_info(value: &DeleteFileValue) -> DeleteFileInfo {
    DeleteFileInfo {
        id: DeleteFileId::new(value.delete_file_id),
        data_file_id: DataFileId::new(value.data_file_id),
        path: value.path.clone(),
        path_is_relative: value.path_is_relative,
        format: value.format.clone(),
        delete_count: value.delete_count,
        file_size_bytes: value.file_size_bytes,
        footer_size: value.footer_size,
        encryption_key: value.encryption_key.clone(),
    }
}

fn table_stats_from_proto(value: &TableStatsValue) -> TableStats {
    TableStats {
        record_count: value.record_count,
        file_size_bytes: value.file_size_bytes,
        next_row_id: value.next_row_id,
    }
}

fn column_stats_from_proto(value: &TableColumnStatsValue) -> ColumnStats {
    ColumnStats {
        contains_null: value.contains_null,
        contains_nan: value.contains_nan,
        min_value: value.min_value.clone(),
        max_value: value.max_value.clone(),
        extra_stats: value.extra_stats.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::DataFileId;

    fn snap(id: u64) -> SnapshotValue {
        SnapshotValue {
            snapshot_id: id,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        }
    }

    fn schema_rec(id: u64, name: &str, begin: u64, end: Option<u64>) -> SchemaValue {
        SchemaValue {
            schema_id: id,
            schema_uuid: format!("uuid-{id}"),
            begin_snapshot: begin,
            end_snapshot: end,
            schema_name: name.into(),
            path: format!("{name}/"),
            path_is_relative: true,
        }
    }

    fn table_rec(id: u64, schema: u64, name: &str, begin: u64) -> TableValue {
        TableValue {
            table_id: id,
            table_uuid: format!("uuid-t{id}"),
            begin_snapshot: begin,
            end_snapshot: None,
            schema_id: schema,
            table_name: name.into(),
            path: format!("{name}/"),
            path_is_relative: true,
            next_column_id: 0,
        }
    }

    fn column_rec(table: u64, id: u64, name: &str, order: u64, begin: u64) -> ColumnValue {
        ColumnValue {
            column_id: id,
            begin_snapshot: begin,
            end_snapshot: None,
            table_id: table,
            column_order: order,
            column_name: name.into(),
            column_type: "BIGINT".into(),
            initial_default: None,
            default_value: None,
            nulls_allowed: true,
            parent_column: None,
            default_value_type: None,
            default_value_dialect: None,
            tags: vec![],
        }
    }

    fn file_rec(table: u64, id: u64, rows: u64, begin: u64, end: Option<u64>) -> DataFileValue {
        DataFileValue {
            data_file_id: id,
            table_id: table,
            begin_snapshot: begin,
            end_snapshot: end,
            file_order: None,
            path: format!("f{id}.parquet"),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: rows,
            file_size_bytes: rows * 10,
            footer_size: 4,
            row_id_start: Some(0),
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        }
    }

    fn view_rec(id: u64, schema: u64, name: &str, begin: u64, end: Option<u64>) -> ViewValue {
        ViewValue {
            view_id: id,
            view_uuid: format!("uuid-v{id}"),
            begin_snapshot: begin,
            end_snapshot: end,
            schema_id: schema,
            view_name: name.into(),
            dialect: "duckdb".into(),
            sql: format!("select * from {name}"),
            column_aliases: None,
        }
    }

    fn macro_rec(id: u64, schema: u64, name: &str, begin: u64, end: Option<u64>) -> MacroValue {
        MacroValue {
            macro_id: id,
            begin_snapshot: begin,
            end_snapshot: end,
            schema_id: schema,
            macro_name: name.into(),
            implementations: vec![crate::store::proto::MacroImplementation {
                impl_id: 0,
                dialect: "duckdb".into(),
                sql: "1".into(),
                macro_type: "scalar".into(),
                parameters: vec![],
            }],
        }
    }

    fn tstat_rec(table: u64, count: u64, next_row: u64) -> TableStatsValue {
        TableStatsValue {
            table_id: table,
            record_count: count,
            next_row_id: next_row,
            file_size_bytes: count * 10,
        }
    }

    #[test]
    fn head_view_indexes_live_entities() {
        let current = vec![
            EntityRecord::Schema(schema_rec(0, "main", 1, None)),
            EntityRecord::Table(table_rec(1, 0, "orders", 2)),
            EntityRecord::Column(column_rec(1, 0, "id", 0, 2)),
            EntityRecord::Column(column_rec(1, 1, "amount", 1, 2)),
        ];
        let view = CatalogSnapshot::build(snap(2), current, vec![], None);

        let s = view.schema_by_name("main").unwrap();
        assert_eq!(s.id, SchemaId::new(0));
        let t = view.table_by_name(s.id, "orders").unwrap();
        assert_eq!(t.id, TableId::new(1));
        assert_eq!(view.tables_in(s.id), vec![t.clone()]);
        let cols = view.columns_of(t.id);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[1].position, 1);
        assert_eq!(view.current_snapshot().id, SnapshotId::new(2));
    }

    #[test]
    fn time_travel_filters_by_begin_and_end() {
        // Schema 0 lived [1, 3); its replacement version lives [3, ∞).
        // Table 1 was created at 4.
        let current = vec![
            EntityRecord::Schema(schema_rec(0, "renamed", 3, None)),
            EntityRecord::Table(table_rec(1, 0, "orders", 4)),
        ];
        let history = vec![EntityRecord::Schema(schema_rec(0, "original", 1, Some(3)))];

        let at2 = CatalogSnapshot::build(snap(2), current.clone(), history.clone(), Some(2));
        assert!(at2.schema_by_name("original").is_some());
        assert!(at2.schema_by_name("renamed").is_none());
        assert!(at2.table_by_id(TableId::new(1)).is_none());

        let at4 = CatalogSnapshot::build(snap(4), current, history, Some(4));
        assert!(at4.schema_by_name("renamed").is_some());
        assert!(at4.table_by_id(TableId::new(1)).is_some());
    }

    #[test]
    fn history_only_records_are_excluded_from_the_head_view() {
        // Column allocation now reads the table's persisted counter, not
        // `history` — so `build` at head must not resurrect a dropped column
        // (or any other purely-historical record) into the live view.
        let history = vec![EntityRecord::Column(ColumnValue {
            end_snapshot: Some(3),
            ..column_rec(1, 5, "gone", 0, 1)
        })];
        let view = CatalogSnapshot::build(snap(3), vec![], history, None);
        assert!(view.columns_of(TableId::new(1)).is_empty());
    }

    #[test]
    fn mutation_helpers_keep_indexes_coherent() {
        let mut view = CatalogSnapshot::build(snap(1), vec![], vec![], None);
        view.put_schema(schema_rec(0, "main", 2, None));
        view.put_table(table_rec(1, 0, "orders", 2));
        view.put_column(column_rec(1, 0, "id", 0, 2));
        assert!(view.table_by_name(SchemaId::new(0), "orders").is_some());

        let mut renamed = view.tables[&1].clone();
        renamed.table_name = "orders_v2".into();
        view.put_table(renamed);
        assert!(view.table_by_name(SchemaId::new(0), "orders").is_none());
        assert!(view.table_by_name(SchemaId::new(0), "orders_v2").is_some());

        view.delete_table(1);
        assert!(view.table_by_id(TableId::new(1)).is_none());
        assert!(view.columns_of(TableId::new(1)).is_empty());

        view.delete_schema(0);
        assert!(view.schema_by_name("main").is_none());
    }

    #[test]
    fn data_files_filter_by_version_but_stats_do_not() {
        let current = vec![
            EntityRecord::File(file_rec(1, 10, 100, 5, None)),
            EntityRecord::TableStats(tstat_rec(1, 100, 100)),
        ];
        let history = vec![EntityRecord::File(file_rec(1, 9, 50, 1, Some(5)))];

        // Head view: only the live file; stats present.
        let head = CatalogSnapshot::build(snap(6), current.clone(), vec![], None);
        let files = head.data_files_of(TableId::new(1));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, DataFileId::new(10));
        assert_eq!(head.table_stats(TableId::new(1)).unwrap().record_count, 100);

        // Time travel to snapshot 3: the ended file was live, the new one
        // not yet; stats are unversioned and served as-is.
        let past = CatalogSnapshot::build(snap(3), current, history, Some(3));
        let files = past.data_files_of(TableId::new(1));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, DataFileId::new(9));
        assert_eq!(past.table_stats(TableId::new(1)).unwrap().next_row_id, 100);
    }

    #[test]
    fn stats_accessors_and_helpers() {
        let mut view = CatalogSnapshot::build(snap(1), vec![], vec![], None);
        assert!(view.table_stats(TableId::new(1)).is_none());
        assert!(
            view.column_stats(TableId::new(1), ColumnId::new(1))
                .is_none()
        );

        view.put_table_stats(tstat_rec(1, 7, 7));
        view.put_table_column_stats(TableColumnStatsValue {
            table_id: 1,
            column_id: 2,
            contains_null: Some(true),
            contains_nan: None,
            min_value: Some("9".into()),
            max_value: Some("10".into()),
            extra_stats: None,
        });
        // Verbatim strings: '9'/'10' come back untouched, never compared.
        let stats = view
            .column_stats(TableId::new(1), ColumnId::new(2))
            .unwrap();
        assert_eq!(stats.min_value.as_deref(), Some("9"));
        assert_eq!(stats.max_value.as_deref(), Some("10"));

        view.put_data_file(file_rec(1, 3, 10, 1, None));
        view.put_delete_file(DeleteFileValue {
            delete_file_id: 4,
            table_id: 1,
            begin_snapshot: 1,
            end_snapshot: None,
            data_file_id: 3,
            path: "d.parquet".into(),
            path_is_relative: true,
            format: "parquet".into(),
            delete_count: 2,
            file_size_bytes: 20,
            footer_size: 4,
            encryption_key: None,
            partial_max: None,
        });
        assert_eq!(
            view.delete_files_of(TableId::new(1))[0].data_file_id,
            DataFileId::new(3)
        );

        view.put_file_column_stats(FileColumnStatsValue {
            data_file_id: 3,
            table_id: 1,
            column_id: 2,
            column_size_bytes: 10,
            value_count: 10,
            null_count: 0,
            min_value: Some("1".into()),
            max_value: Some("2".into()),
            contains_nan: None,
            extra_stats: None,
            variant_stats: vec![],
        });

        // Dropping the table clears every per-table map except
        // file_column_stats, which outlives the table's live version
        // until the file's history is garbage-collected.
        view.delete_table(1);
        assert!(view.data_files_of(TableId::new(1)).is_empty());
        assert!(view.delete_files_of(TableId::new(1)).is_empty());
        assert!(view.table_stats(TableId::new(1)).is_none());
        assert!(
            view.column_stats(TableId::new(1), ColumnId::new(2))
                .is_none()
        );
        assert!(
            view.file_column_stats
                .get(&1)
                .is_some_and(|cols| cols.contains_key(&(3, 2)))
        );
    }

    #[test]
    fn views_are_versioned_and_indexed() {
        let current = vec![EntityRecord::View(view_rec(3, 0, "v2", 4, None))];
        let history = vec![EntityRecord::View(view_rec(3, 0, "v1", 1, Some(4)))];
        let head = CatalogSnapshot::build(snap(5), current.clone(), vec![], None);
        assert_eq!(
            head.view_by_name(SchemaId::new(0), "v2").unwrap().id,
            ViewId::new(3)
        );
        assert!(head.view_by_name(SchemaId::new(0), "v1").is_none());
        let past = CatalogSnapshot::build(snap(2), current, history, Some(2));
        assert_eq!(past.view_by_id(ViewId::new(3)).unwrap().name, "v1");
    }

    #[test]
    fn mappings_are_table_scoped_and_survive_time_travel() {
        let mapping = MappingValue {
            mapping_id: 21,
            table_id: 4,
            map_type: "map_by_name".into(),
            name_mappings: vec![crate::store::proto::NameMapping {
                column_id: 0,
                source_name: "id".into(),
                target_field_id: 1,
                parent_column: None,
                is_partition: false,
            }],
        };
        // Unversioned: included regardless of the time-travel target.
        let past = CatalogSnapshot::build(
            snap(12),
            vec![EntityRecord::Mapping(mapping)],
            vec![],
            Some(1),
        );
        let mappings = past.mappings_of(TableId::new(4));
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].id, MappingId::new(21));
        assert_eq!(mappings[0].map_type, "map_by_name");
        assert_eq!(mappings[0].name_mappings[0].source_name, "id");
        assert!(past.mappings_of(TableId::new(5)).is_empty());
    }

    #[test]
    fn macros_are_versioned_and_indexed() {
        let current = vec![EntityRecord::Macro(macro_rec(1, 5, "add", 10, None))];
        let history = vec![EntityRecord::Macro(macro_rec(2, 5, "old", 1, Some(10)))];
        let head = CatalogSnapshot::build(snap(12), current.clone(), vec![], None);
        assert_eq!(
            head.macro_by_name(SchemaId::new(5), "add").unwrap().id,
            MacroId::new(1)
        );
        assert!(head.macro_by_name(SchemaId::new(5), "old").is_none());
        assert_eq!(head.macros_in(SchemaId::new(5)).len(), 1);
        let past = CatalogSnapshot::build(snap(2), current, history, Some(2));
        assert_eq!(past.macro_by_id(MacroId::new(2)).unwrap().name, "old");
        assert_eq!(
            past.macro_by_id(MacroId::new(2)).unwrap().implementations[0].macro_type,
            "scalar"
        );
    }

    #[test]
    fn option_resolution_cascades() {
        let mut view = CatalogSnapshot::build(snap(1), vec![], vec![], None);
        view.put_schema(schema_rec(0, "s", 1, None));
        view.put_table(table_rec(1, 0, "t", 1));
        let mk = |pairs: &[(&str, &str)]| OptionScopeValue {
            options: pairs
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
        };
        view.set_option_record((0, 0), mk(&[("a", "global"), ("b", "global")]));
        view.set_option_record((1, 0), mk(&[("a", "schema")]));
        view.set_option_record((2, 1), mk(&[("c", "table")]));
        let t = TableId::new(1);
        assert_eq!(
            view.option(OptionScope::Table(t), "c").as_deref(),
            Some("table")
        );
        assert_eq!(
            view.option(OptionScope::Table(t), "a").as_deref(),
            Some("schema")
        );
        assert_eq!(
            view.option(OptionScope::Table(t), "b").as_deref(),
            Some("global")
        );
        assert_eq!(
            view.option(OptionScope::Schema(SchemaId::new(0)), "a")
                .as_deref(),
            Some("schema")
        );
        assert_eq!(
            view.option(OptionScope::Global, "a").as_deref(),
            Some("global")
        );
        assert!(view.option(OptionScope::Global, "zz").is_none());

        view.delete_table(1);
        assert!(
            !view.options.contains_key(&(2, 1)),
            "table options die with the table"
        );
    }
}
