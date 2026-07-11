//! An immutable, materialized catalog view. Built once from a consistent
//! store scan; every accessor is an in-memory lookup afterwards.

use std::collections::{BTreeMap, HashMap};

use crate::{
    catalog::types::{
        ColumnId, ColumnInfo, ColumnStats, DataFileId, DataFileInfo, DeleteFileId, DeleteFileInfo,
        SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, TableId, TableInfo, TableStats,
    },
    store::{proto, read::EntityRecord},
};

/// An immutable catalog view at one snapshot.
///
/// Reads issue no store I/O after the view is built — a `CatalogSnapshot`
/// is a value, not a cursor.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub(crate) snap: proto::SnapshotValue,
    pub(crate) schemas: BTreeMap<u64, proto::SchemaValue>,
    pub(crate) tables: BTreeMap<u64, proto::TableValue>,
    pub(crate) columns: BTreeMap<u64, BTreeMap<u64, proto::ColumnValue>>,
    pub(crate) schema_names: HashMap<String, u64>,
    pub(crate) table_names: HashMap<(u64, String), u64>,
    pub(crate) data_files: BTreeMap<u64, BTreeMap<u64, proto::DataFileValue>>,
    pub(crate) delete_files: BTreeMap<u64, BTreeMap<u64, proto::DeleteFileValue>>,
    pub(crate) table_stats: BTreeMap<u64, proto::TableStatsValue>,
    pub(crate) table_column_stats: BTreeMap<u64, BTreeMap<u64, proto::TableColumnStatsValue>>,
    pub(crate) file_column_stats: BTreeMap<u64, BTreeMap<(u64, u64), proto::FileColumnStatsValue>>,
}

impl CatalogSnapshot {
    /// Builds the view. With `at: None` every `cur` record is live and
    /// `hist` is unused (allocation reads the table's persisted counter,
    /// not history); with `at: Some(s)` a record is included iff
    /// `begin_snapshot <= s` and it had not ended by `s` (`cur` records
    /// never have; `hist` records carry their end).
    pub(crate) fn build(
        snap: proto::SnapshotValue,
        cur: Vec<EntityRecord>,
        hist: Vec<EntityRecord>,
        at: Option<u64>,
    ) -> Self {
        let mut view = Self {
            snap,
            schemas: BTreeMap::new(),
            tables: BTreeMap::new(),
            columns: BTreeMap::new(),
            schema_names: HashMap::new(),
            table_names: HashMap::new(),
            data_files: BTreeMap::new(),
            delete_files: BTreeMap::new(),
            table_stats: BTreeMap::new(),
            table_column_stats: BTreeMap::new(),
            file_column_stats: BTreeMap::new(),
        };
        let included = |begin: u64, end: Option<u64>| match at {
            None => end.is_none(),
            Some(s) => begin <= s && end.is_none_or(|e| e > s),
        };
        for record in cur.into_iter().chain(hist) {
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
                EntityRecord::FileColumnStats(fcs) => {
                    view.put_file_column_stats(fcs);
                }
                EntityRecord::TableStats(ts) => {
                    view.put_table_stats(ts);
                }
                EntityRecord::TableColumnStats(tcs) => {
                    view.put_table_column_stats(tcs);
                }
                EntityRecord::Schema(_)
                | EntityRecord::Table(_)
                | EntityRecord::Column(_)
                | EntityRecord::File(_)
                | EntityRecord::DeleteFile(_) => {
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
            id: SnapshotId::new(self.snap.snapshot_id),
            time_micros: self.snap.snapshot_time_micros,
            schema_version: self.snap.schema_version,
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
            })
            .collect();
        infos.sort_by_key(|c| c.position);
        infos
    }

    pub(crate) fn put_schema(&mut self, value: proto::SchemaValue) {
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
    }

    pub(crate) fn put_table(&mut self, value: proto::TableValue) {
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
        self.table_stats.remove(&table_id);
        self.table_column_stats.remove(&table_id);
        // file_column_stats is kept: per-file stats outlive the file's
        // live version until its history is garbage-collected.
    }

    pub(crate) fn put_column(&mut self, value: proto::ColumnValue) {
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

    pub(crate) fn put_data_file(&mut self, value: proto::DataFileValue) {
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

    pub(crate) fn put_delete_file(&mut self, value: proto::DeleteFileValue) {
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

    pub(crate) fn put_table_stats(&mut self, value: proto::TableStatsValue) {
        self.table_stats.insert(value.table_id, value);
    }

    pub(crate) fn put_table_column_stats(&mut self, value: proto::TableColumnStatsValue) {
        self.table_column_stats
            .entry(value.table_id)
            .or_default()
            .insert(value.column_id, value);
    }

    pub(crate) fn put_file_column_stats(&mut self, value: proto::FileColumnStatsValue) {
        self.file_column_stats
            .entry(value.table_id)
            .or_default()
            .insert((value.data_file_id, value.column_id), value);
    }
}

fn schema_info(value: &proto::SchemaValue) -> SchemaInfo {
    SchemaInfo {
        id: SchemaId::new(value.schema_id),
        name: value.schema_name.clone(),
    }
}

fn table_info(value: &proto::TableValue) -> TableInfo {
    TableInfo {
        id: TableId::new(value.table_id),
        schema_id: SchemaId::new(value.schema_id),
        name: value.table_name.clone(),
    }
}

fn data_file_info(value: &proto::DataFileValue) -> DataFileInfo {
    DataFileInfo {
        id: DataFileId::new(value.data_file_id),
        path: value.path.clone(),
        path_is_relative: value.path_is_relative,
        file_format: value.file_format.clone(),
        record_count: value.record_count,
        file_size_bytes: value.file_size_bytes,
        footer_size: value.footer_size,
        row_id_start: value.row_id_start,
    }
}

fn delete_file_info(value: &proto::DeleteFileValue) -> DeleteFileInfo {
    DeleteFileInfo {
        id: DeleteFileId::new(value.delete_file_id),
        data_file_id: DataFileId::new(value.data_file_id),
        path: value.path.clone(),
        path_is_relative: value.path_is_relative,
        format: value.format.clone(),
        delete_count: value.delete_count,
        file_size_bytes: value.file_size_bytes,
        footer_size: value.footer_size,
    }
}

fn table_stats_from_proto(value: &proto::TableStatsValue) -> TableStats {
    TableStats {
        record_count: value.record_count,
        file_size_bytes: value.file_size_bytes,
        next_row_id: value.next_row_id,
    }
}

fn column_stats_from_proto(value: &proto::TableColumnStatsValue) -> ColumnStats {
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

    fn snap(id: u64) -> proto::SnapshotValue {
        proto::SnapshotValue {
            snapshot_id: id,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 10,
            next_file_id: 0,
            next_deletion_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
        }
    }

    fn schema_rec(id: u64, name: &str, begin: u64, end: Option<u64>) -> proto::SchemaValue {
        proto::SchemaValue {
            schema_id: id,
            schema_uuid: format!("uuid-{id}"),
            begin_snapshot: begin,
            end_snapshot: end,
            schema_name: name.into(),
            path: format!("{name}/"),
            path_is_relative: true,
        }
    }

    fn table_rec(id: u64, schema: u64, name: &str, begin: u64) -> proto::TableValue {
        proto::TableValue {
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

    fn column_rec(table: u64, id: u64, name: &str, order: u64, begin: u64) -> proto::ColumnValue {
        proto::ColumnValue {
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

    fn file_rec(
        table: u64,
        id: u64,
        rows: u64,
        begin: u64,
        end: Option<u64>,
    ) -> proto::DataFileValue {
        proto::DataFileValue {
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
            row_id_start: 0,
            partition_id: None,
            encryption_key: None,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        }
    }

    fn tstat_rec(table: u64, count: u64, next_row: u64) -> proto::TableStatsValue {
        proto::TableStatsValue {
            table_id: table,
            record_count: count,
            next_row_id: next_row,
            file_size_bytes: count * 10,
        }
    }

    #[test]
    fn head_view_indexes_live_entities() {
        let cur = vec![
            EntityRecord::Schema(schema_rec(0, "main", 1, None)),
            EntityRecord::Table(table_rec(1, 0, "orders", 2)),
            EntityRecord::Column(column_rec(1, 0, "id", 0, 2)),
            EntityRecord::Column(column_rec(1, 1, "amount", 1, 2)),
        ];
        let view = CatalogSnapshot::build(snap(2), cur, vec![], None);

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
        let cur = vec![
            EntityRecord::Schema(schema_rec(0, "renamed", 3, None)),
            EntityRecord::Table(table_rec(1, 0, "orders", 4)),
        ];
        let hist = vec![EntityRecord::Schema(schema_rec(0, "original", 1, Some(3)))];

        let at2 = CatalogSnapshot::build(snap(2), cur.clone(), hist.clone(), Some(2));
        assert!(at2.schema_by_name("original").is_some());
        assert!(at2.schema_by_name("renamed").is_none());
        assert!(at2.table_by_id(TableId::new(1)).is_none());

        let at4 = CatalogSnapshot::build(snap(4), cur, hist, Some(4));
        assert!(at4.schema_by_name("renamed").is_some());
        assert!(at4.table_by_id(TableId::new(1)).is_some());
    }

    #[test]
    fn hist_only_records_are_excluded_from_the_head_view() {
        // Column allocation now reads the table's persisted counter, not
        // `hist` — so `build` at head must not resurrect a dropped column
        // (or any other purely-historical record) into the live view.
        let hist = vec![EntityRecord::Column(proto::ColumnValue {
            end_snapshot: Some(3),
            ..column_rec(1, 5, "gone", 0, 1)
        })];
        let view = CatalogSnapshot::build(snap(3), vec![], hist, None);
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
        let cur = vec![
            EntityRecord::File(file_rec(1, 10, 100, 5, None)),
            EntityRecord::TableStats(tstat_rec(1, 100, 100)),
        ];
        let hist = vec![EntityRecord::File(file_rec(1, 9, 50, 1, Some(5)))];

        // Head view: only the live file; stats present.
        let head = CatalogSnapshot::build(snap(6), cur.clone(), vec![], None);
        let files = head.data_files_of(TableId::new(1));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, DataFileId::new(10));
        assert_eq!(head.table_stats(TableId::new(1)).unwrap().record_count, 100);

        // Time travel to snapshot 3: the ended file was live, the new one
        // not yet; stats are unversioned and served as-is.
        let past = CatalogSnapshot::build(snap(3), cur, hist, Some(3));
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
        view.put_table_column_stats(proto::TableColumnStatsValue {
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
        view.put_delete_file(proto::DeleteFileValue {
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

        view.put_file_column_stats(proto::FileColumnStatsValue {
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
}
