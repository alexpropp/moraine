//! An immutable, materialized catalog view. Built once from a consistent
//! store scan; every accessor is an in-memory lookup afterwards.

use std::collections::{BTreeMap, HashMap};

use crate::{
    catalog::types::{
        ColumnId, ColumnInfo, SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, TableId, TableInfo,
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
                _ => {}
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
