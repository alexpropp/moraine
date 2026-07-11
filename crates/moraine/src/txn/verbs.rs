//! The mutation handle passed to a commit closure.

use std::collections::BTreeMap;
use std::ops::Deref;

use uuid::Uuid;

use crate::{
    catalog::{CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, SchemaId, TableId},
    error::{Error, Result},
    store::proto,
    txn::ops::Op,
};

/// The mutation handle a commit closure receives.
///
/// Dereferences to [`CatalogSnapshot`], so every read accessor is
/// available for name resolution and validation — reads observe the
/// transaction's own staged mutations. Mutators validate eagerly and
/// stage; nothing touches the store until the closure returns.
pub struct Txn {
    state: CatalogSnapshot,
    ops: Vec<Op>,
    next_catalog_id: u64,
    new_snapshot_id: u64,
}

impl Deref for Txn {
    type Target = CatalogSnapshot;

    fn deref(&self) -> &CatalogSnapshot {
        &self.state
    }
}

impl Txn {
    pub(crate) fn new(state: CatalogSnapshot, new_snapshot_id: u64) -> Self {
        let next_catalog_id = state.snap.next_catalog_id;
        Self {
            state,
            ops: Vec::new(),
            next_catalog_id,
            new_snapshot_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> &CatalogSnapshot {
        &self.state
    }

    pub(crate) fn into_parts(self) -> (Vec<Op>, CatalogSnapshot, u64) {
        (self.ops, self.state, self.next_catalog_id)
    }

    fn alloc_catalog_id(&mut self) -> u64 {
        let id = self.next_catalog_id;
        self.next_catalog_id += 1;
        id
    }

    /// Creates a schema. Errors with [`Error::AlreadyExists`] if a live
    /// schema of that name exists.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if a schema with that name already
    /// exists.
    pub fn create_schema(&mut self, name: &str) -> Result<SchemaId> {
        if self.state.schema_names.contains_key(name) {
            return Err(Error::AlreadyExists(format!("schema {name}")));
        }
        let schema_id = self.alloc_catalog_id();
        self.state.put_schema(proto::SchemaValue {
            schema_id,
            schema_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_name: name.to_owned(),
            path: format!("{name}/"),
            path_is_relative: true,
        });
        self.ops.push(Op::CreateSchema {
            schema_id,
            name: name.to_owned(),
        });
        Ok(SchemaId::new(schema_id))
    }

    /// Drops a schema. Errors with [`Error::NotFound`] if it does not
    /// exist and [`Error::Constraint`] if it still contains tables.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::Constraint`] if the schema still contains live
    /// tables.
    pub fn drop_schema(&mut self, schema: SchemaId) -> Result<()> {
        if !self.state.schemas.contains_key(&schema.get()) {
            return Err(Error::NotFound(format!("schema {schema}")));
        }
        if self
            .state
            .tables
            .values()
            .any(|t| t.schema_id == schema.get())
        {
            return Err(Error::Constraint(format!(
                "schema {schema} still contains tables"
            )));
        }
        self.state.delete_schema(schema.get());
        self.ops.push(Op::DropSchema {
            schema_id: schema.get(),
        });
        Ok(())
    }

    /// Creates a table with its columns. Errors with
    /// [`Error::NotFound`] for a missing schema,
    /// [`Error::AlreadyExists`] for a name collision, and
    /// [`Error::Constraint`] for an empty or duplicate column list.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::AlreadyExists`] if a table with that name already
    /// exists in the schema.
    /// Returns [`Error::Constraint`] if the column list is empty or contains
    /// duplicate column names.
    pub fn create_table(
        &mut self,
        schema: SchemaId,
        name: &str,
        columns: &[ColumnDef],
    ) -> Result<TableId> {
        if !self.state.schemas.contains_key(&schema.get()) {
            return Err(Error::NotFound(format!("schema {schema}")));
        }
        if self
            .state
            .table_names
            .contains_key(&(schema.get(), name.to_owned()))
        {
            return Err(Error::AlreadyExists(format!("table {name}")));
        }
        if columns.is_empty() {
            return Err(Error::Constraint(format!(
                "table {name} needs at least one column"
            )));
        }
        for (i, a) in columns.iter().enumerate() {
            if columns[..i].iter().any(|b| b.name == a.name) {
                return Err(Error::Constraint(format!("duplicate column {}", a.name)));
            }
        }
        let table_id = self.alloc_catalog_id();
        let column_count = columns.len() as u64;
        self.state.put_table(proto::TableValue {
            table_id,
            table_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_id: schema.get(),
            table_name: name.to_owned(),
            path: format!("{name}/"),
            path_is_relative: true,
            next_column_id: column_count + 1,
        });
        // Field ids are assigned from 1 in declaration order, matching
        // DuckLake; column_order (the position) stays 0-based.
        for (order, def) in columns.iter().enumerate() {
            self.state.put_column(new_column(
                table_id,
                order as u64 + 1,
                order as u64,
                self.new_snapshot_id,
                def,
            ));
        }
        let schema_name = self.state.schemas[&schema.get()].schema_name.clone();
        self.ops.push(Op::CreateTable {
            schema_id: schema.get(),
            table_id,
            schema_name,
            table_name: name.to_owned(),
        });
        Ok(TableId::new(table_id))
    }

    fn live_table(&self, table: TableId) -> Result<proto::TableValue> {
        self.state
            .tables
            .get(&table.get())
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("table {table}")))
    }

    fn mark_altered(&mut self, table_id: u64) {
        self.ops.push(Op::AlterTable { table_id });
    }

    /// Renames a table within its schema.
    ///
    /// Renaming a table to its current name errors with
    /// [`Error::AlreadyExists`] — the name is held by the table itself,
    /// matching SQL engine behavior.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::AlreadyExists`] if a table with that name already
    /// exists in the same schema (including this table itself).
    pub fn rename_table(&mut self, table: TableId, new_name: &str) -> Result<()> {
        let mut value = self.live_table(table)?;
        if self
            .state
            .table_names
            .contains_key(&(value.schema_id, new_name.to_owned()))
        {
            return Err(Error::AlreadyExists(format!("table {new_name}")));
        }
        new_name.clone_into(&mut value.table_name);
        value.begin_snapshot = self.new_snapshot_id;
        self.state.put_table(value);
        self.mark_altered(table.get());
        Ok(())
    }

    /// Moves a table to another schema.
    ///
    /// Moving a table to its current schema errors with
    /// [`Error::AlreadyExists`] — the name is held by the table itself,
    /// matching SQL engine behavior.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or target schema does not
    /// exist.
    /// Returns [`Error::AlreadyExists`] if a table with the same name
    /// already exists in the target schema (including this table itself
    /// when the target is its current schema).
    pub fn set_table_schema(&mut self, table: TableId, new_schema: SchemaId) -> Result<()> {
        let mut value = self.live_table(table)?;
        if !self.state.schemas.contains_key(&new_schema.get()) {
            return Err(Error::NotFound(format!("schema {new_schema}")));
        }
        if self
            .state
            .table_names
            .contains_key(&(new_schema.get(), value.table_name.clone()))
        {
            return Err(Error::AlreadyExists(format!("table {}", value.table_name)));
        }
        value.schema_id = new_schema.get();
        value.begin_snapshot = self.new_snapshot_id;
        self.state.put_table(value);
        self.mark_altered(table.get());
        Ok(())
    }

    /// Drops a table and its columns.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    pub fn drop_table(&mut self, table: TableId) -> Result<()> {
        self.live_table(table)?;
        self.state.delete_table(table.get());
        self.ops.push(Op::DropTable {
            table_id: table.get(),
        });
        Ok(())
    }

    /// Adds a column. The new column's field id is allocated from the
    /// table's persisted `next_column_id` counter, floored so it never
    /// lands at or below a currently live column id — a defensive floor
    /// for table versions authored without the counter. The id is then
    /// never reused: the table record's counter is advanced past it in
    /// the same version transition.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::AlreadyExists`] if a column with that name already
    /// exists in the table.
    pub fn add_column(&mut self, table: TableId, def: &ColumnDef) -> Result<ColumnId> {
        let mut value = self.live_table(table)?;
        self.column_name_free(table, &def.name)?;
        let live_columns = self.state.columns.get(&table.get());
        let live_max_id = live_columns
            .and_then(|cols| cols.keys().max())
            .copied()
            .unwrap_or(0);
        let column_id = value.next_column_id.max(live_max_id + 1);
        let position = live_columns
            .and_then(|cols| cols.values().map(|c| c.column_order).max())
            .map_or(0, |max| max + 1);
        self.state.put_column(new_column(
            table.get(),
            column_id,
            position,
            self.new_snapshot_id,
            def,
        ));
        value.next_column_id = column_id + 1;
        value.begin_snapshot = self.new_snapshot_id;
        self.state.put_table(value);
        self.mark_altered(table.get());
        Ok(ColumnId::new(column_id))
    }

    fn live_column(&self, table: TableId, column: ColumnId) -> Result<proto::ColumnValue> {
        self.state
            .columns
            .get(&table.get())
            .and_then(|cols| cols.get(&column.get()))
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("column {column} of table {table}")))
    }

    fn column_name_free(&self, table: TableId, name: &str) -> Result<()> {
        let taken = self
            .state
            .columns
            .get(&table.get())
            .is_some_and(|cols| cols.values().any(|c| c.column_name == name));
        if taken {
            return Err(Error::AlreadyExists(format!("column {name}")));
        }
        Ok(())
    }

    /// Renames a column.
    ///
    /// Renaming a column to its current name errors with
    /// [`Error::AlreadyExists`] — the name is held by the column itself,
    /// matching SQL engine behavior.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    /// Returns [`Error::AlreadyExists`] if a column with that name already
    /// exists in the table (including this column itself).
    pub fn rename_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        new_name: &str,
    ) -> Result<()> {
        let mut value = self.live_column(table, column)?;
        self.column_name_free(table, new_name)?;
        new_name.clone_into(&mut value.column_name);
        value.begin_snapshot = self.new_snapshot_id;
        self.state.put_column(value);
        self.mark_altered(table.get());
        Ok(())
    }

    /// Alters a column's type, nullability, and/or default.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    pub fn alter_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        alteration: ColumnAlteration,
    ) -> Result<()> {
        let mut value = self.live_column(table, column)?;
        if let Some(column_type) = alteration.column_type {
            value.column_type = column_type;
        }
        if let Some(nulls_allowed) = alteration.nulls_allowed {
            value.nulls_allowed = nulls_allowed;
        }
        if let Some(default_value) = alteration.default_value {
            value.default_value = default_value;
        }
        value.begin_snapshot = self.new_snapshot_id;
        self.state.put_column(value);
        self.mark_altered(table.get());
        Ok(())
    }

    /// Drops a column. Errors with [`Error::Constraint`] for the table's
    /// last live column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    /// Returns [`Error::Constraint`] if this is the last live column of the
    /// table.
    pub fn drop_column(&mut self, table: TableId, column: ColumnId) -> Result<()> {
        self.live_column(table, column)?;
        let live = self
            .state
            .columns
            .get(&table.get())
            .map_or(0, BTreeMap::len);
        if live <= 1 {
            return Err(Error::Constraint(format!(
                "column {column} is the last column of table {table}"
            )));
        }
        self.state.delete_column(table.get(), column.get());
        self.mark_altered(table.get());
        Ok(())
    }
}

fn new_column(
    table_id: u64,
    column_id: u64,
    column_order: u64,
    begin_snapshot: u64,
    def: &ColumnDef,
) -> proto::ColumnValue {
    proto::ColumnValue {
        column_id,
        begin_snapshot,
        end_snapshot: None,
        table_id,
        column_order,
        column_name: def.name.clone(),
        column_type: def.column_type.clone(),
        initial_default: None,
        default_value: def.default_value.clone(),
        nulls_allowed: def.nulls_allowed,
        parent_column: None,
        default_value_type: None,
        default_value_dialect: None,
        tags: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::CatalogSnapshot;
    use crate::store::proto;

    fn empty_txn() -> Txn {
        let snap = proto::SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 2,
            next_catalog_id: 10,
            next_file_id: 0,
            next_deletion_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
        };
        Txn::new(CatalogSnapshot::build(snap, vec![], vec![], None), 5)
    }

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: "BIGINT".into(),
            nulls_allowed: true,
            default_value: None,
        }
    }

    #[test]
    fn create_read_your_own_writes_and_id_allocation() {
        let mut txn = empty_txn();
        let s = txn.create_schema("sales").unwrap();
        assert_eq!(s, SchemaId::new(10));
        let t = txn
            .create_table(s, "orders", &[col("id"), col("qty")])
            .unwrap();
        assert_eq!(t, TableId::new(11));
        // Reads on the Txn see staged state (Deref to the working view).
        assert_eq!(txn.schema_by_name("sales").unwrap().id, s);
        let cols = txn.columns_of(t);
        assert_eq!(cols[0].id, ColumnId::new(1));
        assert_eq!(cols[1].id, ColumnId::new(2));
        // Records are stamped with the commit's snapshot id.
        assert_eq!(txn.state().tables[&11].begin_snapshot, 5);
        // The counter is seeded past the ids just handed out.
        assert_eq!(txn.state().tables[&11].next_column_id, 3);

        let (ops, _, next_catalog_id) = txn.into_parts();
        assert_eq!(next_catalog_id, 12);
        assert_eq!(
            ops,
            vec![
                Op::CreateSchema {
                    schema_id: 10,
                    name: "sales".into(),
                },
                Op::CreateTable {
                    schema_id: 10,
                    table_id: 11,
                    schema_name: "sales".into(),
                    table_name: "orders".into(),
                },
            ]
        );
    }

    #[test]
    fn name_collisions_and_missing_entities() {
        let mut txn = empty_txn();
        let s = txn.create_schema("sales").unwrap();
        assert!(matches!(
            txn.create_schema("sales"),
            Err(Error::AlreadyExists(_))
        ));
        txn.create_table(s, "orders", &[col("id")]).unwrap();
        assert!(matches!(
            txn.create_table(s, "orders", &[col("id")]),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            txn.create_table(SchemaId::new(99), "x", &[col("id")]),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            txn.rename_table(TableId::new(99), "y"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn constraints() {
        let mut txn = empty_txn();
        let s = txn.create_schema("sales").unwrap();
        let t = txn.create_table(s, "orders", &[col("id")]).unwrap();
        // A schema with live tables cannot be dropped.
        assert!(matches!(txn.drop_schema(s), Err(Error::Constraint(_))));
        // The last live column cannot be dropped.
        assert!(matches!(
            txn.drop_column(t, ColumnId::new(1)),
            Err(Error::Constraint(_))
        ));
        // Tables need at least one column, without duplicate names.
        assert!(matches!(
            txn.create_table(s, "empty", &[]),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(
            txn.create_table(s, "dup", &[col("a"), col("a")]),
            Err(Error::Constraint(_))
        ));
        // Drop the table, then the schema drop succeeds.
        txn.drop_table(t).unwrap();
        txn.drop_schema(s).unwrap();
    }

    #[test]
    fn column_ddl_allocates_fresh_field_ids() {
        let mut txn = empty_txn();
        let s = txn.create_schema("s").unwrap();
        let t = txn.create_table(s, "t", &[col("a"), col("b")]).unwrap();
        txn.drop_column(t, ColumnId::new(2)).unwrap();
        // The dropped column's field id is never reused.
        let c = txn.add_column(t, &col("c")).unwrap();
        assert_eq!(c, ColumnId::new(3));
        let cols = txn.columns_of(t);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].position, 1);

        txn.rename_column(t, ColumnId::new(1), "a2").unwrap();
        assert_eq!(txn.columns_of(t)[0].name, "a2");
        assert!(matches!(
            txn.rename_column(t, ColumnId::new(1), "c"),
            Err(Error::AlreadyExists(_))
        ));

        txn.alter_column(
            t,
            c,
            ColumnAlteration {
                column_type: Some("VARCHAR".into()),
                nulls_allowed: Some(false),
                default_value: Some(Some("''".into())),
            },
        )
        .unwrap();
        let altered = &txn.columns_of(t)[1];
        assert_eq!(altered.column_type, "VARCHAR");
        assert!(!altered.nulls_allowed);
        assert_eq!(altered.default_value, Some("''".into()));
    }

    #[test]
    fn table_moves_and_renames_validate_against_target() {
        let mut txn = empty_txn();
        let s1 = txn.create_schema("a").unwrap();
        let s2 = txn.create_schema("b").unwrap();
        let t1 = txn.create_table(s1, "t", &[col("x")]).unwrap();
        let _t2 = txn.create_table(s2, "t", &[col("x")]).unwrap();
        // Moving into a schema that already has a table of that name fails.
        assert!(matches!(
            txn.set_table_schema(t1, s2),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            txn.set_table_schema(t1, SchemaId::new(99)),
            Err(Error::NotFound(_))
        ));
        txn.rename_table(t1, "t_renamed").unwrap();
        txn.set_table_schema(t1, s2).unwrap();
        assert_eq!(txn.tables_in(s2).len(), 2);
        // Each mutation of an existing table classifies as an alter.
        let (ops, _, _) = txn.into_parts();
        let alters = ops
            .iter()
            .filter(|op| matches!(op, Op::AlterTable { table_id } if *table_id == t1.get()))
            .count();
        assert_eq!(alters, 2);
    }

    #[test]
    fn self_targeted_renames_and_moves_error() {
        let mut txn = empty_txn();
        let s = txn.create_schema("s").unwrap();
        let t = txn.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            txn.rename_table(t, "t"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            txn.set_table_schema(t, s),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            txn.rename_column(t, ColumnId::new(1), "a"),
            Err(Error::AlreadyExists(_))
        ));
    }

    #[test]
    fn add_column_floors_allocation_at_live_ids() {
        // A table version authored without the counter (next_column_id
        // absent, i.e. 0) must not let allocation regress below the ids
        // already live on the table.
        use crate::catalog::CatalogSnapshot;
        use crate::store::read::EntityRecord;

        let snap = proto::SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 10,
            next_file_id: 0,
            next_deletion_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
        };
        let table = proto::TableValue {
            table_id: 1,
            table_uuid: "uuid-t1".into(),
            begin_snapshot: 1,
            end_snapshot: None,
            schema_id: 0,
            table_name: "t".into(),
            path: "t/".into(),
            path_is_relative: true,
            next_column_id: 0,
        };
        let columns = [1u64, 2].map(|id| {
            EntityRecord::Column(proto::ColumnValue {
                column_id: id,
                begin_snapshot: 1,
                end_snapshot: None,
                table_id: 1,
                column_order: id - 1,
                column_name: format!("c{id}"),
                column_type: "BIGINT".into(),
                initial_default: None,
                default_value: None,
                nulls_allowed: true,
                parent_column: None,
                default_value_type: None,
                default_value_dialect: None,
                tags: vec![],
            })
        });
        let mut cur = vec![EntityRecord::Table(table)];
        cur.extend(columns);
        let state = CatalogSnapshot::build(snap, cur, vec![], None);
        let mut txn = Txn::new(state, 5);
        let c = txn.add_column(TableId::new(1), &col("c")).unwrap();
        assert_eq!(c, ColumnId::new(3));
    }
}
