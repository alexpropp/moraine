//! The mutation handle passed to a commit closure.

use std::{
    collections::{BTreeMap, HashSet},
    ops::Deref,
};

use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, ColumnStats, DataFile, DataFileId,
        DeleteFile, DeleteFileId, OptionScope, SchemaId, TableId, ViewId,
    },
    error::{Error, Result},
    store::proto::{
        ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, SchemaValue,
        TableColumnStatsValue, TableStatsValue, TableValue, ViewValue,
    },
    transaction::operations::Operation,
};

/// The mutation handle a commit closure receives.
///
/// Dereferences to [`CatalogSnapshot`]; reads observe the transaction's
/// own staged mutations. Nothing touches the store until the closure
/// returns.
pub struct Transaction {
    state: CatalogSnapshot,
    ops: Vec<Operation>,
    next_catalog_id: u64,
    next_file_id: u64,
    new_snapshot_id: u64,
}

impl Deref for Transaction {
    type Target = CatalogSnapshot;

    fn deref(&self) -> &CatalogSnapshot {
        &self.state
    }
}

impl Transaction {
    pub(crate) fn new(state: CatalogSnapshot, new_snapshot_id: u64) -> Self {
        let next_catalog_id = state.snapshot.next_catalog_id;
        let next_file_id = state.snapshot.next_file_id;

        Self {
            state,
            ops: Vec::new(),
            next_catalog_id,
            next_file_id,
            new_snapshot_id,
        }
    }

    pub(crate) fn into_parts(self) -> (Vec<Operation>, CatalogSnapshot, u64, u64) {
        (
            self.ops,
            self.state,
            self.next_catalog_id,
            self.next_file_id,
        )
    }

    fn alloc_catalog_id(&mut self) -> u64 {
        let id = self.next_catalog_id;
        self.next_catalog_id += 1;
        id
    }

    fn alloc_file_id(&mut self) -> u64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }

    /// Creates a schema.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if a schema with that name already
    /// exists, or [`Error::Constraint`] if the name is empty or unsafe in
    /// the storage path derived from it.
    pub fn create_schema(&mut self, name: &str) -> Result<SchemaId> {
        path_safe_name("schema", name)?;
        if self.state.schema_names.contains_key(name) {
            return Err(Error::AlreadyExists(format!("schema {name}")));
        }
        let schema_id = self.alloc_catalog_id();
        self.state.put_schema(SchemaValue {
            schema_id,
            schema_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_name: name.to_owned(),
            path: format!("{name}/"),
            path_is_relative: true,
        });
        self.ops.push(Operation::CreateSchema {
            schema_id,
            name: name.to_owned(),
        });
        Ok(SchemaId::new(schema_id))
    }

    /// Drops a schema.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::Constraint`] if the schema still contains live
    /// tables, or is the bootstrap `main` schema (the default DuckDB
    /// resolves unqualified names against).
    pub fn drop_schema(&mut self, schema: SchemaId) -> Result<()> {
        // Schema id 0 is the bootstrap `main` schema — the default DuckDB
        // resolves unqualified names against; a catalog without it is a
        // shape DuckLake never produces or attaches.
        if schema.get() == 0 {
            return Err(Error::Constraint(
                "the bootstrap `main` schema cannot be dropped".to_string(),
            ));
        }
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
        if self
            .state
            .views
            .values()
            .any(|v| v.schema_id == schema.get())
        {
            return Err(Error::Constraint(format!(
                "schema {schema} still contains views"
            )));
        }
        self.state.delete_schema(schema.get());
        self.ops.push(Operation::DropSchema {
            schema_id: schema.get(),
        });
        Ok(())
    }

    /// Creates a table with its columns.
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

        path_safe_name("table", name)?;
        self.relation_name_free(schema.get(), name)?;
        if columns.is_empty() {
            return Err(Error::Constraint(format!(
                "table {name} needs at least one column"
            )));
        }
        let mut seen = HashSet::with_capacity(columns.len());
        for def in columns {
            nonempty_name("column", &def.name)?;
            if !seen.insert(&def.name) {
                return Err(Error::Constraint(format!("duplicate column {}", def.name)));
            }
        }

        let table_id = self.alloc_catalog_id();
        let column_count = columns.len() as u64;
        self.state.put_table(TableValue {
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
        // Field ids are assigned from 1 in declaration order;
        // column_order (the position) stays 0-based.
        for (order, def) in columns.iter().enumerate() {
            self.state.put_column(new_column(
                table_id,
                order as u64 + 1,
                order as u64,
                self.new_snapshot_id,
                def,
            ));
        }
        self.state.put_table_stats(TableStatsValue {
            table_id,
            record_count: 0,
            next_row_id: 0,
            file_size_bytes: 0,
        });
        let schema_name = self.state.schemas[&schema.get()].schema_name.clone();
        self.ops.push(Operation::CreateTable {
            schema_id: schema.get(),
            table_id,
            schema_name,
            table_name: name.to_owned(),
        });
        Ok(TableId::new(table_id))
    }

    fn live_table(&self, table: TableId) -> Result<TableValue> {
        self.state
            .tables
            .get(&table.get())
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("table {table}")))
    }

    /// Tables and views share one name namespace per schema.
    fn relation_name_free(&self, schema_id: u64, name: &str) -> Result<()> {
        let key = (schema_id, name.to_owned());
        let taken =
            self.state.table_names.contains_key(&key) || self.state.view_names.contains_key(&key);
        if taken {
            return Err(Error::AlreadyExists(format!("relation {name}")));
        }
        Ok(())
    }

    fn mark_altered(&mut self, table_id: u64) {
        self.ops.push(Operation::AlterTable { table_id });
    }

    /// Renames a table within its schema. Renaming to the current name
    /// errors, matching SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::AlreadyExists`] if a table with that name already
    /// exists in the same schema (including this table itself).
    pub fn rename_table(&mut self, table: TableId, new_name: &str) -> Result<()> {
        path_safe_name("table", new_name)?;
        let value = self.live_table(table)?;
        self.relation_name_free(value.schema_id, new_name)?;
        self.state.put_table(TableValue {
            table_name: new_name.to_owned(),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());

        Ok(())
    }

    /// Moves a table to another schema. Moving to the current schema
    /// errors, matching SQL engines.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or target schema does not
    /// exist.
    /// Returns [`Error::AlreadyExists`] if a table with the same name
    /// already exists in the target schema (including this table itself
    /// when the target is its current schema).
    pub fn set_table_schema(&mut self, table: TableId, new_schema: SchemaId) -> Result<()> {
        if !self.state.schemas.contains_key(&new_schema.get()) {
            return Err(Error::NotFound(format!("schema {new_schema}")));
        }
        let value = self.live_table(table)?;
        self.relation_name_free(new_schema.get(), &value.table_name)?;
        self.state.put_table(TableValue {
            schema_id: new_schema.get(),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
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
        self.ops.push(Operation::DropTable {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Adds a column. Its field id comes from the table's persisted
    /// counter, floored above every live id, and is never reused.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::AlreadyExists`] if a column with that name already
    /// exists in the table.
    pub fn add_column(&mut self, table: TableId, def: &ColumnDef) -> Result<ColumnId> {
        nonempty_name("column", &def.name)?;
        let value = self.live_table(table)?;
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
        self.state.put_table(TableValue {
            next_column_id: column_id + 1,
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());

        Ok(ColumnId::new(column_id))
    }

    fn live_column(&self, table: TableId, column: ColumnId) -> Result<ColumnValue> {
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

    /// Renames a column. Renaming to the current name errors, matching
    /// SQL engines.
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
        nonempty_name("column", new_name)?;
        self.column_name_free(table, new_name)?;
        let value = ColumnValue {
            begin_snapshot: self.new_snapshot_id,
            column_name: new_name.to_string(),
            ..self.live_column(table, column)?
        };
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
        let value = self.live_column(table, column)?;
        let ColumnAlteration {
            column_type,
            nulls_allowed,
            default_value,
        } = alteration;
        self.state.put_column(ColumnValue {
            column_type: column_type.unwrap_or(value.column_type),
            nulls_allowed: nulls_allowed.unwrap_or(value.nulls_allowed),
            default_value: default_value.unwrap_or(value.default_value),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.mark_altered(table.get());

        Ok(())
    }

    /// Drops a column.
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

    fn live_table_stats(&self, table: TableId) -> Result<TableStatsValue> {
        self.state
            .table_stats
            .get(&table.get())
            .copied()
            .ok_or_else(|| Error::Corruption(format!("table {table} has no statistics record")))
    }

    /// Registers a data file, allocating its dense row-id range from the
    /// table's row-id counter and folding its size into the table's
    /// statistics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist, or if any
    /// entry in `file.column_stats` names a column that is not live on the
    /// table.
    /// Returns [`Error::Corruption`] if the table has no statistics
    /// record (impossible for a table created by [`Self::create_table`],
    /// which always mints one).
    pub fn register_data_file(&mut self, table: TableId, file: DataFile) -> Result<DataFileId> {
        self.live_table(table)?;
        for entry in &file.column_stats {
            self.live_column(table, entry.column_id)?;
        }
        let data_file_id = self.alloc_file_id();
        let tstat = self.live_table_stats(table)?;
        let row_id_start = tstat.next_row_id;
        self.state.put_table_stats(TableStatsValue {
            next_row_id: tstat.next_row_id.saturating_add(file.record_count),
            record_count: tstat.record_count.saturating_add(file.record_count),
            file_size_bytes: tstat.file_size_bytes.saturating_add(file.file_size_bytes),
            ..tstat
        });
        self.state.put_data_file(DataFileValue {
            data_file_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            file_order: None,
            path: file.path,
            path_is_relative: file.path_is_relative,
            file_format: file.file_format,
            record_count: file.record_count,
            file_size_bytes: file.file_size_bytes,
            footer_size: file.footer_size,
            row_id_start: Some(row_id_start),
            partition_id: None,
            encryption_key: file.encryption_key,
            mapping_id: None,
            partial_max: None,
            partition_values: vec![],
        });
        for entry in file.column_stats {
            self.state.put_file_column_stats(FileColumnStatsValue {
                data_file_id,
                table_id: table.get(),
                column_id: entry.column_id.get(),
                column_size_bytes: entry.column_size_bytes,
                value_count: entry.value_count,
                null_count: entry.null_count,
                min_value: entry.min_value,
                max_value: entry.max_value,
                contains_nan: entry.contains_nan,
                extra_stats: entry.extra_stats,
                variant_stats: vec![],
            });
        }
        self.ops.push(Operation::RegisterDataFile {
            table_id: table.get(),
        });
        Ok(DataFileId::new(data_file_id))
    }

    /// Expires a data file, removing it and subtracting its contribution
    /// from the table's statistics.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the file is not live on the table.
    pub fn expire_data_file(&mut self, table: TableId, file: DataFileId) -> Result<()> {
        let data_file = self
            .state
            .data_files
            .get(&table.get())
            .and_then(|files| files.get(&file.get()))
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("data file {file} of table {table}")))?;

        let cascaded: Vec<u64> = self
            .state
            .delete_files
            .get(&table.get())
            .into_iter()
            .flat_map(BTreeMap::values)
            .filter(|d| d.data_file_id == file.get())
            .map(|d| d.delete_file_id)
            .collect();
        for delete_file_id in cascaded {
            self.state.delete_delete_file(table.get(), delete_file_id);
        }

        self.state.delete_data_file(table.get(), file.get());

        let tstat = self.live_table_stats(table)?;
        self.state.put_table_stats(TableStatsValue {
            record_count: tstat.record_count.saturating_sub(data_file.record_count),
            file_size_bytes: tstat
                .file_size_bytes
                .saturating_sub(data_file.file_size_bytes),
            ..tstat
        });

        self.ops.push(Operation::ExpireDataFile {
            table_id: table.get(),
        });
        Ok(())
    }

    /// Registers a delete file targeting a live data file's rows.
    ///
    /// Delete files do not change table statistics — `record_count`
    /// counts data-file rows, not delete markers.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist, or if
    /// `file.data_file_id` is not live on the table.
    pub fn register_delete_file(
        &mut self,
        table: TableId,
        file: DeleteFile,
    ) -> Result<DeleteFileId> {
        self.live_table(table)?;
        let data_file_live = self
            .state
            .data_files
            .get(&table.get())
            .is_some_and(|files| files.contains_key(&file.data_file_id.get()));
        if !data_file_live {
            return Err(Error::NotFound(format!(
                "data file {} of table {table}",
                file.data_file_id
            )));
        }

        // One live delete file per data file: a new one carries all deletes
        // and must supersede its predecessor, never sit beside it.
        let already_targeted = self
            .state
            .delete_files
            .get(&table.get())
            .is_some_and(|files| {
                files
                    .values()
                    .any(|existing| existing.data_file_id == file.data_file_id.get())
            });
        if already_targeted {
            return Err(Error::Constraint(format!(
                "data file {} of table {table} already has a live delete file; \
                 expire it first",
                file.data_file_id
            )));
        }
        let delete_file_id = self.alloc_file_id();

        self.state.put_delete_file(DeleteFileValue {
            delete_file_id,
            table_id: table.get(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            data_file_id: file.data_file_id.get(),
            path: file.path,
            path_is_relative: file.path_is_relative,
            format: file.format,
            delete_count: file.delete_count,
            file_size_bytes: file.file_size_bytes,
            footer_size: file.footer_size,
            encryption_key: file.encryption_key,
            partial_max: None,
        });
        self.ops.push(Operation::RegisterDeleteFile {
            table_id: table.get(),
        });

        Ok(DeleteFileId::new(delete_file_id))
    }

    /// Expires a delete file, removing it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the file is not live on the table.
    pub fn expire_delete_file(&mut self, table: TableId, file: DeleteFileId) -> Result<()> {
        let live = self
            .state
            .delete_files
            .get(&table.get())
            .is_some_and(|files| files.contains_key(&file.get()));
        if !live {
            return Err(Error::NotFound(format!(
                "delete file {file} of table {table}"
            )));
        }
        self.state.delete_delete_file(table.get(), file.get());
        self.ops.push(Operation::ExpireDeleteFile {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Overrides a table's row-count and size statistics. `next_row_id`
    /// is preserved and never regresses; only
    /// [`Self::register_data_file`] advances it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table does not exist.
    /// Returns [`Error::Corruption`] if the table has no statistics
    /// record.
    pub fn update_table_stats(
        &mut self,
        table: TableId,
        record_count: u64,
        file_size_bytes: u64,
    ) -> Result<()> {
        self.live_table(table)?;
        let tstat = self.live_table_stats(table)?;
        self.state.put_table_stats(TableStatsValue {
            record_count,
            file_size_bytes,
            ..tstat
        });
        self.ops.push(Operation::UpdateStats {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Overrides a column's table-level statistics, verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table or column does not exist.
    pub fn update_column_stats(
        &mut self,
        table: TableId,
        column: ColumnId,
        stats: ColumnStats,
    ) -> Result<()> {
        self.live_column(table, column)?;
        self.state.put_table_column_stats(TableColumnStatsValue {
            table_id: table.get(),
            column_id: column.get(),
            contains_null: stats.contains_null,
            contains_nan: stats.contains_nan,
            min_value: stats.min_value,
            max_value: stats.max_value,
            extra_stats: stats.extra_stats,
        });
        self.ops.push(Operation::UpdateStats {
            table_id: table.get(),
        });

        Ok(())
    }

    /// Creates a view.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the schema does not exist.
    /// Returns [`Error::AlreadyExists`] if a relation with that name
    /// already exists in the schema.
    pub fn create_view(
        &mut self,
        schema: SchemaId,
        name: &str,
        dialect: &str,
        sql: &str,
    ) -> Result<ViewId> {
        path_safe_name("view", name)?;
        let Some(schema_rec) = self.state.schemas.get(&schema.get()) else {
            return Err(Error::NotFound(format!("schema {schema}")));
        };
        let schema_name = schema_rec.schema_name.clone();
        self.relation_name_free(schema.get(), name)?;
        let view_id = self.alloc_catalog_id();

        self.state.put_view(ViewValue {
            view_id,
            view_uuid: Uuid::new_v4().to_string(),
            begin_snapshot: self.new_snapshot_id,
            end_snapshot: None,
            schema_id: schema.get(),
            view_name: name.to_owned(),
            dialect: dialect.to_owned(),
            sql: sql.to_owned(),
            column_aliases: None,
        });
        self.ops.push(Operation::CreateView {
            schema_id: schema.get(),
            view_id,
            schema_name,
            view_name: name.to_owned(),
        });

        Ok(ViewId::new(view_id))
    }

    /// Replaces a view's definition as a new version.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the view does not exist.
    pub fn alter_view(&mut self, view: ViewId, dialect: &str, sql: &str) -> Result<()> {
        let value = self
            .state
            .views
            .get(&view.get())
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("view {view}")))?;
        self.state.put_view(ViewValue {
            dialect: dialect.to_owned(),
            sql: sql.to_owned(),
            begin_snapshot: self.new_snapshot_id,
            ..value
        });
        self.ops.push(Operation::AlterView {
            view_id: view.get(),
        });
        Ok(())
    }

    /// Drops a view.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the view does not exist.
    pub fn drop_view(&mut self, view: ViewId) -> Result<()> {
        if !self.state.views.contains_key(&view.get()) {
            return Err(Error::NotFound(format!("view {view}")));
        }
        self.state.delete_view(view.get());
        self.ops.push(Operation::DropView {
            view_id: view.get(),
        });
        Ok(())
    }

    /// Sets an option in a scope. Last-write-wins; an options-only
    /// commit mints no snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the scope's schema or table does
    /// not exist, or [`Error::Constraint`] for the reserved global
    /// `encrypted` key.
    pub fn set_option(&mut self, scope: OptionScope, key: &str, value: &str) -> Result<()> {
        nonempty_name("option key", key)?;
        self.live_scope(scope)?;
        reserved_option(scope, key)?;
        let components = scope.key_components();
        let mut record = self
            .state
            .options
            .get(&components)
            .cloned()
            .unwrap_or_default();
        record.options.insert(key.to_owned(), value.to_owned());
        self.state.set_option_record(components, record);
        Ok(())
    }

    /// Removes an option from a scope; absent keys are a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the scope's schema or table does
    /// not exist, or [`Error::Constraint`] for the reserved global
    /// `encrypted` key.
    pub fn unset_option(&mut self, scope: OptionScope, key: &str) -> Result<()> {
        self.live_scope(scope)?;
        reserved_option(scope, key)?;
        let components = scope.key_components();
        let Some(mut record) = self.state.options.get(&components).cloned() else {
            return Ok(());
        };
        if record.options.remove(key).is_none() {
            return Ok(());
        }
        if record.options.is_empty() {
            self.state.remove_option_record(components);
        } else {
            self.state.set_option_record(components, record);
        }
        Ok(())
    }

    fn live_scope(&self, scope: OptionScope) -> Result<()> {
        match scope {
            OptionScope::Global => Ok(()),
            OptionScope::Schema(s) => {
                if self.state.schemas.contains_key(&s.get()) {
                    Ok(())
                } else {
                    Err(Error::NotFound(format!("schema {s}")))
                }
            }
            OptionScope::Table(t) => {
                if self.state.tables.contains_key(&t.get()) {
                    Ok(())
                } else {
                    Err(Error::NotFound(format!("table {t}")))
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> &CatalogSnapshot {
        &self.state
    }
}

/// Refuses the global `encrypted` key: whether data files are encrypted is
/// fixed when the catalog is created and recorded at bootstrap, never
/// mutated afterward.
fn reserved_option(scope: OptionScope, key: &str) -> Result<()> {
    if scope == OptionScope::Global && key == "encrypted" {
        return Err(Error::Constraint(
            "the global `encrypted` option is fixed at catalog creation".to_string(),
        ));
    }
    Ok(())
}

/// Refuses an empty name; `what` names the rejected item in the error.
fn nonempty_name(what: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Constraint(format!("{what} name must not be empty")));
    }
    Ok(())
}

/// Refuses a relation name unsafe in the storage path derived from it: a
/// separator nests or collides prefixes, and a dot segment escapes the
/// catalog root.
fn path_safe_name(what: &str, name: &str) -> Result<()> {
    nonempty_name(what, name)?;
    if name.contains(['/', '\\']) || name == "." || name == ".." {
        return Err(Error::Constraint(format!(
            "{what} name {name:?} is unsafe in a storage path"
        )));
    }
    Ok(())
}

fn new_column(
    table_id: u64,
    column_id: u64,
    column_order: u64,
    begin_snapshot: u64,
    def: &ColumnDef,
) -> ColumnValue {
    ColumnValue {
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
    use crate::{
        catalog::{CatalogSnapshot, FileColumnStats},
        store::proto::SnapshotValue,
    };

    fn empty_transaction() -> Transaction {
        let snapshot = SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 2,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        };
        Transaction::new(CatalogSnapshot::build(snapshot, vec![], vec![], None), 5)
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
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("sales").unwrap();
        assert_eq!(s, SchemaId::new(10));
        let t = transaction
            .create_table(s, "orders", &[col("id"), col("qty")])
            .unwrap();
        assert_eq!(t, TableId::new(11));
        // Reads on the Transaction see staged state (Deref to the working view).
        assert_eq!(transaction.schema_by_name("sales").unwrap().id, s);
        let cols = transaction.columns_of(t);
        assert_eq!(cols[0].id, ColumnId::new(1));
        assert_eq!(cols[1].id, ColumnId::new(2));
        // Records are stamped with the commit's snapshot id.
        assert_eq!(transaction.state().tables[&11].begin_snapshot, 5);
        // The counter is seeded past the ids just handed out.
        assert_eq!(transaction.state().tables[&11].next_column_id, 3);

        let (ops, _, next_catalog_id, _) = transaction.into_parts();
        assert_eq!(next_catalog_id, 12);
        assert_eq!(
            ops,
            vec![
                Operation::CreateSchema {
                    schema_id: 10,
                    name: "sales".into(),
                },
                Operation::CreateTable {
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
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("sales").unwrap();
        assert!(matches!(
            transaction.create_schema("sales"),
            Err(Error::AlreadyExists(_))
        ));
        transaction.create_table(s, "orders", &[col("id")]).unwrap();
        assert!(matches!(
            transaction.create_table(s, "orders", &[col("id")]),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.create_table(SchemaId::new(99), "x", &[col("id")]),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            transaction.rename_table(TableId::new(99), "y"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn constraints() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("sales").unwrap();
        let t = transaction.create_table(s, "orders", &[col("id")]).unwrap();
        // A schema with live tables cannot be dropped.
        assert!(matches!(
            transaction.drop_schema(s),
            Err(Error::Constraint(_))
        ));
        // The last live column cannot be dropped.
        assert!(matches!(
            transaction.drop_column(t, ColumnId::new(1)),
            Err(Error::Constraint(_))
        ));
        // Tables need at least one column, without duplicate names.
        assert!(matches!(
            transaction.create_table(s, "empty", &[]),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(
            transaction.create_table(s, "dup", &[col("a"), col("a")]),
            Err(Error::Constraint(_))
        ));
        // Drop the table, then the schema drop succeeds.
        transaction.drop_table(t).unwrap();
        transaction.drop_schema(s).unwrap();
    }

    #[test]
    fn column_ddl_allocates_fresh_field_ids() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction
            .create_table(s, "t", &[col("a"), col("b")])
            .unwrap();
        transaction.drop_column(t, ColumnId::new(2)).unwrap();
        // The dropped column's field id is never reused.
        let c = transaction.add_column(t, &col("c")).unwrap();
        assert_eq!(c, ColumnId::new(3));
        let cols = transaction.columns_of(t);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].position, 1);

        transaction
            .rename_column(t, ColumnId::new(1), "a2")
            .unwrap();
        assert_eq!(transaction.columns_of(t)[0].name, "a2");
        assert!(matches!(
            transaction.rename_column(t, ColumnId::new(1), "c"),
            Err(Error::AlreadyExists(_))
        ));

        transaction
            .alter_column(
                t,
                c,
                ColumnAlteration {
                    column_type: Some("VARCHAR".into()),
                    nulls_allowed: Some(false),
                    default_value: Some(Some("''".into())),
                },
            )
            .unwrap();
        let altered = &transaction.columns_of(t)[1];
        assert_eq!(altered.column_type, "VARCHAR");
        assert!(!altered.nulls_allowed);
        assert_eq!(altered.default_value, Some("''".into()));
    }

    #[test]
    fn table_moves_and_renames_validate_against_target() {
        let mut transaction = empty_transaction();
        let s1 = transaction.create_schema("a").unwrap();
        let s2 = transaction.create_schema("b").unwrap();
        let t1 = transaction.create_table(s1, "t", &[col("x")]).unwrap();
        let _t2 = transaction.create_table(s2, "t", &[col("x")]).unwrap();
        // Moving into a schema that already has a table of that name fails.
        assert!(matches!(
            transaction.set_table_schema(t1, s2),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.set_table_schema(t1, SchemaId::new(99)),
            Err(Error::NotFound(_))
        ));
        transaction.rename_table(t1, "t_renamed").unwrap();
        transaction.set_table_schema(t1, s2).unwrap();
        assert_eq!(transaction.tables_in(s2).len(), 2);
        // Each mutation of an existing table classifies as an alter.
        let (ops, _, _, _) = transaction.into_parts();
        let alters = ops
            .iter()
            .filter(|op| matches!(op, Operation::AlterTable { table_id } if *table_id == t1.get()))
            .count();
        assert_eq!(alters, 2);
    }

    #[test]
    fn self_targeted_renames_and_moves_error() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.rename_table(t, "t"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.set_table_schema(t, s),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.rename_column(t, ColumnId::new(1), "a"),
            Err(Error::AlreadyExists(_))
        ));
    }

    #[test]
    fn add_column_floors_allocation_at_live_ids() {
        // A table version authored without the counter (next_column_id
        // absent, i.e. 0) must not let allocation regress below the ids
        // already live on the table.
        use crate::{catalog::CatalogSnapshot, store::read::EntityRecord};

        let snapshot = SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        };
        let table = TableValue {
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
            EntityRecord::Column(ColumnValue {
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
        let mut current = vec![EntityRecord::Table(table)];
        current.extend(columns);
        let state = CatalogSnapshot::build(snapshot, current, vec![], None);
        let mut transaction = Transaction::new(state, 5);
        let c = transaction.add_column(TableId::new(1), &col("c")).unwrap();
        assert_eq!(c, ColumnId::new(3));
    }

    fn datafile(rows: u64, stats: Vec<FileColumnStats>) -> DataFile {
        DataFile {
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: rows,
            file_size_bytes: rows * 10,
            footer_size: 4,
            encryption_key: None,
            column_stats: stats,
        }
    }

    #[test]
    fn register_allocates_row_ids_and_maintains_stats() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        // create_table minted the stats record.
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!((stats.record_count, stats.next_row_id), (0, 0));

        let f1 = transaction
            .register_data_file(t, datafile(100, vec![]))
            .unwrap();
        let f2 = transaction
            .register_data_file(t, datafile(50, vec![]))
            .unwrap();
        assert_ne!(f1, f2);
        let files = transaction.data_files_of(t);
        assert_eq!(files[0].row_id_start, Some(0));
        assert_eq!(files[1].row_id_start, Some(100));
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!(stats.record_count, 150);
        assert_eq!(stats.next_row_id, 150);
        assert_eq!(stats.file_size_bytes, 1500);
    }

    #[test]
    fn register_validates_table_and_stat_columns() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.register_data_file(TableId::new(99), datafile(1, vec![])),
            Err(Error::NotFound(_))
        ));
        let bad_stats = vec![FileColumnStats {
            column_id: ColumnId::new(99),
            column_size_bytes: 1,
            value_count: 1,
            null_count: 0,
            min_value: None,
            max_value: None,
            contains_nan: None,
            extra_stats: None,
        }];
        assert!(matches!(
            transaction.register_data_file(t, datafile(1, bad_stats)),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn expire_cascades_delete_files_and_preserves_next_row_id() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let f = transaction
            .register_data_file(t, datafile(100, vec![]))
            .unwrap();
        let d = transaction
            .register_delete_file(
                t,
                DeleteFile {
                    data_file_id: f,
                    path: "d.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 5,
                    file_size_bytes: 50,
                    footer_size: 4,
                    encryption_key: None,
                },
            )
            .unwrap();
        assert_eq!(transaction.delete_files_of(t)[0].id, d);

        transaction.expire_data_file(t, f).unwrap();
        assert!(transaction.data_files_of(t).is_empty());
        assert!(
            transaction.delete_files_of(t).is_empty(),
            "delete file cascades"
        );
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!(stats.record_count, 0);
        // The row-id counter never regresses.
        assert_eq!(stats.next_row_id, 100);
        let f2 = transaction
            .register_data_file(t, datafile(10, vec![]))
            .unwrap();
        assert_eq!(transaction.data_files_of(t)[0].row_id_start, Some(100));
        let _ = f2;
    }

    #[test]
    fn delete_file_requires_live_data_file() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.register_delete_file(
                t,
                DeleteFile {
                    data_file_id: DataFileId::new(99),
                    path: "d.parquet".into(),
                    path_is_relative: true,
                    format: "parquet".into(),
                    delete_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                },
            ),
            Err(Error::NotFound(_))
        ));
    }

    /// One live delete file per data file: a second registration against
    /// the same target is refused until the first is expired (a new delete
    /// file carries all deletes and supersedes its predecessor).
    #[test]
    fn second_live_delete_file_for_same_data_file_is_refused() {
        let delete_file = |f| DeleteFile {
            data_file_id: f,
            path: "d.parquet".into(),
            path_is_relative: true,
            format: "parquet".into(),
            delete_count: 1,
            file_size_bytes: 10,
            footer_size: 4,
            encryption_key: None,
        };

        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let f = transaction
            .register_data_file(t, datafile(100, vec![]))
            .unwrap();

        let first = transaction.register_delete_file(t, delete_file(f)).unwrap();
        assert!(matches!(
            transaction.register_delete_file(t, delete_file(f)),
            Err(Error::Constraint(_))
        ));

        // Expiring the predecessor frees the slot.
        transaction.expire_delete_file(t, first).unwrap();
        transaction.register_delete_file(t, delete_file(f)).unwrap();
    }

    #[test]
    fn stats_verbs_update_verbatim_and_preserve_row_counter() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        transaction
            .register_data_file(t, datafile(100, vec![]))
            .unwrap();
        transaction.update_table_stats(t, 42, 420).unwrap();
        let stats = transaction.table_stats(t).unwrap();
        assert_eq!((stats.record_count, stats.file_size_bytes), (42, 420));
        assert_eq!(
            stats.next_row_id, 100,
            "override cannot regress the counter"
        );

        transaction
            .update_column_stats(
                t,
                ColumnId::new(1),
                ColumnStats {
                    contains_null: Some(false),
                    contains_nan: None,
                    min_value: Some("9".into()),
                    max_value: Some("10".into()),
                    extra_stats: None,
                },
            )
            .unwrap();
        let cs = transaction.column_stats(t, ColumnId::new(1)).unwrap();
        assert_eq!(cs.min_value.as_deref(), Some("9"));
        assert!(matches!(
            transaction.update_column_stats(t, ColumnId::new(9), ColumnStats::default()),
            Err(Error::NotFound(_))
        ));

        // Dropping a column removes its table-level stats too, symmetric
        // with delete_table removing table_stats.
        let c2 = transaction.add_column(t, &col("b")).unwrap();
        transaction
            .update_column_stats(
                t,
                c2,
                ColumnStats {
                    contains_null: Some(true),
                    contains_nan: None,
                    min_value: Some("1".into()),
                    max_value: Some("2".into()),
                    extra_stats: None,
                },
            )
            .unwrap();
        assert!(transaction.column_stats(t, c2).is_some());
        transaction.drop_column(t, c2).unwrap();
        assert!(transaction.column_stats(t, c2).is_none());
    }

    #[test]
    fn views_share_the_relation_namespace() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "orders", &[col("a")]).unwrap();
        assert!(matches!(
            transaction.create_view(s, "orders", "duckdb", "SELECT 1"),
            Err(Error::AlreadyExists(_))
        ));
        let v = transaction
            .create_view(s, "v_orders", "duckdb", "SELECT 1")
            .unwrap();
        assert!(matches!(
            transaction.create_table(s, "v_orders", &[col("a")]),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            transaction.rename_table(t, "v_orders"),
            Err(Error::AlreadyExists(_))
        ));
        assert_eq!(transaction.view_by_name(s, "v_orders").unwrap().id, v);
        // A schema with live views cannot be dropped.
        transaction.drop_table(t).unwrap();
        assert!(matches!(
            transaction.drop_schema(s),
            Err(Error::Constraint(_))
        ));
        transaction.drop_view(v).unwrap();
        transaction.drop_schema(s).unwrap();
    }

    #[test]
    fn alter_view_replaces_definition() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let v = transaction
            .create_view(s, "v", "duckdb", "SELECT 1")
            .unwrap();
        transaction.alter_view(v, "duckdb", "SELECT 2").unwrap();
        assert_eq!(transaction.view_by_id(v).unwrap().sql, "SELECT 2");
        assert!(matches!(
            transaction.alter_view(ViewId::new(99), "d", "s"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn options_set_unset_and_validate_scopes() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        transaction
            .set_option(OptionScope::Global, "k", "g")
            .unwrap();
        transaction
            .set_option(OptionScope::Table(t), "k", "t")
            .unwrap();
        assert_eq!(
            transaction.option(OptionScope::Table(t), "k").as_deref(),
            Some("t")
        );
        transaction
            .unset_option(OptionScope::Table(t), "k")
            .unwrap();
        assert_eq!(
            transaction.option(OptionScope::Table(t), "k").as_deref(),
            Some("g")
        );
        transaction
            .unset_option(OptionScope::Table(t), "missing")
            .unwrap();
        assert!(matches!(
            transaction.set_option(OptionScope::Table(TableId::new(99)), "k", "v"),
            Err(Error::NotFound(_))
        ));
        // Option mutations stage no ops; the two DDL ops remain.
        let (ops, _, _, _) = transaction.into_parts();
        assert_eq!(ops.len(), 2);
    }

    /// The bootstrap `main` schema (id 0) is the catalog's default —
    /// DuckDB resolves unqualified names against it — and must survive
    /// every commit. Only the verb path could drop it; DuckDB refuses the
    /// SQL-level drop before it ever reaches the staged path.
    #[test]
    fn bootstrap_main_schema_cannot_be_dropped() {
        let snapshot = SnapshotValue {
            snapshot_id: 4,
            snapshot_time_micros: 1,
            schema_version: 2,
            next_catalog_id: 10,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
        };
        let main = SchemaValue {
            schema_id: 0,
            schema_uuid: "u".into(),
            begin_snapshot: 0,
            end_snapshot: None,
            schema_name: "main".into(),
            path: "main/".into(),
            path_is_relative: true,
        };
        let mut transaction = Transaction::new(
            CatalogSnapshot::build(
                snapshot,
                vec![crate::store::read::EntityRecord::Schema(main)],
                vec![],
                None,
            ),
            5,
        );

        assert!(matches!(
            transaction.drop_schema(SchemaId::new(0)),
            Err(Error::Constraint(_))
        ));

        // Any other empty schema still drops.
        let other = transaction.create_schema("other").unwrap();
        transaction.drop_schema(other).unwrap();
    }

    /// Relation names flow into derived storage paths: a separator nests
    /// or collides prefixes and a dot segment escapes the catalog root,
    /// so schema, table, and view names refuse them.
    #[test]
    fn path_unsafe_names_are_refused() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();

        for name in ["a/b", "a\\b", ".", ".."] {
            assert!(
                matches!(transaction.create_schema(name), Err(Error::Constraint(_))),
                "schema {name:?}"
            );
            assert!(
                matches!(
                    transaction.create_table(s, name, &[col("a")]),
                    Err(Error::Constraint(_))
                ),
                "table {name:?}"
            );
            assert!(
                matches!(transaction.rename_table(t, name), Err(Error::Constraint(_))),
                "rename {name:?}"
            );
            assert!(
                matches!(
                    transaction.create_view(s, name, "duckdb", "SELECT 1"),
                    Err(Error::Constraint(_))
                ),
                "view {name:?}"
            );
        }
    }

    /// Every name-taking verb refuses the empty string: an empty name is
    /// unaddressable and an empty schema name persists the path `"/"`.
    #[test]
    fn empty_names_are_refused() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();
        let column = transaction.columns_of(t)[0].id;

        let refused = [
            transaction.create_schema("").err(),
            transaction.create_table(s, "", &[col("a")]).err(),
            transaction.create_table(s, "t2", &[col("")]).err(),
            transaction.rename_table(t, "").err(),
            transaction.add_column(t, &col("")).err(),
            transaction.rename_column(t, column, "").err(),
            transaction.create_view(s, "", "duckdb", "SELECT 1").err(),
            transaction.set_option(OptionScope::Global, "", "v").err(),
        ];
        for err in refused {
            assert!(matches!(err, Some(Error::Constraint(_))), "{err:?}");
        }
    }

    /// The global `encrypted` option is fixed at catalog creation: set and
    /// unset both refuse it, while a non-global `encrypted` key (or any
    /// other global key) stays writable.
    #[test]
    fn global_encrypted_option_is_reserved() {
        let mut transaction = empty_transaction();
        let s = transaction.create_schema("s").unwrap();
        let t = transaction.create_table(s, "t", &[col("a")]).unwrap();

        assert!(matches!(
            transaction.set_option(OptionScope::Global, "encrypted", "true"),
            Err(Error::Constraint(_))
        ));
        assert!(matches!(
            transaction.unset_option(OptionScope::Global, "encrypted"),
            Err(Error::Constraint(_))
        ));

        transaction
            .set_option(OptionScope::Table(t), "encrypted", "x")
            .unwrap();
        transaction
            .set_option(OptionScope::Global, "other", "v")
            .unwrap();
    }
}
