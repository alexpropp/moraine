//! Public domain types: ids and value structs, hand-written and decoupled
//! from the wire types so on-disk field evolution never becomes a public
//! breaking change.

/// Declares a newtype id over the catalog's `u64` id space.
macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            /// Wraps a raw id.
            #[must_use]
            pub const fn new(id: u64) -> Self {
                Self(id)
            }

            /// The raw id.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(
    /// Identifies a committed snapshot.
    SnapshotId
);
id_type!(
    /// Identifies a schema.
    SchemaId
);
id_type!(
    /// Identifies a table.
    TableId
);
id_type!(
    /// Identifies a column within its table (a field id: stable across
    /// renames and never reused, even after the column is dropped).
    ColumnId
);
id_type!(
    /// Identifies a data file.
    DataFileId
);
id_type!(
    /// Identifies a delete file.
    DeleteFileId
);
id_type!(
    /// Identifies a view.
    ViewId
);
id_type!(
    /// Identifies a macro.
    MacroId
);
id_type!(
    /// Identifies a column mapping (allocated by DuckLake from the same
    /// counter as data-file ids).
    MappingId
);
id_type!(
    /// Identifies an equality index within its table (allocated from the
    /// global catalog-id counter).
    IndexId
);

/// A data file to register: the file already exists on object storage
/// (data before metadata). `row_id_start` is allocated by the commit,
/// never caller-provided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFile {
    /// Object-store path of the file.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format (e.g. `"parquet"`).
    pub file_format: String,
    /// Number of rows in the file.
    pub record_count: u64,
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// Encryption key material, verbatim — an opaque string moraine
    /// stores and returns but never interprets.
    pub encryption_key: Option<String>,
    /// Per-column statistics carried with the registration. Every entry
    /// must reference a live column of the table.
    pub column_stats: Vec<FileColumnStats>,
}

/// Per-column statistics of one data file. Min/max are opaque strings,
/// stored verbatim — moraine never interprets or merges them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileColumnStats {
    /// The column these statistics describe.
    pub column_id: ColumnId,
    /// Compressed size of this column in the file.
    pub column_size_bytes: u64,
    /// Number of values (rows) for this column.
    pub value_count: u64,
    /// Number of NULLs.
    pub null_count: u64,
    /// Minimum value, verbatim.
    pub min_value: Option<String>,
    /// Maximum value, verbatim.
    pub max_value: Option<String>,
    /// Whether the column contains NaN (floating-point columns).
    pub contains_nan: Option<bool>,
    /// Extra statistics, verbatim.
    pub extra_stats: Option<String>,
}

/// A delete file to register, targeting one data file's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFile {
    /// The data file whose rows this delete file removes.
    pub data_file_id: DataFileId,
    /// Object-store path of the delete file.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format (e.g. `"parquet"`).
    pub format: String,
    /// Number of deleted rows recorded in the file.
    pub delete_count: u64,
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// Encryption key material, verbatim — an opaque string moraine
    /// stores and returns but never interprets.
    pub encryption_key: Option<String>,
}

/// A live data file, as read from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileInfo {
    /// The file's id.
    pub id: DataFileId,
    /// Object-store path.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format.
    pub file_format: String,
    /// Number of rows.
    pub record_count: u64,
    /// Total size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// First row id of the file's dense per-table row-id range; `None`
    /// when the file's rows carry explicit per-row ids instead
    /// (compaction outputs).
    pub row_id_start: Option<u64>,
    /// Encryption key material, verbatim.
    pub encryption_key: Option<String>,
}

/// A live delete file, as read from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFileInfo {
    /// The delete file's id.
    pub id: DeleteFileId,
    /// The data file it targets.
    pub data_file_id: DataFileId,
    /// Object-store path.
    pub path: String,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// File format.
    pub format: String,
    /// Number of deleted rows.
    pub delete_count: u64,
    /// Total size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
    /// Encryption key material, verbatim.
    pub encryption_key: Option<String>,
}

/// A table's statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStats {
    /// Total rows across the table's live data files.
    pub record_count: u64,
    /// Total bytes across the table's live data files.
    pub file_size_bytes: u64,
    /// The next row id to allocate; advances with every registration and
    /// never regresses.
    pub next_row_id: u64,
}

/// A column's table-level statistics. Min/max are opaque strings, stored
/// verbatim — moraine never interprets or merges them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnStats {
    /// Whether the column contains NULLs.
    pub contains_null: Option<bool>,
    /// Whether the column contains NaN.
    pub contains_nan: Option<bool>,
    /// Minimum value, verbatim.
    pub min_value: Option<String>,
    /// Maximum value, verbatim.
    pub max_value: Option<String>,
    /// Extra statistics, verbatim.
    pub extra_stats: Option<String>,
}

/// A live schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    /// The schema's id.
    pub id: SchemaId,
    /// The schema's name, unique among live schemas.
    pub name: String,
}

/// A live table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// The table's id.
    pub id: TableId,
    /// The schema the table belongs to.
    pub schema_id: SchemaId,
    /// The table's name, unique among live tables of its schema.
    pub name: String,
}

/// A live view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewInfo {
    /// The view's id.
    pub id: ViewId,
    /// The schema the view belongs to.
    pub schema_id: SchemaId,
    /// The view's name, unique among the schema's live tables and views.
    pub name: String,
    /// SQL dialect of the definition.
    pub dialect: String,
    /// The view's defining SQL.
    pub sql: String,
}

/// One parameter of a macro implementation. An absent default stores
/// `default_value: None` with `default_value_type` `"unknown"`, matching
/// the row DuckLake writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroParameterDef {
    /// Parameter name.
    pub name: String,
    /// DuckLake type string; `"unknown"` for an untyped parameter.
    pub parameter_type: String,
    /// Default value rendered as a string, if the parameter has one.
    pub default_value: Option<String>,
    /// DuckLake type string of the default; `"unknown"` when absent.
    pub default_value_type: String,
}

/// One implementation (arity overload) of a macro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroImplementationDef {
    /// SQL dialect of the body (DuckLake writes `"duckdb"`).
    pub dialect: String,
    /// The macro body: an expression (scalar) or a SELECT (table).
    pub sql: String,
    /// `"scalar"` or `"table"`; every implementation of one macro must
    /// carry the same value.
    pub macro_type: String,
    /// Parameters in positional order.
    pub parameters: Vec<MacroParameterDef>,
}

/// A macro with its implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroInfo {
    /// The macro's id.
    pub id: MacroId,
    /// The schema the macro belongs to.
    pub schema_id: SchemaId,
    /// The macro's name, unique among the schema's live macros (macros
    /// have their own namespace, separate from tables and views).
    pub name: String,
    /// Implementations in `impl_id` order.
    pub implementations: Vec<MacroImplementationDef>,
}

/// One `ducklake_name_mapping` row: how one physical column of an
/// externally written file resolves to a table field. `column_id` is a
/// 0-based ordinal local to the mapping; `parent_column`, when present,
/// references a smaller ordinal (parents precede children in preorder).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameMappingDef {
    /// The row's ordinal within its mapping.
    pub column_id: u64,
    /// The physical column name in the file (or hive path).
    pub source_name: String,
    /// The table field the column resolves to.
    pub target_field_id: u64,
    /// The parent row's ordinal for nested columns; `None` for roots.
    pub parent_column: Option<u64>,
    /// Whether the value comes from the file's hive path, not its body.
    pub is_partition: bool,
}

/// A column mapping for externally written Parquet: immutable once
/// created, referenced by data files via their `mapping_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingInfo {
    /// The mapping's id.
    pub id: MappingId,
    /// The table the mapping belongs to.
    pub table_id: TableId,
    /// The mapping strategy, stored verbatim (DuckLake writes
    /// `"map_by_name"`).
    pub map_type: String,
    /// The mapping's rows in `column_id` order.
    pub name_mappings: Vec<NameMappingDef>,
}

/// One writer-supplied index entry for a row of a registered data file.
/// The ordinal is the row's 0-based position in the file; the commit maps
/// it to a row id (`row_id_start + ordinal`).
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexEntry {
    /// The index this entry belongs to.
    pub index: IndexId,
    /// The row's 0-based position within the file.
    pub ordinal: u64,
    /// The indexed column values, positionally matching the index's
    /// columns; a `None` is SQL NULL and yields no entry.
    pub values: Vec<Option<crate::store::index_encoding::IndexKeyValue>>,
}

/// One writer-supplied entry removal for a registered delete file: the
/// killed row's id and the values it was indexed under. Against a
/// dense-range target the id must lie inside the target's row-id range;
/// against a per-row-id target it is the file's embedded id, supplied
/// verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexRemoval {
    /// The index the removal belongs to.
    pub index: IndexId,
    /// The killed row's id.
    pub row_id: u64,
    /// The indexed column values the dead row held, positionally
    /// matching the index's columns; a `None` is SQL NULL (no entry
    /// existed, none is removed).
    pub values: Vec<Option<crate::store::index_encoding::IndexKeyValue>>,
}

/// The build lifecycle of an equality index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Fully built: serving lookups and enforcing uniqueness.
    Ready,
    /// A staged backfill is in progress; lookups fail typed and a unique
    /// violation poisons the build rather than failing the writer.
    Building,
    /// A duplicate was discovered during a staged build; the definition is
    /// terminally poisoned and will be dropped by its driver.
    Poisoned,
}

/// A live equality index, as read from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    /// The index's id.
    pub id: IndexId,
    /// The table the index covers.
    pub table_id: TableId,
    /// The index name, unique among the table's live indexes.
    pub name: String,
    /// Indexed columns by field id, in declared order.
    pub columns: Vec<ColumnId>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// The build lifecycle state.
    pub state: IndexState,
}

/// The definition of an equality index to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// The index name, unique among the table's live indexes.
    pub name: String,
    /// Indexed columns by field id, in declared order.
    pub columns: Vec<ColumnId>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
}

/// One writer-supplied index entry: a row and its indexed column values,
/// in the index's column order. A `None` in any position is SQL NULL, and
/// a row with any NULL indexed value gets no entry — the caller passes the
/// row through and the maintenance layer skips it.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// The row this entry points at.
    pub row_id: u64,
    /// The indexed column values, positionally matching the index's
    /// columns.
    pub values: Vec<Option<crate::store::index_encoding::IndexKeyValue>>,
}

/// Where a row a lookup found currently lives. moraine returns candidates;
/// the consumer applies delete files, as any DuckLake scan does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowHolder {
    /// A data file whose live row-id range contains the row.
    DataFile(DataFileId),
    /// The row is inlined (or its holder is not a dense-range data file),
    /// so it is not resolvable from data-file ranges alone.
    Inline,
}

/// A row an index lookup resolved: its stable id and current holder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowLocation {
    /// The stable row id the entry points at.
    pub row_id: u64,
    /// The row's current holder in this snapshot.
    pub holder: RowHolder,
}

/// A column definition: the input to table creation and column addition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name, unique among the table's live columns.
    pub name: String,
    /// Column type, as a DuckLake type string (e.g. `"BIGINT"`).
    pub column_type: String,
    /// Whether NULL values are allowed.
    pub nulls_allowed: bool,
    /// Default value expression, if any.
    pub default_value: Option<String>,
}

/// A live column: its definition plus identity and position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// The column's field id.
    pub id: ColumnId,
    /// Column name.
    pub name: String,
    /// Column type, as a DuckLake type string.
    pub column_type: String,
    /// Whether NULL values are allowed.
    pub nulls_allowed: bool,
    /// Default value expression, if any.
    pub default_value: Option<String>,
    /// Ordinal position in the table (0-based).
    pub position: u64,
    /// The parent column's field id for a nested child column (a `STRUCT`
    /// field, `LIST` element, or `MAP` key/value), or `None` for a
    /// top-level column.
    pub parent_column: Option<ColumnId>,
}

/// A change to one column. `None` fields leave the attribute untouched;
/// `default_value` uses a nested `Option` so "clear the default"
/// (`Some(None)`) is distinct from "leave it" (`None`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnAlteration {
    /// New column type, if changing.
    pub column_type: Option<String>,
    /// New nullability, if changing.
    pub nulls_allowed: Option<bool>,
    /// New default value: `Some(Some(expr))` sets, `Some(None)` clears.
    pub default_value: Option<Option<String>>,
}

/// Identity and metadata of a committed snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// The snapshot's id.
    pub id: SnapshotId,
    /// Commit time, microseconds since the Unix epoch (UTC).
    pub time_micros: i64,
    /// Catalog schema version: advances only when a commit changes the
    /// catalog's shape, so clients can key schema caches on it.
    pub schema_version: u64,
}

/// One tag on a catalog object: a key/value row, begin/end-versioned
/// like any temporal row. Ended entries stay readable for time travel
/// until garbage-collected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagEntry {
    /// Snapshot at which this tag value became visible.
    pub begin_snapshot: u64,
    /// Snapshot at which it was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// Tag key (e.g. `comment`).
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// One `ducklake_files_scheduled_for_deletion` row: a path awaiting
/// physical deletion, decoupled from the expiry that scheduled it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledDeletion {
    /// The data or delete file id the path belonged to (the schedule's
    /// row identity).
    pub data_file_id: u64,
    /// Object-store path, relative iff `path_is_relative`.
    pub path: String,
    /// Whether `path` is relative to the table's data prefix.
    pub path_is_relative: bool,
    /// Microseconds since epoch, UTC, when the file was scheduled.
    pub schedule_start_micros: i64,
}

/// An option scope: global, or one schema or table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionScope {
    /// Catalog-wide.
    Global,
    /// One schema.
    Schema(SchemaId),
    /// One table.
    Table(TableId),
}

impl OptionScope {
    /// Returns scope key components as (`scope_type`, `id`).
    pub(crate) fn key_components(self) -> (u64, u64) {
        match self {
            Self::Global => (0, 0),
            Self::Schema(id) => (1, id.get()),
            Self::Table(id) => (2, id.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_and_display() {
        let id = TableId::new(7);
        assert_eq!(id.get(), 7);
        assert_eq!(id.to_string(), "7");
        assert_eq!(SnapshotId::new(0).get(), 0);
        assert_eq!(ColumnId::new(3).get(), 3);
        assert_eq!(SchemaId::new(4).to_string(), "4");
        assert_eq!(DataFileId::new(9).get(), 9);
        assert_eq!(DeleteFileId::new(8).to_string(), "8");
        assert_eq!(ViewId::new(5).get(), 5);
    }
}
