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
    /// First row id of the file's dense per-table row-id range.
    pub row_id_start: u64,
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
