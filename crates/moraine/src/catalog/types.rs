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
    }
}
