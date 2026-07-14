//! The DuckLake domain model: snapshots, schemas, tables, data-file metadata.
//!
//! This layer never performs store I/O itself; the commit protocol in
//! [`crate::transaction`] drives it.

mod handle;
pub(crate) mod inline;
pub(crate) mod projection;
mod snapshot;
mod types;

pub use handle::{Catalog, CatalogOptions};
pub use snapshot::CatalogSnapshot;
pub use types::{
    ColumnAlteration, ColumnDef, ColumnId, ColumnInfo, ColumnStats, DataFile, DataFileId,
    DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo, FileColumnStats, MacroId,
    MacroImplementationDef, MacroInfo, MacroParameterDef, MappingId, MappingInfo, NameMappingDef,
    OptionScope, ScheduledDeletion, SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, TableId,
    TableInfo, TableStats, TagEntry, ViewId, ViewInfo,
};
