//! The DuckLake domain model: snapshots, schemas, tables, data-file metadata.
//!
//! This layer never performs store I/O itself; the commit protocol in
//! [`crate::transaction`] drives it.

mod handle;
pub(crate) mod index_policy;
pub(crate) mod inline;
pub(crate) mod projection;
pub(crate) mod scoped_read;
mod snapshot;
mod types;

pub use handle::{Catalog, CatalogOptions};
pub use snapshot::CatalogSnapshot;
pub(crate) use snapshot::ScopedNames;
pub use types::{
    ColumnAlteration, ColumnDef, ColumnId, ColumnInfo, ColumnOrder, ColumnStats, DataFile,
    DataFileId, DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo, FileColumnStats,
    FileIndexEntry, FileIndexRemoval, IndexDef, IndexEntry, IndexId, IndexInfo, IndexState,
    MacroId, MacroImplementationDef, MacroInfo, MacroParameterDef, MappingId, MappingInfo,
    NameMappingDef, OptionScope, RowHolder, RowLocation, ScheduledDeletion, SchemaId, SchemaInfo,
    SnapshotId, SnapshotInfo, TableId, TableInfo, TableStats, TagEntry, ViewId, ViewInfo,
};
