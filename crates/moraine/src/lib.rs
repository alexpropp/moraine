//! Moraine brings a [SlateDB](https://slatedb.io) backend to
//! [DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
//! transactional KV store over object storage, instead of the usual
//! relational catalog database.
//!
//! Exactly one process holds a read-write [`Catalog`] per store at a time
//! (opening a second fences the first); any number of processes may read
//! snapshots concurrently. Schemas, tables, views, data files, and
//! statistics all commit through the same transaction; catalog options
//! live outside the snapshot protocol (last-write-wins, no snapshot
//! minted).
//!
//! # A worked example
//!
//! Open a catalog on a store, evolve its schema through commits, and read
//! both the current state and any past snapshot:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine::{Catalog, CatalogOptions, ColumnDef};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
//!
//! let v1 = catalog
//!     .commit(|tx| {
//!         let sales = tx.create_schema("sales")?;
//!         tx.create_table(
//!             sales,
//!             "orders",
//!             &[ColumnDef {
//!                 name: "id".into(),
//!                 column_type: "BIGINT".into(),
//!                 nulls_allowed: false,
//!                 default_value: None,
//!             }],
//!         )?;
//!         Ok(())
//!     })
//!     .await?;
//!
//! // The head sees the latest shape (plus the bootstrap-minted `main`)...
//! let head = catalog.snapshot().await?;
//! assert_eq!(head.schemas().len(), 2);
//!
//! // ...while `v1` remains queryable by time travel, forever.
//! let past = catalog.snapshot_at(v1).await?;
//! assert_eq!(past.schemas().len(), 2);
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! See `examples/walkthrough.rs` for a longer tour that also alters and
//! renames a table across a second commit. Data files register through the
//! same [`Catalog::commit`] path, minting a snapshot without bumping the
//! schema version.
//!
//! # Maintenance
//!
//! Dropping an equality index (or a table that has one) makes its entries
//! invisible immediately but does not delete them, so the range would leak
//! without a sweep. [`Catalog::maintain`] reclaims those ranges:
//!
//! ```
//! # use std::sync::Arc;
//! # use moraine::{Catalog, CatalogOptions, MaintenanceRequest};
//! # use object_store::memory::InMemory;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
//!
//! let report = catalog.maintain(MaintenanceRequest::default()).await?;
//! // Nothing has been dropped, so there is nothing to reclaim.
//! assert_eq!(report.indexes_swept, 0);
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! A pass mints no snapshot and leaves head unchanged, and deletes in
//! bounded batches (`MaintenanceRequest::batch_size`) so a large
//! reclamation never holds the writer. It is safe to interrupt: an
//! abandoned pass leaves a smaller one for next time, never a torn one.
//! Deciding what is dead rests on catalog ids never being reused, so a
//! sweep can run alongside a live writer without coordinating with it.
//!
//! This is the *only* reclamation moraine owns. Snapshot expiry, file
//! cleanup, and compaction belong to DuckLake and run through its own
//! functions; the SlateDB store collects its superseded objects itself,
//! unprompted. A host embedding this crate calls `maintain` on whatever
//! cadence it likes — the crate spawns no threads and schedules nothing.
//! Under the DuckDB extension, all of it is sequenced for you; see that
//! crate's docs for `moraine_maintenance`.
//!
//! # Diagnostics
//!
//! The crate emits [`tracing`](https://docs.rs/tracing) events and installs
//! no subscriber. Rare, consequential moments log at `info` (an open, the
//! once-ever bootstrap, a format-stamp upgrade) or `warn` (a spent retry
//! budget, an oversized commit refused); per-commit summaries and retry
//! traces log at `debug`. Every event is per-operation — never per row,
//! entry, or file. An embedding host sees them by installing any
//! subscriber. The DuckDB extension consumes its own — it cannot share the
//! host's — and forwards them to `duckdb_logs`; see that crate's `logging`
//! module.
//!
//! # Layering
//!
//! - `catalog` — the DuckLake domain model. Never touches SlateDB directly.
//! - `store` — the SlateDB layer: key layout and value codecs. Knows nothing
//!   about DuckLake semantics.
//! - `transaction` — the commit protocol turning a catalog transaction into an
//!   atomic store write.

#![forbid(unsafe_code)]

mod catalog;
mod error;
#[doc(hidden)]
pub mod ffi_support;
mod store;
mod transaction;

pub use catalog::{
    Catalog, CatalogOptions, CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, ColumnInfo,
    ColumnOrder, ColumnStats, Contention, DataFile, DataFileId, DataFileInfo, DeleteFile,
    DeleteFileId, DeleteFileInfo, FileColumnStats, FileIndexEntry, FileIndexRemoval, IndexDef,
    IndexEntry, IndexId, IndexInfo, IndexState, MacroId, MacroImplementationDef, MacroInfo,
    MacroParameterDef, MaintenanceReport, MaintenanceRequest, MappingId, MappingInfo,
    NameMappingDef, OptionScope, RowHolder, RowLocation, ScheduledDeletion, SchemaId, SchemaInfo,
    SnapshotId, SnapshotInfo, TableId, TableInfo, TableStats, TagEntry, ViewId, ViewInfo,
};
pub use error::{Error, Result};
pub use moraine_wal::FoldReport;
pub use store::index_encoding::{Direction, IndexKeyValue, IntWidth, NullOrder};
pub use transaction::Transaction;
