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
//! # Layering
//!
//! - `catalog` — the DuckLake domain model. Never touches SlateDB directly.
//! - `store` — the SlateDB layer: key layout and value codecs. Knows nothing
//!   about DuckLake semantics.
//! - `tx` — the commit protocol turning a catalog transaction into an atomic
//!   store write.

#![forbid(unsafe_code)]

mod catalog;
mod error;
#[doc(hidden)]
pub mod ffi_support;
mod store;
mod transaction;

pub use catalog::{
    Catalog, CatalogOptions, CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, ColumnInfo,
    ColumnStats, DataFile, DataFileId, DataFileInfo, DeleteFile, DeleteFileId, DeleteFileInfo,
    FileColumnStats, MacroId, MacroImplementationDef, MacroInfo, MacroParameterDef, OptionScope,
    ScheduledDeletion, SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, TableId, TableInfo,
    TableStats, TagEntry, ViewId, ViewInfo,
};
pub use error::{Error, Result};
pub use transaction::Transaction;
