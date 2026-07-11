//! Moraine brings a [SlateDB](https://slatedb.io) backend to
//! [DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
//! transactional KV store over object storage, instead of the usual
//! relational catalog database.
//!
//! Exactly one process holds a read-write [`Catalog`] per store at a time
//! (opening a second fences the first); any number of processes may read
//! snapshots concurrently.
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
//!     .commit(|txn| {
//!         let sales = txn.create_schema("sales")?;
//!         txn.create_table(
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
//! // The head sees the latest shape...
//! let head = catalog.snapshot().await?;
//! assert_eq!(head.schemas().len(), 1);
//!
//! // ...while `v1` remains queryable by time travel, forever.
//! let past = catalog.snapshot_at(v1).await?;
//! assert_eq!(past.schemas().len(), 1);
//! # Ok::<(), moraine::Error>(()) }).unwrap();
//! ```
//!
//! See `examples/walkthrough.rs` for a longer tour that also alters and
//! renames a table across a second commit.
//!
//! # Layering
//!
//! - `catalog` — the DuckLake domain model. Never touches SlateDB directly.
//! - `store` — the SlateDB layer: key layout and value codecs. Knows nothing
//!   about DuckLake semantics.
//! - `txn` — the commit protocol turning a catalog transaction into an atomic
//!   store write.

#![forbid(unsafe_code)]

mod catalog;
mod error;
mod store;
mod txn;

pub use catalog::{
    Catalog, CatalogOptions, CatalogSnapshot, ColumnAlteration, ColumnDef, ColumnId, ColumnInfo,
    SchemaId, SchemaInfo, SnapshotId, SnapshotInfo, TableId, TableInfo,
};
pub use error::{Error, Result};
pub use txn::Txn;
