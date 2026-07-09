//! Moraine brings a [SlateDB](https://slatedb.io) backend to
//! [DuckLake](https://ducklake.select): a DuckLake catalog implemented on a
//! transactional KV store over object storage, instead of the usual
//! relational catalog database.
//!
//! **Status: pre-alpha.** The API is a skeleton; nothing works yet.
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

pub use error::{Error, Result};
