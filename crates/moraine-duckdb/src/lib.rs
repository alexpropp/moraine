//! DuckDB extension for [moraine]: attaches a moraine store as a DuckDB
//! catalog. Three layers, thin by policy — no DuckLake domain logic lives
//! outside the core crate:
//!
//! 1. a **C++ shim** (`cpp/*.cpp`, compiled by `build.rs`) links DuckDB's
//!    internal C++ API and registers a `StorageExtension`;
//! 2. a **C ABI** (the [`abi`] module, mirrored by hand in
//!    `cpp/moraine_abi.h`) marshals calls across the language boundary and
//!    owns the sync↔async bridge — one tokio runtime per attached catalog,
//!    `block_on` at every entry point, `catch_unwind` so a core panic
//!    surfaces as an error code, never an unwind into C++;
//! 3. the async [moraine] core, unaware any of this exists.
//!
//! **Works, against a real DuckDB CLI:** `LOAD`; `ATTACH '<path>' AS m
//! (TYPE moraine)` (a standalone attach type — `ducklake:` chaining is a
//! later integration); schema/table/view listing (`duckdb_databases()`,
//! `duckdb_tables()`, `duckdb_views()`, `duckdb_columns()`); `SELECT`/
//! `DESCRIBE` on a table, which scans its live data files by resolving
//! their paths through the listing ABI and delegating to DuckDB's own
//! `read_parquet` (see `README.md`'s "Table scans" section).
//!
//! **Not implemented, throws `NotImplementedException`:** every write
//! path, and querying a view's definition (no SQL parser vendored).
//!
//! **Single writer.** Attach always opens a read-write [`moraine::Catalog`]
//! — there is no read-only attach option yet — so only one process may
//! hold a given store attached; a second attach fences the first's writer
//! rather than failing itself. See `README.md` for the pinned build shape.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod abi;
pub mod error;
pub mod runtime;
