//! DuckDB extension for [moraine]: attaches a moraine store as a DuckDB
//! catalog. Three layers, thin by policy — no DuckLake domain logic lives
//! outside the core crate:
//!
//! 1. a **C++ shim** (`cpp/*.cpp`, compiled by the DuckDB extension
//!    toolchain) links DuckDB's internal C++ API and registers a
//!    `StorageExtension`;
//! 2. a **C ABI** (the [`abi`] module, mirrored by hand in
//!    `cpp/moraine_abi.h`) marshals calls across the language boundary and
//!    owns the sync↔async bridge — one tokio runtime per attached catalog,
//!    `block_on` at every entry point, `catch_unwind` so a core panic
//!    surfaces as an error code, never an unwind into C++;
//! 3. the async [moraine] core, unaware any of this exists.
//!
//! **Primary path:** `ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH
//! '<data-path>')` — DuckLake drives moraine as its own metadata catalog.
//! `CREATE`/`INSERT`/`UPDATE`/`DELETE`/`DROP`/rename against `lake.*`
//! translate to staged row mutations (the [`staged`] module) committed as
//! one atomic batch; reads (`SELECT`, time travel, `ducklake_snapshots()`)
//! go through DuckLake's own reader over the `ducklake_*` rows this crate
//! projects (the [`dumps`] module). See `README.md`'s "Serving as
//! DuckLake's metadata catalog" section.
//!
//!
//! **Secondary path — metadata-only inspection:** `ATTACH '<path>' AS m
//! (TYPE moraine)`, or the bare `moraine:<path>` prefix (the same form
//! DuckLake's nested attach uses internally). Schema/table/view listing
//! (`duckdb_databases()`, `duckdb_tables()`, `duckdb_views()`,
//! `duckdb_columns()`), `DESCRIBE`, and every `ducklake_*` metadata table
//! work through this attach. User-table *data* does not: a `SELECT`
//! against a real user table binds normally (so `DESCRIBE`/`EXPLAIN` still
//! work) but raises `InvalidInputException` at execution time, naming the
//! `ducklake:moraine:` attach to use instead. See `README.md`'s
//! "User-table data" section.
//!
//! **Not implemented, throws `NotImplementedException`:** DDL issued
//! directly against a user schema/table (`CREATE`/`DROP`/`ALTER` outside
//! DuckLake's own `ducklake_*` writes) and querying a view's definition
//! (no SQL parser vendored).
//!
//! **Single writer.** Attach always opens a read-write [`moraine::Catalog`]
//! — there is no read-only attach option yet — so only one process may
//! hold a given store attached; a second attach fences the first's writer
//! rather than failing itself. See `README.md` for the pinned build shape.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod abi;
pub mod arrow_ipc;
pub mod dumps;
pub mod error;
pub mod inline;
pub mod runtime;
pub mod staged;
#[cfg(test)]
mod test_support;
