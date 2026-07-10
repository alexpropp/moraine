//! DuckDB extension packaging for [moraine].
//!
//! Thin by policy: no DuckLake domain logic — only `StorageExtension`
//! registration, C-ABI marshalling, and the sync↔async bridge. If logic
//! accumulates here, it belongs in the core crate.
//!
//! moraine is a DuckLake catalog backend:
//! a thin C++ shim registers a DuckDB `StorageExtension` and delegates over a
//! C ABI to this crate's Rust core, which bridges to async [moraine].
//!
//! **Status: stub.** Extension entry points are not built yet; until then
//! this builds an empty cdylib so packaging and CI plumbing can be exercised.

// The `moraine` dependency is declared now so the crates are wired; silence
// the unused warning until entry points land.
use moraine as _;
