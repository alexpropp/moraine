//! DuckDB extension packaging for [moraine].
//!
//! Thin by policy: extension entry points and the sync↔async bridge only.
//! If logic accumulates here, it belongs in the core crate.
//!
//! **Status: stub.** Extension entry points arrive with the DuckDB
//! integration RFC; until then this builds an empty cdylib so packaging and
//! CI plumbing can be exercised.

// The `moraine` dependency is declared now so the crates are wired; silence
// the unused warning until entry points land.
use moraine as _;
