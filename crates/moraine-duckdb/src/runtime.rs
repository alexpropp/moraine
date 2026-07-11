//! Opaque handles owned across the FFI boundary, and the sync↔async
//! bridge: one tokio multi-threaded runtime per attached catalog.

use moraine::{Catalog, CatalogSnapshot};

/// An attached catalog: owns the tokio runtime created at `ATTACH` and
/// the [`Catalog`] handle opened on it.
///
/// Opaque to C — only ever seen as a `MoraineCatalogHandle*` obtained
/// from [`moraine_attach`](crate::abi::moraine_attach) and released via
/// [`moraine_detach`](crate::abi::moraine_detach).
///
/// Every FFI entry point `block_on`s through `runtime`; nothing in
/// `moraine` core ever blocks on itself.
pub struct MoraineCatalogHandle {
    pub(crate) runtime: tokio::runtime::Runtime,
    pub(crate) catalog: Catalog,
    /// The cancellation seam [`moraine_interrupt`](crate::abi::moraine_interrupt)
    /// signals and read paths `select!` against.
    ///
    /// One-shot [`tokio::sync::Notify`] permit: `notify_one` either wakes
    /// an already-waiting read or stores one permit consumed by the next
    /// `notified()` call; the signal is consumed by the read that
    /// observes it and never carries over. Assumes at most one read in
    /// flight per handle at a time.
    pub(crate) interrupt: tokio::sync::Notify,
}

impl MoraineCatalogHandle {
    pub(crate) fn new(runtime: tokio::runtime::Runtime, catalog: Catalog) -> Self {
        Self {
            runtime,
            catalog,
            interrupt: tokio::sync::Notify::new(),
        }
    }
}

/// A materialized snapshot view, held across the FFI boundary so
/// listing calls need no further store I/O.
///
/// Opaque to C — only ever seen as a `MoraineSnapshotHandle*` obtained
/// from [`moraine_snapshot`](crate::abi::moraine_snapshot) and released
/// via [`moraine_snapshot_free`](crate::abi::moraine_snapshot_free).
pub struct MoraineSnapshotHandle {
    pub(crate) snapshot: CatalogSnapshot,
}

impl MoraineSnapshotHandle {
    pub(crate) fn new(snapshot: CatalogSnapshot) -> Self {
        Self { snapshot }
    }
}

/// Builds the one multi-threaded tokio runtime an attached catalog owns
/// for the lifetime of its handle.
pub(crate) fn new_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}
