//! Opaque handles owned across the FFI boundary, and the sync↔async
//! bridge: one tokio multi-threaded runtime per attached catalog.

use std::{ffi::c_void, future::Future, sync::Arc, time::Duration};

use moraine::{Catalog, CatalogSnapshot};
use object_store::ObjectStore;
use tokio::runtime::{Builder, Runtime};

use crate::{
    error::AbiError,
    logging::{HandleId, enter_handle, tag_thread_for_handle},
};

/// A C-side cancellation probe polled while a cancellable call's core
/// future is pending; returning `true` cancels the call. `None` disables
/// the pull channel for that call. Mirrors `MoraineInterruptProbe` in
/// `cpp/moraine_abi.h`.
pub type MoraineInterruptProbe = Option<unsafe extern "C" fn(probe_ctx: *mut c_void) -> bool>;

/// How often a cancellable call polls its interrupt probe while the core
/// future is pending. The first poll fires immediately, so a pending
/// interrupt cancels before the future does any work.
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    pub(crate) runtime: Runtime,
    pub(crate) catalog: Catalog,
    /// Routes this handle's `tracing` events to its registered log sink:
    /// the runtime's worker threads carry it for life, and the `block_on`
    /// wrappers below tag the calling thread with it per call.
    pub(crate) log_id: HandleId,
    /// The `DATA_PATH` object store, resolved at attach from `META_DATA_PATH`.
    /// Present only when that option was given; index maintenance and
    /// scoped-read backfill need it, and are skipped when it is absent.
    pub(crate) data_store: Option<Arc<dyn ObjectStore>>,
    /// The bucket-relative key prefix of `DATA_PATH` (empty for a local or
    /// bare-bucket store), prepended to a data file's stored path.
    pub(crate) data_prefix: String,
}

impl MoraineCatalogHandle {
    pub(crate) fn new(runtime: Runtime, catalog: Catalog, log_id: HandleId) -> Self {
        Self {
            runtime,
            catalog,
            log_id,
            data_store: None,
            data_prefix: String::new(),
        }
    }

    /// Runs `future` on the handle's runtime, attributing events the
    /// calling thread emits to this handle.
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _guard = enter_handle(self.log_id);
        self.runtime.block_on(future)
    }

    /// Runs `future` on the handle's runtime unless cancelled first by
    /// `probe` returning `true` (polled immediately, then every
    /// [`INTERRUPT_POLL_INTERVAL`]). Cancellation drops the future and
    /// returns the interrupted error.
    ///
    /// # Safety
    ///
    /// `probe`, if `Some`, must be safe to call with `probe_ctx` from any
    /// thread for the duration of this call.
    pub(crate) unsafe fn block_on_cancellable<T, E>(
        &self,
        probe: MoraineInterruptProbe,
        probe_ctx: *mut c_void,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, AbiError>
    where
        AbiError: From<E>,
    {
        // Checked before the future is first polled, not left to the
        // interval below: a timer's first tick is pending at the poll
        // level even when already elapsed, and a future that completes on
        // its first poll would otherwise win over a pending interrupt.
        if let Some(probe) = probe {
            // SAFETY: caller contract — `probe` is callable with
            // `probe_ctx` for the duration of this call.
            if unsafe { probe(probe_ctx) } {
                return Err(AbiError::interrupted());
            }
        }

        let _guard = enter_handle(self.log_id);
        self.runtime.block_on(async {
            let probe_fired = async {
                let Some(probe) = probe else {
                    return std::future::pending::<()>().await;
                };
                let mut ticks = tokio::time::interval(INTERRUPT_POLL_INTERVAL);
                loop {
                    ticks.tick().await;
                    // SAFETY: caller contract — `probe` is callable with
                    // `probe_ctx` for the duration of this call.
                    if unsafe { probe(probe_ctx) } {
                        return;
                    }
                }
            };

            // `biased`: a cancellation signal wins whenever ready, even if
            // the core future is also immediately ready.
            tokio::select! {
                biased;
                () = probe_fired => Err(AbiError::interrupted()),
                result = future => result.map_err(AbiError::from),
            }
        })
    }
}

/// A materialized snapshot view, held across the FFI boundary so
/// listing calls need no further store I/O.
///
/// Opaque to C — only ever seen as a `MoraineSnapshotHandle*` obtained
/// from [`moraine_snapshot`](crate::abi::moraine_snapshot) and released
/// via [`moraine_snapshot_free`](crate::abi::moraine_snapshot_free).
pub struct MoraineSnapshotHandle {
    pub(crate) snapshot: Arc<CatalogSnapshot>,
}

impl MoraineSnapshotHandle {
    pub(crate) fn new(snapshot: Arc<CatalogSnapshot>) -> Self {
        Self { snapshot }
    }
}

/// Builds the one multi-threaded tokio runtime an attached catalog owns
/// for the lifetime of its handle. Worker threads exist only to run that
/// handle's work, so each is tagged with `log_id` at spawn — every event
/// they emit routes to the handle's log sink.
pub(crate) fn new_runtime(log_id: HandleId) -> std::io::Result<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(move || tag_thread_for_handle(log_id))
        .build()
}
