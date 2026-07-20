//! The staged-row transaction seam: DuckLake authors rows over the ABI
//! instead of `moraine`'s own verb API. Re-exports `transaction::staged`'s
//! types (`transaction` is otherwise private to the crate) and adds the
//! one entry point that needs a [`Catalog`]: opening the underlying
//! transaction. `#[doc(hidden)]`, unstable, as with all of
//! [`crate::ffi_support`].

use std::sync::Arc;

use object_store::ObjectStore;

#[doc(hidden)]
pub use crate::transaction::staged::{Cell, RowOperation, StagedTransaction, TableKind};
use crate::{catalog::Catalog, error::Result};

/// Begins a staged-row transaction at the current head. `data_store` (with
/// its bucket-relative `data_prefix`) is the `DATA_PATH` object store the
/// commit scoped-reads registered files from to maintain equality indexes;
/// `None` disables that maintenance.
///
/// # Errors
///
/// Returns an error if the underlying store transaction cannot be opened.
#[doc(hidden)]
pub async fn staged_begin(
    catalog: &Catalog,
    data_store: Option<Arc<dyn ObjectStore>>,
    data_prefix: String,
) -> Result<StagedTransaction> {
    let db_tx = catalog.begin_write_tx().await?;
    Ok(StagedTransaction::begin(
        db_tx,
        catalog.projections().clone(),
        data_store,
        data_prefix,
    ))
}
