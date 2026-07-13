//! The staged-row transaction seam: DuckLake authors rows over the ABI
//! instead of `moraine`'s own verb API. Re-exports `transaction::staged`'s
//! types (`transaction` is otherwise private to the crate) and adds the
//! one entry point that needs a [`Catalog`]: opening the underlying
//! transaction. `#[doc(hidden)]`, unstable, as with all of
//! [`crate::ffi_support`].

use crate::{catalog::Catalog, error::Result};

#[doc(hidden)]
pub use crate::transaction::staged::{Cell, RowOp, StagedTransaction, TableKind};

/// Begins a staged-row transaction at the current head.
///
/// # Errors
///
/// Returns an error if the underlying store transaction cannot be opened.
#[doc(hidden)]
pub async fn staged_begin(catalog: &Catalog) -> Result<StagedTransaction> {
    let db_tx = catalog.begin_read_tx().await?;
    Ok(StagedTransaction::begin(db_tx))
}
