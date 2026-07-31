//! The folder-role session: the fenced writer that derived-state maintenance
//! runs under, so a slot-backed store keeps exactly one direct writer.
//!
//! With commits in the log, nothing may write the store directly except the
//! one fenced writer, or replay and the store diverge. Opening that writer is
//! the whole license: concurrent sessions fence each other, the newest winning,
//! so a loser surfaces [`Error::Fenced`](crate::Error::Fenced) rather than
//! corrupting anything.

use std::sync::Arc;

use slatedb::Db;

use crate::{
    catalog::SlotStore,
    error::{Error, Result},
    store::open::StoreBuilder,
};

/// Opens the fenced writer over `store`, runs `body` against it, and closes it.
/// The writer is the single direct writer of the slot-backed store; a second
/// session opened concurrently fences this one, which surfaces the fencing as
/// an error from `body` rather than a corrupt store.
pub(crate) async fn with_folder<T, F>(store: &SlotStore, body: F) -> Result<T>
where
    F: AsyncFnOnce(&Db) -> Result<T>,
{
    let db = StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_writer()
        .await?;

    let outcome = body(&db).await;

    // A close failure surfaces only when the body itself succeeded; a body
    // error is the primary cause and keeps precedence.
    match db.close().await {
        Ok(()) => outcome,
        Err(err) => outcome.and(Err(Error::from(err))),
    }
}
