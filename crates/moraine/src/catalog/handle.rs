//! The catalog handle: the entry point a host opens, reads, and commits
//! through.

use std::sync::Arc;

use object_store::ObjectStore;
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::{
    catalog::{CatalogSnapshot, SnapshotId},
    error::{Error, Result},
    transaction::{Transaction, commit},
};

/// Options for opening a catalog.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CatalogOptions {
    /// Path prefix of the catalog within the bucket. Empty (the default)
    /// places the catalog at the bucket root; set it when several stores
    /// share a bucket.
    pub path: String,
}

/// A handle to a moraine catalog: cheap to clone, drives reads and
/// commits. The storage substrate never appears in this API — a catalog
/// lives in a bucket reachable through any [`ObjectStore`].
#[derive(Clone)]
pub struct Catalog {
    db: Arc<Db>,
}

impl std::fmt::Debug for Catalog {
    // `slatedb::Db` carries no `Debug` impl.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog").finish_non_exhaustive()
    }
}

impl Catalog {
    /// Opens (creating and initializing if empty) the catalog in
    /// `object_store` at `options.path`.
    ///
    /// Exactly one process may hold a read-write catalog per store —
    /// opening a second fences the first.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be opened, is mid-migration,
    /// or is stamped with a structural format this binary does not
    /// understand.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// // Bootstrap mints the default `main` schema.
    /// assert_eq!(catalog.snapshot().await?.schemas().len(), 1);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn open(object_store: Arc<dyn ObjectStore>, options: CatalogOptions) -> Result<Self> {
        let db = commit::open_initialized(&options.path, object_store).await?;
        Ok(Self { db: Arc::new(db) })
    }

    /// An immutable view of the catalog at the latest committed snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be read.
    pub async fn snapshot(&self) -> Result<CatalogSnapshot> {
        self.view(None).await
    }

    /// An immutable view of the catalog as of `snapshot` (time travel).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if `snapshot` is beyond the head, or
    /// another error if the store cannot be read.
    pub async fn snapshot_at(&self, snapshot: SnapshotId) -> Result<CatalogSnapshot> {
        self.view(Some(snapshot.get())).await
    }

    async fn view(&self, at: Option<u64>) -> Result<CatalogSnapshot> {
        let tx = self.begin_read_tx().await?;
        let view = commit::materialize(&tx, at).await;
        tx.rollback();

        view
    }

    /// Opens a fresh transaction at the current head, the same isolation
    /// [`snapshot`](Self::snapshot)/[`snapshot_at`](Self::snapshot_at) use.
    /// Used by [`crate::ffi_support`]'s raw current+history dumps and the
    /// staged-row commit path; every other caller goes through
    /// `snapshot`/`snapshot_at`/`commit`.
    pub(crate) async fn begin_read_tx(&self) -> Result<DbTransaction> {
        self.db
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(Error::from)
    }

    /// Closes the catalog, flushing background work.
    ///
    /// A [`Catalog`] is cheaply cloneable, and all clones share one
    /// underlying store handle: closing through any clone shuts that
    /// store down for every clone, so subsequent operations on any of
    /// them — this one included — fail.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails to close cleanly.
    pub async fn close(&self) -> Result<()> {
        self.db.close().await.map_err(Error::from)
    }

    /// Commits catalog mutations atomically, producing one new snapshot.
    ///
    /// The closure stages mutations on the [`Transaction`]; reads on the
    /// `Transaction` observe its own staged state. It may be re-run against
    /// fresh state after a lost race with a concurrent commit, so it must
    /// be pure: no I/O, no effects other than the `Transaction` calls. A
    /// closure that stages nothing commits nothing and returns the
    /// unchanged head snapshot id.
    ///
    /// # Errors
    ///
    /// Returns whatever error the closure returns (the commit is
    /// aborted), or an error from the underlying store. Returns
    /// [`Error::CommitConflict`] when a concurrent commit truly conflicts
    /// — it touched the same tables or the schema list — or when the
    /// bounded internal retry budget is exhausted before a benign race
    /// resolves.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, ColumnDef};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// # let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// let snapshot = catalog
    ///     .commit(|tx| {
    ///         let sales = tx.create_schema("sales")?;
    ///         tx.create_table(
    ///             sales,
    ///             "orders",
    ///             &[ColumnDef {
    ///                 name: "id".into(),
    ///                 column_type: "BIGINT".into(),
    ///                 nulls_allowed: false,
    ///                 default_value: None,
    ///             }],
    ///         )?;
    ///         Ok(())
    ///     })
    ///     .await?;
    /// // `main` plus the newly created `sales` schema.
    /// assert_eq!(catalog.snapshot_at(snapshot).await?.schemas().len(), 2);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn commit<F>(&self, f: F) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        commit::commit_cycle(&self.db, &f).await
    }
}
