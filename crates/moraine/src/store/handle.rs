//! A read handle over either a read-write transaction or a read-only reader.
//!
//! SlateDB's `DbReader` (read-only, manifest-following) exposes the same
//! `get`/`scan_prefix` surface as a `DbTransaction` but has no `begin`, so
//! every typed read in `store` takes a [`ReadHandle`] and dispatches. A
//! read-only catalog holds a `DbReader` and never opens a `Db`, so it never
//! fences a live writer (single-writer/many-reader topology).

use std::sync::Arc;

use bytes::Bytes;
use slatedb::{ByteRangeBounds, DbIterator, DbReader, DbTransaction};

/// A read over a read-write transaction or a read-only reader. Cheap to
/// copy — it holds a borrow, not a session.
#[derive(Clone, Copy)]
pub(crate) enum ReadHandle<'a> {
    /// A snapshot-isolated read-write transaction (`Db::begin`).
    Tx(&'a DbTransaction),
    /// A read-only reader following the manifest.
    Reader(&'a DbReader),
}

impl ReadHandle<'_> {
    /// Point read of one key.
    pub(crate) async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        match self {
            Self::Tx(tx) => tx.get(key).await,
            Self::Reader(reader) => reader.get(key).await,
        }
    }

    /// Scan keys sharing `prefix`, restricted to `subrange`.
    pub(crate) async fn scan_prefix<P, T>(
        &self,
        prefix: P,
        subrange: T,
    ) -> Result<DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        match self {
            Self::Tx(tx) => tx.scan_prefix(prefix, subrange).await,
            Self::Reader(reader) => reader.scan_prefix(prefix, subrange).await,
        }
    }
}

/// An owned read session backing one materialization: a snapshot-isolated
/// transaction (read-write catalog) or a shared reader (read-only). Borrow a
/// [`ReadHandle`] from it for the typed reads, then [`finish`](Self::finish)
/// to roll back the transaction (a reader has nothing to roll back).
pub(crate) enum ReadSession {
    /// A read-write transaction, rolled back on `finish`.
    Tx(DbTransaction),
    /// A read-only reader, shared with the catalog.
    Reader(Arc<DbReader>),
}

impl ReadSession {
    /// Borrows a read handle over this session.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Tx(tx) => ReadHandle::Tx(tx),
            Self::Reader(reader) => ReadHandle::Reader(reader),
        }
    }

    /// Releases the session, rolling back a read-write transaction.
    pub(crate) fn finish(self) {
        if let Self::Tx(tx) = self {
            tx.rollback();
        }
    }
}
