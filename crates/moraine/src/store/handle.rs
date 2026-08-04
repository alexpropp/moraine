//! A read handle over either a read-write transaction or a read-only reader.
//!
//! SlateDB's `DbReader` (read-only, manifest-following) exposes the same
//! `get`/`scan_prefix` surface as a `DbTransaction` but has no `begin`, so
//! every typed read in `store` takes a [`ReadHandle`] and dispatches. A
//! read-only catalog holds a `DbReader` and never opens a `Db`, so it never
//! fences a live writer (single-writer/many-reader topology).

use std::sync::Arc;

use bytes::Bytes;
use slatedb::{ByteRangeBounds, DbIterator, DbReader, DbTransaction, config::ScanOptions};

/// Read-ahead for a scan, in bytes, rounded up to a block by SlateDB.
///
/// The default is one byte — one block — fetched with no concurrency, so a
/// scan of a whole subspace costs one object-store round trip per block.
/// That is invisible on local storage and ruinous on remote: a 12.8 MB
/// subspace measured 276 s against S3, ~46 KB/s, which is the round-trip
/// latency of ~3 200 sequential 4 KB fetches and nothing else.
const SCAN_READ_AHEAD_BYTES: usize = 4 * 1024 * 1024;

/// How many block fetches a scan may have in flight. Read-ahead alone
/// still serializes on latency; the concurrency is what converts a scan
/// from a round-trip count into a throughput number.
const SCAN_FETCH_TASKS: usize = 8;

/// Scan options for reading a whole subspace, as every materialization
/// does.
fn bulk_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: SCAN_READ_AHEAD_BYTES,
        max_fetch_tasks: SCAN_FETCH_TASKS,
        ..ScanOptions::default()
    }
}

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
    ///
    /// Reads ahead and fetches concurrently: every caller here walks a
    /// range rather than probing it, so paying a round trip per block is
    /// never what is wanted.
    pub(crate) async fn scan_prefix<P, T>(
        &self,
        prefix: P,
        subrange: T,
    ) -> Result<DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        let options = bulk_scan_options();
        match self {
            Self::Tx(tx) => {
                tx.scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
            Self::Reader(reader) => {
                reader
                    .scan_prefix_with_options(prefix, subrange, &options)
                    .await
            }
        }
    }
}

/// An owned read session backing one materialization: a shared reader
/// following the manifest. Borrow a [`ReadHandle`] from it for the typed
/// reads, then [`finish`](Self::finish) to release it.
pub(crate) enum ReadSession {
    /// A read-only reader, shared with the catalog.
    Reader(Arc<DbReader>),
}

impl ReadSession {
    /// Borrows a read handle over this session.
    pub(crate) fn handle(&self) -> ReadHandle<'_> {
        match self {
            Self::Reader(reader) => ReadHandle::Reader(reader),
        }
    }

    /// Releases the session.
    pub(crate) fn finish(self) {
        let Self::Reader(_) = self;
    }
}
