//! A read handle over either a read-write transaction or a read-only reader.
//!
//! SlateDB's `DbReader` (read-only, manifest-following) exposes the same
//! `get`/`scan_prefix` surface as a `DbTransaction` but has no `begin`, so
//! every typed read in `store` takes a [`ReadHandle`] and dispatches. A
//! read-only catalog holds a `DbReader` and never opens a `Db`, so it never
//! fences a live writer (single-writer/many-reader topology).

use std::sync::Arc;

use bytes::Bytes;
use slatedb::{
    ByteRangeBounds, DbIterator, DbReader, DbTransaction, IterationOrder, config::ScanOptions,
};

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

/// The shape of a scan, which decides its block-cache admission. Every
/// scanning read path names one; none inherits a default it never chose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanShape {
    /// A whole-subspace walk (materialization, census, reclamation).
    /// Its reuse is absorbed by the row-level caches, so its blocks are
    /// not admitted — caching them would only evict the probe working set.
    Bulk,
    /// A targeted lookup (index probes, changelog replays). Its reuse is
    /// real and block-grained, so its blocks are admitted.
    Probe,
}

/// The direction SlateDB traverses a scan range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanOrder {
    /// Lowest encoded key first.
    Ascending,
    /// Highest encoded key first.
    Descending,
}

impl ScanOrder {
    /// Selects the direction requested by an index read.
    pub(crate) fn from_reverse(reverse: bool) -> Self {
        if reverse {
            Self::Descending
        } else {
            Self::Ascending
        }
    }

    fn iteration_order(self) -> IterationOrder {
        match self {
            Self::Ascending => IterationOrder::Ascending,
            Self::Descending => IterationOrder::Descending,
        }
    }
}

/// Scan options for reading a whole subspace, as every materialization
/// does. Blocks are not admitted to the cache.
fn bulk_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: SCAN_READ_AHEAD_BYTES,
        max_fetch_tasks: SCAN_FETCH_TASKS,
        cache_blocks: false,
        ..ScanOptions::default()
    }
}

/// Scan options for a targeted lookup. Same read-ahead as a bulk scan —
/// the fetch stops at the range's end, so a small probe never over-reads —
/// but its blocks are admitted to the cache.
fn probe_scan_options() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: SCAN_READ_AHEAD_BYTES,
        max_fetch_tasks: SCAN_FETCH_TASKS,
        cache_blocks: true,
        ..ScanOptions::default()
    }
}

impl ScanShape {
    fn options(self, order: ScanOrder) -> ScanOptions {
        let mut options = match self {
            Self::Bulk => bulk_scan_options(),
            Self::Probe => probe_scan_options(),
        };
        options.order = order.iteration_order();
        options
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

    /// Whether one pass of several reads through this handle observes a
    /// single store state on its own. A transaction reads at its own start
    /// sequence, so it does; a reader follows the manifest and advances
    /// between calls, so it does not.
    pub(crate) fn is_isolated(&self) -> bool {
        matches!(self, Self::Tx(_))
    }

    /// Scan keys sharing `prefix`, restricted to `subrange`, with the
    /// admission behaviour `shape` names.
    ///
    /// Reads ahead and fetches concurrently either way: even a probe walks
    /// its matching range, and paying a round trip per block is never what
    /// is wanted.
    pub(crate) async fn scan_prefix<P, T>(
        &self,
        prefix: P,
        subrange: T,
        shape: ScanShape,
    ) -> Result<DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        self.scan_prefix_ordered(prefix, subrange, shape, ScanOrder::Ascending)
            .await
    }

    /// Scan keys sharing `prefix` in the requested key order.
    pub(crate) async fn scan_prefix_ordered<P, T>(
        &self,
        prefix: P,
        subrange: T,
        shape: ScanShape,
        order: ScanOrder,
    ) -> Result<DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: ByteRangeBounds + Send,
    {
        let options = shape.options(order);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A bulk scan reads ahead but admits nothing: its reuse is served by
    /// the row-level caches, so caching its blocks is pure pollution.
    #[test]
    fn bulk_scans_read_ahead_and_admit_nothing() {
        let options = ScanShape::Bulk.options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(!options.cache_blocks);
    }

    /// A probe admits its blocks — its reuse is real and block-grained —
    /// and keeps the same read-ahead, which stops at the range's end.
    #[test]
    fn probe_scans_admit_their_blocks() {
        let options = ScanShape::Probe.options(ScanOrder::Ascending);
        assert_eq!(options.read_ahead_bytes, SCAN_READ_AHEAD_BYTES);
        assert_eq!(options.max_fetch_tasks, SCAN_FETCH_TASKS);
        assert!(options.cache_blocks);
    }

    /// Reverse index reads ask SlateDB to iterate backwards instead of
    /// materializing an ascending result and reversing it afterward.
    #[test]
    fn descending_probes_request_descending_iteration() {
        let options = ScanShape::Probe.options(ScanOrder::Descending);
        assert!(matches!(options.order, IterationOrder::Descending));
    }
}
