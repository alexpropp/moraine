//! An object store that stops accepting writes partway through.
//!
//! This is how the suite stages "the process died": everything written
//! before the freeze is durable, nothing after it ever lands. Reads keep
//! working, so an operation caught by the freeze fails on the write it
//! could not persist rather than on garbage it read back.
//!
//! The allowance is a write count rather than a flag, so a test can sweep
//! it and stop the same operation at every durability boundary it has.

use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
};

use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// Wraps an in-memory store, permitting a bounded number of writes.
#[derive(Debug)]
pub struct FreezingStore {
    inner: Arc<InMemory>,
    /// Writes still permitted. Every write attempt decrements it; once it
    /// would go negative the store is frozen and stays frozen.
    allowance: AtomicI64,
    attempted: AtomicU64,
}

impl FreezingStore {
    /// A store that never freezes — used to build the pre-crash state, and
    /// to count how many writes an operation issues.
    pub fn thawed(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            allowance: AtomicI64::new(i64::MAX),
            attempted: AtomicU64::new(0),
        }
    }

    /// Freezes after `writes` more writes land. `0` freezes immediately.
    pub fn freeze_after(&self, writes: i64) {
        self.allowance.store(writes, Ordering::SeqCst);
    }

    /// How many writes have been attempted, permitted or not.
    pub fn writes_attempted(&self) -> u64 {
        self.attempted.load(Ordering::SeqCst)
    }

    /// Accounts for one write, refusing it once the allowance is spent.
    fn permit(&self, operation: &'static str) -> object_store::Result<()> {
        self.attempted.fetch_add(1, Ordering::SeqCst);
        if self.allowance.fetch_sub(1, Ordering::SeqCst) > 0 {
            return Ok(());
        }
        Err(object_store::Error::Generic {
            store: "FreezingStore",
            source: Box::new(std::io::Error::other(format!(
                "simulated process death: {operation} after the store froze"
            ))),
        })
    }
}

impl std::fmt::Display for FreezingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FreezingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FreezingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.permit("put")?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.permit("multipart put")?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    /// Deletion is one logical operation, so a frozen store spends one
    /// allowance on the stream rather than one per location.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        if let Err(err) = self.permit("delete") {
            return futures::stream::once(async move { Err(err) }).boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.permit("copy")?;
        self.inner.copy_opts(from, to, options).await
    }
}
