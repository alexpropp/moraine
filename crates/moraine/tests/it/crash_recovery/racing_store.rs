//! An object store that loses one compare-and-swap, the way a second
//! process creating the same store at the same moment makes it lose.
//!
//! Two initializers race to write the store's first manifest, and the
//! loser's conditional write is refused because the object it was creating
//! already exists. Staging that from outside is what lets a test assert the
//! *shape* of the loser's error without racing real threads for it.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// Wraps an in-memory store, refusing the first write to the manifest.
#[derive(Debug)]
pub struct RacingStore {
    inner: Arc<InMemory>,
    /// Cleared once the refusal has been served, so only the first write
    /// loses and everything after it behaves normally.
    pending: AtomicBool,
}

impl RacingStore {
    /// A store whose first manifest write loses its compare-and-swap.
    pub fn losing_the_first_manifest_write(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            pending: AtomicBool::new(true),
        }
    }

    /// Whether this write is the manifest write that must lose.
    fn loses(&self, location: &Path) -> bool {
        location.to_string().contains("manifest")
            && self
                .pending
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

impl std::fmt::Display for RacingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RacingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RacingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.loses(location) {
            // Exactly what a conditional write finds when the object it
            // meant to create arrived first.
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: Box::new(std::io::Error::other(
                    "another process created this object first",
                )),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
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

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
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
        self.inner.copy_opts(from, to, options).await
    }
}
