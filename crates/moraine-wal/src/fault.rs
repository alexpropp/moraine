//! A fault-injecting [`ObjectStore`] over a real [`InMemory`] store: the
//! three ways a conditional put's outcome can be unreadable. Test support.

use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// How a put fails, relative to the object landing.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PutFault {
    /// The object lands, then the response is lost.
    LostResponse,
    /// The put never reaches the store, so the slot stays absent.
    Unreachable,
    /// The store answers `AlreadyExists` with nothing written. Real S3
    /// returns 409 while a competing conditional create is in flight, and
    /// `object_store` maps every 409 to `AlreadyExists`.
    PrematureAlreadyExists,
}

/// Wraps a real [`InMemory`] store and fails `put_opts` at [`PutFault`] while
/// faults remain; every other operation forwards untouched.
#[derive(Debug)]
pub(crate) struct FaultyPut {
    inner: InMemory,
    fault: PutFault,
    remaining: AtomicU64,
}

impl FaultyPut {
    /// Faults every put until disarmed.
    pub(crate) fn armed(fault: PutFault) -> Self {
        Self::failing(fault, u64::MAX)
    }

    /// Forwards every put until armed.
    pub(crate) fn disarmed(fault: PutFault) -> Self {
        Self::failing(fault, 0)
    }

    /// Faults the next `puts` puts, then forwards.
    pub(crate) fn failing(fault: PutFault, puts: u64) -> Self {
        Self {
            inner: InMemory::new(),
            fault,
            remaining: AtomicU64::new(puts),
        }
    }

    pub(crate) fn arm(&self) {
        self.remaining.store(u64::MAX, Ordering::Relaxed);
    }

    pub(crate) fn disarm(&self) {
        self.remaining.store(0, Ordering::Relaxed);
    }

    /// Claims one fault, if any remain. `u64::MAX` saturates, so an armed
    /// store stays armed.
    fn claim_fault(&self) -> Option<PutFault> {
        let previous = self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                Some(remaining.saturating_sub(1))
            })
            .unwrap_or(0);

        (previous > 0).then_some(self.fault)
    }
}

impl std::fmt::Display for FaultyPut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyPut({})", self.inner)
    }
}

/// What a lost response looks like from the caller's side: no information
/// about whether the object exists. Its text carries every substring a retry
/// loop keys on, so the wrapping is exercised too.
pub(crate) fn unknown_outcome() -> object_store::Error {
    object_store::Error::Generic {
        store: "fault",
        source: "the put's outcome is unknown: conflict, concurrent, unique, primary key".into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultyPut {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        match self.claim_fault() {
            None => self.inner.put_opts(location, payload, opts).await,
            Some(PutFault::LostResponse) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(unknown_outcome())
            }
            Some(PutFault::Unreachable) => Err(unknown_outcome()),
            Some(PutFault::PrematureAlreadyExists) => Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "409 while a competing conditional create is in flight".into(),
            }),
        }
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
