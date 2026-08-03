//! The C ABI: `extern "C"` entry points the C++ shim calls into. Every
//! function here does the same four things, in order: null
//! checks, UTF-8 validation, a `catch_unwind`-guarded body that
//! `block_on`s into [`moraine`], and translation of the outcome into a
//! `(code, message)` pair (see [`crate::error`]).
//!
//! Two owned, opaque handle types cross the boundary as raw pointers:
//! [`MoraineCatalogHandle`] (one tokio runtime plus one open [`Catalog`]
//! per `ATTACH`) and [`MoraineSnapshotHandle`] (one materialized
//! [`CatalogSnapshot`] per `moraine_snapshot` call). Listing calls return
//! heap-allocated arrays of C descriptor structs; each has a paired
//! `_free` function that must be called exactly once.
//!
//! [`Catalog`]: moraine::Catalog
//! [`CatalogSnapshot`]: moraine::CatalogSnapshot

use std::{
    ffi::{CStr, CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::Arc,
};

use moraine::CatalogOptions;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use tracing::warn;

use crate::{
    error::{AbiError, INTERNAL_PANIC_MESSAGE, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe, MoraineSnapshotHandle, new_runtime},
};

/// Runs `body`, containing any panic and turning both panics and `Err`
/// results into a `(code, message)` pair written to `err`.
///
/// # Safety
///
/// `err`, if non-null, must point to a valid, writable [`MoraineError`]
/// for the duration of this call.
pub(crate) unsafe fn guard<T>(
    err: *mut MoraineError,
    body: impl FnOnce() -> Result<T, AbiError>,
) -> Result<T, i32> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(abi_err)) => {
            let code = abi_err.code;
            // SAFETY: `err` forwarded unchanged under this function's contract.
            unsafe {
                abi_err.write_into(err);
            }
            Err(code)
        }
        Err(_panic) => {
            // SAFETY: same as above.
            unsafe {
                AbiError::new(codes::INTERNAL, INTERNAL_PANIC_MESSAGE).write_into(err);
            }
            Err(codes::INTERNAL)
        }
    }
}

/// Converts a Rust string to an owned [`CString`].
///
/// An embedded NUL byte is reported as [`codes::CORRUPTION`] rather than
/// panicking.
pub(crate) fn to_c_string(s: &str) -> Result<CString, AbiError> {
    CString::new(s).map_err(|_| {
        AbiError::new(
            codes::CORRUPTION,
            format!("catalog string contains an embedded NUL byte: {s:?}"),
        )
    })
}

/// Frees a C string previously minted via `CString::into_raw`, if
/// non-null.
///
/// # Safety
///
/// `ptr`, if non-null, must be a pointer previously returned by
/// `CString::into_raw` and not yet freed.
pub(crate) unsafe fn free_c_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: caller contract above.
    drop(unsafe { CString::from_raw(ptr) });
}

/// Hands a `Vec<T>` to C as a heap array: writes the (pointer, length)
/// pair through `out_items`/`out_len`.
///
/// # Safety
///
/// `out_items` and `out_len` must be valid, writable pointers for the
/// duration of this call.
pub(crate) unsafe fn write_array<T>(items: Vec<T>, out_items: *mut *mut T, out_len: *mut usize) {
    let boxed = items.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed).cast::<T>();
    // SAFETY: caller contract above.
    unsafe {
        *out_len = len;
        *out_items = ptr;
    }
}

/// Reclaims an array written by [`write_array`], running `drop_elem` on
/// every element first (to release any owned C strings inside) before
/// dropping the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`write_array`] call, not yet freed.
pub(crate) unsafe fn free_array<T>(items: *mut T, len: usize, mut drop_elem: impl FnMut(&mut T)) {
    if items.is_null() {
        return;
    }
    // SAFETY: caller contract above.
    let slice = unsafe { std::slice::from_raw_parts_mut(items, len) };
    for elem in &mut *slice {
        drop_elem(elem);
    }
    let raw_slice = ptr::slice_from_raw_parts_mut(items, len);
    // SAFETY: reconstructs the exact `Box<[T]>` `write_array` produced.
    drop(unsafe { Box::from_raw(raw_slice) });
}

/// The shared shell of a **snapshot** list export: null-check the outputs,
/// borrow the snapshot, run `produce` under the panic/error guard, and write
/// the array. `produce` returns owned `Row`s — any raw pointers built only
/// after every conversion succeeds, so a partial failure leaks nothing.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null, must
/// be writable.
unsafe fn snapshot_list<Row>(
    snapshot: *mut MoraineSnapshotHandle,
    out_items: *mut *mut Row,
    out_len: *mut usize,
    err: *mut MoraineError,
    produce: impl FnOnce(&moraine::CatalogSnapshot) -> Result<Vec<Row>, AbiError>,
) -> i32 {
    let attempt = || -> Result<Vec<Row>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        produce(unsafe { &(*snapshot).snapshot })
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// The shared shell of a **handle** list export: null-check the handle and
/// outputs, borrow the handle, run `produce` (which drives its own
/// `block_on_cancellable`) under the guard, and write the array.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null, must
/// be writable.
unsafe fn handle_list<Row>(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut Row,
    out_len: *mut usize,
    err: *mut MoraineError,
    produce: impl FnOnce(&MoraineCatalogHandle) -> Result<Vec<Row>, AbiError>,
) -> i32 {
    let attempt = || -> Result<Vec<Row>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        produce(unsafe { &*handle })
    };

    // SAFETY: `err` validity is the caller's contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Mirrors the C `MoraineS3Config`: S3 credentials for an `s3://` store,
/// sourced from a DuckDB secret. Null/empty fields fall back to the AWS_*
/// environment; `use_ssl` is -1 unset, 0 false, 1 true.
#[repr(C)]
pub struct MoraineS3Config {
    /// AWS access key id.
    pub key_id: *const c_char,
    /// AWS secret access key.
    pub secret: *const c_char,
    /// AWS region.
    pub region: *const c_char,
    /// AWS session token, for temporary credentials.
    pub session_token: *const c_char,
    /// Endpoint URL for S3-compatible stores (e.g. MinIO).
    pub endpoint: *const c_char,
    /// Addressing style: `"path"` or `"vhost"`.
    pub url_style: *const c_char,
    /// TLS toggle: -1 unset, 0 plain HTTP, 1 HTTPS.
    pub use_ssl: i32,
}

/// S3 credentials borrowed from a [`MoraineS3Config`]. Every field is
/// optional; an absent field defers to the AWS_* environment.
struct S3Creds<'a> {
    key_id: Option<&'a str>,
    secret: Option<&'a str>,
    region: Option<&'a str>,
    session_token: Option<&'a str>,
    endpoint: Option<&'a str>,
    url_style: Option<&'a str>,
    use_ssl: Option<bool>,
}

/// Borrows a nullable C string as `Some(&str)`, mapping null, empty, and
/// non-UTF-8 to `None` — for S3 secret fields, where a missing or
/// malformed value defers to the environment rather than failing the
/// attach. Paths use [`opt_borrow_str`], which errors on bad UTF-8.
///
/// # Safety
///
/// `ptr`, if non-null, must point to a NUL-terminated C string valid for
/// reads for the duration of this call.
unsafe fn opt_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: caller contract; non-null checked above.
    let s = unsafe { CStr::from_ptr(ptr) }.to_str().ok()?;
    (!s.is_empty()).then_some(s)
}

/// Borrows a nullable C string as `Some(&str)`: null and empty mean "not
/// given", but invalid UTF-8 fails the call — for path fields, where
/// silently ignoring a malformed value would degrade into a confusing
/// later failure.
///
/// # Safety
///
/// `ptr`, if non-null, must point to a NUL-terminated C string valid for
/// reads for the duration of this call.
unsafe fn opt_borrow_str<'a>(
    ptr: *const c_char,
    arg_name: &str,
) -> Result<Option<&'a str>, AbiError> {
    if ptr.is_null() {
        return Ok(None);
    }
    // SAFETY: caller contract above.
    let s = unsafe { borrow_str(ptr, arg_name) }?;
    Ok((!s.is_empty()).then_some(s))
}

/// The object store an attach path resolves to.
enum StoreKind {
    /// A directory on the local filesystem, created if absent.
    LocalFile,
    /// A fresh, empty in-memory store.
    Memory,
    /// An S3 (or S3-compatible) bucket.
    S3 { bucket: String },
}

impl StoreKind {
    /// Classifies an attach path by scheme, returning the store kind and the
    /// bucket-relative key prefix (empty for local and in-memory stores).
    fn from_path(path: &str) -> Result<(Self, String), AbiError> {
        if let Some(rest) = path.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(AbiError::invalid_argument(
                    "moraine_attach: s3:// URL is missing a bucket",
                ));
            }
            return Ok((
                Self::S3 {
                    bucket: bucket.to_string(),
                },
                prefix.to_string(),
            ));
        }
        for scheme in [
            "gs://", "gcs://", "azure://", "az://", "http://", "https://",
        ] {
            if path.starts_with(scheme) {
                return Err(AbiError::invalid_argument(format!(
                    "moraine_attach: unsupported store scheme in `{path}` \
                     (supported: a local path, `memory://`, or `s3://`)"
                )));
            }
        }
        if path == "memory://" || path == "memory:" {
            return Ok((Self::Memory, String::new()));
        }
        Ok((Self::LocalFile, String::new()))
    }

    fn open(&self, path: &str, s3: Option<&S3Creds>) -> Result<Arc<dyn ObjectStore>, AbiError> {
        match self {
            Self::LocalFile => {
                std::fs::create_dir_all(path).map_err(|e| {
                    AbiError::invalid_argument(format!(
                        "moraine_attach: cannot create directory `{path}`: {e}"
                    ))
                })?;
                let fs = LocalFileSystem::new_with_prefix(path).map_err(|e| {
                    AbiError::invalid_argument(format!(
                        "moraine_attach: cannot open `{path}` as a store root: {e}"
                    ))
                })?;
                Ok(Arc::new(fs))
            }
            Self::Memory => Ok(Arc::new(InMemory::new())),
            Self::S3 { bucket } => {
                // With a secret, build from ONLY the secret's values so no
                // ambient AWS environment (endpoint/profile/session token/region
                // from `~/.aws`, an IMDS provider, …) can leak into the store.
                // Without a secret, fall back to the environment credential chain.
                let base = if s3.is_some() {
                    AmazonS3Builder::new()
                } else {
                    AmazonS3Builder::from_env()
                };
                let mut builder = base.with_bucket_name(bucket);
                if let Some(c) = s3 {
                    if let Some(v) = c.key_id {
                        builder = builder.with_access_key_id(v);
                    }
                    if let Some(v) = c.secret {
                        builder = builder.with_secret_access_key(v);
                    }
                    if let Some(v) = c.region {
                        builder = builder.with_region(v);
                    }
                    if let Some(v) = c.session_token {
                        builder = builder.with_token(v);
                    }
                    // DuckDB's S3 secret defaults `endpoint` to the
                    // region-less AWS host (`s3.amazonaws.com`) even when the
                    // user set none. Forwarding that to object_store overrides
                    // its region-derived endpoint and misroutes every request.
                    // Only apply a genuinely custom (non-AWS) endpoint; for AWS,
                    // let object_store derive the endpoint from the region.
                    if let Some(v) = c.endpoint {
                        if !v.is_empty() && !v.contains("amazonaws.com") {
                            builder = builder.with_endpoint(v);
                        }
                    }
                    if c.url_style == Some("path") {
                        builder = builder.with_virtual_hosted_style_request(false);
                    }
                    if c.use_ssl == Some(false) {
                        builder = builder.with_allow_http(true);
                    }
                }
                let store = builder.build().map_err(|e| {
                    AbiError::invalid_argument(format!(
                        "moraine_attach: cannot open s3 bucket `{bucket}`: {e} \
                         (check the s3 secret or the AWS_* environment)"
                    ))
                })?;
                Ok(Arc::new(store))
            }
        }
    }
}

/// Borrows a raw pointer argument as a `&str`, checking it for null and
/// UTF-8 validity.
///
/// # Safety
///
/// `ptr`, if non-null, must point to a NUL-terminated C string valid for
/// reads for the duration of this call.
pub(crate) unsafe fn borrow_str<'a>(
    ptr: *const c_char,
    arg_name: &str,
) -> Result<&'a str, AbiError> {
    if ptr.is_null() {
        return Err(AbiError::invalid_argument(format!("`{arg_name}` is null")));
    }
    // SAFETY: caller contract above.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| AbiError::invalid_argument(format!("`{arg_name}` is not valid UTF-8")))
}

/// Borrows a raw byte-buffer argument as a `&[u8]`. A null `ptr` is valid
/// only when `len` is `0`.
///
/// # Safety
///
/// `ptr`, if non-null, must point to `len` valid, readable bytes for the
/// duration of this call.
pub(crate) unsafe fn borrow_bytes<'a>(
    ptr: *const u8,
    len: usize,
    arg_name: &str,
) -> Result<&'a [u8], AbiError> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(AbiError::invalid_argument(format!(
            "`{arg_name}` is null but its length is nonzero"
        )));
    }
    // SAFETY: caller contract above.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Refuses an attach whose catalog store and data root sit on the same
/// object store with one containing the other.
///
/// DuckLake's orphan cleanup lists the data root and deletes every object
/// the catalog does not reference; it cannot know that some of those
/// objects *are* the catalog. Nesting the two would let one cleanup call
/// delete the store's SSTs, manifests, and WAL.
///
/// Containment is compared by path component, so sibling prefixes that
/// merely share a textual prefix (`…/lake` and `…/lakehouse`) are
/// unaffected. Symlinks and `..` in local paths are not resolved — the
/// comparison is lexical.
fn refuse_overlapping_data_path(store_path: &str, data_path: &str) -> Result<(), AbiError> {
    let (store_kind, store_prefix) = StoreKind::from_path(store_path)?;
    let (data_kind, data_prefix) = StoreKind::from_path(data_path)?;

    let overlaps = |a: &str, b: &str| {
        let (a, b) = (std::path::Path::new(a), std::path::Path::new(b));
        a.starts_with(b) || b.starts_with(a)
    };

    let nested = match (&store_kind, &data_kind) {
        // Same bucket: compare the bucket-relative key prefixes. An empty
        // prefix is the bucket root, which contains everything in it.
        (StoreKind::S3 { bucket: store }, StoreKind::S3 { bucket: data }) => {
            store == data && overlaps(&store_prefix, &data_prefix)
        }
        // The whole path is the location for a local store.
        (StoreKind::LocalFile, StoreKind::LocalFile) => overlaps(store_path, data_path),
        // A `memory://` attach opens a fresh, empty store that shares
        // objects with nothing; differing kinds and buckets are separate
        // stores either way.
        _ => false,
    };

    if nested {
        return Err(AbiError::new(
            codes::CONSTRAINT,
            format!(
                "the catalog store `{store_path}` and DATA_PATH `{data_path}` are nested on the \
                 same object store; DuckLake's orphaned-file cleanup lists DATA_PATH and would \
                 delete the catalog's own objects. Put them in sibling locations."
            ),
        ));
    }
    Ok(())
}

/// Resolves the `DATA_PATH` object store a catalog maintains equality
/// indexes against, and its bucket-relative key prefix.
///
/// A lake's data root is fixed once recorded: the recorded value is
/// authoritative, so a re-attach need not repeat it, and one that supplies a
/// differing `data_path_arg` is refused. A lake with none recorded yet
/// (freshly bootstrapped without one, or predating the option) adopts the
/// given value — recording it, unless read-only, so it is served and enforced
/// from then on. `None`/`None` yields no store.
fn resolve_data_store(
    runtime: &tokio::runtime::Runtime,
    catalog: &moraine::Catalog,
    store_path: &str,
    data_path_arg: Option<String>,
    read_only: bool,
    s3_creds: Option<&S3Creds>,
) -> Result<(Option<Arc<dyn ObjectStore>>, String), AbiError> {
    let recorded = runtime
        .block_on(catalog.snapshot())
        .map_err(AbiError::from)?
        .data_path();
    // Whether this attach is the one adopting the value, recorded only
    // after the overlap check below — a refused attach must not leave the
    // dangerous path behind for the next one to inherit.
    let mut adopting = false;
    let data_root = match (data_path_arg, recorded) {
        (Some(given), Some(recorded)) => {
            if given.trim_end_matches('/') != recorded.trim_end_matches('/') {
                return Err(AbiError::invalid_argument(format!(
                    "META_DATA_PATH `{given}` does not match the data path recorded for this \
                     lake (`{recorded}`); a lake's data path is fixed when it is created"
                )));
            }
            Some(recorded)
        }
        (Some(given), None) => {
            adopting = !read_only;
            Some(given)
        }
        (None, recorded) => recorded,
    };

    if let Some(root) = data_root.as_deref() {
        refuse_overlapping_data_path(store_path, root)?;
    }

    if adopting {
        let to_record = data_root.clone().unwrap_or_default();
        runtime
            .block_on(catalog.commit(move |tx| {
                tx.set_option(moraine::OptionScope::Global, "data_path", &to_record)?;
                Ok(())
            }))
            .map_err(AbiError::from)?;
    }

    match data_root {
        Some(path) => {
            let (kind, prefix) = StoreKind::from_path(&path)?;
            Ok((Some(kind.open(&path, s3_creds)?), prefix))
        }
        None => Ok((None, String::new())),
    }
}

/// Attaches a moraine catalog: creates the runtime this handle owns for
/// its lifetime, opens (creating and initializing if empty) the catalog,
/// and writes the resulting handle to `*out`.
///
/// `path`'s scheme selects the store: a local filesystem directory
/// (created if absent) by default, `memory://` for an in-memory store, or
/// `s3://<bucket>[/<prefix>]` for S3. For an `s3://` path, `s3` supplies
/// credentials (any field unset falls back to the AWS_* environment); it
/// may be null to use the environment alone and is ignored otherwise.
///
/// `encrypted` requests DuckLake data-file encryption. Creation-time
/// only: it is recorded when a fresh store bootstraps and ignored on an
/// already-initialized store, whose stored flag
/// ([`moraine_catalog_encrypted`]) is authoritative.
///
/// Returns [`codes::OK`] on success. On failure, `*out` is left
/// unwritten and, if `err` is non-null, `*err` carries the code and a
/// message.
///
/// # Safety
///
/// `path` must be a valid NUL-terminated C string. `s3`, if non-null,
/// must point to a valid [`MoraineS3Config`] whose non-null fields are
/// valid NUL-terminated C strings. `out` must be a valid, writable
/// `*mut *mut MoraineCatalogHandle`. `err`, if non-null, must be a valid,
/// writable [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_attach(
    path: *const c_char,
    s3: *const MoraineS3Config,
    read_only: bool,
    encrypted: bool,
    flush_interval_ms: u64,
    cache_dir: *const c_char,
    data_path: *const c_char,
    out: *mut *mut MoraineCatalogHandle,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineCatalogHandle>, AbiError> {
        // Before anything that could emit an event, so an attach failure is
        // itself drainable.
        crate::logging::install();
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: `path` validity is this function's own safety contract.
        let path_str = unsafe { borrow_str(path, "path") }?;
        // SAFETY: `cache_dir` validity is this function's own safety contract;
        // null (or empty) means "no on-disk cache".
        let cache_dir = unsafe { opt_borrow_str(cache_dir, "cache_dir") }?;

        let (store_kind, prefix) = StoreKind::from_path(path_str)?;

        // SAFETY: `s3` validity is this function's own safety contract; null
        // means "no secret — the environment supplies credentials".
        let s3_config = unsafe { s3.as_ref() };
        let s3_creds = s3_config.map(|c| {
            // SAFETY: each string field of `*c` is null or a NUL-terminated C
            // string valid for this call (the shim keeps them alive across it).
            unsafe {
                S3Creds {
                    key_id: opt_str(c.key_id),
                    secret: opt_str(c.secret),
                    region: opt_str(c.region),
                    session_token: opt_str(c.session_token),
                    endpoint: opt_str(c.endpoint),
                    url_style: opt_str(c.url_style),
                    use_ssl: match c.use_ssl {
                        0 => Some(false),
                        1 => Some(true),
                        _ => None,
                    },
                }
            }
        });

        // Open the store first: it is synchronous and fallible, and a bad
        // path must not cost a runtime spun up just to be torn down.
        let object_store = store_kind.open(path_str, s3_creds.as_ref())?;
        // Allocated before the runtime so its worker threads are tagged
        // from their first instant; the guard attributes the open's own
        // events (run below on this thread) the same way.
        let log_id = crate::logging::allocate_handle_id();
        let _log_guard = crate::logging::enter_handle(log_id);
        let runtime = new_runtime(log_id).map_err(|e| {
            AbiError::new(
                codes::INTERNAL,
                format!("failed to start tokio runtime: {e}"),
            )
        })?;

        // The DATA_PATH given at this attach (via `META_DATA_PATH`), if any.
        // SAFETY: `data_path` validity is this function's own safety contract;
        // null or empty means none was given.
        let data_path_arg = unsafe { opt_borrow_str(data_path, "data_path") }?.map(str::to_owned);

        // Check the given path *before* opening: bootstrapping a fresh
        // store records `data_path`, so a check that waited until after
        // the open would leave the dangerous value behind for the next
        // attach to inherit. The recorded-value case is checked again in
        // `resolve_data_store`, for lakes stamped before this guard.
        if let Some(given) = data_path_arg.as_deref() {
            refuse_overlapping_data_path(path_str, given)?;
        }

        // `CatalogOptions` is `#[non_exhaustive]`, so it is built through
        // `default()` and field assignment rather than a struct literal.
        let mut options = CatalogOptions::default();
        options.path = prefix;
        options.encrypted = encrypted;
        // `flush_interval_ms` is a deprecated alias for the commit batch
        // window: no commit touches SlateDB's WAL flush timer any more, but an
        // operator's existing value still expresses "how long a commit may wait
        // to be batched". 0 means "not given" (the default window); `u64::MAX`
        // is the shim's sentinel for an explicit zero; any other value is that
        // many milliseconds.
        match flush_interval_ms {
            0 => {}
            u64::MAX => options.commit_batch_window = std::time::Duration::ZERO,
            ms => options.commit_batch_window = std::time::Duration::from_millis(ms),
        }
        options.cache_dir = cache_dir.map(std::path::PathBuf::from);
        // Persist the data root at bootstrap so a later attach reads it back
        // without being told it again.
        options.data_path.clone_from(&data_path_arg);
        let catalog = if read_only {
            // A read-only attach never bootstraps; on a fresh store the open
            // fails, so surface the reason (DuckDB defaults remote attaches to
            // read-only) and the fix (add READ_WRITE).
            runtime
                .block_on(moraine::Catalog::open_read_only(object_store, options))
                .map_err(|e| AbiError::from(e).with_read_only_attach_hint())?
        } else {
            runtime
                .block_on(moraine::Catalog::open(object_store, options))
                .map_err(AbiError::from)?
        };

        // Resolve the DATA_PATH object store index maintenance and backfill
        // scoped-read against. Reuse the catalog store's S3 secret; DuckLake
        // uses one for both.
        let resolved = resolve_data_store(
            &runtime,
            &catalog,
            path_str,
            data_path_arg,
            read_only,
            s3_creds.as_ref(),
        );
        let (data_store, data_prefix) = match resolved {
            Ok(parts) => parts,
            Err(error) => {
                // The catalog is already open (and may have committed the
                // adopted data_path); flush and release it before failing
                // the attach instead of dropping it un-closed.
                let _ = runtime.block_on(catalog.close());
                return Err(error);
            }
        };

        let mut handle = MoraineCatalogHandle::new(runtime, catalog, log_id);
        handle.data_store = data_store;
        handle.data_prefix = data_prefix;
        Ok(Box::new(handle))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(handle) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out = Box::into_raw(handle);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Writes the lake's recorded data root — the stored global `data_path`
/// option, set when the store was created — to `*out` as an owned C string,
/// or null when none was recorded. Free a non-null result exactly once with
/// [`moraine_string_free`]. The shim serves this back as DuckLake's
/// `ducklake_metadata` `data_path` row, so a re-attach need not repeat it.
///
/// Cancellable via `probe`/`probe_ctx`, exactly as
/// [`moraine_snapshot`].
///
/// # Safety
///
/// `handle` must be a live handle from [`moraine_attach`]. `out` must be a
/// valid, writable `*mut *mut c_char`. `probe`/`probe_ctx` follow the ABI
/// cancellation contract. `err`, if non-null, must be writable. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_data_path(
    handle: *mut MoraineCatalogHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    out: *mut *mut c_char,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let path_ptr = match snapshot.data_path() {
            Some(path) => to_c_string(&path)?.into_raw(),
            None => ptr::null_mut(),
        };
        // SAFETY: `out` is non-null and writable per the caller contract.
        unsafe { *out = path_ptr };
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Frees a string previously written through [`moraine_data_path`]'s `out`.
/// A null pointer is ignored.
///
/// # Safety
///
/// `ptr` must be a value written by [`moraine_data_path`] and not yet
/// freed, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_string_free(ptr: *mut c_char) {
    // SAFETY: caller contract — a `moraine_data_path` string or null.
    unsafe { free_c_string(ptr) };
}

/// Whether the catalog encrypts its data files: the stored global
/// `encrypted` option, fixed when the store was created. A store created
/// before the flag existed reads as not encrypted.
///
/// Cancellable via `probe`/`probe_ctx`, exactly as
/// [`moraine_snapshot`].
///
/// # Safety
///
/// `handle` must be a live handle from [`moraine_attach`].
/// `out_encrypted` must be a valid, writable `*mut bool`. `probe`, if
/// non-null, must be safe to call with `probe_ctx` from any thread.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_catalog_encrypted(
    handle: *mut MoraineCatalogHandle,
    out_encrypted: *mut bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<bool, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_encrypted.is_null() {
            return Err(AbiError::invalid_argument("`out_encrypted` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;

        Ok(snapshot
            .option(moraine::OptionScope::Global, "encrypted")
            .as_deref()
            == Some("true"))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(encrypted) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { *out_encrypted = encrypted };
            codes::OK
        }
        Err(code) => code,
    }
}

/// Closes the catalog (flushing background work) and drops the runtime,
/// consuming `handle`.
///
/// Best-effort: a failure while closing the store is swallowed, since
/// this `void` entry point has no error channel. A null `handle` is a
/// no-op.
///
/// # Safety
///
/// `handle`, if non-null, must be a pointer previously returned by
/// [`moraine_attach`] and not yet passed to `moraine_detach`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_detach(handle: *mut MoraineCatalogHandle) {
    if handle.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above; dropped exactly once.
        let boxed = unsafe { Box::from_raw(handle) };
        if let Err(err) = boxed.block_on(boxed.catalog.close()) {
            // Detach has no error channel, so the failed close (a final
            // flush that did not land) is logged rather than lost. The
            // event surfaces through any remaining drain point or a host
            // subscriber.
            warn!(error = %err, "catalog close failed during detach");
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Materializes the catalog's current snapshot and writes the resulting
/// handle to `*out`.
///
/// Cancellable: races the core read against `probe` (polled
/// immediately, then ~100 ms; a null `probe` disables polling). If a
/// cancellation wins, returns [`codes::INTERRUPTED`] and `*out` is left
/// unwritten.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by [`moraine_attach`]
/// and not yet detached. `out` must be a valid, writable
/// `*mut *mut MoraineSnapshotHandle`. `probe`, if non-null, must be safe
/// to call with `probe_ctx` from any thread. `err`, if non-null, must be
/// a valid, writable [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot(
    handle: *mut MoraineCatalogHandle,
    out: *mut *mut MoraineSnapshotHandle,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineSnapshotHandle>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: `handle` validity is this function's own safety contract.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        Ok(Box::new(MoraineSnapshotHandle::new(snapshot)))
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(handle) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out = Box::into_raw(handle);
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Frees a snapshot handle previously returned by [`moraine_snapshot`].
/// A null `snapshot` is a no-op.
///
/// # Safety
///
/// `snapshot`, if non-null, must be a pointer previously returned by
/// [`moraine_snapshot`] and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_free(snapshot: *mut MoraineSnapshotHandle) {
    if snapshot.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above.
        drop(unsafe { Box::from_raw(snapshot) });
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Frees the message of an error previously populated by a `moraine_*`
/// call. A null `message` is a no-op.
///
/// # Safety
///
/// `message`, if non-null, must be the exact pointer a `moraine_*` call
/// wrote into [`MoraineError::message`], not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_error_free(message: *mut c_char) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe { free_c_string(message) };
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One schema, as returned by [`moraine_snapshot_schemas`].
#[repr(C)]
pub struct MoraineSchemaDesc {
    /// The schema's id.
    pub id: u64,
    /// The schema's name, owned — free via
    /// [`moraine_snapshot_schemas_free`].
    pub name: *mut c_char,
}

/// Lists the snapshot's live schemas into `*out_items`/`*out_len`.
///
/// # Safety
///
/// `snapshot` must be a pointer previously returned by
/// [`moraine_snapshot`]. `out_items`/`out_len` must be valid, writable
/// pointers. `err`, if non-null, must be a valid, writable
/// [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_schemas(
    snapshot: *mut MoraineSnapshotHandle,
    out_items: *mut *mut MoraineSchemaDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            // Owned-first: no raw pointers until every string converts, so a
            // partial failure leaks nothing.
            let owned: Vec<(u64, CString)> = snapshot
                .schemas()
                .into_iter()
                .map(|s| Ok((s.id.get(), to_c_string(&s.name)?)))
                .collect::<Result<_, AbiError>>()?;

            Ok(owned
                .into_iter()
                .map(|(id, name)| MoraineSchemaDesc {
                    id,
                    name: name.into_raw(),
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_schemas`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_schemas`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_schemas_free(items: *mut MoraineSchemaDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.name));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One table, as returned by [`moraine_snapshot_tables_in`].
#[repr(C)]
pub struct MoraineTableDesc {
    /// The table's id.
    pub id: u64,
    /// The schema the table belongs to.
    pub schema_id: u64,
    /// The table's name, owned — free via
    /// [`moraine_snapshot_tables_in_free`].
    pub name: *mut c_char,
}

/// Lists the live tables of schema `schema_id` into
/// `*out_items`/`*out_len`. A schema with no live tables (or an unknown
/// `schema_id`) yields an empty array, not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_tables_in(
    snapshot: *mut MoraineSnapshotHandle,
    schema_id: u64,
    out_items: *mut *mut MoraineTableDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            // Owned-first: no raw pointers until every string converts, so a
            // partial failure leaks nothing.
            let owned: Vec<(u64, u64, CString)> = snapshot
                .tables_in(moraine::SchemaId::new(schema_id))
                .into_iter()
                .map(|t| Ok((t.id.get(), t.schema_id.get(), to_c_string(&t.name)?)))
                .collect::<Result<_, AbiError>>()?;

            Ok(owned
                .into_iter()
                .map(|(id, schema_id, name)| MoraineTableDesc {
                    id,
                    schema_id,
                    name: name.into_raw(),
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_tables_in`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_tables_in`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_tables_in_free(items: *mut MoraineTableDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.name));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One column, as returned by [`moraine_snapshot_columns_of`].
#[repr(C)]
pub struct MoraineColumnDesc {
    /// The column's field id.
    pub id: u64,
    /// The column's name, owned — free via
    /// [`moraine_snapshot_columns_of_free`].
    pub name: *mut c_char,
    /// The column's DuckLake type string, owned — free via
    /// [`moraine_snapshot_columns_of_free`].
    pub sql_type: *mut c_char,
    /// Whether NULL values are allowed.
    pub nulls_allowed: bool,
    /// Whether this is a nested child column (a `STRUCT` field, `LIST`
    /// element, or `MAP` key/value); `parent_column` is meaningful iff set.
    pub has_parent_column: bool,
    /// The parent column's field id when `has_parent_column`.
    pub parent_column: u64,
}

/// Lists the live columns of table `table_id`, ordered by position, into
/// `*out_items`/`*out_len`. An unknown `table_id` yields an empty array,
/// not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_columns_of(
    snapshot: *mut MoraineSnapshotHandle,
    table_id: u64,
    out_items: *mut *mut MoraineColumnDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            // Owned-first: no raw pointers until every string converts, so a
            // partial failure leaks nothing.
            let owned: Vec<(u64, CString, CString, bool, Option<u64>)> = snapshot
                .columns_of(moraine::TableId::new(table_id))
                .into_iter()
                .map(|c| {
                    Ok((
                        c.id.get(),
                        to_c_string(&c.name)?,
                        to_c_string(&c.column_type)?,
                        c.nulls_allowed,
                        c.parent_column.map(moraine::ColumnId::get),
                    ))
                })
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(
                    |(id, name, sql_type, nulls_allowed, parent)| MoraineColumnDesc {
                        id,
                        name: name.into_raw(),
                        sql_type: sql_type.into_raw(),
                        nulls_allowed,
                        has_parent_column: parent.is_some(),
                        parent_column: parent.unwrap_or(0),
                    },
                )
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_columns_of`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_columns_of`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_columns_of_free(
    items: *mut MoraineColumnDesc,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.name);
                free_c_string(d.sql_type);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One view, as returned by [`moraine_snapshot_views_in`].
#[repr(C)]
pub struct MoraineViewDesc {
    /// The view's id.
    pub id: u64,
    /// The schema the view belongs to.
    pub schema_id: u64,
    /// The view's name, owned — free via
    /// [`moraine_snapshot_views_in_free`].
    pub name: *mut c_char,
    /// SQL dialect of the definition, owned — free via
    /// [`moraine_snapshot_views_in_free`].
    pub dialect: *mut c_char,
    /// The view's defining SQL, owned — free via
    /// [`moraine_snapshot_views_in_free`].
    pub sql: *mut c_char,
}

/// Lists the live views of schema `schema_id` into
/// `*out_items`/`*out_len`. A schema with no live views (or an unknown
/// `schema_id`) yields an empty array, not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_views_in(
    snapshot: *mut MoraineSnapshotHandle,
    schema_id: u64,
    out_items: *mut *mut MoraineViewDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            // Owned-first: no raw pointers until every string converts, so a
            // partial failure leaks nothing.
            let owned: Vec<(u64, u64, CString, CString, CString)> = snapshot
                .views_in(moraine::SchemaId::new(schema_id))
                .into_iter()
                .map(|v| {
                    Ok((
                        v.id.get(),
                        v.schema_id.get(),
                        to_c_string(&v.name)?,
                        to_c_string(&v.dialect)?,
                        to_c_string(&v.sql)?,
                    ))
                })
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(id, schema_id, name, dialect, sql)| MoraineViewDesc {
                    id,
                    schema_id,
                    name: name.into_raw(),
                    dialect: dialect.into_raw(),
                    sql: sql.into_raw(),
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_views_in`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_views_in`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_views_in_free(items: *mut MoraineViewDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.name);
                free_c_string(d.dialect);
                free_c_string(d.sql);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One live data file, as returned by [`moraine_snapshot_data_files_of`].
#[repr(C)]
pub struct MoraineDataFileDesc {
    /// The file's id.
    pub id: u64,
    /// Object-store path, owned — free via
    /// [`moraine_snapshot_data_files_of_free`].
    pub path: *mut c_char,
    /// Whether `path` is relative to the table's location.
    pub path_is_relative: bool,
    /// Number of rows in the file.
    pub record_count: u64,
    /// Whether `row_id_start` is present (absent when the file's rows
    /// carry explicit per-row ids, e.g. compaction outputs).
    pub has_row_id_start: bool,
    /// First row id of the file's dense per-table row-id range, valid
    /// iff `has_row_id_start`.
    pub row_id_start: u64,
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Footer size in bytes.
    pub footer_size: u64,
}

/// Lists the live data files of table `table_id` into
/// `*out_items`/`*out_len`. An unknown `table_id` yields an empty array,
/// not an error.
///
/// # Safety
///
/// Same pointer contract as [`moraine_snapshot_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_data_files_of(
    snapshot: *mut MoraineSnapshotHandle,
    table_id: u64,
    out_items: *mut *mut MoraineDataFileDesc,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    // SAFETY: caller contract for the pointers.
    unsafe {
        snapshot_list(snapshot, out_items, out_len, err, |snapshot| {
            // Owned-first: no raw pointers until every string converts, so a
            // partial failure leaks nothing.
            let owned: Vec<(CString, moraine::DataFileInfo)> = snapshot
                .data_files_of(moraine::TableId::new(table_id))
                .into_iter()
                .map(|f| Ok((to_c_string(&f.path)?, f)))
                .collect::<Result<_, AbiError>>()?;
            Ok(owned
                .into_iter()
                .map(|(path, f)| MoraineDataFileDesc {
                    id: f.id.get(),
                    path: path.into_raw(),
                    path_is_relative: f.path_is_relative,
                    record_count: f.record_count,
                    has_row_id_start: f.row_id_start.is_some(),
                    row_id_start: f.row_id_start.unwrap_or_default(),
                    file_size_bytes: f.file_size_bytes,
                    footer_size: f.footer_size,
                })
                .collect())
        })
    }
}

/// Frees an array returned by [`moraine_snapshot_data_files_of`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_snapshot_data_files_of`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_snapshot_data_files_of_free(
    items: *mut MoraineDataFileDesc,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.path));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One index, as returned by [`moraine_indexes`].
#[repr(C)]
pub struct MoraineIndexDesc {
    /// The index's id.
    pub index_id: u64,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Whether a staged build is still in progress.
    pub building: bool,
    /// The index name, owned — free via [`moraine_indexes_free`].
    pub name: *mut c_char,
}

fn resolve_table(
    snapshot: &moraine::CatalogSnapshot,
    schema: &str,
    table: &str,
) -> Result<moraine::TableId, AbiError> {
    let schema = snapshot
        .schema_by_name(schema)
        .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("schema {schema}"))))?;
    let table = snapshot
        .table_by_name(schema.id, table)
        .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("table {table}"))))?;
    Ok(table.id)
}

/// Borrows an inbound array of C strings.
///
/// # Safety
///
/// `names`/`count` must describe a valid array of `count` non-null,
/// NUL-terminated C strings, valid for the duration of the borrow.
unsafe fn borrow_str_array<'a>(
    names: *const *const c_char,
    count: usize,
    arg: &str,
) -> Result<Vec<&'a str>, AbiError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if names.is_null() {
        return Err(AbiError::invalid_argument(format!("`{arg}` is null")));
    }
    // SAFETY: caller contract that `names`/`count` describe a valid array.
    let slice = unsafe { std::slice::from_raw_parts(names, count) };
    slice
        .iter()
        // SAFETY: each element is a valid C string per the caller contract.
        .map(|&ptr| unsafe { borrow_str(ptr, arg) })
        .collect()
}

/// Builds the per-column [`moraine::ColumnOrder`]s from the ABI's parallel
/// direction / null-placement flag arrays — one `0`/`1` byte per column
/// (`bool` is avoided at the array boundary, where reinterpreting a C++
/// `uint8_t` buffer as `bool` is undefined). Each null pointer defaults its
/// axis (ascending / NULLS LAST); both null yields an empty vec.
///
/// # Safety
///
/// Each non-null pointer must point to `column_count` bytes.
unsafe fn column_orders(
    column_descending: *const u8,
    column_nulls_first: *const u8,
    column_count: usize,
) -> Vec<moraine::ColumnOrder> {
    if column_descending.is_null() && column_nulls_first.is_null() {
        return Vec::new();
    }
    let descending = (!column_descending.is_null()).then(|| {
        // SAFETY: caller contract — non-null points to `column_count` bytes.
        unsafe { std::slice::from_raw_parts(column_descending, column_count) }
    });
    let nulls_first = (!column_nulls_first.is_null()).then(|| {
        // SAFETY: caller contract — non-null points to `column_count` bytes.
        unsafe { std::slice::from_raw_parts(column_nulls_first, column_count) }
    });
    (0..column_count)
        .map(|i| moraine::ColumnOrder {
            direction: if descending.is_some_and(|flags| flags[i] != 0) {
                moraine::Direction::Descending
            } else {
                moraine::Direction::Ascending
            },
            nulls: if nulls_first.is_some_and(|flags| flags[i] != 0) {
                moraine::NullOrder::First
            } else {
                moraine::NullOrder::Last
            },
        })
        .collect()
}

/// Derives the whole backfill and creates the index in one commit — the
/// single-commit build, for a table small enough that its entries fit one
/// batch. `data_store` is the `DATA_PATH` store when the table holds files
/// to scoped-read, `None` when it holds only inline rows.
///
/// # Safety
///
/// `probe`/`probe_ctx` must satisfy the ABI's cancellation contract.
unsafe fn create_index_in_one_commit(
    handle: &MoraineCatalogHandle,
    table_id: moraine::TableId,
    def: &moraine::IndexDef,
    orders: &[moraine::ColumnOrder],
    data_store: Option<std::sync::Arc<dyn object_store::ObjectStore>>,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
) -> Result<(), AbiError> {
    let mut backfill = match data_store {
        Some(store) => {
            // SAFETY: caller contract for `probe`/`probe_ctx`.
            unsafe {
                handle.block_on_cancellable(
                    probe,
                    probe_ctx,
                    handle.catalog.scoped_backfill_entries(
                        store,
                        &handle.data_prefix,
                        table_id,
                        &def.columns,
                    ),
                )
            }?
        }
        None => Vec::new(),
    };
    // SAFETY: caller contract for `probe`/`probe_ctx`.
    let inline = unsafe {
        handle.block_on_cancellable(
            probe,
            probe_ctx,
            handle
                .catalog
                .inline_backfill_entries(table_id, &def.columns),
        )
    }?;
    backfill.extend(inline);

    // SAFETY: caller contract for `probe`/`probe_ctx`.
    unsafe {
        handle.block_on_cancellable(
            probe,
            probe_ctx,
            handle.catalog.commit(|tx| {
                if orders.is_empty() {
                    tx.create_index(table_id, def, &backfill)?;
                } else {
                    tx.create_index_ordered(table_id, def, orders, &backfill)?;
                }
                Ok(())
            }),
        )
    }?;
    Ok(())
}

/// Creates an equality index, committing autonomously. With `staged`, runs
/// the multi-commit build — required when the table's backfill exceeds what
/// one commit may stage — and returns once the index is ready; interrupting
/// it leaves the build resumable by the same call.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_create(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    column_names: *const *const c_char,
    column_count: usize,
    column_descending: *const u8,
    column_nulls_first: *const u8,
    unique: bool,
    staged: bool,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;
        // SAFETY: caller contract for the column-name array.
        let columns = unsafe { borrow_str_array(column_names, column_count, "column_names") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let live_columns = snapshot.columns_of(table_id);
        let mut column_ids = Vec::with_capacity(columns.len());
        for column in &columns {
            // Indexability (e.g. the 128-bit refusal) is enforced by
            // `create_index` itself now, so no per-caller check here.
            let found = live_columns
                .iter()
                .find(|c| c.name == *column)
                .ok_or_else(|| {
                    AbiError::from(moraine::Error::NotFound(format!("column {column}")))
                })?;
            column_ids.push(found.id);
        }

        // A table that already holds data must be backfilled from the
        // DATA_PATH store (resolved at attach from `META_DATA_PATH`) —
        // without it, refuse rather than under-cover. Inline rows come from
        // the catalog store, which is always reachable.
        let holds_files = !snapshot.data_files_of(table_id).is_empty();
        let data_store = handle_ref.data_store.clone();
        if holds_files && data_store.is_none() {
            return Err(AbiError::from(moraine::Error::Constraint(
                "the table already holds data; attach with META_DATA_PATH so its files can be \
                 scoped-read"
                    .to_owned(),
            )));
        }

        // SAFETY: each non-null orders pointer points to `column_count` bools,
        // per the caller contract.
        let orders = unsafe { column_orders(column_descending, column_nulls_first, column_count) };

        let def = moraine::IndexDef {
            name: name.to_owned(),
            columns: column_ids,
            unique,
        };

        // The staged build derives its own backfill, one bounded step at a
        // time; the single-commit path derives it all up front.
        if staged {
            // SAFETY: caller contract for `probe`/`probe_ctx`.
            unsafe {
                handle_ref.block_on_cancellable(
                    probe,
                    probe_ctx,
                    handle_ref.catalog.create_index_staged(
                        table_id,
                        &def,
                        &orders,
                        data_store,
                        &handle_ref.data_prefix,
                        None,
                    ),
                )
            }?;
            return Ok(());
        }

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe {
            create_index_in_one_commit(
                handle_ref,
                table_id,
                &def,
                &orders,
                data_store.filter(|_| holds_files),
                probe,
                probe_ctx,
            )
        }
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Drops an equality index by name, committing autonomously.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_drop(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let index = snapshot
            .index_by_name(table_id, name)
            .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("index {name}"))))?;
        let index_id = index.id;
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                handle_ref.catalog.commit(move |tx| tx.drop_index(index_id)),
            )
        }?;
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Runs one moraine-owned maintenance pass, reclaiming the entry ranges
/// of indexes no longer live, and writes what it reclaimed to
/// `*indexes_swept` and `*entries_reclaimed`.
///
/// The pass mints no snapshot and leaves head unchanged. `batch_size` of
/// 0 means "not given" and takes the core default; the pass commits at
/// most that many deletes per batch.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; the out-parameters,
/// if non-null, must be writable, and `err`, if non-null, must be
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_maintain(
    handle: *mut MoraineCatalogHandle,
    batch_size: u64,
    indexes_swept: *mut u64,
    entries_reclaimed: *mut u64,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };

        // `MaintenanceRequest` is `#[non_exhaustive]`, so it is built
        // through `default()` and field assignment.
        let mut request = moraine::MaintenanceRequest::default();
        if batch_size > 0 {
            // Refused rather than clamped: saturating to `usize::MAX`
            // would silently turn a bounded batch into an unbounded one,
            // and would do so only on targets where the value does not
            // fit — a behaviour difference between builds.
            request.batch_size = usize::try_from(batch_size).map_err(|_| {
                AbiError::invalid_argument(format!(
                    "batch_size {batch_size} does not fit this platform's pointer width"
                ))
            })?;
        }

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let report = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.maintain(request))
        }?;

        if !indexes_swept.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *indexes_swept = report.indexes_swept };
        }
        if !entries_reclaimed.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *entries_reclaimed = report.index_entries_reclaimed };
        }
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Runs one bounded fold pass: applies up to `limit` unfolded slots into
/// the store, advancing the durable fold cursor, and writes the count
/// applied to `*out_slots_folded` and the slots still unfolded to
/// `*out_tail_remaining`. `limit` of 0 folds nothing and only reports the
/// tail.
///
/// Folding is invisible to readers — the served state is byte-identical
/// before and after — so a pass may run whenever. A read-only attach is
/// refused with [`codes::CONSTRAINT`]; a concurrent folder fencing this
/// session surfaces as [`codes::FENCED`], which the caller treats as
/// wasted work rather than an error.
///
/// # Safety
///
/// `handle` must be a live handle from [`moraine_attach`]. The
/// out-parameters, if non-null, must be writable, and `err`, if non-null,
/// must be writable. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_fold_sprint(
    handle: *mut MoraineCatalogHandle,
    limit: u64,
    out_slots_folded: *mut u64,
    out_tail_remaining: *mut u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };

        // The scheduler drives this on its own thread, which stops through
        // the stop flag rather than a query interrupt, so no probe.
        // SAFETY: a `None` probe polls nothing.
        let report = unsafe {
            handle_ref.block_on_cancellable(
                None,
                ptr::null_mut(),
                handle_ref.catalog.fold_sprint(limit),
            )
        }?;

        if !out_slots_folded.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *out_slots_folded = report.slots_folded };
        }
        if !out_tail_remaining.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *out_tail_remaining = report.tail_remaining };
        }
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Deletes slots durably folded into the store, oldest first, and writes
/// the count removed to `*out_slots_removed`. The horizon is bounded by
/// both the durable fold cursor and what live readers still need, so a
/// pass may remove nothing when readers lag. A read-only attach is refused
/// with [`codes::CONSTRAINT`].
///
/// # Safety
///
/// `handle` must be a live handle from [`moraine_attach`].
/// `out_slots_removed`, if non-null, must be writable, and `err`, if
/// non-null, must be writable. All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_truncate_slots(
    handle: *mut MoraineCatalogHandle,
    out_slots_removed: *mut u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };

        // SAFETY: a `None` probe polls nothing.
        let removed = unsafe {
            handle_ref.block_on_cancellable(
                None,
                ptr::null_mut(),
                handle_ref.catalog.truncate_folded_slots(),
            )
        }?;

        if !out_slots_removed.is_null() {
            // SAFETY: caller contract — non-null means writable.
            unsafe { *out_slots_removed = removed };
        }
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Lists a table's live equality indexes.
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `err`, if non-null,
/// must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_indexes(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    out_items: *mut *mut MoraineIndexDesc,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineIndexDesc>, AbiError> {
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        // Owned-first: no raw pointers until every string converts.
        let owned: Vec<(u64, bool, bool, CString)> = snapshot
            .indexes_of(table_id)
            .into_iter()
            .map(|index| {
                Ok((
                    index.id.get(),
                    index.unique,
                    index.state != moraine::IndexState::Ready,
                    to_c_string(&index.name)?,
                ))
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|(index_id, unique, building, name)| MoraineIndexDesc {
                index_id,
                unique,
                building,
                name: name.into_raw(),
            })
            .collect())
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_indexes`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_indexes`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_indexes_free(items: *mut MoraineIndexDesc, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| free_c_string(d.name));
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One row an index lookup resolved, as returned by [`moraine_index_lookup`].
#[repr(C)]
pub struct MoraineRowLocation {
    /// The row id the entry points at.
    pub row_id: u64,
    /// The data file holding the row (valid when `is_inline` is false).
    pub data_file_id: u64,
    /// Whether the row is inlined (or not resolvable to a dense-range file).
    pub is_inline: bool,
}

/// A value passed to [`moraine_index_lookup`], tagged by kind. The shim
/// fills the field matching `kind`; the ABI coerces it to the indexed
/// column's canonical form.
#[repr(C)]
pub struct MoraineLookupValue {
    /// `0`=IS NULL (a prefix predicate for [`moraine_index_nulls`]), `1`=i64,
    /// `2`=u64, `3`=f64, `4`=bool, `5`=string, `6`=bytes.
    pub kind: i32,
    /// Valid iff `kind == 1`.
    pub i64_value: i64,
    /// Valid iff `kind == 2`.
    pub u64_value: u64,
    /// Valid iff `kind == 3`.
    pub f64_value: f64,
    /// Valid iff `kind == 4`.
    pub bool_value: bool,
    /// Valid iff `kind == 5`: a borrowed, NUL-terminated UTF-8 string.
    pub str_value: *const c_char,
    /// Valid iff `kind == 6`: a borrowed byte buffer of `bytes_len` bytes.
    pub bytes_value: *const u8,
    /// Length of `bytes_value` when `kind == 6`.
    pub bytes_len: usize,
}

/// Coerces a lookup value to the canonical [`IndexKeyValue`] for a column of
/// DuckLake type `ducklake_type`: marshals the tagged union into an owned
/// [`LookupInput`], then defers to the core's coercion table so the type
/// vocabulary cannot drift from index maintenance.
///
/// # Safety
///
/// If `raw.kind` is `5` (string) or `6` (bytes), its pointer fields must be
/// valid per the ABI contract for the duration of this call.
unsafe fn coerce_lookup_value(
    raw: &MoraineLookupValue,
    ducklake_type: &str,
) -> Result<moraine::IndexKeyValue, AbiError> {
    use moraine::ffi_support::index::{LookupInput, coerce_lookup_value};

    let input = match raw.kind {
        1 => LookupInput::Int(raw.i64_value),
        2 => LookupInput::UInt(raw.u64_value),
        3 => LookupInput::Float(raw.f64_value),
        4 => LookupInput::Bool(raw.bool_value),
        5 => {
            // SAFETY: caller contract — a `kind == 5` value's string pointer
            // is a valid NUL-terminated C string for this call.
            let text = unsafe { borrow_str(raw.str_value, "lookup value") }?;
            LookupInput::Str(text.to_owned())
        }
        6 => {
            // SAFETY: caller contract — a `kind == 6` value's byte pointer is
            // valid for `bytes_len` bytes for this call.
            let bytes = unsafe { borrow_bytes(raw.bytes_value, raw.bytes_len, "lookup value") }?;
            LookupInput::Bytes(bytes.to_vec())
        }
        other => {
            return Err(AbiError::invalid_argument(format!(
                "index lookup: unknown value kind {other}"
            )));
        }
    };
    coerce_lookup_value(&input, ducklake_type).map_err(AbiError::invalid_argument)
}

/// The refusal shared by every entry point that matches on values: a NULL
/// matches nothing, so it is never part of an equality key or a range bound.
fn no_null_in_key() -> AbiError {
    AbiError::invalid_argument(
        "NULL is not a value to match; use moraine_index_nulls for an IS NULL query",
    )
}

/// Coerces a run of ABI values against the index's leading columns, in the
/// index's column order. A `kind == 0` value is the `IS NULL` predicate and
/// yields `None`; callers that admit no NULL reject it before calling.
///
/// # Safety
///
/// Each value's string/bytes fields, where its kind uses them, must be valid
/// for this call.
unsafe fn coerce_index_key(
    index: &moraine::IndexInfo,
    index_name: &str,
    table_id: moraine::TableId,
    columns: &[moraine::ColumnInfo],
    raw: &[MoraineLookupValue],
) -> Result<Vec<Option<moraine::IndexKeyValue>>, AbiError> {
    let mut coerced = Vec::with_capacity(raw.len());
    for (position, value) in raw.iter().enumerate() {
        if value.kind == 0 {
            coerced.push(None);
            continue;
        }
        let column_id = index.columns.get(position).ok_or_else(|| {
            AbiError::invalid_argument(format!(
                "index key of {} values does not fit the {}-column index {index_name}",
                raw.len(),
                index.columns.len()
            ))
        })?;
        let column = columns.iter().find(|c| c.id == *column_id).ok_or_else(|| {
            AbiError::from(moraine::Error::Corruption(format!(
                "index {index_name} covers column {column_id} absent from table {table_id}"
            )))
        })?;
        // SAFETY: caller contract for the value's string/bytes fields.
        coerced.push(Some(unsafe {
            coerce_lookup_value(value, &column.column_type)
        }?));
    }

    Ok(coerced)
}

/// Resolves an equality lookup to the rows currently holding `values` — one
/// [`MoraineLookupValue`] per indexed column, in the index's column order,
/// each coerced to its column's type. The count must equal the index's
/// column count: a composite equality key names every column (a leading
/// prefix is not an equality lookup — use [`moraine_index_nulls`] or
/// [`moraine_index_range`] for that).
///
/// # Safety
///
/// Every pointer must be valid per the ABI contract; `values` points to
/// `values_len` values; `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_lookup(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    values: *const MoraineLookupValue,
    values_len: usize,
    out_items: *mut *mut MoraineRowLocation,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowLocation>, AbiError> {
        if values_len == 0 {
            return Err(AbiError::invalid_argument("index lookup: no value given"));
        }
        if values.is_null() {
            return Err(AbiError::invalid_argument("`values` is null"));
        }
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let index = snapshot
            .index_by_name(table_id, name)
            .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("index {name}"))))?;
        if values_len != index.columns.len() {
            return Err(AbiError::invalid_argument(format!(
                "index lookup: {values_len} values do not address the {}-column index {name}; an \
                 equality lookup names every column",
                index.columns.len()
            )));
        }
        let columns = snapshot.columns_of(table_id);
        // SAFETY: non-null checked; caller contract — `values` points to
        // `values_len` values whose string/bytes fields (if used) are valid.
        let raw_values = unsafe { std::slice::from_raw_parts(values, values_len) };
        // SAFETY: caller contract for each value's string/bytes fields.
        let key = unsafe { coerce_index_key(&index, name, table_id, &columns, raw_values) }?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(no_null_in_key)?;
        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let locations = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                handle_ref.catalog.index_lookup(table_id, index.id, &key),
            )
        }?;
        Ok(locations
            .into_iter()
            .map(|location| {
                let (data_file_id, is_inline) = match location.holder {
                    moraine::RowHolder::DataFile(id) => (id.get(), false),
                    moraine::RowHolder::Inline => (0, true),
                };
                MoraineRowLocation {
                    row_id: location.row_id,
                    data_file_id,
                    is_inline,
                }
            })
            .collect())
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_lookup`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_index_lookup`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_lookup_free(items: *mut MoraineRowLocation, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Resolves a comparison query to the rows whose leading indexed values fall
/// between the bounds. Each bound is a run of `lower_len`/`upper_len`
/// [`MoraineLookupValue`]s over the index's leading columns — equality on all
/// but the last named column, a comparison on the last; a null pointer or a
/// zero length is an open (unbounded) side. A present bound is `Included`
/// when its `*_inclusive` flag is set, `Excluded` otherwise. Results come
/// back in the index's stored order, or its opposite when `reverse` is set.
///
/// # Safety
///
/// Every non-null pointer must be valid per the ABI contract; `lower_values`
/// points to `lower_len` values and `upper_values` to `upper_len`; `err`, if
/// non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_range(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    lower_values: *const MoraineLookupValue,
    lower_len: usize,
    lower_inclusive: bool,
    upper_values: *const MoraineLookupValue,
    upper_len: usize,
    upper_inclusive: bool,
    reverse: bool,
    out_items: *mut *mut MoraineRowLocation,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    use std::ops::Bound;

    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowLocation>, AbiError> {
        let lower_empty = lower_values.is_null() || lower_len == 0;
        let upper_empty = upper_values.is_null() || upper_len == 0;
        if lower_empty && upper_empty {
            return Err(AbiError::invalid_argument(
                "index range: at least one bound must be present",
            ));
        }
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let index = snapshot
            .index_by_name(table_id, name)
            .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("index {name}"))))?;
        let columns = snapshot.columns_of(table_id);

        let build_bound = |values: *const MoraineLookupValue,
                           len: usize,
                           inclusive: bool|
         -> Result<Bound<Vec<moraine::IndexKeyValue>>, AbiError> {
            if values.is_null() || len == 0 {
                return Ok(Bound::Unbounded);
            }
            if len > index.columns.len() {
                return Err(AbiError::invalid_argument(format!(
                    "index range: a bound of {len} values does not fit the {}-column index {name}",
                    index.columns.len()
                )));
            }
            // SAFETY: non-null checked; caller contract — `values` points to
            // `len` values whose string/bytes fields (if used) are valid.
            let raw = unsafe { std::slice::from_raw_parts(values, len) };
            // SAFETY: caller contract for each value's string/bytes fields.
            let coerced = unsafe { coerce_index_key(&index, name, table_id, &columns, raw) }?
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(no_null_in_key)?;
            Ok(if inclusive {
                Bound::Included(coerced)
            } else {
                Bound::Excluded(coerced)
            })
        };
        let lower = build_bound(lower_values, lower_len, lower_inclusive)?;
        let upper = build_bound(upper_values, upper_len, upper_inclusive)?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let locations = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                handle_ref
                    .catalog
                    .index_range(table_id, index.id, lower, upper, reverse),
            )
        }?;
        Ok(locations
            .into_iter()
            .map(|location| {
                let (data_file_id, is_inline) = match location.holder {
                    moraine::RowHolder::DataFile(id) => (id.get(), false),
                    moraine::RowHolder::Inline => (0, true),
                };
                MoraineRowLocation {
                    row_id: location.row_id,
                    data_file_id,
                    is_inline,
                }
            })
            .collect())
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_range`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a matching
/// [`moraine_index_range`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_range_free(items: *mut MoraineRowLocation, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Resolves an `IS NULL` query on an index to the matching rows. `prefix` is a
/// leading run of predicates over the index's columns: a `MoraineLookupValue`
/// of `kind == 0` is `IS NULL` for that column, any other kind is `= value`.
/// At least one must be `IS NULL`; a bare non-leading `IS NULL` is not
/// expressible (the prefix covers the leading columns).
///
/// # Safety
///
/// Every non-null pointer must be valid per the ABI contract; `prefix` points
/// to `prefix_len` values; `err`, if non-null, must be writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_index_nulls(
    handle: *mut MoraineCatalogHandle,
    schema_name: *const c_char,
    table_name: *const c_char,
    index_name: *const c_char,
    prefix: *const MoraineLookupValue,
    prefix_len: usize,
    reverse: bool,
    out_items: *mut *mut MoraineRowLocation,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let produce = |handle_ref: &MoraineCatalogHandle| -> Result<Vec<MoraineRowLocation>, AbiError> {
        if prefix_len == 0 {
            return Err(AbiError::invalid_argument(
                "index nulls: the prefix names no predicate",
            ));
        }
        if prefix.is_null() {
            return Err(AbiError::invalid_argument("`prefix` is null"));
        }
        // SAFETY: caller contract for the string pointers.
        let schema = unsafe { borrow_str(schema_name, "schema_name") }?;
        // SAFETY: caller contract.
        let table = unsafe { borrow_str(table_name, "table_name") }?;
        // SAFETY: caller contract.
        let name = unsafe { borrow_str(index_name, "index_name") }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let snapshot = unsafe {
            handle_ref.block_on_cancellable(probe, probe_ctx, handle_ref.catalog.snapshot())
        }?;
        let table_id = resolve_table(&snapshot, schema, table)?;
        let index = snapshot
            .index_by_name(table_id, name)
            .ok_or_else(|| AbiError::from(moraine::Error::NotFound(format!("index {name}"))))?;
        if prefix_len > index.columns.len() {
            return Err(AbiError::invalid_argument(
                "index nulls: the prefix is longer than the index",
            ));
        }
        let columns = snapshot.columns_of(table_id);
        // SAFETY: non-null checked; caller contract — `prefix` points to
        // `prefix_len` values.
        let prefix_slice = unsafe { std::slice::from_raw_parts(prefix, prefix_len) };
        // SAFETY: caller contract for each predicate's string/bytes fields.
        let values = unsafe { coerce_index_key(&index, name, table_id, &columns, prefix_slice) }?;

        // SAFETY: caller contract for `probe`/`probe_ctx`.
        let locations = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                handle_ref
                    .catalog
                    .index_nulls(table_id, index.id, values, reverse),
            )
        }?;
        Ok(locations
            .into_iter()
            .map(|location| {
                let (data_file_id, is_inline) = match location.holder {
                    moraine::RowHolder::DataFile(id) => (id.get(), false),
                    moraine::RowHolder::Inline => (0, true),
                };
                MoraineRowLocation {
                    row_id: location.row_id,
                    data_file_id,
                    is_inline,
                }
            })
            .collect())
    };

    // SAFETY: caller contract for the pointers.
    unsafe { handle_list(handle, out_items, out_len, err, produce) }
}

/// Frees the array a [`moraine_index_nulls`] call returned.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a matching
/// [`moraine_index_nulls`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_index_nulls_free(items: *mut MoraineRowLocation, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above. The descriptor owns no heap.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use moraine::{ColumnDef, DataFile};
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::test_support::{TempDir, attach_ok};

    /// Seeds a catalog directly through the `moraine` API with one
    /// schema, one table with two columns and one data file, and one
    /// view.
    fn seed(dir: &Path) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");

        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("sales")?;
                    let table = tx.create_table(
                        schema,
                        "orders",
                        &[
                            ColumnDef {
                                name: "id".into(),
                                column_type: "BIGINT".into(),
                                nulls_allowed: false,
                                default_value: None,
                            },
                            ColumnDef {
                                name: "amount".into(),
                                column_type: "DOUBLE".into(),
                                nulls_allowed: true,
                                default_value: None,
                            },
                        ],
                    )?;
                    tx.register_data_file(
                        table,
                        DataFile {
                            path: "orders/data-1.parquet".into(),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 10,
                            file_size_bytes: 1024,
                            footer_size: 64,
                            encryption_key: None,
                            column_stats: vec![],
                        },
                        &[],
                    )?;
                    tx.create_view(schema, "orders_v", "duckdb", "select * from orders")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// Seeds a catalog with a two-column table (`a BIGINT`, `b VARCHAR`), a
    /// three-row data file, and a composite unique index over `(a, b)` with
    /// one entry per row: `(5, "x")`, `(5, "y")`, `(7, "x")`.
    fn seed_composite(dir: &Path) {
        use moraine::{ColumnId, IndexDef, IndexEntry, IndexKeyValue, IntWidth};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");
        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("sales")?;
                    let table = tx.create_table(
                        schema,
                        "t",
                        &[
                            ColumnDef {
                                name: "a".into(),
                                column_type: "BIGINT".into(),
                                nulls_allowed: false,
                                default_value: None,
                            },
                            ColumnDef {
                                name: "b".into(),
                                column_type: "VARCHAR".into(),
                                nulls_allowed: false,
                                default_value: None,
                            },
                        ],
                    )?;
                    tx.register_data_file(
                        table,
                        DataFile {
                            path: "t/data-1.parquet".into(),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 3,
                            file_size_bytes: 1024,
                            footer_size: 64,
                            encryption_key: None,
                            column_stats: vec![],
                        },
                        &[],
                    )?;
                    let a = |v: i128| {
                        Some(IndexKeyValue::Int {
                            value: v,
                            width: IntWidth::I64,
                        })
                    };
                    let b = |s: &str| Some(IndexKeyValue::Str(s.to_owned()));
                    tx.create_index(
                        table,
                        &IndexDef {
                            name: "by_ab".into(),
                            columns: vec![ColumnId::new(1), ColumnId::new(2)],
                            unique: true,
                        },
                        &[
                            IndexEntry {
                                row_id: 0,
                                values: vec![a(5), b("x")],
                            },
                            IndexEntry {
                                row_id: 1,
                                values: vec![a(5), b("y")],
                            },
                            IndexEntry {
                                row_id: 2,
                                values: vec![a(7), b("x")],
                            },
                        ],
                    )?;
                    Ok(())
                })
                .await
                .expect("test setup: commit composite fixtures");
            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// Builds an integer lookup value.
    fn i64_lookup(v: i64) -> MoraineLookupValue {
        MoraineLookupValue {
            kind: 1,
            i64_value: v,
            u64_value: 0,
            f64_value: 0.0,
            bool_value: false,
            str_value: ptr::null(),
            bytes_value: ptr::null(),
            bytes_len: 0,
        }
    }

    /// Builds a string lookup value borrowing `text` for the call.
    fn str_lookup(text: &CStr) -> MoraineLookupValue {
        MoraineLookupValue {
            kind: 5,
            i64_value: 0,
            u64_value: 0,
            f64_value: 0.0,
            bool_value: false,
            str_value: text.as_ptr(),
            bytes_value: ptr::null(),
            bytes_len: 0,
        }
    }

    /// Drives `moraine_index_lookup`, returning the resolved row ids on success
    /// or the error message on failure.
    fn composite_lookup(
        handle: *mut MoraineCatalogHandle,
        schema: &str,
        table: &str,
        index: &str,
        values: &[MoraineLookupValue],
    ) -> Result<Vec<u64>, String> {
        let c_schema = CString::new(schema).expect("no NUL");
        let c_table = CString::new(table).expect("no NUL");
        let c_index = CString::new(index).expect("no NUL");
        let mut items: *mut MoraineRowLocation = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; the C strings and `values` slice are
        // valid for the call; the out-slots are writable locals.
        let code = unsafe {
            moraine_index_lookup(
                handle,
                c_schema.as_ptr(),
                c_table.as_ptr(),
                c_index.as_ptr(),
                values.as_ptr(),
                values.len(),
                &raw mut items,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        if code != codes::OK {
            // SAFETY: a failed call wrote a non-null message.
            let message = unsafe { CStr::from_ptr(err.message) }
                .to_str()
                .expect("utf-8")
                .to_owned();
            // SAFETY: the message was minted by the failed call, freed once.
            unsafe { moraine_error_free(err.message) };
            return Err(message);
        }
        // SAFETY: on success `items`/`len` describe a valid slice.
        let rows = unsafe { std::slice::from_raw_parts(items, len) }
            .iter()
            .map(|location| location.row_id)
            .collect();
        // SAFETY: `items`/`len` are exactly what the call above wrote.
        unsafe { moraine_index_lookup_free(items, len) };
        Ok(rows)
    }

    /// A composite index resolves a full multi-column equality key: the two
    /// values, in the index's column order, pin the one matching row, and a
    /// value count that does not match the index's column count is refused.
    #[test]
    fn index_lookup_resolves_a_composite_key() {
        let dir = TempDir::new("composite-lookup");
        seed_composite(dir.path());
        let handle = attach_ok(dir.path());

        let y = CString::new("y").expect("no NUL");
        let hit = composite_lookup(
            handle,
            "sales",
            "t",
            "by_ab",
            &[i64_lookup(5), str_lookup(&y)],
        )
        .expect("the composite key resolves");
        assert_eq!(hit, vec![1], "(5, \"y\") pins exactly row 1");

        let x = CString::new("x").expect("no NUL");
        let miss = composite_lookup(
            handle,
            "sales",
            "t",
            "by_ab",
            &[i64_lookup(9), str_lookup(&x)],
        )
        .expect("an absent composite key resolves to no rows");
        assert!(miss.is_empty(), "(9, \"x\") matches no row");

        let short = composite_lookup(handle, "sales", "t", "by_ab", &[i64_lookup(5)])
            .expect_err("one value cannot address a two-column index");
        assert!(
            short.contains("2-column"),
            "the arity error names the index width, got: {short}"
        );

        // SAFETY: `handle` was minted by `attach_ok`, detached once.
        unsafe { moraine_detach(handle) };
    }

    /// Drives `moraine_index_range`, returning the resolved row ids (sorted)
    /// on success or the error message on failure. An empty bound slice is an
    /// open side.
    #[allow(clippy::too_many_arguments)]
    fn composite_range(
        handle: *mut MoraineCatalogHandle,
        schema: &str,
        table: &str,
        index: &str,
        lower: &[MoraineLookupValue],
        lower_inclusive: bool,
        upper: &[MoraineLookupValue],
        upper_inclusive: bool,
    ) -> Result<Vec<u64>, String> {
        let c_schema = CString::new(schema).expect("no NUL");
        let c_table = CString::new(table).expect("no NUL");
        let c_index = CString::new(index).expect("no NUL");
        let mut items: *mut MoraineRowLocation = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; the C strings and bound slices are
        // valid for the call; the out-slots are writable locals.
        let code = unsafe {
            moraine_index_range(
                handle,
                c_schema.as_ptr(),
                c_table.as_ptr(),
                c_index.as_ptr(),
                lower.as_ptr(),
                lower.len(),
                lower_inclusive,
                upper.as_ptr(),
                upper.len(),
                upper_inclusive,
                false,
                &raw mut items,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        if code != codes::OK {
            // SAFETY: a failed call wrote a non-null message.
            let message = unsafe { CStr::from_ptr(err.message) }
                .to_str()
                .expect("utf-8")
                .to_owned();
            // SAFETY: the message was minted by the failed call, freed once.
            unsafe { moraine_error_free(err.message) };
            return Err(message);
        }
        // SAFETY: on success `items`/`len` describe a valid slice.
        let mut rows: Vec<u64> = unsafe { std::slice::from_raw_parts(items, len) }
            .iter()
            .map(|location| location.row_id)
            .collect();
        // SAFETY: `items`/`len` are exactly what the call above wrote.
        unsafe { moraine_index_range_free(items, len) };
        rows.sort_unstable();
        Ok(rows)
    }

    /// A composite index answers a range whose bounds run over its leading
    /// columns: a leading-column equality window, a full-tuple window, and a
    /// half-open window with one open side.
    #[test]
    fn index_range_spans_a_composite_window() {
        let dir = TempDir::new("composite-range");
        seed_composite(dir.path());
        let handle = attach_ok(dir.path());

        // a = 5 (a one-column prefix bound on the two-column index): the two
        // rows sharing that leading value, whatever their second column.
        let equal_a = composite_range(
            handle,
            "sales",
            "t",
            "by_ab",
            &[i64_lookup(5)],
            true,
            &[i64_lookup(5)],
            true,
        )
        .expect("a leading-column equality window resolves");
        assert_eq!(equal_a, vec![0, 1], "a = 5 spans rows 0 and 1");

        // (5, "y") ..= (7, "x") over the full tuple: excludes (5, "x") below
        // the lower bound, includes (5, "y") and (7, "x").
        let y = CString::new("y").expect("no NUL");
        let x = CString::new("x").expect("no NUL");
        let window = composite_range(
            handle,
            "sales",
            "t",
            "by_ab",
            &[i64_lookup(5), str_lookup(&y)],
            true,
            &[i64_lookup(7), str_lookup(&x)],
            true,
        )
        .expect("a full-tuple window resolves");
        assert_eq!(
            window,
            vec![1, 2],
            "(5, \"y\")..=(7, \"x\") spans rows 1 and 2"
        );

        // a >= 7 with an open upper side: only the (7, _) row.
        let high = composite_range(
            handle,
            "sales",
            "t",
            "by_ab",
            &[i64_lookup(7)],
            true,
            &[],
            true,
        )
        .expect("a half-open window resolves");
        assert_eq!(high, vec![2], "a >= 7 spans only row 2");

        // A bound wider than the index is refused, naming the index width.
        let z = CString::new("z").expect("no NUL");
        let too_wide = composite_range(
            handle,
            "sales",
            "t",
            "by_ab",
            &[i64_lookup(5), str_lookup(&z), i64_lookup(1)],
            true,
            &[],
            true,
        )
        .expect_err("a three-value bound cannot fit a two-column index");
        assert!(
            too_wide.contains("2-column"),
            "the width error names the index, got: {too_wide}"
        );

        // SAFETY: `handle` was minted by `attach_ok`, detached once.
        unsafe { moraine_detach(handle) };
    }

    /// Reads the stored `encrypted` flag over the ABI.
    fn catalog_encrypted(handle: *mut MoraineCatalogHandle) -> bool {
        let mut encrypted = false;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots; a
        // null probe disables polling.
        let code = unsafe {
            moraine_catalog_encrypted(
                handle,
                &raw mut encrypted,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or just written; `as_ref` allows null.
        assert_eq!(code, codes::OK, "getter failed: {:?}", unsafe {
            err.message.as_ref()
        });
        encrypted
    }

    /// Bootstraps a fresh store at `dir` recording `data_path`, the way an
    /// attach with `META_DATA_PATH` does.
    fn seed_with_data_path(dir: &Path, data_path: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");
        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"),
            );
            let mut options = moraine::CatalogOptions::default();
            options.data_path = Some(data_path.to_owned());
            let catalog = moraine::Catalog::open(store, options)
                .await
                .expect("test setup: open catalog");
            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// A lake's data path is fixed at creation: re-attaching with a
    /// conflicting `META_DATA_PATH` is refused, while the recorded value
    /// (trailing separator and all) attaches cleanly.
    #[test]
    fn attach_refuses_a_conflicting_data_path() {
        let dir = TempDir::new("data-path-fixed");
        let data = TempDir::new("data-path-fixed-root");
        let recorded = data.path().to_str().expect("utf-8").to_owned();
        seed_with_data_path(dir.path(), &recorded);
        let c_path = dir.c_path();

        // A different data path is refused with a clear message.
        let c_bad = CString::new("/lake/other").expect("no NUL");
        let mut bad_handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut bad_err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let bad_code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                c_bad.as_ptr(),
                &raw mut bad_handle,
                &raw mut bad_err,
            )
        };
        assert_ne!(
            bad_code,
            codes::OK,
            "a conflicting data path must be refused"
        );
        // SAFETY: on failure `guard` wrote a non-null message.
        let message = unsafe { CStr::from_ptr(bad_err.message) }
            .to_str()
            .unwrap()
            .to_owned();
        assert!(message.contains("does not match"), "got: {message}");
        // SAFETY: `bad_err.message` was minted by the failed call, freed once.
        unsafe { moraine_error_free(bad_err.message) };

        // The recorded path, with a trailing separator, still attaches.
        let c_good = CString::new(format!("{recorded}/")).expect("no NUL");
        let mut good_handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut good_err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let good_code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                c_good.as_ptr(),
                &raw mut good_handle,
                &raw mut good_err,
            )
        };
        // SAFETY: `good_err.message` is null or just written; `as_ref` allows null.
        let good_message = unsafe { good_err.message.as_ref() };
        assert_eq!(
            good_code,
            codes::OK,
            "matching path failed: {good_message:?}"
        );
        // SAFETY: freed exactly once.
        unsafe { moraine_detach(good_handle) };
    }

    /// The maintenance ABI runs a pass and writes its counts through the
    /// out-parameters, tolerating null slots for either.
    #[test]
    fn maintain_reports_through_the_out_parameters() {
        let dir = TempDir::new("maintain-abi");
        seed(dir.path());
        let c_path = dir.c_path();

        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                ptr::null(),
                &raw mut handle,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK, "attach failed");

        // A seeded store has no dropped indexes, so a pass reclaims
        // nothing and says so rather than failing.
        let mut indexes = u64::MAX;
        let mut entries = u64::MAX;
        // SAFETY: `handle` is live; both slots are writable locals.
        let code = unsafe {
            moraine_maintain(
                handle,
                0,
                &raw mut indexes,
                &raw mut entries,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK, "maintain failed");
        assert_eq!(indexes, 0);
        assert_eq!(entries, 0);

        // Null out-parameters are accepted: a caller that wants only the
        // status code passes neither slot.
        // SAFETY: `handle` is live; null slots are explicitly allowed.
        let code = unsafe {
            moraine_maintain(
                handle,
                64,
                ptr::null_mut(),
                ptr::null_mut(),
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK, "maintain with null out-params failed");

        // SAFETY: freed exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// Nesting the catalog store inside `DATA_PATH` (or the reverse) on one
    /// object store is refused; sibling locations, separate buckets, and
    /// differing store kinds are not.
    #[test]
    fn overlapping_store_and_data_paths_are_refused() {
        let nested = [
            // The catalog sits under the swept data prefix.
            ("s3://bucket/lake/catalog", "s3://bucket/lake"),
            ("s3://bucket/lake/catalog", "s3://bucket/lake/"),
            // ...and the reverse nesting is equally unsafe.
            ("s3://bucket/lake", "s3://bucket/lake/data"),
            // Identical locations.
            ("s3://bucket/lake", "s3://bucket/lake"),
            // An empty prefix is the bucket root, containing everything.
            ("s3://bucket", "s3://bucket/data"),
            ("/tmp/lake/catalog", "/tmp/lake"),
            ("/tmp/lake", "/tmp/lake/data"),
        ];
        for (store, data) in nested {
            let error = refuse_overlapping_data_path(store, data)
                .expect_err("nested `{store}` / `{data}` must be refused");
            assert_eq!(error.code, codes::CONSTRAINT, "for {store} / {data}");
        }

        let separate = [
            // Sibling prefixes that merely share leading text.
            ("s3://bucket/lakehouse", "s3://bucket/lake"),
            ("s3://bucket/lake-catalog", "s3://bucket/lake"),
            ("/tmp/lakehouse", "/tmp/lake"),
            // True siblings.
            ("s3://bucket/catalog", "s3://bucket/data"),
            ("/tmp/catalog", "/tmp/data"),
            // Different buckets, and different store kinds.
            ("s3://catalogs/lake", "s3://data/lake"),
            ("/tmp/catalog", "s3://bucket/data"),
            ("memory://", "/tmp/data"),
        ];
        for (store, data) in separate {
            assert!(
                refuse_overlapping_data_path(store, data).is_ok(),
                "`{store}` / `{data}` are separate and must attach"
            );
        }
    }

    /// The overlap guard runs at attach, and refuses *before* an adopted
    /// data path is recorded — a refused attach must leave nothing behind
    /// for the next one to inherit.
    #[test]
    fn attach_refuses_a_data_path_containing_the_store() {
        // A *fresh* store, deliberately not seeded: bootstrapping records
        // `data_path`, so this is the case where a late check would
        // persist the dangerous value before refusing.
        let dir = TempDir::new("overlap-guard");
        let c_path = dir.c_path();

        // DATA_PATH is the store's own parent, so orphan cleanup would
        // sweep the catalog's objects.
        let parent = dir
            .path()
            .parent()
            .expect("temp dir has a parent")
            .to_str()
            .expect("utf-8")
            .to_owned();
        let c_data = CString::new(parent).expect("no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                c_data.as_ptr(),
                &raw mut handle,
                &raw mut err,
            )
        };
        assert_eq!(
            code,
            codes::CONSTRAINT,
            "a nested DATA_PATH must be refused"
        );
        // SAFETY: on failure `guard` wrote a non-null message.
        let message = unsafe { CStr::from_ptr(err.message) }
            .to_str()
            .unwrap()
            .to_owned();
        assert!(message.contains("nested"), "got: {message}");
        // SAFETY: minted by the failed call, freed once.
        unsafe { moraine_error_free(err.message) };

        // Nothing was recorded, so a later attach with a safe path still
        // adopts it.
        let safe = TempDir::new("overlap-guard-data");
        let c_safe = CString::new(safe.path().to_str().expect("utf-8")).expect("no NUL");
        let mut ok_handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut ok_err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let ok_code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                c_safe.as_ptr(),
                &raw mut ok_handle,
                &raw mut ok_err,
            )
        };
        // SAFETY: null or just written; `as_ref` allows null.
        let ok_message = unsafe { ok_err.message.as_ref() };
        assert_eq!(ok_code, codes::OK, "safe path failed: {ok_message:?}");
        // SAFETY: freed exactly once.
        unsafe { moraine_detach(ok_handle) };
    }

    /// A lake with no data path recorded yet (created before the option
    /// existed) adopts the one given at its next attach, and enforces it
    /// thereafter.
    #[test]
    fn attach_records_a_missing_data_path_then_fixes_it() {
        let dir = TempDir::new("legacy-data-path");
        seed(dir.path()); // a store with no data_path recorded
        let data = TempDir::new("legacy-data-path-root");
        let recorded = data.path().to_str().expect("utf-8").to_owned();
        let c_path = dir.c_path();

        // The first attach records the data path.
        let c_first = CString::new(recorded.clone()).expect("no NUL");
        let mut first_handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut first_err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let first_code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                c_first.as_ptr(),
                &raw mut first_handle,
                &raw mut first_err,
            )
        };
        // SAFETY: `first_err.message` is null or just written; `as_ref` allows null.
        let first_message = unsafe { first_err.message.as_ref() };
        assert_eq!(
            first_code,
            codes::OK,
            "recording attach failed: {first_message:?}"
        );
        // SAFETY: freed exactly once.
        unsafe { moraine_detach(first_handle) };

        // A later attach with a different data path is now refused.
        let c_other = CString::new("/lake/elsewhere").expect("no NUL");
        let mut other_handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut other_err = MoraineError::default();
        // SAFETY: all pointers are valid C strings / local slots.
        let other_code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                c_other.as_ptr(),
                &raw mut other_handle,
                &raw mut other_err,
            )
        };
        assert_ne!(other_code, codes::OK, "the recorded path is now enforced");
        // SAFETY: on failure `guard` wrote a non-null message.
        let other_message = unsafe { CStr::from_ptr(other_err.message) }
            .to_str()
            .unwrap()
            .to_owned();
        assert!(
            other_message.contains("does not match"),
            "got: {other_message}"
        );
        // SAFETY: minted by the failed call, freed once.
        unsafe { moraine_error_free(other_err.message) };
    }

    /// A read-only attach of an uninitialized store fails with guidance to
    /// add `READ_WRITE`: a read-only attach cannot bootstrap, which is how a
    /// fresh remote (DuckDB-defaulted-read-only) lake presents.
    #[test]
    fn read_only_attach_of_fresh_store_hints_read_write() {
        let dir = TempDir::new("ro-fresh");
        let c_path =
            CString::new(dir.path().to_str().expect("test path is UTF-8")).expect("no NUL in path");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                true,
                false,
                0,
                ptr::null(),
                ptr::null(),
                &raw mut handle,
                &raw mut err,
            )
        };
        assert_ne!(
            code,
            codes::OK,
            "read-only attach of a fresh store should fail"
        );
        assert!(handle.is_null());
        // SAFETY: on failure `err.message` is a valid, just-written C string.
        let message = unsafe { CStr::from_ptr(err.message) }
            .to_str()
            .expect("message is UTF-8")
            .to_owned();
        // SAFETY: frees the message allocated by the failed attach, exactly once.
        unsafe { moraine_error_free(err.message) };
        assert!(
            message.contains("READ_WRITE"),
            "read-only attach error should point at READ_WRITE: {message}"
        );
    }

    /// The `encrypted` flag is fixed by the attach that bootstraps the
    /// store; later attaches requesting a different value do not flip it,
    /// and the getter always reports the stored flag.
    #[test]
    fn attach_encrypted_is_fixed_at_bootstrap_and_reported() {
        let dir = TempDir::new("encrypted");
        let c_path = dir.c_path();

        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                true,
                0,
                ptr::null(),
                ptr::null(),
                &raw mut handle,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert!(catalog_encrypted(handle));
        // SAFETY: `handle` came from the attach above, detached exactly once.
        unsafe { moraine_detach(handle) };

        // Re-attach without requesting encryption: the stored flag wins.
        let handle = attach_ok(dir.path());
        assert!(catalog_encrypted(handle));
        // SAFETY: same as above.
        unsafe { moraine_detach(handle) };

        // A default-attached fresh store reports unencrypted.
        let dir_plain = TempDir::new("unencrypted");
        let handle = attach_ok(dir_plain.path());
        assert!(!catalog_encrypted(handle));
        // SAFETY: same as above.
        unsafe { moraine_detach(handle) };
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one end-to-end attach→list assertion chain
    fn attach_snapshot_and_list_round_trip() {
        let dir = TempDir::new("roundtrip");
        seed(dir.path());

        let handle = attach_ok(dir.path());

        let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; `snapshot`/`err` are valid local slots.
        let code = unsafe {
            moraine_snapshot(
                handle,
                &raw mut snapshot,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert!(!snapshot.is_null());

        let mut schemas: *mut MoraineSchemaDesc = ptr::null_mut();
        let mut schemas_len: usize = 0;
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_schemas(
                snapshot,
                &raw mut schemas,
                &raw mut schemas_len,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // Bootstrap mints `main` (id 0); the seeded `sales` follows at id 1.
        assert_eq!(schemas_len, 2);
        // SAFETY: just populated above with `schemas_len` live elements.
        let schema_descs = unsafe { std::slice::from_raw_parts(schemas, schemas_len) };
        let schema_pairs: Vec<(u64, &str)> = schema_descs
            .iter()
            // SAFETY: owned C strings written above, not yet freed.
            .map(|s| (s.id, unsafe { CStr::from_ptr(s.name) }.to_str().unwrap()))
            .collect();
        assert_eq!(schema_pairs, [(0, "main"), (1, "sales")]);
        let schema_id = schema_descs[1].id;

        let mut tables: *mut MoraineTableDesc = ptr::null_mut();
        let mut tables_len: usize = 0;
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_tables_in(
                snapshot,
                schema_id,
                &raw mut tables,
                &raw mut tables_len,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(tables_len, 1);
        // SAFETY: just populated by `moraine_snapshot_tables_in` above.
        let table_id = unsafe { (*tables).id };
        // SAFETY: same as above.
        let table_name = unsafe { CStr::from_ptr((*tables).name) }.to_str().unwrap();
        assert_eq!(table_name, "orders");

        let mut columns: *mut MoraineColumnDesc = ptr::null_mut();
        let mut columns_len: usize = 0;
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_columns_of(
                snapshot,
                table_id,
                &raw mut columns,
                &raw mut columns_len,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(columns_len, 2);
        // SAFETY: just populated above with `columns_len` live elements.
        let cols = unsafe { std::slice::from_raw_parts(columns, columns_len) };
        let names: Vec<&str> = cols
            .iter()
            // SAFETY: owned C strings written above, not yet freed.
            .map(|c| unsafe { CStr::from_ptr(c.name) }.to_str().unwrap())
            .collect();
        assert_eq!(names, vec!["id", "amount"]);
        assert!(!cols[0].nulls_allowed);
        assert!(cols[1].nulls_allowed);

        let mut views: *mut MoraineViewDesc = ptr::null_mut();
        let mut views_len: usize = 0;
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_views_in(
                snapshot,
                schema_id,
                &raw mut views,
                &raw mut views_len,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(views_len, 1);
        // SAFETY: just populated by `moraine_snapshot_views_in` above.
        let view_sql = unsafe { CStr::from_ptr((*views).sql) }.to_str().unwrap();
        assert_eq!(view_sql, "select * from orders");

        let mut files: *mut MoraineDataFileDesc = ptr::null_mut();
        let mut files_len: usize = 0;
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_data_files_of(
                snapshot,
                table_id,
                &raw mut files,
                &raw mut files_len,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(files_len, 1);
        // SAFETY: just populated by `moraine_snapshot_data_files_of` above.
        let file_path = unsafe { CStr::from_ptr((*files).path) }.to_str().unwrap();
        assert_eq!(file_path, "orders/data-1.parquet");
        // SAFETY: same as above.
        assert_eq!(unsafe { (*files).record_count }, 10);
        // SAFETY: same as above.
        assert_eq!(unsafe { (*files).row_id_start }, 0);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_snapshot_schemas_free(schemas, schemas_len);
            moraine_snapshot_tables_in_free(tables, tables_len);
            moraine_snapshot_columns_of_free(columns, columns_len);
            moraine_snapshot_views_in_free(views, views_len);
            moraine_snapshot_data_files_of_free(files, files_len);
            moraine_snapshot_free(snapshot);
            moraine_detach(handle);
        }
    }

    /// A catalog string with an embedded NUL (reachable via a view's SQL,
    /// since `moraine` stores `\0` verbatim) cannot cross the C boundary:
    /// the listing call must fail with `CORRUPTION`, leaving the outputs
    /// untouched.
    #[test]
    fn embedded_nul_in_catalog_data_reports_corruption() {
        let dir = TempDir::new("embedded-nul");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");
        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("s")?;
                    // Two views: the clean one converts first (ordered by
                    // id) and must drop cleanly when the second fails.
                    tx.create_view(schema, "clean", "duckdb", "select 1")?;
                    tx.create_view(schema, "poisoned", "duckdb", "select 1 as a\0b")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");
            catalog.close().await.expect("test setup: close catalog");
        });

        let handle = attach_ok(dir.path());
        let mut snap: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; `snapshot`/`err` are valid local slots.
        let code =
            unsafe { moraine_snapshot(handle, &raw mut snap, None, ptr::null_mut(), &raw mut err) };
        assert_eq!(code, codes::OK);

        let mut views: *mut MoraineViewDesc = ptr::null_mut();
        let mut views_len: usize = 0;
        // Schema `s` has id 1: bootstrap's `main` schema holds id 0.
        //
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_views_in(snap, 1, &raw mut views, &raw mut views_len, &raw mut err)
        };
        assert_eq!(code, codes::CORRUPTION);
        assert_eq!(err.code, codes::CORRUPTION);
        // The outputs stay untouched on failure: nothing was handed to
        // the caller, so there is nothing for the caller to free.
        assert!(views.is_null());
        assert_eq!(views_len, 0);
        assert!(!err.message.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("NUL"), "message: {msg}");

        // SAFETY: `err.message` was just populated and not yet freed;
        // `snapshot`/`handle` came from the calls above and are freed exactly
        // once.
        unsafe {
            moraine_error_free(err.message);
            moraine_snapshot_free(snap);
            moraine_detach(handle);
        }
    }

    #[test]
    fn empty_table_lists_no_data_files() {
        let dir = TempDir::new("empty-table");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");
        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("test setup: open local store"),
            );
            let catalog = moraine::Catalog::open(store, moraine::CatalogOptions::default())
                .await
                .expect("test setup: open catalog");
            catalog
                .commit(|tx| {
                    let schema = tx.create_schema("s")?;
                    tx.create_table(
                        schema,
                        "empty",
                        &[ColumnDef {
                            name: "id".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                        }],
                    )?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");
            catalog.close().await.expect("test setup: close catalog");
        });

        let handle = attach_ok(dir.path());
        let mut snap: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; `snapshot`/`err` are valid local slots.
        let code =
            unsafe { moraine_snapshot(handle, &raw mut snap, None, ptr::null_mut(), &raw mut err) };
        assert_eq!(code, codes::OK);

        let mut tables: *mut MoraineTableDesc = ptr::null_mut();
        let mut tables_len: usize = 0;
        // Schema `s` has id 1: bootstrap's `main` schema holds id 0.
        //
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_tables_in(snap, 1, &raw mut tables, &raw mut tables_len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        assert_eq!(tables_len, 1);
        // SAFETY: just populated by `moraine_snapshot_tables_in` above.
        let table_id = unsafe { (*tables).id };

        let mut files: *mut MoraineDataFileDesc = ptr::null_mut();
        let mut files_len: usize = 0;
        // SAFETY: `snapshot` is live; outputs are valid local slots.
        let code = unsafe {
            moraine_snapshot_data_files_of(
                snap,
                table_id,
                &raw mut files,
                &raw mut files_len,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(files_len, 0);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_snapshot_tables_in_free(tables, tables_len);
            moraine_snapshot_data_files_of_free(files, files_len);
            moraine_snapshot_free(snap);
            moraine_detach(handle);
        }
    }

    #[test]
    fn attach_on_unwritable_path_reports_invalid_argument() {
        // A path nested under a file (not a directory) can never be
        // created: `create_dir_all` fails with `NotADirectory`/`ENOTDIR`.
        let dir = TempDir::new("bad-path");
        let file_path = dir.path().join("not-a-directory");
        std::fs::write(&file_path, b"not a directory").expect("test setup: write file");
        let bogus = file_path.join("nested");

        let c_path = CString::new(bogus.to_str().expect("UTF-8")).expect("no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid NUL-terminated C string; `handle`/`err`
        // are valid, writable local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                ptr::null(),
                &raw mut handle,
                &raw mut err,
            )
        };

        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert_eq!(err.code, codes::INVALID_ARGUMENT);
        assert!(handle.is_null());
        assert!(!err.message.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("cannot create directory"), "message: {msg}");

        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { moraine_error_free(err.message) };
    }

    #[test]
    fn attach_rejects_unknown_store_scheme() {
        // A remote scheme moraine doesn't back is rejected from the path
        // itself, before any store is opened.
        let c_path = CString::new("gs://some-bucket").expect("no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid NUL-terminated C string; `s3` is null
        // (env-only); `handle`/`err` are valid, writable local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                ptr::null(),
                &raw mut handle,
                &raw mut err,
            )
        };

        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(handle.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("unsupported store scheme"), "message: {msg}");
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { moraine_error_free(err.message) };
    }

    #[test]
    fn store_kind_parses_s3_bucket_and_prefix() {
        let (kind, prefix) =
            StoreKind::from_path("s3://my-bucket/catalogs/lake").expect("s3 with prefix parses");
        assert!(matches!(kind, StoreKind::S3 { ref bucket } if bucket == "my-bucket"));
        assert_eq!(prefix, "catalogs/lake");

        let (kind, prefix) = StoreKind::from_path("s3://my-bucket").expect("bare bucket parses");
        assert!(matches!(kind, StoreKind::S3 { ref bucket } if bucket == "my-bucket"));
        assert_eq!(prefix, "");

        let (kind, prefix) = StoreKind::from_path("/tmp/lake").expect("local path parses");
        assert!(matches!(kind, StoreKind::LocalFile));
        assert_eq!(prefix, "");

        assert!(
            StoreKind::from_path("s3://").is_err(),
            "empty bucket is rejected"
        );
        assert!(
            StoreKind::from_path("gs://b").is_err(),
            "unknown scheme is rejected"
        );
    }

    #[test]
    fn attach_null_path_reports_invalid_argument_without_crashing() {
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: a null `path` is exactly the input this test exercises;
        // `handle`/`err` are valid, writable local slots.
        let code = unsafe {
            moraine_attach(
                ptr::null(),
                ptr::null(),
                false,
                false,
                0,
                ptr::null(),
                ptr::null(),
                &raw mut handle,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(handle.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("path"), "message: {msg}");
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { moraine_error_free(err.message) };
    }

    #[test]
    fn snapshot_on_null_handle_reports_invalid_argument() {
        let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: a null `handle` is exactly the input this test exercises;
        // `snapshot`/`err` are valid, writable local slots.
        let code = unsafe {
            moraine_snapshot(
                ptr::null_mut(),
                &raw mut snapshot,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(snapshot.is_null());
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { moraine_error_free(err.message) };
    }

    #[test]
    fn detach_and_frees_tolerate_null() {
        // Every teardown function must be a safe no-op on null.
        //
        // SAFETY: every argument below is null, which each function's own
        // contract documents as a no-op.
        unsafe {
            moraine_detach(ptr::null_mut());
            moraine_snapshot_free(ptr::null_mut());
            moraine_error_free(ptr::null_mut());
            moraine_snapshot_schemas_free(ptr::null_mut(), 0);
            moraine_snapshot_tables_in_free(ptr::null_mut(), 0);
            moraine_snapshot_columns_of_free(ptr::null_mut(), 0);
            moraine_snapshot_views_in_free(ptr::null_mut(), 0);
            moraine_snapshot_data_files_of_free(ptr::null_mut(), 0);
        }
    }

    /// Drives `guard` directly with a body engineered to panic, and
    /// checks the panic surfaces as `codes::INTERNAL` with the fixed
    /// message instead of unwinding across the FFI boundary. No public
    /// entry point can be driven to panic without UB, since each
    /// validates its inputs first.
    #[test]
    fn guard_contains_a_panic_as_the_internal_error_code() {
        let mut err = MoraineError::default();
        // SAFETY: `err` is a valid, writable local slot.
        let outcome: Result<(), i32> =
            unsafe { guard(&raw mut err, || -> Result<(), AbiError> { panic!("boom") }) };
        assert_eq!(outcome, Err(codes::INTERNAL));
        assert_eq!(err.code, codes::INTERNAL);
        assert!(!err.message.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert_eq!(msg, INTERNAL_PANIC_MESSAGE);
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { moraine_error_free(err.message) };
    }

    unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
        false
    }

    unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
        true
    }

    /// A probe that stays quiet forever must leave the core future to win.
    #[test]
    fn cancellable_block_on_completes_when_probe_never_fires() {
        let dir = TempDir::new("probe-quiet");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        // SAFETY: `handle` came from `attach_ok` and is still attached.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe_never` is callable with a null context from any
        // thread.
        let result = unsafe {
            handle_ref.block_on_cancellable(Some(probe_never), ptr::null_mut(), async {
                Ok::<_, moraine::Error>(7u32)
            })
        };
        assert_eq!(result.unwrap(), 7);

        // SAFETY: freed exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// A null probe is the non-cancellable configuration: the future runs.
    #[test]
    fn cancellable_block_on_with_null_probe_completes() {
        let dir = TempDir::new("probe-null");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        // SAFETY: `handle` came from `attach_ok` and is still attached.
        let handle_ref = unsafe { &*handle };
        // SAFETY: a `None` probe never dereferences `probe_ctx`.
        let result = unsafe {
            handle_ref.block_on_cancellable(None, ptr::null_mut(), async {
                Ok::<_, moraine::Error>(7u32)
            })
        };
        assert_eq!(result.unwrap(), 7);

        // SAFETY: freed exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// A probe firing while the future is pending cancels it: the poll
    /// loop, not just the immediate first check, is live. The future never
    /// resolves, so only the probe can end this call.
    #[test]
    fn cancellable_block_on_cancels_pending_future_when_probe_fires() {
        // First poll false (the immediate pre-flight check), every later
        // poll true.
        unsafe extern "C" fn probe_true_after_first(probe_ctx: *mut c_void) -> bool {
            // SAFETY: this test passes a valid `AtomicU64` pointer below.
            let calls = unsafe { &*probe_ctx.cast::<AtomicU64>() };
            calls.fetch_add(1, Ordering::SeqCst) >= 1
        }

        let dir = TempDir::new("probe-mid-flight");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let calls = AtomicU64::new(0);

        // SAFETY: `handle` came from `attach_ok` and is still attached.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `calls` outlives the call; the probe only reads it
        // atomically.
        let result: Result<(), AbiError> = unsafe {
            handle_ref.block_on_cancellable(
                Some(probe_true_after_first),
                (&raw const calls).cast_mut().cast(),
                std::future::pending::<Result<(), moraine::Error>>(),
            )
        };
        let error = result.unwrap_err();
        assert_eq!(error.code, codes::INTERRUPTED);
        assert!(calls.load(Ordering::SeqCst) >= 2);

        // SAFETY: freed exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// The pull channel end to end: a probe reporting an interrupt cancels
    /// the snapshot (out-param unwritten), and the same handle with a
    /// quiet probe succeeds right after — the signal is level-triggered
    /// and scoped to the call that observed it.
    #[test]
    fn probe_cancels_snapshot_then_quiet_probe_succeeds() {
        let dir = TempDir::new("probe-snapshot");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let mut snapshot: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; `snapshot`/`err` are valid local
        // slots; `probe_always` accepts a null context.
        let code = unsafe {
            moraine_snapshot(
                handle,
                &raw mut snapshot,
                Some(probe_always),
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INTERRUPTED);
        assert_eq!(err.code, codes::INTERRUPTED);
        assert!(snapshot.is_null());
        // SAFETY: populated by the failed call above, freed exactly once.
        unsafe { moraine_error_free(err.message) };

        let mut snap2: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err2 = MoraineError::default();
        // SAFETY: same contracts; `probe_never` accepts a null context.
        let code2 = unsafe {
            moraine_snapshot(
                handle,
                &raw mut snap2,
                Some(probe_never),
                ptr::null_mut(),
                &raw mut err2,
            )
        };
        assert_eq!(code2, codes::OK);
        assert!(!snap2.is_null());

        // SAFETY: freed exactly once each.
        unsafe {
            moraine_snapshot_free(snap2);
            moraine_detach(handle);
        }
    }

    /// A lookup value coerces to the same canonical `IndexKeyValue` the
    /// scoped read derives for the column's type — width and all — so a
    /// lookup matches a stored key.
    #[test]
    fn coerce_lookup_value_matches_column_types() {
        use moraine::{IndexKeyValue, IntWidth};

        let blank = MoraineLookupValue {
            kind: 0,
            i64_value: 0,
            u64_value: 0,
            f64_value: 0.0,
            bool_value: false,
            str_value: ptr::null(),
            bytes_value: ptr::null(),
            bytes_len: 0,
        };

        let int_value = MoraineLookupValue {
            kind: 1,
            i64_value: 42,
            ..blank
        };
        // The same integer takes the column's width, not the literal's — and
        // DuckLake's bit-width spelling (`INT64`) resolves like the SQL name.
        // SAFETY: an integer-kind value dereferences no pointer fields.
        let as_bigint = unsafe { coerce_lookup_value(&int_value, "INT64") }.unwrap();
        assert_eq!(
            as_bigint,
            IndexKeyValue::Int {
                value: 42,
                width: IntWidth::I64
            }
        );
        // SAFETY: as above.
        let as_integer = unsafe { coerce_lookup_value(&int_value, "INT32") }.unwrap();
        assert_eq!(
            as_integer,
            IndexKeyValue::Int {
                value: 42,
                width: IntWidth::I32
            }
        );

        // A UUID arrives as 16 bytes.
        let uuid = [0x5Au8; 16];
        let bytes_value = MoraineLookupValue {
            kind: 6,
            bytes_value: uuid.as_ptr(),
            bytes_len: uuid.len(),
            ..blank
        };
        // SAFETY: `uuid` outlives the call.
        let as_uuid = unsafe { coerce_lookup_value(&bytes_value, "UUID") }.unwrap();
        assert_eq!(as_uuid, IndexKeyValue::Bytes(uuid.to_vec()));

        let text = CString::new("hello").expect("no NUL");
        let str_value = MoraineLookupValue {
            kind: 5,
            str_value: text.as_ptr(),
            ..blank
        };
        // SAFETY: `text` outlives the call.
        let as_varchar = unsafe { coerce_lookup_value(&str_value, "VARCHAR") }.unwrap();
        assert_eq!(as_varchar, IndexKeyValue::Str("hello".to_owned()));

        // A kind that cannot represent the column, and an unsupported type,
        // are both refused rather than silently mis-encoded.
        // SAFETY: integer-kind value, no pointer fields.
        let wrong_kind = unsafe { coerce_lookup_value(&int_value, "UUID") };
        assert!(wrong_kind.is_err());
        // SAFETY: as above.
        let unsupported = unsafe { coerce_lookup_value(&int_value, "DECIMAL(18,3)") };
        assert!(unsupported.is_err());
    }
}
