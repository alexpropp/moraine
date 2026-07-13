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

use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};

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

/// The object store an attach path resolves to: a local filesystem
/// directory or an in-memory store. Credentialed remote stores are
/// deferred.
enum StoreKind {
    /// `path` is a directory on the local filesystem, created if absent.
    LocalFile,
    /// `path` is ignored; a fresh, empty in-memory store.
    Memory,
}

impl StoreKind {
    fn parse(s: &str) -> Result<Self, AbiError> {
        match s {
            "" | "file" => Ok(Self::LocalFile),
            "memory" => Ok(Self::Memory),
            other => Err(AbiError::invalid_argument(format!(
                "moraine_attach: unsupported object_store_uri `{other}` \
                 (expected `file` or `memory`)"
            ))),
        }
    }

    fn open(&self, path: &str) -> Result<Arc<dyn ObjectStore>, AbiError> {
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

/// Attaches a moraine catalog: creates the runtime this handle owns for
/// its lifetime, opens (creating and initializing if empty) the catalog,
/// and writes the resulting handle to `*out`.
///
/// `path` is a local filesystem directory (created if absent) unless
/// `object_store_uri` selects otherwise. `object_store_uri` may be null
/// (defaults to `"file"`), `"file"`, or `"memory"`.
///
/// Returns [`codes::OK`] on success. On failure, `*out` is left
/// unwritten and, if `err` is non-null, `*err` carries the code and a
/// message.
///
/// # Safety
///
/// `path` must be a valid NUL-terminated C string. `object_store_uri`
/// must be null or a valid NUL-terminated C string. `out` must be a
/// valid, writable `*mut *mut MoraineCatalogHandle`. `err`, if non-null,
/// must be a valid, writable [`MoraineError`]. All for the duration of
/// this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_attach(
    path: *const c_char,
    object_store_uri: *const c_char,
    read_only: bool,
    out: *mut *mut MoraineCatalogHandle,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineCatalogHandle>, AbiError> {
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: `path` validity is this function's own safety contract.
        let path_str = unsafe { borrow_str(path, "path") }?;

        let store_kind = if object_store_uri.is_null() {
            StoreKind::LocalFile
        } else {
            // SAFETY: caller contract; checked non-null above.
            let s = unsafe { borrow_str(object_store_uri, "object_store_uri") }?;
            StoreKind::parse(s)?
        };

        let runtime = new_runtime().map_err(|e| {
            AbiError::new(
                codes::INTERNAL,
                format!("failed to start tokio runtime: {e}"),
            )
        })?;
        let object_store = store_kind.open(path_str)?;
        let options = moraine::CatalogOptions::default();
        let open = async {
            if read_only {
                moraine::Catalog::open_read_only(object_store, options).await
            } else {
                moraine::Catalog::open(object_store, options).await
            }
        };
        let catalog = runtime.block_on(open).map_err(AbiError::from)?;

        Ok(Box::new(MoraineCatalogHandle::new(runtime, catalog)))
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
        let _ = boxed.runtime.block_on(boxed.catalog.close());
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Materializes the catalog's current snapshot and writes the resulting
/// handle to `*out`.
///
/// Cancellable: races the core read against [`moraine_interrupt`]'s
/// signal and against `probe` (polled immediately, then ~100 ms; a null
/// `probe` disables polling). If a cancellation wins (pending already, or
/// arriving mid-read), returns [`codes::INTERRUPTED`] and `*out` is left
/// unwritten. The interrupt signal is consumed either way, so the next
/// `moraine_snapshot` call is unaffected.
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

/// Signals cancellation of the read currently in flight on `handle`, or,
/// if none is in flight, the very next one.
///
/// The signal is consumed by the read that observes it and does not carry
/// over, so reads after the one that consumes it are unaffected. Repeated
/// calls before any read consumes it coalesce to one pending interrupt.
///
/// A null `handle` is a no-op.
///
/// # Safety
///
/// `handle`, if non-null, must be a pointer previously returned by
/// [`moraine_attach`] and not yet passed to [`moraine_detach`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_interrupt(handle: *mut MoraineCatalogHandle) {
    if handle.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above.
        let handle_ref = unsafe { &*handle };
        handle_ref.interrupt.notify_one();
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
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
    let attempt = || -> Result<Vec<MoraineSchemaDesc>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &(*snapshot).snapshot };
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
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
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
    let attempt = || -> Result<Vec<MoraineTableDesc>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &(*snapshot).snapshot };
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
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
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
    let attempt = || -> Result<Vec<MoraineColumnDesc>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &(*snapshot).snapshot };
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
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
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
    let attempt = || -> Result<Vec<MoraineViewDesc>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &(*snapshot).snapshot };
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
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
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
    /// First row id of the file's dense per-table row-id range.
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
    let attempt = || -> Result<Vec<MoraineDataFileDesc>, AbiError> {
        if snapshot.is_null() {
            return Err(AbiError::invalid_argument("`snapshot` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `snapshot`.
        let snapshot = unsafe { &(*snapshot).snapshot };
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
                row_id_start: f.row_id_start,
                file_size_bytes: f.file_size_bytes,
                footer_size: f.footer_size,
            })
            .collect())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(items) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe { write_array(items, out_items, out_len) };
            codes::OK
        }
        Err(code) => code,
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

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use moraine::{ColumnDef, DataFile};
    use object_store::local::LocalFileSystem;

    use super::*;

    /// A directory under the OS temp dir, unique per call, removed on
    /// drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "moraine-duckdb-abi-{tag}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn c_path(&self) -> CString {
            CString::new(self.0.to_str().expect("test path is UTF-8")).expect("no NUL in path")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

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
                    )?;
                    tx.create_view(schema, "orders_v", "duckdb", "select * from orders")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    fn attach_ok(dir: &Path) -> *mut MoraineCatalogHandle {
        let c_path = CString::new(dir.to_str().expect("test path is UTF-8")).expect("no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                &raw mut handle,
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or just written; `as_ref` allows null.
        assert_eq!(code, codes::OK, "attach failed: {:?}", unsafe {
            err.message.as_ref()
        });
        assert!(!handle.is_null());
        handle
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
        let dir = TempDir::new("bad-scheme");
        let c_path = dir.c_path();
        let scheme = CString::new("s3").expect("no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path`/`scheme` are valid NUL-terminated C strings;
        // `handle`/`err` are valid, writable local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                scheme.as_ptr(),
                false,
                &raw mut handle,
                &raw mut err,
            )
        };

        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(handle.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("s3"), "message: {msg}");
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { moraine_error_free(err.message) };
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

    /// A signal delivered before the read starts must still cancel it.
    /// Pins the `select!` race deterministically, since real reads here
    /// complete too fast to reliably interrupt mid-flight.
    #[test]
    fn interrupt_before_snapshot_returns_interrupted_then_next_snapshot_succeeds() {
        let dir = TempDir::new("interrupt");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        // SAFETY: `handle` came from `attach_ok`/`moraine_attach` and is
        // still attached.
        unsafe { moraine_interrupt(handle) };

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
        assert_eq!(code, codes::INTERRUPTED);
        assert_eq!(err.code, codes::INTERRUPTED);
        assert!(snapshot.is_null());
        assert!(!err.message.is_null());
        // SAFETY: `err.message` was just populated above and not yet
        // freed.
        unsafe { moraine_error_free(err.message) };

        // The signal was consumed above, so this next snapshot succeeds.
        let mut snap2: *mut MoraineSnapshotHandle = ptr::null_mut();
        let mut err2 = MoraineError::default();
        // SAFETY: `handle` is still attached and live; `snap2`/`err2` are
        // valid, writable local slots.
        let code2 = unsafe {
            moraine_snapshot(handle, &raw mut snap2, None, ptr::null_mut(), &raw mut err2)
        };
        // SAFETY: `err2.message` was either just written by `moraine_snapshot`
        // above or is still null on success; `as_ref` on a possibly-null raw
        // pointer is exactly what it is documented to support.
        let err2_message = unsafe { err2.message.as_ref() };
        assert_eq!(code2, codes::OK, "second snapshot failed: {err2_message:?}");
        assert!(!snap2.is_null());

        // SAFETY: `snap2`/`handle` came from the calls above and are each
        // freed exactly once.
        unsafe {
            moraine_snapshot_free(snap2);
            moraine_detach(handle);
        }
    }

    #[test]
    fn interrupt_on_null_handle_is_a_no_op() {
        // SAFETY: a null `handle` is exactly the input this test
        // exercises, documented as a no-op.
        unsafe { moraine_interrupt(ptr::null_mut()) };
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

    /// A pending push signal (`moraine_interrupt`) wins before the future
    /// starts, exactly as it does for the probe-less path.
    #[test]
    fn cancellable_block_on_pending_interrupt_signal_wins() {
        let dir = TempDir::new("probe-push");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        // SAFETY: `handle` came from `attach_ok` and is still attached.
        unsafe { moraine_interrupt(handle) };

        // SAFETY: `handle` came from `attach_ok` and is still attached.
        let handle_ref = unsafe { &*handle };
        // SAFETY: a `None` probe never dereferences `probe_ctx`.
        let result: Result<u32, AbiError> = unsafe {
            handle_ref.block_on_cancellable(None, ptr::null_mut(), async {
                Ok::<_, moraine::Error>(7u32)
            })
        };
        assert_eq!(result.unwrap_err().code, codes::INTERRUPTED);

        // SAFETY: freed exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// `cpp/moraine_abi.h` is a hand-written C mirror of this module's
    /// `extern "C"` surface, kept in lockstep by hand (no `cbindgen`
    /// step). Checks textual presence of each symbol/struct name only —
    /// catches a rename or removal, not a reordered or retyped field.
    #[test]
    fn header_declares_every_abi_symbol() {
        let header = include_str!("../cpp/moraine_abi.h");

        let functions = [
            "moraine_attach",
            "moraine_detach",
            "moraine_snapshot",
            "moraine_interrupt",
            "moraine_snapshot_free",
            "moraine_error_free",
            "moraine_snapshot_schemas",
            "moraine_snapshot_schemas_free",
            "moraine_snapshot_tables_in",
            "moraine_snapshot_tables_in_free",
            "moraine_snapshot_columns_of",
            "moraine_snapshot_columns_of_free",
            "moraine_snapshot_views_in",
            "moraine_snapshot_views_in_free",
            "moraine_snapshot_data_files_of",
            "moraine_snapshot_data_files_of_free",
            "moraine_arrow_encode_schema",
            "moraine_arrow_encode_chunk",
            "moraine_arrow_decode_stream",
            "moraine_arrow_decode_body",
            "moraine_arrow_bytes_free",
            "moraine_arrow_error_free",
        ];
        let structs = [
            "MoraineCatalogHandle",
            "MoraineSnapshotHandle",
            "MoraineInterruptProbe",
            "MoraineError",
            "MoraineSchemaDesc",
            "MoraineTableDesc",
            "MoraineColumnDesc",
            "MoraineViewDesc",
            "MoraineDataFileDesc",
            "MoraineArrowBytes",
            "MoraineArrowError",
        ];
        let error_codes = [
            "MORAINE_OK",
            "MORAINE_NOT_FOUND",
            "MORAINE_ALREADY_EXISTS",
            "MORAINE_CONSTRAINT",
            "MORAINE_COMMIT_CONFLICT",
            "MORAINE_CORRUPTION",
            "MORAINE_STORE",
            "MORAINE_INVALID_ARGUMENT",
            "MORAINE_INTERNAL",
            "MORAINE_INTERRUPTED",
        ];

        for name in functions.iter().chain(&structs).chain(&error_codes) {
            assert!(
                header.contains(name),
                "cpp/moraine_abi.h is missing `{name}`, declared in src/abi.rs / \
                 src/error.rs — the two must be kept in lockstep by hand"
            );
        }
    }
}
