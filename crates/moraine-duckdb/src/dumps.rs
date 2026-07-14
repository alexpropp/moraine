//! Row-faithful `ducklake_*` dumps: one C array per table kind the store
//! models, carrying every current and history row with every lifecycle column
//! verbatim and unfiltered (DuckLake filters `begin_snapshot`/
//! `end_snapshot` itself, in SQL).
//!
//! Shares [`crate::abi`]'s conventions: `catch_unwind`/null/UTF-8
//! discipline via [`guard`](crate::abi), owned-first `CString`
//! construction, one `_free` per dump. Every function opens its own fresh
//! transaction against [`moraine::ffi_support`] — no snapshot handle is
//! involved, and no two dump calls are guaranteed to observe the same
//! head.
//!
//! Two nullability conventions cross the C boundary:
//! - an optional **string** is a null pointer for `None`;
//! - an optional **scalar** (`u64`/`bool`) is carried as a `has_<field>`
//!   companion flag next to the raw field, meaningless when the flag is
//!   `false`.

use std::{
    ffi::{CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use crate::{
    abi::{free_array, free_c_string, guard, to_c_string, write_array},
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

/// Splits an optional `u64` into the `(has, value)` pair the C structs
/// below carry.
fn opt_u64(v: Option<u64>) -> (bool, u64) {
    v.map_or((false, 0), |x| (true, x))
}

/// Splits an optional `bool` into the `(has, value)` pair the C structs
/// below carry.
fn opt_bool(v: Option<bool>) -> (bool, bool) {
    v.map_or((false, false), |x| (true, x))
}

/// Converts an optional string to an owned, possibly-null `CString`.
fn opt_c_string(s: Option<&str>) -> Result<Option<CString>, AbiError> {
    s.map(to_c_string).transpose()
}

/// The raw pointer for an optional owned `CString`: null for `None`.
fn opt_into_raw(s: Option<CString>) -> *mut c_char {
    s.map_or(ptr::null_mut(), CString::into_raw)
}

/// One `ducklake_schema` row, as returned by [`moraine_dump_schemas`].
#[repr(C)]
pub struct MoraineSchemaRow {
    /// `schema_id`.
    pub schema_id: u64,
    /// `schema_uuid`, owned.
    pub schema_uuid: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present (`false` for a live/current row).
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `schema_name`, owned.
    pub schema_name: *mut c_char,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
}

/// Dumps every `ducklake_schema` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// Cancellable, like every dump in this module: races the core read
/// against [`moraine_interrupt`](crate::abi::moraine_interrupt)'s signal
/// and against `probe` (polled immediately, then ~100 ms; a null `probe`
/// disables polling). If a cancellation wins, returns
/// [`codes::INTERRUPTED`] and the out-params are left unwritten.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach`](crate::abi::moraine_attach) and not yet detached.
/// `out_items`/`out_len` must be valid, writable pointers. `probe`, if
/// non-null, must be safe to call with `probe_ctx` from any thread.
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schemas(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSchemaRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSchemaRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_schemas(&handle_ref.catalog),
            )
        }?;
        // Owned-first: every string in the whole batch converts before any
        // raw pointer is minted, so a partial failure leaks nothing.
        let owned = rows
            .into_iter()
            .map(|v| {
                let schema_uuid = to_c_string(&v.schema_uuid)?;
                let schema_name = to_c_string(&v.schema_name)?;
                let path = to_c_string(&v.path)?;
                Ok((v, schema_uuid, schema_name, path))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(v, schema_uuid, schema_name, path)| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                MoraineSchemaRow {
                    schema_id: v.schema_id,
                    schema_uuid: schema_uuid.into_raw(),
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    schema_name: schema_name.into_raw(),
                    path: path.into_raw(),
                    path_is_relative: v.path_is_relative,
                }
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

/// Frees an array returned by [`moraine_dump_schemas`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_schemas`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schemas_free(items: *mut MoraineSchemaRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.schema_uuid);
                free_c_string(d.schema_name);
                free_c_string(d.path);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_table` row, as returned by [`moraine_dump_tables`].
/// `next_column_id` is moraine-internal field-id bookkeeping, not a
/// DuckLake column, and is not carried here.
#[repr(C)]
pub struct MoraineTableRow {
    /// `table_id`.
    pub table_id: u64,
    /// `table_uuid`, owned.
    pub table_uuid: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `schema_id`.
    pub schema_id: u64,
    /// `table_name`, owned.
    pub table_name: *mut c_char,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
}

/// Dumps every `ducklake_table` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tables(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTableRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineTableRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_tables(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let table_uuid = to_c_string(&v.table_uuid)?;
                let table_name = to_c_string(&v.table_name)?;
                let path = to_c_string(&v.path)?;
                Ok((v, table_uuid, table_name, path))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|(v, table_uuid, table_name, path)| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                MoraineTableRow {
                    table_id: v.table_id,
                    table_uuid: table_uuid.into_raw(),
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    schema_id: v.schema_id,
                    table_name: table_name.into_raw(),
                    path: path.into_raw(),
                    path_is_relative: v.path_is_relative,
                }
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

/// Frees an array returned by [`moraine_dump_tables`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_tables`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tables_free(items: *mut MoraineTableRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.table_uuid);
                free_c_string(d.table_name);
                free_c_string(d.path);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_view` row, as returned by [`moraine_dump_views`].
#[repr(C)]
pub struct MoraineViewRow {
    /// `view_id`.
    pub view_id: u64,
    /// `view_uuid`, owned.
    pub view_uuid: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `schema_id`.
    pub schema_id: u64,
    /// `view_name`, owned.
    pub view_name: *mut c_char,
    /// `dialect`, owned.
    pub dialect: *mut c_char,
    /// `sql`, owned.
    pub sql: *mut c_char,
    /// `column_aliases`, owned, null if absent.
    pub column_aliases: *mut c_char,
}

/// Dumps every `ducklake_view` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_views(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineViewRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineViewRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_views(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let view_uuid = to_c_string(&v.view_uuid)?;
                let view_name = to_c_string(&v.view_name)?;
                let dialect = to_c_string(&v.dialect)?;
                let sql = to_c_string(&v.sql)?;
                let column_aliases = opt_c_string(v.column_aliases.as_deref())?;
                Ok((v, view_uuid, view_name, dialect, sql, column_aliases))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(v, view_uuid, view_name, dialect, sql, column_aliases)| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                MoraineViewRow {
                    view_id: v.view_id,
                    view_uuid: view_uuid.into_raw(),
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    schema_id: v.schema_id,
                    view_name: view_name.into_raw(),
                    dialect: dialect.into_raw(),
                    sql: sql.into_raw(),
                    column_aliases: opt_into_raw(column_aliases),
                }
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

/// Frees an array returned by [`moraine_dump_views`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_views`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_views_free(items: *mut MoraineViewRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.view_uuid);
                free_c_string(d.view_name);
                free_c_string(d.dialect);
                free_c_string(d.sql);
                free_c_string(d.column_aliases);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_macro` row, as returned by [`moraine_dump_macros`].
#[repr(C)]
pub struct MoraineMacroRow {
    /// `schema_id`.
    pub schema_id: u64,
    /// `macro_id`.
    pub macro_id: u64,
    /// `macro_name`, owned.
    pub macro_name: *mut c_char,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
}

/// Dumps every `ducklake_macro` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macros(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMacroRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineMacroRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_macros(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|m| {
                let macro_name = to_c_string(&m.macro_name)?;
                Ok((m, macro_name))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(m, macro_name)| {
                let (has_end, end) = opt_u64(m.end_snapshot);
                MoraineMacroRow {
                    schema_id: m.schema_id,
                    macro_id: m.macro_id,
                    macro_name: macro_name.into_raw(),
                    begin_snapshot: m.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                }
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

/// Frees an array returned by [`moraine_dump_macros`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_macros`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macros_free(items: *mut MoraineMacroRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.macro_name);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_macro_impl` row, as returned by
/// [`moraine_dump_macro_impls`].
#[repr(C)]
pub struct MoraineMacroImplRow {
    /// `macro_id`.
    pub macro_id: u64,
    /// `impl_id`.
    pub impl_id: u64,
    /// `dialect`, owned.
    pub dialect: *mut c_char,
    /// `sql`, owned.
    pub sql: *mut c_char,
    /// `type`, owned.
    pub macro_type: *mut c_char,
}

/// Dumps every `ducklake_macro_impl` row — flattened from the embedded
/// implementations of every macro record, history included, ordered by
/// `(macro_id, impl_id)`. DuckLake's macro reconstruction consumes rows
/// in served order.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_impls(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMacroImplRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineMacroImplRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let macros = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_macros(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = macros
            .iter()
            .flat_map(|m| m.implementations.iter().map(move |i| (m.macro_id, i)))
            .map(|(macro_id, i)| {
                let dialect = to_c_string(&i.dialect)?;
                let sql = to_c_string(&i.sql)?;
                let macro_type = to_c_string(&i.macro_type)?;
                Ok((macro_id, i.impl_id, dialect, sql, macro_type))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(
                |(macro_id, impl_id, dialect, sql, macro_type)| MoraineMacroImplRow {
                    macro_id,
                    impl_id,
                    dialect: dialect.into_raw(),
                    sql: sql.into_raw(),
                    macro_type: macro_type.into_raw(),
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

/// Frees an array returned by [`moraine_dump_macro_impls`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_macro_impls`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_impls_free(
    items: *mut MoraineMacroImplRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.dialect);
                free_c_string(d.sql);
                free_c_string(d.macro_type);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_macro_parameters` row, as returned by
/// [`moraine_dump_macro_parameters`].
#[repr(C)]
pub struct MoraineMacroParameterRow {
    /// `macro_id`.
    pub macro_id: u64,
    /// `impl_id`.
    pub impl_id: u64,
    /// `column_id`: the parameter's 0-based position within its impl.
    pub column_id: u64,
    /// `parameter_name`, owned.
    pub parameter_name: *mut c_char,
    /// `parameter_type`, owned.
    pub parameter_type: *mut c_char,
    /// `default_value`, owned, null if absent.
    pub default_value: *mut c_char,
    /// `default_value_type`, owned.
    pub default_value_type: *mut c_char,
}

/// Dumps every `ducklake_macro_parameters` row — flattened from the
/// embedded parameters of every implementation, history included, ordered
/// by `(macro_id, impl_id, column_id)`. DuckLake's macro reconstruction
/// consumes rows in served order.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_parameters(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineMacroParameterRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineMacroParameterRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let macros = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_macros(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = macros
            .iter()
            .flat_map(|m| {
                m.implementations
                    .iter()
                    .flat_map(move |i| i.parameters.iter().map(move |p| (m.macro_id, i.impl_id, p)))
            })
            .map(|(macro_id, impl_id, p)| {
                let parameter_name = to_c_string(&p.parameter_name)?;
                let parameter_type = to_c_string(&p.parameter_type)?;
                let default_value = opt_c_string(p.default_value.as_deref())?;
                let default_value_type = to_c_string(&p.default_value_type)?;
                Ok((
                    macro_id,
                    impl_id,
                    p.column_id,
                    parameter_name,
                    parameter_type,
                    default_value,
                    default_value_type,
                ))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(
                |(
                    macro_id,
                    impl_id,
                    column_id,
                    parameter_name,
                    parameter_type,
                    default_value,
                    default_value_type,
                )| MoraineMacroParameterRow {
                    macro_id,
                    impl_id,
                    column_id,
                    parameter_name: parameter_name.into_raw(),
                    parameter_type: parameter_type.into_raw(),
                    default_value: opt_into_raw(default_value),
                    default_value_type: default_value_type.into_raw(),
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

/// Frees an array returned by [`moraine_dump_macro_parameters`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_macro_parameters`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_macro_parameters_free(
    items: *mut MoraineMacroParameterRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.parameter_name);
                free_c_string(d.parameter_type);
                free_c_string(d.default_value);
                free_c_string(d.default_value_type);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_column_mapping` row, as returned by
/// [`moraine_dump_column_mappings`].
#[repr(C)]
pub struct MoraineColumnMappingRow {
    /// `mapping_id`.
    pub mapping_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `type`, owned.
    pub map_type: *mut c_char,
}

/// Dumps every `ducklake_column_mapping` row into
/// `*out_items`/`*out_len`. Mappings are unversioned create-only records,
/// so this is exactly the live rows.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_mappings(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineColumnMappingRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineColumnMappingRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_mappings(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|m| {
                let map_type = to_c_string(&m.map_type)?;
                Ok((m, map_type))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(m, map_type)| MoraineColumnMappingRow {
                mapping_id: m.mapping_id,
                table_id: m.table_id,
                map_type: map_type.into_raw(),
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

/// Frees an array returned by [`moraine_dump_column_mappings`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_column_mappings`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_mappings_free(
    items: *mut MoraineColumnMappingRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.map_type);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_name_mapping` row, as returned by
/// [`moraine_dump_name_mappings`].
#[repr(C)]
pub struct MoraineNameMappingRow {
    /// `mapping_id`.
    pub mapping_id: u64,
    /// `column_id`: the row's 0-based ordinal within its mapping.
    pub column_id: u64,
    /// `source_name`, owned.
    pub source_name: *mut c_char,
    /// `target_field_id`.
    pub target_field_id: u64,
    /// Whether `parent_column` is present.
    pub has_parent_column: bool,
    /// `parent_column`, valid iff `has_parent_column`.
    pub parent_column: u64,
    /// `is_partition`.
    pub is_partition: bool,
}

/// Dumps every `ducklake_name_mapping` row — flattened from the embedded
/// rows of every mapping record, ordered by `(mapping_id, column_id)` —
/// into `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_name_mappings(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineNameMappingRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineNameMappingRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let mappings = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_mappings(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = mappings
            .iter()
            .flat_map(|m| m.name_mappings.iter().map(move |row| (m.mapping_id, row)))
            .map(|(mapping_id, row)| {
                let source_name = to_c_string(&row.source_name)?;
                Ok((mapping_id, row, source_name))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(mapping_id, row, source_name)| {
                let (has_parent, parent) = opt_u64(row.parent_column);
                MoraineNameMappingRow {
                    mapping_id,
                    column_id: row.column_id,
                    source_name: source_name.into_raw(),
                    target_field_id: row.target_field_id,
                    has_parent_column: has_parent,
                    parent_column: parent,
                    is_partition: row.is_partition,
                }
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

/// Frees an array returned by [`moraine_dump_name_mappings`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_name_mappings`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_name_mappings_free(
    items: *mut MoraineNameMappingRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.source_name);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_column` row, as returned by [`moraine_dump_columns`].
/// Column tags (`ducklake_column_tag`) are a separate table, not a
/// column, and are not carried here.
#[repr(C)]
pub struct MoraineColumnRow {
    /// `column_id`.
    pub column_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `column_order`.
    pub column_order: u64,
    /// `column_name`, owned.
    pub column_name: *mut c_char,
    /// `column_type`, owned.
    pub column_type: *mut c_char,
    /// `initial_default`, owned, null if absent.
    pub initial_default: *mut c_char,
    /// `default_value`, owned, null if absent.
    pub default_value: *mut c_char,
    /// `nulls_allowed`.
    pub nulls_allowed: bool,
    /// Whether `parent_column` is present.
    pub has_parent_column: bool,
    /// `parent_column`, valid iff `has_parent_column`.
    pub parent_column: u64,
    /// `default_value_type`, owned, null if absent.
    pub default_value_type: *mut c_char,
    /// `default_value_dialect`, owned, null if absent.
    pub default_value_dialect: *mut c_char,
}

/// Dumps every `ducklake_column` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_columns(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineColumnRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineColumnRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_columns(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let column_name = to_c_string(&v.column_name)?;
                let column_type = to_c_string(&v.column_type)?;
                let initial_default = opt_c_string(v.initial_default.as_deref())?;
                let default_value = opt_c_string(v.default_value.as_deref())?;
                let default_value_type = opt_c_string(v.default_value_type.as_deref())?;
                let default_value_dialect = opt_c_string(v.default_value_dialect.as_deref())?;
                Ok((
                    v,
                    column_name,
                    column_type,
                    initial_default,
                    default_value,
                    default_value_type,
                    default_value_dialect,
                ))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(
                |(
                    v,
                    column_name,
                    column_type,
                    initial_default,
                    default_value,
                    default_value_type,
                    default_value_dialect,
                )| {
                    let (has_end, end) = opt_u64(v.end_snapshot);
                    let (has_parent, parent) = opt_u64(v.parent_column);

                    MoraineColumnRow {
                        column_id: v.column_id,
                        begin_snapshot: v.begin_snapshot,
                        has_end_snapshot: has_end,
                        end_snapshot: end,
                        table_id: v.table_id,
                        column_order: v.column_order,
                        column_name: column_name.into_raw(),
                        column_type: column_type.into_raw(),
                        initial_default: opt_into_raw(initial_default),
                        default_value: opt_into_raw(default_value),
                        nulls_allowed: v.nulls_allowed,
                        has_parent_column: has_parent,
                        parent_column: parent,
                        default_value_type: opt_into_raw(default_value_type),
                        default_value_dialect: opt_into_raw(default_value_dialect),
                    }
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

/// Frees an array returned by [`moraine_dump_columns`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_columns`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_columns_free(items: *mut MoraineColumnRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.column_name);
                free_c_string(d.column_type);
                free_c_string(d.initial_default);
                free_c_string(d.default_value);
                free_c_string(d.default_value_type);
                free_c_string(d.default_value_dialect);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_data_file` row, as returned by
/// [`moraine_dump_data_files`]. Per-file partition values
/// (`ducklake_file_partition_value`) are a separate table and not
/// carried here.
#[repr(C)]
pub struct MoraineDataFileRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// Whether `file_order` is present.
    pub has_file_order: bool,
    /// `file_order`, valid iff `has_file_order`.
    pub file_order: u64,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
    /// `file_format`, owned.
    pub file_format: *mut c_char,
    /// `record_count`.
    pub record_count: u64,
    /// `file_size_bytes`.
    pub file_size_bytes: u64,
    /// `footer_size`.
    pub footer_size: u64,
    /// Whether `row_id_start` is present (absent when the file's rows
    /// carry explicit per-row ids, e.g. compaction outputs).
    pub has_row_id_start: bool,
    /// `row_id_start`, valid iff `has_row_id_start`.
    pub row_id_start: u64,
    /// Whether `partition_id` is present.
    pub has_partition_id: bool,
    /// `partition_id`, valid iff `has_partition_id`.
    pub partition_id: u64,
    /// `encryption_key`, owned, null if absent.
    pub encryption_key: *mut c_char,
    /// Whether `mapping_id` is present.
    pub has_mapping_id: bool,
    /// `mapping_id`, valid iff `has_mapping_id`.
    pub mapping_id: u64,
    /// Whether `partial_max` is present.
    pub has_partial_max: bool,
    /// `partial_max`, valid iff `has_partial_max`.
    pub partial_max: u64,
}

/// Dumps every `ducklake_data_file` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_data_files(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineDataFileRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineDataFileRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_data_files(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let path = to_c_string(&v.path)?;
                let file_format = to_c_string(&v.file_format)?;
                let encryption_key = opt_c_string(v.encryption_key.as_deref())?;
                Ok((v, path, file_format, encryption_key))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(v, path, file_format, encryption_key)| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                let (has_order, order) = opt_u64(v.file_order);
                let (has_partition, partition) = opt_u64(v.partition_id);
                let (has_mapping, mapping) = opt_u64(v.mapping_id);
                let (has_partial_max, partial_max) = opt_u64(v.partial_max);

                MoraineDataFileRow {
                    data_file_id: v.data_file_id,
                    table_id: v.table_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    has_file_order: has_order,
                    file_order: order,
                    path: path.into_raw(),
                    path_is_relative: v.path_is_relative,
                    file_format: file_format.into_raw(),
                    record_count: v.record_count,
                    file_size_bytes: v.file_size_bytes,
                    footer_size: v.footer_size,
                    has_row_id_start: v.row_id_start.is_some(),
                    row_id_start: v.row_id_start.unwrap_or_default(),
                    has_partition_id: has_partition,
                    partition_id: partition,
                    encryption_key: opt_into_raw(encryption_key),
                    has_mapping_id: has_mapping,
                    mapping_id: mapping,
                    has_partial_max,
                    partial_max,
                }
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

/// Frees an array returned by [`moraine_dump_data_files`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_data_files`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_data_files_free(items: *mut MoraineDataFileRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.path);
                free_c_string(d.file_format);
                free_c_string(d.encryption_key);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_delete_file` row, as returned by
/// [`moraine_dump_delete_files`].
#[repr(C)]
pub struct MoraineDeleteFileRow {
    /// `delete_file_id`.
    pub delete_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
    /// `format`, owned.
    pub format: *mut c_char,
    /// `delete_count`.
    pub delete_count: u64,
    /// `file_size_bytes`.
    pub file_size_bytes: u64,
    /// `footer_size`.
    pub footer_size: u64,
    /// `encryption_key`, owned, null if absent.
    pub encryption_key: *mut c_char,
    /// Whether `partial_max` is present.
    pub has_partial_max: bool,
    /// `partial_max`, valid iff `has_partial_max`.
    pub partial_max: u64,
}

/// Dumps every `ducklake_delete_file` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_delete_files(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineDeleteFileRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineDeleteFileRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_delete_files(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let path = to_c_string(&v.path)?;
                let format = to_c_string(&v.format)?;
                let encryption_key = opt_c_string(v.encryption_key.as_deref())?;
                Ok((v, path, format, encryption_key))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|(v, path, format, encryption_key)| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                let (has_partial_max, partial_max) = opt_u64(v.partial_max);
                MoraineDeleteFileRow {
                    delete_file_id: v.delete_file_id,
                    table_id: v.table_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    data_file_id: v.data_file_id,
                    path: path.into_raw(),
                    path_is_relative: v.path_is_relative,
                    format: format.into_raw(),
                    delete_count: v.delete_count,
                    file_size_bytes: v.file_size_bytes,
                    footer_size: v.footer_size,
                    encryption_key: opt_into_raw(encryption_key),
                    has_partial_max,
                    partial_max,
                }
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

/// Frees an array returned by [`moraine_dump_delete_files`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_delete_files`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_delete_files_free(
    items: *mut MoraineDeleteFileRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.path);
                free_c_string(d.format);
                free_c_string(d.encryption_key);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_table_stats` row, as returned by
/// [`moraine_dump_table_stats`]. Unversioned — no lifecycle columns.
#[repr(C)]
pub struct MoraineTableStatsRow {
    /// `table_id`.
    pub table_id: u64,
    /// `record_count`.
    pub record_count: u64,
    /// `next_row_id`.
    pub next_row_id: u64,
    /// `file_size_bytes`.
    pub file_size_bytes: u64,
}

/// Dumps every `ducklake_table_stats` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_table_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTableStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineTableStatsRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_table_stats(&handle_ref.catalog),
            )
        }?;
        Ok(rows
            .into_iter()
            .map(|v| MoraineTableStatsRow {
                table_id: v.table_id,
                record_count: v.record_count,
                next_row_id: v.next_row_id,
                file_size_bytes: v.file_size_bytes,
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

/// Frees an array returned by [`moraine_dump_table_stats`]. No owned
/// strings inside — releases only the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_table_stats`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_table_stats_free(
    items: *mut MoraineTableStatsRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_table_column_stats` row, as returned by
/// [`moraine_dump_table_column_stats`]. Unversioned.
#[repr(C)]
pub struct MoraineTableColumnStatsRow {
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// Whether `contains_null` is present.
    pub has_contains_null: bool,
    /// `contains_null`, valid iff `has_contains_null`.
    pub contains_null: bool,
    /// Whether `contains_nan` is present.
    pub has_contains_nan: bool,
    /// `contains_nan`, valid iff `has_contains_nan`.
    pub contains_nan: bool,
    /// `min_value`, owned, null if absent.
    pub min_value: *mut c_char,
    /// `max_value`, owned, null if absent.
    pub max_value: *mut c_char,
    /// `extra_stats`, owned, null if absent.
    pub extra_stats: *mut c_char,
}

/// Dumps every `ducklake_table_column_stats` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_table_column_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTableColumnStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineTableColumnStatsRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_table_column_stats(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let min_value = opt_c_string(v.min_value.as_deref())?;
                let max_value = opt_c_string(v.max_value.as_deref())?;
                let extra_stats = opt_c_string(v.extra_stats.as_deref())?;
                Ok((v, min_value, max_value, extra_stats))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|(v, min_value, max_value, extra_stats)| {
                let (has_null, contains_null) = opt_bool(v.contains_null);
                let (has_nan, contains_nan) = opt_bool(v.contains_nan);
                MoraineTableColumnStatsRow {
                    table_id: v.table_id,
                    column_id: v.column_id,
                    has_contains_null: has_null,
                    contains_null,
                    has_contains_nan: has_nan,
                    contains_nan,
                    min_value: opt_into_raw(min_value),
                    max_value: opt_into_raw(max_value),
                    extra_stats: opt_into_raw(extra_stats),
                }
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

/// Frees an array returned by [`moraine_dump_table_column_stats`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_table_column_stats`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_table_column_stats_free(
    items: *mut MoraineTableColumnStatsRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.min_value);
                free_c_string(d.max_value);
                free_c_string(d.extra_stats);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_file_column_stats` row, as returned by
/// [`moraine_dump_file_column_stats`]. Unversioned. Variant stats
/// (`ducklake_file_variant_stats`) are a separate table and not carried
/// here.
#[repr(C)]
pub struct MoraineFileColumnStatsRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `column_size_bytes`.
    pub column_size_bytes: u64,
    /// `value_count`.
    pub value_count: u64,
    /// `null_count`.
    pub null_count: u64,
    /// `min_value`, owned, null if absent.
    pub min_value: *mut c_char,
    /// `max_value`, owned, null if absent.
    pub max_value: *mut c_char,
    /// Whether `contains_nan` is present.
    pub has_contains_nan: bool,
    /// `contains_nan`, valid iff `has_contains_nan`.
    pub contains_nan: bool,
    /// `extra_stats`, owned, null if absent.
    pub extra_stats: *mut c_char,
}

/// Dumps every `ducklake_file_column_stats` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_column_stats(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineFileColumnStatsRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineFileColumnStatsRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_file_column_stats(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|v| {
                let min_value = opt_c_string(v.min_value.as_deref())?;
                let max_value = opt_c_string(v.max_value.as_deref())?;
                let extra_stats = opt_c_string(v.extra_stats.as_deref())?;
                Ok((v, min_value, max_value, extra_stats))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|(v, min_value, max_value, extra_stats)| {
                let (has_nan, contains_nan) = opt_bool(v.contains_nan);
                MoraineFileColumnStatsRow {
                    data_file_id: v.data_file_id,
                    table_id: v.table_id,
                    column_id: v.column_id,
                    column_size_bytes: v.column_size_bytes,
                    value_count: v.value_count,
                    null_count: v.null_count,
                    min_value: opt_into_raw(min_value),
                    max_value: opt_into_raw(max_value),
                    has_contains_nan: has_nan,
                    contains_nan,
                    extra_stats: opt_into_raw(extra_stats),
                }
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

/// Frees an array returned by [`moraine_dump_file_column_stats`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_file_column_stats`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_column_stats_free(
    items: *mut MoraineFileColumnStatsRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.min_value);
                free_c_string(d.max_value);
                free_c_string(d.extra_stats);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_snapshot`/`ducklake_snapshot_changes` row (merged), as
/// returned by [`moraine_dump_snapshots`]. `next_deletion_id` is
/// moraine-internal gc-file counter bookkeeping, not a DuckLake column,
/// and is not carried here.
#[repr(C)]
pub struct MoraineSnapshotRow {
    /// `snapshot_id`.
    pub snapshot_id: u64,
    /// `snapshot_time`, microseconds since the Unix epoch (UTC).
    pub snapshot_time_micros: i64,
    /// `schema_version`.
    pub schema_version: u64,
    /// `next_catalog_id`.
    pub next_catalog_id: u64,
    /// `next_file_id`.
    pub next_file_id: u64,
    /// `changes_made`, owned.
    pub changes_made: *mut c_char,
    /// `author`, owned, null if absent.
    pub author: *mut c_char,
    /// `commit_message`, owned, null if absent.
    pub commit_message: *mut c_char,
    /// `commit_extra_info`, owned, null if absent.
    pub commit_extra_info: *mut c_char,
}

/// One snapshot record's fields, in `MoraineSnapshotRow` order — the
/// nameable shape the wire value maps into (its own type is internal to
/// the core crate).
pub(crate) type SnapshotRowFields = (
    u64,
    i64,
    u64,
    u64,
    u64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// Converts snapshot records to C rows, owned-first (see
/// `moraine_dump_schemas`): every string in the whole batch converts
/// before any raw pointer is minted.
pub(crate) fn snapshot_rows(
    rows: Vec<SnapshotRowFields>,
) -> Result<Vec<MoraineSnapshotRow>, AbiError> {
    let owned = rows
        .into_iter()
        .map(|v| {
            let changes_made = to_c_string(&v.5)?;
            let author = opt_c_string(v.6.as_deref())?;
            let commit_message = opt_c_string(v.7.as_deref())?;
            let commit_extra_info = opt_c_string(v.8.as_deref())?;
            Ok((v, changes_made, author, commit_message, commit_extra_info))
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok(owned
        .into_iter()
        .map(
            |(v, changes_made, author, commit_message, commit_extra_info)| MoraineSnapshotRow {
                snapshot_id: v.0,
                snapshot_time_micros: v.1,
                schema_version: v.2,
                next_catalog_id: v.3,
                next_file_id: v.4,
                changes_made: changes_made.into_raw(),
                author: opt_into_raw(author),
                commit_message: opt_into_raw(commit_message),
                commit_extra_info: opt_into_raw(commit_extra_info),
            },
        )
        .collect())
}

/// Dumps every `ducklake_snapshot` row into `*out_items`/`*out_len`.
/// Snapshots carry no begin/end lifecycle of their own — this is the
/// full committed history, not a current/history split.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_snapshots(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSnapshotRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSnapshotRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_snapshots(&handle_ref.catalog),
            )
        }?;
        snapshot_rows(
            rows.into_iter()
                .map(|v| {
                    (
                        v.snapshot_id,
                        v.snapshot_time_micros,
                        v.schema_version,
                        v.next_catalog_id,
                        v.next_file_id,
                        v.changes_made,
                        v.author,
                        v.commit_message,
                        v.commit_extra_info,
                    )
                })
                .collect(),
        )
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

/// Frees an array returned by [`moraine_dump_snapshots`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_snapshots`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_snapshots_free(items: *mut MoraineSnapshotRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |d| {
                free_c_string(d.changes_made);
                free_c_string(d.author);
                free_c_string(d.commit_message);
                free_c_string(d.commit_extra_info);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_schema_versions` row, as returned by
/// [`moraine_dump_schema_versions`]: DuckLake's per-table schema-change
/// history, flattened from the snapshot records it folds into (see
/// `moraine::ffi_support::SchemaVersionRow`).
#[repr(C)]
pub struct MoraineSchemaVersionRow {
    /// `begin_snapshot` — the snapshot the schema change landed in.
    pub begin_snapshot: u64,
    /// `schema_version` — that snapshot's schema version.
    pub schema_version: u64,
    /// `table_id` — the created-or-schema-altered table.
    pub table_id: u64,
}

/// Dumps every `ducklake_schema_versions` row into
/// `*out_items`/`*out_len`, in snapshot order.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schema_versions(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSchemaVersionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSchemaVersionRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_schema_versions(&handle_ref.catalog),
            )
        }?;
        Ok(rows
            .into_iter()
            .map(|v| MoraineSchemaVersionRow {
                begin_snapshot: v.begin_snapshot,
                schema_version: v.schema_version,
                table_id: v.table_id,
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

/// Frees an array returned by [`moraine_dump_schema_versions`]. No owned
/// strings inside — releases only the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_schema_versions`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schema_versions_free(
    items: *mut MoraineSchemaVersionRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_partition_info` row, as returned by
/// [`moraine_dump_partition_info`]. Partition columns
/// (`ducklake_partition_column`) are a separate table served by
/// [`moraine_dump_partition_columns`].
#[repr(C)]
pub struct MorainePartitionInfoRow {
    /// `partition_id`.
    pub partition_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
}

/// Dumps every `ducklake_partition_info` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_partition_info(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MorainePartitionInfoRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MorainePartitionInfoRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_partition_info(&handle_ref.catalog),
            )
        }?;

        Ok(rows
            .into_iter()
            .map(|v| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                MorainePartitionInfoRow {
                    partition_id: v.partition_id,
                    table_id: v.table_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                }
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

/// Frees an array returned by [`moraine_dump_partition_info`]. No owned
/// strings inside — releases only the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_partition_info`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_partition_info_free(
    items: *mut MorainePartitionInfoRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_partition_column` row, as returned by
/// [`moraine_dump_partition_columns`] — flattened from the partition
/// record's embedded columns.
#[repr(C)]
pub struct MorainePartitionColumnRow {
    /// `partition_id`.
    pub partition_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `partition_key_index`.
    pub partition_key_index: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `transform`, owned.
    pub transform: *mut c_char,
}

/// Dumps every `ducklake_partition_column` row — one per embedded column
/// of every partition record, current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_partition_columns(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MorainePartitionColumnRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MorainePartitionColumnRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let specs = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_partition_info(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = specs
            .into_iter()
            .flat_map(|spec| {
                spec.columns
                    .into_iter()
                    .map(move |column| (spec.partition_id, spec.table_id, column))
            })
            .map(|(partition_id, table_id, column)| {
                let transform = to_c_string(&column.transform)?;
                Ok((partition_id, table_id, column, transform))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(
                |(partition_id, table_id, column, transform)| MorainePartitionColumnRow {
                    partition_id,
                    table_id,
                    partition_key_index: column.partition_key_index,
                    column_id: column.column_id,
                    transform: transform.into_raw(),
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

/// Frees an array returned by [`moraine_dump_partition_columns`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_partition_columns`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_partition_columns_free(
    items: *mut MorainePartitionColumnRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |c| {
                free_c_string(c.transform);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_file_partition_value` row, as returned by
/// [`moraine_dump_file_partition_values`] — flattened from the data-file
/// record's embedded partition values.
#[repr(C)]
pub struct MoraineFilePartitionValueRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `partition_key_index`.
    pub partition_key_index: u64,
    /// `partition_value`, owned.
    pub partition_value: *mut c_char,
}

/// Dumps every `ducklake_file_partition_value` row — one per embedded
/// partition value of every data-file record, current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_partition_values(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineFilePartitionValueRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineFilePartitionValueRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let files = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_data_files(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = files
            .into_iter()
            .flat_map(|file| {
                file.partition_values
                    .into_iter()
                    .map(move |value| (file.data_file_id, file.table_id, value))
            })
            .map(|(data_file_id, table_id, value)| {
                let partition_value = to_c_string(&value.partition_value)?;
                Ok((
                    data_file_id,
                    table_id,
                    value.partition_key_index,
                    partition_value,
                ))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(
                |(data_file_id, table_id, partition_key_index, partition_value)| {
                    MoraineFilePartitionValueRow {
                        data_file_id,
                        table_id,
                        partition_key_index,
                        partition_value: partition_value.into_raw(),
                    }
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

/// Frees an array returned by [`moraine_dump_file_partition_values`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_file_partition_values`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_file_partition_values_free(
    items: *mut MoraineFilePartitionValueRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |v| {
                free_c_string(v.partition_value);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_sort_info` row, as returned by
/// [`moraine_dump_sort_info`]. Sort expressions
/// (`ducklake_sort_expression`) are a separate table served by
/// [`moraine_dump_sort_expressions`].
#[repr(C)]
pub struct MoraineSortInfoRow {
    /// `sort_id`.
    pub sort_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
}

/// Dumps every `ducklake_sort_info` row — current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_sort_info(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSortInfoRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSortInfoRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_sort_info(&handle_ref.catalog),
            )
        }?;

        Ok(rows
            .into_iter()
            .map(|v| {
                let (has_end, end) = opt_u64(v.end_snapshot);
                MoraineSortInfoRow {
                    sort_id: v.sort_id,
                    table_id: v.table_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                }
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

/// Frees an array returned by [`moraine_dump_sort_info`]. No owned
/// strings inside — releases only the backing allocation.
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_sort_info`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_sort_info_free(items: *mut MoraineSortInfoRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |_| {});
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_sort_expression` row, as returned by
/// [`moraine_dump_sort_expressions`] — flattened from the sort record's
/// embedded expressions.
#[repr(C)]
pub struct MoraineSortExpressionRow {
    /// `sort_id`.
    pub sort_id: u64,
    /// `table_id`.
    pub table_id: u64,
    /// `sort_key_index`.
    pub sort_key_index: u64,
    /// `expression`, owned.
    pub expression: *mut c_char,
    /// `dialect`, owned.
    pub dialect: *mut c_char,
    /// `sort_direction`, owned.
    pub sort_direction: *mut c_char,
    /// `null_order`, owned.
    pub null_order: *mut c_char,
}

/// Dumps every `ducklake_sort_expression` row — one per embedded
/// expression of every sort record, current and history — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_sort_expressions(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSortExpressionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSortExpressionRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let specs = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_sort_info(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = specs
            .into_iter()
            .flat_map(|spec| {
                spec.expressions
                    .into_iter()
                    .map(move |expression| (spec.sort_id, spec.table_id, expression))
            })
            .map(|(sort_id, table_id, e)| {
                let expression = to_c_string(&e.expression)?;
                let dialect = to_c_string(&e.dialect)?;
                let sort_direction = to_c_string(&e.sort_direction)?;
                let null_order = to_c_string(&e.null_order)?;
                Ok((
                    sort_id,
                    table_id,
                    e.sort_key_index,
                    expression,
                    dialect,
                    sort_direction,
                    null_order,
                ))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(
                |(
                    sort_id,
                    table_id,
                    sort_key_index,
                    expression,
                    dialect,
                    sort_direction,
                    null_order,
                )| {
                    MoraineSortExpressionRow {
                        sort_id,
                        table_id,
                        sort_key_index,
                        expression: expression.into_raw(),
                        dialect: dialect.into_raw(),
                        sort_direction: sort_direction.into_raw(),
                        null_order: null_order.into_raw(),
                    }
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

/// Frees an array returned by [`moraine_dump_sort_expressions`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_sort_expressions`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_sort_expressions_free(
    items: *mut MoraineSortExpressionRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |e| {
                free_c_string(e.expression);
                free_c_string(e.dialect);
                free_c_string(e.sort_direction);
                free_c_string(e.null_order);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_files_scheduled_for_deletion` row, as returned by
/// [`moraine_dump_scheduled_deletions`].
#[repr(C)]
pub struct MoraineScheduledDeletionRow {
    /// `data_file_id`.
    pub data_file_id: u64,
    /// `path`, owned.
    pub path: *mut c_char,
    /// `path_is_relative`.
    pub path_is_relative: bool,
    /// `schedule_start`, microseconds since epoch (UTC).
    pub schedule_start_micros: i64,
}

/// Dumps every `ducklake_files_scheduled_for_deletion` row into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_scheduled_deletions(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineScheduledDeletionRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineScheduledDeletionRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_scheduled_deletions(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|row| {
                let path = to_c_string(&row.path)?;
                Ok((row, path))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(row, path)| MoraineScheduledDeletionRow {
                data_file_id: row.data_file_id,
                path: path.into_raw(),
                path_is_relative: row.path_is_relative,
                schedule_start_micros: row.schedule_start_micros,
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

/// Frees an array returned by [`moraine_dump_scheduled_deletions`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_scheduled_deletions`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_scheduled_deletions_free(
    items: *mut MoraineScheduledDeletionRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |r| {
                free_c_string(r.path);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_tag` row, as returned by [`moraine_dump_tags`] —
/// flattened from the object's container record; ended entries included,
/// lifecycle carried verbatim.
#[repr(C)]
pub struct MoraineTagRow {
    /// `object_id`.
    pub object_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `key`, owned.
    pub key: *mut c_char,
    /// `value`, owned.
    pub value: *mut c_char,
}

/// Dumps every `ducklake_tag` row into `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tags(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineTagRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_tags(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|row| {
                let key = to_c_string(&row.key)?;
                let value = to_c_string(&row.value)?;
                Ok((row, key, value))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(row, key, value)| {
                let (has_end, end) = opt_u64(row.end_snapshot);
                MoraineTagRow {
                    object_id: row.object_id,
                    begin_snapshot: row.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    key: key.into_raw(),
                    value: value.into_raw(),
                }
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

/// Frees an array returned by [`moraine_dump_tags`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_tags`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_tags_free(items: *mut MoraineTagRow, len: usize) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |t| {
                free_c_string(t.key);
                free_c_string(t.value);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// One `ducklake_column_tag` row, as returned by
/// [`moraine_dump_column_tags`] — flattened from the column's latest
/// record (a version transition carries entries forward, so only the
/// latest record's set is emitted).
#[repr(C)]
pub struct MoraineColumnTagRow {
    /// `table_id`.
    pub table_id: u64,
    /// `column_id`.
    pub column_id: u64,
    /// `begin_snapshot`.
    pub begin_snapshot: u64,
    /// Whether `end_snapshot` is present.
    pub has_end_snapshot: bool,
    /// `end_snapshot`, valid iff `has_end_snapshot`.
    pub end_snapshot: u64,
    /// `key`, owned.
    pub key: *mut c_char,
    /// `value`, owned.
    pub value: *mut c_char,
}

/// Dumps every `ducklake_column_tag` row into `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_tags(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineColumnTagRow,
    out_len: *mut usize,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineColumnTagRow>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        // SAFETY: `probe`/`probe_ctx` validity is this function's own
        // safety contract.
        let rows = unsafe {
            handle_ref.block_on_cancellable(
                probe,
                probe_ctx,
                moraine::ffi_support::dump_column_tags(&handle_ref.catalog),
            )
        }?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned = rows
            .into_iter()
            .map(|row| {
                let key = to_c_string(&row.key)?;
                let value = to_c_string(&row.value)?;
                Ok((row, key, value))
            })
            .collect::<Result<Vec<_>, AbiError>>()?;

        Ok(owned
            .into_iter()
            .map(|(row, key, value)| {
                let (has_end, end) = opt_u64(row.end_snapshot);
                MoraineColumnTagRow {
                    table_id: row.table_id,
                    column_id: row.column_id,
                    begin_snapshot: row.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    key: key.into_raw(),
                    value: value.into_raw(),
                }
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

/// Frees an array returned by [`moraine_dump_column_tags`].
///
/// # Safety
///
/// `items`/`len` must be exactly the pointer and length written by a
/// matching [`moraine_dump_column_tags`] call, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_column_tags_free(
    items: *mut MoraineColumnTagRow,
    len: usize,
) {
    let attempt = || {
        // SAFETY: caller contract above.
        unsafe {
            free_array(items, len, |t| {
                free_c_string(t.key);
                free_c_string(t.value);
            });
        }
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::CStr,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use moraine::{ColumnDef, ColumnStats, DataFile, DeleteFile, FileColumnStats};
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::abi::{moraine_attach, moraine_detach, moraine_error_free};

    /// A directory under the OS temp dir, unique per call, removed on
    /// drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "moraine-duckdb-dumps-{tag}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Seeds a catalog whose second commit renames the table `orders`,
    /// so `ducklake_table` carries one history row (the old name, ended)
    /// alongside its new current row — the fixture every dump test below
    /// exercises. Also carries a schema, two columns, a data file, a
    /// delete file, a view, and every statistics kind.
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
                    let column = tx.columns_of(table)[0].id;
                    let file = tx.register_data_file(
                        table,
                        DataFile {
                            path: "orders/data-1.parquet".into(),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: 10,
                            file_size_bytes: 1024,
                            footer_size: 64,
                            encryption_key: Some("a2V5LWRhdGE=".into()),
                            column_stats: vec![FileColumnStats {
                                column_id: column,
                                column_size_bytes: 100,
                                value_count: 10,
                                null_count: 0,
                                min_value: Some("1".into()),
                                max_value: Some("10".into()),
                                contains_nan: None,
                                extra_stats: None,
                            }],
                        },
                    )?;
                    tx.register_delete_file(
                        table,
                        DeleteFile {
                            data_file_id: file,
                            path: "orders/delete-1.parquet".into(),
                            path_is_relative: true,
                            format: "parquet".into(),
                            delete_count: 2,
                            file_size_bytes: 128,
                            footer_size: 32,
                            encryption_key: Some("a2V5LWRlbA==".into()),
                        },
                    )?;
                    tx.update_column_stats(
                        table,
                        column,
                        ColumnStats {
                            contains_null: Some(false),
                            contains_nan: None,
                            min_value: Some("1".into()),
                            max_value: Some("10".into()),
                            extra_stats: None,
                        },
                    )?;
                    tx.create_view(schema, "orders_v", "duckdb", "select * from orders")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            catalog
                .commit(|tx| {
                    let table = tx.tables_in(tx.schemas()[1].id)[0].id;
                    tx.rename_table(table, "orders2")
                })
                .await
                .expect("test setup: rename table");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// Seeds a schema + table, then tags both the table and its first
    /// column over the staged-row path — the only writer for tags, as in
    /// production (DuckLake's `COMMENT ON` batch).
    fn seed_with_tags(dir: &Path) {
        use moraine::ffi_support::staged::{Cell, RowOperation, TableKind, staged_begin};

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
                    tx.create_table(
                        schema,
                        "orders",
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

            // Schema `sales` took global id 1, table `orders` id 2; its
            // first column has per-table field id 1.
            let mut tx = staged_begin(&catalog)
                .await
                .expect("test setup: begin staged tx");
            tx.stage(RowOperation::Insert {
                table: TableKind::Tag,
                cells: vec![
                    Cell::U64(2),
                    Cell::U64(2),
                    Cell::Null,
                    Cell::Str("comment".into()),
                    Cell::Str("our table".into()),
                ],
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::ColumnTag,
                cells: vec![
                    Cell::U64(2),
                    Cell::U64(1),
                    Cell::U64(2),
                    Cell::Null,
                    Cell::Str("comment".into()),
                    Cell::Str("our column".into()),
                ],
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::Snapshot,
                cells: vec![
                    Cell::U64(2),
                    Cell::I64(1),
                    Cell::U64(1),
                    Cell::U64(3),
                    Cell::U64(0),
                ],
            });
            tx.stage(RowOperation::Insert {
                table: TableKind::SnapshotChanges,
                cells: vec![
                    Cell::U64(2),
                    Cell::Str("altered_table:2".into()),
                    Cell::Null,
                    Cell::Null,
                    Cell::Null,
                ],
            });
            tx.commit().await.expect("test setup: commit tags");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    fn attach_ok(dir: &Path) -> *mut MoraineCatalogHandle {
        let c_path =
            CString::new(dir.to_str().expect("test path is UTF-8")).expect("test path has no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                &raw mut handle,
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or was just written by the call above.
        let err_message = unsafe { err.message.as_ref() };
        assert_eq!(code, codes::OK, "attach failed: {err_message:?}");
        assert!(!handle.is_null());
        handle
    }

    /// One representative dump pins the pull channel for the whole family —
    /// every dump entry point routes through the same cancellable bridge.
    #[test]
    fn probe_cancels_dump_schemas_then_quiet_probe_succeeds() {
        unsafe extern "C" fn probe_always(_probe_ctx: *mut c_void) -> bool {
            true
        }
        unsafe extern "C" fn probe_never(_probe_ctx: *mut c_void) -> bool {
            false
        }

        let dir = TempDir::new("probe-dump");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let mut items: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; out/err slots are valid; the
        // probes accept a null context.
        let code = unsafe {
            moraine_dump_schemas(
                handle,
                &raw mut items,
                &raw mut len,
                Some(probe_always),
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INTERRUPTED);
        assert_eq!(err.code, codes::INTERRUPTED);
        assert!(items.is_null());
        // SAFETY: populated by the failed call above, freed exactly once.
        unsafe { moraine_error_free(err.message) };

        let mut items2: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len2: usize = 0;
        let mut err2 = MoraineError::default();
        // SAFETY: same contracts as above.
        let code2 = unsafe {
            moraine_dump_schemas(
                handle,
                &raw mut items2,
                &raw mut len2,
                Some(probe_never),
                ptr::null_mut(),
                &raw mut err2,
            )
        };
        assert_eq!(code2, codes::OK);

        // SAFETY: freed exactly once each.
        unsafe {
            moraine_dump_schemas_free(items2, len2);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_schemas_and_tables_return_current_and_history_rows() {
        let dir = TempDir::new("schemas-tables");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let mut schemas: *mut MoraineSchemaRow = ptr::null_mut();
        let mut schemas_len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_schemas(
                handle,
                &raw mut schemas,
                &raw mut schemas_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // `main` (bootstrap) + `sales`; the rename never touched a
        // schema, so neither carries a history row.
        assert_eq!(schemas_len, 2);
        // SAFETY: just populated above with `schemas_len` live elements.
        let schema_slice = unsafe { std::slice::from_raw_parts(schemas, schemas_len) };
        assert!(schema_slice.iter().all(|s| !s.has_end_snapshot));

        let mut tables: *mut MoraineTableRow = ptr::null_mut();
        let mut tables_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_tables(
                handle,
                &raw mut tables,
                &raw mut tables_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(
            tables_len, 2,
            "rename must yield one current row + one history row"
        );
        // SAFETY: just populated above with `tables_len` live elements.
        let table_slice = unsafe { std::slice::from_raw_parts(tables, tables_len) };
        let ended = table_slice
            .iter()
            .find(|t| t.has_end_snapshot)
            .expect("one row must be ended");
        let live = table_slice
            .iter()
            .find(|t| !t.has_end_snapshot)
            .expect("one row must be live");
        // SAFETY: owned C strings written above, not yet freed.
        let ended_name = unsafe { CStr::from_ptr(ended.table_name) }
            .to_str()
            .unwrap();
        // SAFETY: same as above.
        let live_name = unsafe { CStr::from_ptr(live.table_name) }.to_str().unwrap();
        assert_eq!(ended_name, "orders");
        assert_eq!(live_name, "orders2");
        assert_eq!(ended.table_id, live.table_id);
        // Exact lifecycle stitching: the ended row's end is the live
        // row's begin.
        assert_eq!(ended.end_snapshot, live.begin_snapshot);
        assert!(live.begin_snapshot > ended.begin_snapshot);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_dump_schemas_free(schemas, schemas_len);
            moraine_dump_tables_free(tables, tables_len);
            moraine_detach(handle);
        }
    }

    /// A catalog string with an embedded NUL (reachable via a view's SQL)
    /// cannot cross the C boundary: `moraine_dump_views` must fail with
    /// `CORRUPTION`, leaving the outputs untouched. The clean view (ordered
    /// first, by id) converts before the poisoned one fails, so this
    /// exercises the owned-first discipline: no raw pointer is minted until
    /// every row's strings convert, or the clean view's `CString`s leak.
    #[test]
    fn embedded_nul_in_view_sql_reports_corruption_and_leaks_nothing() {
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
                    tx.create_view(schema, "clean", "duckdb", "select 1")?;
                    tx.create_view(schema, "poisoned", "duckdb", "select 1 as a\0b")?;
                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");
            catalog.close().await.expect("test setup: close catalog");
        });

        let handle = attach_ok(dir.path());
        let mut views: *mut MoraineViewRow = ptr::null_mut();
        let mut views_len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_views(
                handle,
                &raw mut views,
                &raw mut views_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::CORRUPTION);
        assert_eq!(err.code, codes::CORRUPTION);
        // Nothing was handed to the caller, so there is nothing to free.
        assert!(views.is_null());
        assert_eq!(views_len, 0);
        // SAFETY: just populated above.
        let msg = unsafe { CStr::from_ptr(err.message) }.to_str().unwrap();
        assert!(msg.contains("NUL"), "message: {msg}");

        // SAFETY: `err.message` was just populated and not yet freed;
        // `handle` came from `attach_ok` and is freed exactly once.
        unsafe {
            moraine_error_free(err.message);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_columns_views_and_files_carry_exact_values() {
        let dir = TempDir::new("columns-views-files");
        seed(dir.path());
        let handle = attach_ok(dir.path());
        let mut err = MoraineError::default();

        let mut columns: *mut MoraineColumnRow = ptr::null_mut();
        let mut columns_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_columns(
                handle,
                &raw mut columns,
                &raw mut columns_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(columns_len, 2);
        // SAFETY: just populated above.
        let col_slice = unsafe { std::slice::from_raw_parts(columns, columns_len) };
        assert!(col_slice.iter().all(|c| !c.has_end_snapshot));
        // Pin `nulls_allowed` per column by name: `id` was created NOT
        // NULL, `amount` nullable.
        for column in col_slice {
            // SAFETY: populated by the dump above.
            let name = unsafe { CStr::from_ptr(column.column_name) }
                .to_str()
                .unwrap();
            match name {
                "id" => assert!(!column.nulls_allowed),
                "amount" => assert!(column.nulls_allowed),
                other => panic!("unexpected column {other}"),
            }
        }

        let mut views: *mut MoraineViewRow = ptr::null_mut();
        let mut views_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_views(
                handle,
                &raw mut views,
                &raw mut views_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(views_len, 1);
        // SAFETY: just populated above.
        let view_sql = unsafe { CStr::from_ptr((*views).sql) }.to_str().unwrap();
        assert_eq!(view_sql, "select * from orders");
        // SAFETY: same as above.
        assert!(unsafe { (*views).column_aliases }.is_null());

        let mut files: *mut MoraineDataFileRow = ptr::null_mut();
        let mut files_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_data_files(
                handle,
                &raw mut files,
                &raw mut files_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(files_len, 1);
        // SAFETY: just populated above.
        let file_path = unsafe { CStr::from_ptr((*files).path) }.to_str().unwrap();
        assert_eq!(file_path, "orders/data-1.parquet");
        // SAFETY: same as above.
        assert_eq!(unsafe { (*files).record_count }, 10);
        // SAFETY: same as above.
        let file_key = unsafe { CStr::from_ptr((*files).encryption_key) }
            .to_str()
            .unwrap();
        assert_eq!(file_key, "a2V5LWRhdGE=");
        // SAFETY: same as above.
        assert!(!unsafe { (*files).has_partition_id });
        // SAFETY: same as above.
        let data_file_id = unsafe { (*files).data_file_id };

        let mut deletes: *mut MoraineDeleteFileRow = ptr::null_mut();
        let mut deletes_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_delete_files(
                handle,
                &raw mut deletes,
                &raw mut deletes_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(deletes_len, 1);
        // SAFETY: just populated above.
        assert_eq!(unsafe { (*deletes).data_file_id }, data_file_id);
        // SAFETY: same as above.
        assert_eq!(unsafe { (*deletes).delete_count }, 2);
        // SAFETY: same as above.
        let delete_key = unsafe { CStr::from_ptr((*deletes).encryption_key) }
            .to_str()
            .unwrap();
        assert_eq!(delete_key, "a2V5LWRlbA==");

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_dump_columns_free(columns, columns_len);
            moraine_dump_views_free(views, views_len);
            moraine_dump_data_files_free(files, files_len);
            moraine_dump_delete_files_free(deletes, deletes_len);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_statistics_and_snapshots_carry_exact_values() {
        let dir = TempDir::new("stats-snapshots");
        seed(dir.path());
        let handle = attach_ok(dir.path());
        let mut err = MoraineError::default();

        let mut tstat_rows: *mut MoraineTableStatsRow = ptr::null_mut();
        let mut tstat_rows_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_table_stats(
                handle,
                &raw mut tstat_rows,
                &raw mut tstat_rows_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(tstat_rows_len, 1);
        // SAFETY: just populated above.
        assert_eq!(unsafe { (*tstat_rows).record_count }, 10);

        let mut col_stat_rows: *mut MoraineTableColumnStatsRow = ptr::null_mut();
        let mut col_stat_rows_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_table_column_stats(
                handle,
                &raw mut col_stat_rows,
                &raw mut col_stat_rows_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(col_stat_rows_len, 1);
        // SAFETY: just populated above.
        assert!(unsafe { (*col_stat_rows).has_contains_null });
        // SAFETY: same as above.
        assert!(!unsafe { (*col_stat_rows).contains_null });

        let mut file_stat_rows: *mut MoraineFileColumnStatsRow = ptr::null_mut();
        let mut file_stat_rows_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_file_column_stats(
                handle,
                &raw mut file_stat_rows,
                &raw mut file_stat_rows_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(file_stat_rows_len, 1);
        // SAFETY: just populated above.
        let min_value = unsafe { CStr::from_ptr((*file_stat_rows).min_value) }
            .to_str()
            .unwrap();
        assert_eq!(min_value, "1");

        let mut snapshots: *mut MoraineSnapshotRow = ptr::null_mut();
        let mut snapshots_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_snapshots(
                handle,
                &raw mut snapshots,
                &raw mut snapshots_len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // Bootstrap (0) + the two `seed` commits.
        assert_eq!(snapshots_len, 3);
        // SAFETY: just populated above with `snapshots_len` live elements.
        let snap_slice = unsafe { std::slice::from_raw_parts(snapshots, snapshots_len) };
        let ids: Vec<u64> = snap_slice.iter().map(|s| s.snapshot_id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
        // SAFETY: bootstrap's `changes_made` is a non-null string.
        let bootstrap_changes = unsafe { CStr::from_ptr(snap_slice[0].changes_made) }
            .to_str()
            .unwrap();
        assert_eq!(bootstrap_changes, "created_schema:\"main\"");
        assert!(snap_slice[0].author.is_null());

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            moraine_dump_table_stats_free(tstat_rows, tstat_rows_len);
            moraine_dump_table_column_stats_free(col_stat_rows, col_stat_rows_len);
            moraine_dump_file_column_stats_free(file_stat_rows, file_stat_rows_len);
            moraine_dump_snapshots_free(snapshots, snapshots_len);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_tags_and_column_tags_carry_exact_values() {
        let dir = TempDir::new("tags");
        seed_with_tags(dir.path());
        let handle = attach_ok(dir.path());

        let mut items: *mut MoraineTagRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; out/err slots are valid.
        let code = unsafe {
            moraine_dump_tags(
                handle,
                &raw mut items,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 1);
        // SAFETY: `items` points to `len` rows written by the call above.
        let row = unsafe { &*items };
        assert_eq!(row.object_id, 2);
        assert_eq!(row.begin_snapshot, 2);
        assert!(!row.has_end_snapshot);
        // SAFETY: owned, NUL-terminated strings written by the dump.
        unsafe {
            assert_eq!(CStr::from_ptr(row.key).to_str().unwrap(), "comment");
            assert_eq!(CStr::from_ptr(row.value).to_str().unwrap(), "our table");
        }

        let mut column_items: *mut MoraineColumnTagRow = ptr::null_mut();
        let mut column_len: usize = 0;
        let mut column_err = MoraineError::default();
        // SAFETY: same contracts as above.
        let column_code = unsafe {
            moraine_dump_column_tags(
                handle,
                &raw mut column_items,
                &raw mut column_len,
                None,
                ptr::null_mut(),
                &raw mut column_err,
            )
        };
        assert_eq!(column_code, codes::OK);
        assert_eq!(column_len, 1);
        // SAFETY: `column_items` points to `column_len` rows written above.
        let column_row = unsafe { &*column_items };
        assert_eq!(column_row.table_id, 2);
        assert_eq!(column_row.column_id, 1);
        assert_eq!(column_row.begin_snapshot, 2);
        assert!(!column_row.has_end_snapshot);
        // SAFETY: owned, NUL-terminated strings written by the dump.
        unsafe {
            assert_eq!(CStr::from_ptr(column_row.key).to_str().unwrap(), "comment");
            assert_eq!(
                CStr::from_ptr(column_row.value).to_str().unwrap(),
                "our column"
            );
        }

        // SAFETY: freed exactly once each.
        unsafe {
            moraine_dump_tags_free(items, len);
            moraine_dump_column_tags_free(column_items, column_len);
            moraine_detach(handle);
        }
    }

    #[test]
    fn dump_on_null_handle_reports_invalid_argument() {
        let mut err = MoraineError::default();
        let mut out: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        // SAFETY: a null `handle` is exactly the input this test exercises.
        let code = unsafe {
            moraine_dump_schemas(
                ptr::null_mut(),
                &raw mut out,
                &raw mut len,
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(out.is_null());
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { free_c_string(err.message) };
    }

    #[test]
    fn dump_frees_tolerate_null() {
        // Every teardown function must be a safe no-op on null.
        //
        // SAFETY: every argument below is null, which each function's own
        // contract documents as a no-op.
        unsafe {
            moraine_dump_schemas_free(ptr::null_mut(), 0);
            moraine_dump_tables_free(ptr::null_mut(), 0);
            moraine_dump_views_free(ptr::null_mut(), 0);
            moraine_dump_columns_free(ptr::null_mut(), 0);
            moraine_dump_data_files_free(ptr::null_mut(), 0);
            moraine_dump_delete_files_free(ptr::null_mut(), 0);
            moraine_dump_table_stats_free(ptr::null_mut(), 0);
            moraine_dump_table_column_stats_free(ptr::null_mut(), 0);
            moraine_dump_file_column_stats_free(ptr::null_mut(), 0);
            moraine_dump_snapshots_free(ptr::null_mut(), 0);
            moraine_dump_schema_versions_free(ptr::null_mut(), 0);
            moraine_dump_partition_info_free(ptr::null_mut(), 0);
            moraine_dump_partition_columns_free(ptr::null_mut(), 0);
            moraine_dump_file_partition_values_free(ptr::null_mut(), 0);
            moraine_dump_sort_info_free(ptr::null_mut(), 0);
            moraine_dump_sort_expressions_free(ptr::null_mut(), 0);
        }
    }

    /// `cpp/moraine_abi.h` is a hand-written C mirror of this module's
    /// `extern "C"` surface, kept in lockstep by hand — parallel to
    /// `abi.rs`'s own `header_declares_every_abi_symbol` test.
    #[test]
    fn header_declares_every_dump_symbol() {
        let header = include_str!("../cpp/moraine_abi.h");

        let functions = [
            "moraine_dump_snapshots",
            "moraine_dump_snapshots_free",
            "moraine_dump_schemas",
            "moraine_dump_schemas_free",
            "moraine_dump_tables",
            "moraine_dump_tables_free",
            "moraine_dump_columns",
            "moraine_dump_columns_free",
            "moraine_dump_views",
            "moraine_dump_views_free",
            "moraine_dump_data_files",
            "moraine_dump_data_files_free",
            "moraine_dump_delete_files",
            "moraine_dump_delete_files_free",
            "moraine_dump_table_stats",
            "moraine_dump_table_stats_free",
            "moraine_dump_table_column_stats",
            "moraine_dump_table_column_stats_free",
            "moraine_dump_file_column_stats",
            "moraine_dump_file_column_stats_free",
            "moraine_dump_schema_versions",
            "moraine_dump_schema_versions_free",
            "moraine_dump_partition_info",
            "moraine_dump_partition_info_free",
            "moraine_dump_partition_columns",
            "moraine_dump_partition_columns_free",
            "moraine_dump_file_partition_values",
            "moraine_dump_file_partition_values_free",
            "moraine_dump_sort_info",
            "moraine_dump_sort_info_free",
            "moraine_dump_sort_expressions",
            "moraine_dump_sort_expressions_free",
            "moraine_dump_tags",
            "moraine_dump_tags_free",
            "moraine_dump_column_tags",
            "moraine_dump_column_tags_free",
            "moraine_dump_scheduled_deletions",
            "moraine_dump_scheduled_deletions_free",
        ];
        let structs = [
            "MoraineSnapshotRow",
            "MoraineSchemaRow",
            "MoraineTableRow",
            "MoraineColumnRow",
            "MoraineViewRow",
            "MoraineDataFileRow",
            "MoraineDeleteFileRow",
            "MoraineTableStatsRow",
            "MoraineTableColumnStatsRow",
            "MoraineFileColumnStatsRow",
            "MoraineSchemaVersionRow",
            "MorainePartitionInfoRow",
            "MorainePartitionColumnRow",
            "MoraineFilePartitionValueRow",
            "MoraineSortInfoRow",
            "MoraineSortExpressionRow",
            "MoraineTagRow",
            "MoraineColumnTagRow",
            "MoraineScheduledDeletionRow",
        ];

        for name in functions.iter().chain(&structs) {
            assert!(
                header.contains(name),
                "cpp/moraine_abi.h is missing `{name}`, declared in src/dumps.rs — \
                 the two must be kept in lockstep by hand"
            );
        }
    }
}
