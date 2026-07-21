//! Dumps for the macro tables: `ducklake_macro`, `ducklake_macro_impl`,
//! and `ducklake_macro_parameters`.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{opt_c_string, opt_into_raw, opt_u64};
use crate::{
    abi::{free_array, free_c_string, guard, to_c_string, write_array},
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
