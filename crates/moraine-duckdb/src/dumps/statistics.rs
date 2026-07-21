//! Dumps for the statistics tables: `ducklake_table_stats`,
//! `ducklake_table_column_stats`, and `ducklake_file_column_stats`.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{opt_bool, opt_c_string, opt_into_raw};
use crate::{
    abi::{free_array, free_c_string, guard, write_array},
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
