//! Dumps for the partition and sort tables: `ducklake_partition_info`,
//! `ducklake_partition_column`, `ducklake_sort_info`, and
//! `ducklake_sort_expression`.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::opt_u64;
use crate::{
    abi::{free_array, free_c_string, guard, to_c_string, write_array},
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
