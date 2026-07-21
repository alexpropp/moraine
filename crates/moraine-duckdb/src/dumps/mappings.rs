//! Dumps for the column-mapping tables: `ducklake_column_mapping` and
//! `ducklake_name_mapping`.

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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
