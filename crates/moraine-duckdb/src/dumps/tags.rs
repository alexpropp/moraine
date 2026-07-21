//! Dumps for the tag tables: `ducklake_tag` and `ducklake_column_tag`.

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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
    use std::{ffi::CStr, ptr};

    use super::*;
    use crate::{
        abi::moraine_detach,
        dumps::test_support::{TempDir, attach_ok, seed_with_tags},
    };

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
}
