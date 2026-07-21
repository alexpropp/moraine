//! Dumps for the snapshot tables: `ducklake_snapshot` (with its merged
//! `ducklake_snapshot_changes` columns) and `ducklake_schema_versions`.

use std::{
    ffi::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{opt_c_string, opt_into_raw};
use crate::{
    abi::{free_array, free_c_string, guard, to_c_string, write_array},
    error::{AbiError, MoraineError, codes},
    runtime::{MoraineCatalogHandle, MoraineInterruptProbe},
};

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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
/// Same pointer contract as [`moraine_dump_schemas`](crate::dumps::moraine_dump_schemas).
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
