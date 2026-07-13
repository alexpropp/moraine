//! The staged-row transaction ABI: the shim translates each row DuckLake
//! writes into a [`MoraineCell`] array and calls into this module. Rows
//! accumulate in memory via
//! [`moraine::ffi_support::staged::StagedTransaction`] until
//! [`moraine_txn_commit`] lands them all in one atomic store batch, or
//! [`moraine_txn_rollback`] discards them.
//!
//! Same conventions as [`crate::abi`]: `catch_unwind`/null discipline via
//! `crate::abi::guard`, no internal retry (a lost race at commit surfaces
//! [`codes::COMMIT_CONFLICT`] with the literal substring `conflict` in the
//! message). This module is translate-only: it decodes [`MoraineCell`]s
//! into [`Cell`]s and forwards them, never interpreting DuckLake's row
//! values itself.
//!
//! [`MoraineTxnHandle`] borrows the owning [`MoraineCatalogHandle`]'s tokio
//! runtime for [`moraine_txn_begin`] and [`moraine_txn_commit`] (the only
//! two async operations here; `stage` and `rollback` are synchronous). The
//! caller contract requires the catalog outlive every open transaction on
//! it.

use std::{
    ffi::c_char,
    panic::{AssertUnwindSafe, catch_unwind},
};

use moraine::ffi_support::staged::{
    Cell, RowOperation, StagedTransaction, TableKind, staged_begin,
};

use crate::{
    abi::{borrow_bytes, borrow_str, guard},
    error::{AbiError, MoraineError, codes},
    runtime::MoraineCatalogHandle,
};

/// One value in a staged row. Mirrors [`Cell`] as a tagged struct across
/// the C boundary; `str_value` is borrowed, valid only for the duration of
/// the [`moraine_txn_stage`] call that reads it.
#[repr(C)]
pub struct MoraineCell {
    /// `0` = NULL, `1` = u64, `2` = i64, `3` = bool, `4` = string.
    pub kind: i32,
    /// Valid iff `kind == 1`.
    pub u64_value: u64,
    /// Valid iff `kind == 2`.
    pub i64_value: i64,
    /// Valid iff `kind == 3`.
    pub bool_value: bool,
    /// Valid iff `kind == 4`: a borrowed, NUL-terminated UTF-8 string.
    pub str_value: *const c_char,
}

/// A staged-row transaction, opaque to C. Owns one [`StagedTransaction`]
/// plus a borrowed pointer to the catalog handle it was opened on, used to
/// `block_on` the async core calls in [`moraine_txn_begin`] and
/// [`moraine_txn_commit`].
pub struct MoraineTxnHandle {
    catalog: *const MoraineCatalogHandle,
    txn: StagedTransaction,
}

fn decode_table_kind(v: i32) -> Result<TableKind, AbiError> {
    match v {
        0 => Ok(TableKind::Snapshot),
        1 => Ok(TableKind::SnapshotChanges),
        2 => Ok(TableKind::Schema),
        3 => Ok(TableKind::Table),
        4 => Ok(TableKind::View),
        5 => Ok(TableKind::Column),
        6 => Ok(TableKind::DataFile),
        7 => Ok(TableKind::DeleteFile),
        8 => Ok(TableKind::TableStats),
        9 => Ok(TableKind::TableColumnStats),
        10 => Ok(TableKind::FileColumnStats),
        11 => Ok(TableKind::SchemaVersions),
        other => Err(AbiError::invalid_argument(format!(
            "moraine_txn_stage: unknown table_kind {other}"
        ))),
    }
}

/// The three [`RowOperation`] shapes, decoded from `op_kind`.
enum OpKind {
    Insert,
    Delete,
    UpdateSetEnd,
}

fn decode_op_kind(v: i32) -> Result<OpKind, AbiError> {
    match v {
        0 => Ok(OpKind::Insert),
        1 => Ok(OpKind::Delete),
        2 => Ok(OpKind::UpdateSetEnd),
        other => Err(AbiError::invalid_argument(format!(
            "moraine_txn_stage: unknown op_kind {other}"
        ))),
    }
}

/// Decodes a borrowed `MoraineCell` array into owned [`Cell`]s. A null
/// `cells` pointer is valid only when `len` is `0`.
///
/// # Safety
///
/// `cells`, if non-null, must point to `len` valid, readable
/// [`MoraineCell`]s; every non-null `str_value` inside must be a valid
/// NUL-terminated UTF-8 C string, valid for reads for the duration of
/// this call.
unsafe fn decode_cells(cells: *const MoraineCell, len: usize) -> Result<Vec<Cell>, AbiError> {
    if cells.is_null() {
        if len == 0 {
            return Ok(Vec::new());
        }
        return Err(AbiError::invalid_argument(
            "moraine_txn_stage: `cells` is null but `cells_len` is nonzero",
        ));
    }
    // SAFETY: caller contract above.
    let slice = unsafe { std::slice::from_raw_parts(cells, len) };
    slice
        .iter()
        .map(|c| match c.kind {
            0 => Ok(Cell::Null),
            1 => Ok(Cell::U64(c.u64_value)),
            2 => Ok(Cell::I64(c.i64_value)),
            3 => Ok(Cell::Bool(c.bool_value)),
            4 => {
                // SAFETY: caller contract above.
                let s = unsafe { borrow_str(c.str_value, "cell.str_value") }?;
                Ok(Cell::Str(s.to_string()))
            }
            other => Err(AbiError::invalid_argument(format!(
                "moraine_txn_stage: unknown cell kind {other}"
            ))),
        })
        .collect()
}

/// Opens a staged-row transaction at the current head and writes the
/// resulting handle to `*out`.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach`](crate::abi::moraine_attach) and not yet detached,
/// and must outlive every operation on the returned transaction handle
/// (through [`moraine_txn_commit`] or [`moraine_txn_rollback`]). `out`
/// must be a valid, writable `*mut *mut MoraineTxnHandle`. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_begin(
    handle: *mut MoraineCatalogHandle,
    out: *mut *mut MoraineTxnHandle,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Box<MoraineTxnHandle>, AbiError> {
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out.is_null() {
            return Err(AbiError::invalid_argument("`out` is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let txn = handle_ref
            .runtime
            .block_on(staged_begin(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        Ok(Box::new(MoraineTxnHandle {
            catalog: handle,
            txn,
        }))
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

/// Accumulates one staged row mutation. Nothing touches the store until
/// [`moraine_txn_commit`]. `table_kind` is [`TableKind`]'s discriminant
/// order (`0` = `Snapshot`, `1` = `SnapshotChanges`, `2` = `Schema`, `3` =
/// `Table`, `4` = `View`, `5` = `Column`, `6` = `DataFile`, `7` =
/// `DeleteFile`, `8` = `TableStats`, `9` = `TableColumnStats`, `10` =
/// `FileColumnStats`, `11` = `SchemaVersions`); `op_kind` is `0` = insert,
/// `1` = delete, `2` = update-sets-`end_snapshot`. `cells` are positional
/// in the column order the shim declares for `table_kind`'s table (a delete
/// or update-set-end row carries only the key columns, per [`RowOperation`]'s
/// variants).
///
/// # Safety
///
/// `txn` must be a pointer previously returned by [`moraine_txn_begin`]
/// and not yet committed or rolled back. `cells`, if `cells_len` is
/// nonzero, must point to `cells_len` valid [`MoraineCell`]s (every
/// non-null `str_value` inside a valid NUL-terminated UTF-8 C string).
/// `err`, if non-null, must be a valid, writable [`MoraineError`]. All
/// for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage(
    txn: *mut MoraineTxnHandle,
    table_kind: i32,
    op_kind: i32,
    cells: *const MoraineCell,
    cells_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        let table = decode_table_kind(table_kind)?;
        let kind = decode_op_kind(op_kind)?;
        // SAFETY: caller contract above.
        let row_cells = unsafe { decode_cells(cells, cells_len) }?;
        let op = match kind {
            OpKind::Insert => RowOperation::Insert {
                table,
                cells: row_cells,
            },
            OpKind::Delete => RowOperation::Delete {
                table,
                cells: row_cells,
            },
            OpKind::UpdateSetEnd => RowOperation::UpdateSetEnd {
                table,
                cells: row_cells,
            },
        };
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(op);
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Translates every staged row and lands them in one atomic batch,
/// consuming `txn`. On success, writes the new snapshot id to
/// `*out_snapshot_id`.
///
/// A lost race against a concurrent commit is never retried internally: it
/// returns [`codes::COMMIT_CONFLICT`] with the literal substring `conflict`
/// in the message, and the loser leaves the store unchanged. `txn` is freed
/// either way; it must not be passed to [`moraine_txn_rollback`]
/// afterward.
///
/// # Safety
///
/// `txn` must be a pointer previously returned by [`moraine_txn_begin`]
/// and not yet committed or rolled back. `out_snapshot_id` must be a
/// valid, writable `*mut u64`. `err`, if non-null, must be a valid,
/// writable [`MoraineError`]. All for the duration of this call. The
/// catalog handle `txn` was opened on ([`moraine_txn_begin`]'s contract)
/// must still be attached.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_commit(
    txn: *mut MoraineTxnHandle,
    out_snapshot_id: *mut u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<u64, AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        if out_snapshot_id.is_null() {
            return Err(AbiError::invalid_argument("`out_snapshot_id` is null"));
        }
        // SAFETY: caller contract above; `txn` consumed exactly once.
        let boxed = unsafe { Box::from_raw(txn) };
        let MoraineTxnHandle { catalog, txn } = *boxed;
        // SAFETY: `catalog` outlives `txn` per `moraine_txn_begin`'s contract.
        let catalog_ref = unsafe { &*catalog };
        let id = catalog_ref
            .runtime
            .block_on(txn.commit())
            .map_err(AbiError::from)?;
        Ok(id.get())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(id) => {
            // SAFETY: checked non-null above; caller contract.
            unsafe {
                *out_snapshot_id = id;
            }
            codes::OK
        }
        Err(code) => code,
    }
}

/// Discards every staged row without writing anything, consuming `txn`.
/// Best-effort: has no error channel. A null `txn` is a no-op.
///
/// # Safety
///
/// `txn`, if non-null, must be a pointer previously returned by
/// [`moraine_txn_begin`] and not yet committed or rolled back.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_rollback(txn: *mut MoraineTxnHandle) {
    if txn.is_null() {
        return;
    }
    let attempt = || {
        // SAFETY: caller contract above.
        let boxed = unsafe { Box::from_raw(txn) };
        boxed.txn.rollback();
    };
    let _ = catch_unwind(AssertUnwindSafe(attempt));
}

/// Stages `inline/schema`: the Arrow IPC schema for one `(table_id,
/// schema_version)`, written once at inline-table creation.
///
/// # Safety
///
/// `txn` must be a pointer previously returned by [`moraine_txn_begin`]
/// and not yet committed or rolled back. `arrow_schema`, if
/// `arrow_schema_len` is nonzero, must point to `arrow_schema_len` valid
/// bytes. `err`, if non-null, must be a valid, writable [`MoraineError`].
/// All for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage_inline_schema(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    schema_version: u64,
    arrow_schema: *const u8,
    arrow_schema_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract above.
        let bytes = unsafe { borrow_bytes(arrow_schema, arrow_schema_len, "arrow_schema") }?;
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineSchema {
            table_id,
            schema_version,
            arrow_schema: bytes.to_vec(),
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/insert`: one Arrow record-batch chunk of inlined rows.
///
/// # Safety
///
/// Same `txn` contract as [`moraine_txn_stage_inline_schema`].
/// `arrow_body`, if `arrow_body_len` is nonzero, must point to
/// `arrow_body_len` valid bytes. `err`, if non-null, must be a valid,
/// writable [`MoraineError`]. All for the duration of this call.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn moraine_txn_stage_inline_insert(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    schema_version: u64,
    begin_snapshot: u64,
    row_id_start: u64,
    row_count: u64,
    arrow_body: *const u8,
    arrow_body_len: usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract above.
        let bytes = unsafe { borrow_bytes(arrow_body, arrow_body_len, "arrow_body") }?;
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineInsert {
            table_id,
            schema_version,
            begin_snapshot,
            row_id_start,
            row_count,
            arrow_body: bytes.to_vec(),
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/inline_delete`: tombstones one inlined-insert row.
///
/// # Safety
///
/// `txn` must be a pointer previously returned by [`moraine_txn_begin`]
/// and not yet committed or rolled back. `err`, if non-null, must be a
/// valid, writable [`MoraineError`]. Both for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage_inline_inline_delete(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    row_id: u64,
    end_snapshot: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineInlineDelete {
            table_id,
            row_id,
            end_snapshot,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages `inline/file_delete`: an inlined delete against a Parquet-file row.
///
/// # Safety
///
/// Same contract as [`moraine_txn_stage_inline_inline_delete`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage_inline_file_delete(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    data_file_id: u64,
    row_id: u64,
    begin_snapshot: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineFileDelete {
            table_id,
            data_file_id,
            row_id,
            begin_snapshot,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages a flush-delete: removes every `inline/insert` chunk begun at or
/// before `flush_snapshot` for `(table_id, schema_version)`, plus the
/// `inline/inline_delete` tombstones those chunks' rows consumed.
///
/// # Safety
///
/// Same contract as [`moraine_txn_stage_inline_inline_delete`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage_inline_flush_delete(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    schema_version: u64,
    flush_snapshot: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineFlushDelete {
            table_id,
            schema_version,
            flush_snapshot,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages a table drop: removes every `inline/*` record for `table_id`.
///
/// # Safety
///
/// `txn` must be a pointer previously returned by [`moraine_txn_begin`]
/// and not yet committed or rolled back. `err`, if non-null, must be a
/// valid, writable [`MoraineError`]. Both for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage_inline_drop(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineDrop { table_id });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

/// Stages a schema-version-scoped deregistration: removes only the
/// `inline/schema` record for `(table_id, schema_version)`, leaving any
/// other schema version's `inline/*` records untouched (unlike
/// [`moraine_txn_stage_inline_drop`], which is table-wide).
///
/// # Safety
///
/// Same contract as [`moraine_txn_stage_inline_drop`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_txn_stage_inline_schema_drop(
    txn: *mut MoraineTxnHandle,
    table_id: u64,
    schema_version: u64,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<(), AbiError> {
        if txn.is_null() {
            return Err(AbiError::invalid_argument("`txn` is null"));
        }
        // SAFETY: caller contract for `txn`.
        let txn_ref = unsafe { &mut *txn };
        txn_ref.txn.stage(RowOperation::InlineSchemaDrop {
            table_id,
            schema_version,
        });
        Ok(())
    };

    // SAFETY: `err` validity is this function's own safety contract.
    match unsafe { guard(err, attempt) } {
        Ok(()) => codes::OK,
        Err(code) => code,
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CString, ptr};

    use super::*;
    use crate::abi::{moraine_attach, moraine_detach};

    /// A directory under the OS temp dir, unique per call, removed on
    /// drop.
    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "moraine-duckdb-staged-{tag}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
            Self(dir)
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

    fn attach_ok(dir: &TempDir) -> *mut MoraineCatalogHandle {
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        let c_path = dir.c_path();
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
        assert_eq!(code, codes::OK);
        assert!(!handle.is_null());
        handle
    }

    fn u64_cell(v: u64) -> MoraineCell {
        MoraineCell {
            kind: 1,
            u64_value: v,
            i64_value: 0,
            bool_value: false,
            str_value: ptr::null(),
        }
    }

    fn i64_cell(v: i64) -> MoraineCell {
        MoraineCell {
            kind: 2,
            u64_value: 0,
            i64_value: v,
            bool_value: false,
            str_value: ptr::null(),
        }
    }

    fn bool_cell(v: bool) -> MoraineCell {
        MoraineCell {
            kind: 3,
            u64_value: 0,
            i64_value: 0,
            bool_value: v,
            str_value: ptr::null(),
        }
    }

    fn null_cell() -> MoraineCell {
        MoraineCell {
            kind: 0,
            u64_value: 0,
            i64_value: 0,
            bool_value: false,
            str_value: ptr::null(),
        }
    }

    /// Owns the `CString`s a `str_cell` borrows from, so the cells stay
    /// valid for the `moraine_txn_stage` call that reads them.
    struct StrArena(Vec<CString>);

    impl StrArena {
        fn new() -> Self {
            Self(Vec::new())
        }

        fn cell(&mut self, s: &str) -> MoraineCell {
            let c = CString::new(s).expect("test string has no NUL");
            let ptr = c.as_ptr();
            self.0.push(c);
            MoraineCell {
                kind: 4,
                u64_value: 0,
                i64_value: 0,
                bool_value: false,
                str_value: ptr,
            }
        }
    }

    fn stage(txn: *mut MoraineTxnHandle, table_kind: i32, op_kind: i32, cells: &[MoraineCell]) {
        let mut err = MoraineError::default();
        // SAFETY: `txn` is a live handle from `moraine_txn_begin`; `cells`
        // is a valid slice for the duration of this call.
        let code = unsafe {
            moraine_txn_stage(
                txn,
                table_kind,
                op_kind,
                cells.as_ptr(),
                cells.len(),
                &raw mut err,
            )
        };
        // SAFETY: `err.message` is null or was just written by `moraine_txn_stage`.
        assert_eq!(code, codes::OK, "stage failed: {:?}", unsafe {
            err.message.as_ref()
        });
    }

    fn begin(handle: *mut MoraineCatalogHandle) -> *mut MoraineTxnHandle {
        let mut txn: *mut MoraineTxnHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe { moraine_txn_begin(handle, &raw mut txn, &raw mut err) };
        // SAFETY: `err.message` is null or was just written by `moraine_txn_begin`.
        assert_eq!(code, codes::OK, "begin failed: {:?}", unsafe {
            err.message.as_ref()
        });
        assert!(!txn.is_null());
        txn
    }

    /// Stages a full `ducklake_table` row (`table_kind` 3, `op_kind` 0):
    /// id, a synthetic uuid, begin/end snapshot (`lifecycle`), schema id,
    /// name, path (always relative) — the shape every test that creates or
    /// renames a table needs.
    fn stage_table_row(
        txn: *mut MoraineTxnHandle,
        arena: &mut StrArena,
        table_id: u64,
        lifecycle: (u64, Option<u64>),
        schema_id: u64,
        name: &str,
        path: &str,
    ) {
        let (begin_snapshot, end_snapshot) = lifecycle;
        stage(
            txn,
            3,
            0,
            &[
                u64_cell(table_id),
                arena.cell(&format!("uuid-t{table_id}")),
                u64_cell(begin_snapshot),
                end_snapshot.map_or_else(null_cell, u64_cell),
                u64_cell(schema_id),
                arena.cell(name),
                arena.cell(path),
                bool_cell(true),
            ],
        );
    }

    /// Stages the `ducklake_snapshot` + `ducklake_snapshot_changes` pair
    /// every commit in this module bumps: id, time (same value as `id` in
    /// every fixture here), schema version, and `next_catalog_id`, plus the
    /// changes-made text DuckLake records for the same snapshot.
    fn stage_snapshot_and_changes(
        txn: *mut MoraineTxnHandle,
        arena: &mut StrArena,
        snapshot_id: u64,
        schema_version: u64,
        next_catalog_id: u64,
        changes_made: &str,
    ) {
        stage(
            txn,
            0,
            0,
            &[
                u64_cell(snapshot_id),
                i64_cell(i64::try_from(snapshot_id).expect("test snapshot id fits i64")),
                u64_cell(schema_version),
                u64_cell(next_catalog_id),
                u64_cell(0),
            ],
        );
        stage(
            txn,
            1,
            0,
            &[
                u64_cell(snapshot_id),
                arena.cell(changes_made),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
    }

    /// A DuckLake-shaped snapshot bump plus table create: table `t` (id
    /// 1, schema 0 = bootstrap's `main`) with one column, staged and
    /// committed over the ABI as one batch, then verified through the
    /// dump ABI (the same view the metadata-table scan serves).
    #[test]
    fn stages_table_create_and_snapshot_bump_over_the_abi() {
        let dir = TempDir::new("create");
        let handle = attach_ok(&dir);
        let txn = begin(handle);

        let mut arena = StrArena::new();
        stage_table_row(txn, &mut arena, 1, (1, None), 0, "t", "t/");
        // ducklake_column: column_id, begin_snapshot, end_snapshot,
        // table_id, column_order, column_name, column_type,
        // initial_default, default_value, nulls_allowed, parent_column,
        // default_value_type, default_value_dialect.
        stage(
            txn,
            5,
            0,
            &[
                u64_cell(1),
                u64_cell(0),
                null_cell(),
                u64_cell(1),
                u64_cell(0),
                arena.cell("a"),
                arena.cell("BIGINT"),
                null_cell(),
                null_cell(),
                bool_cell(true),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );
        stage_snapshot_and_changes(txn, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
        // ducklake_schema_versions: begin_snapshot, schema_version,
        // table_id — DuckLake writes one per created table, carrying this
        // commit's own snapshot values.
        stage(txn, 11, 0, &[u64_cell(1), u64_cell(1), u64_cell(1)]);

        let mut snapshot_id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `txn` is live; outputs are valid local slots.
        let code = unsafe { moraine_txn_commit(txn, &raw mut snapshot_id, &raw mut err) };
        // SAFETY: `err.message` is null or was just written by `moraine_txn_commit`.
        assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
            err.message.as_ref()
        });
        assert_eq!(snapshot_id, 1);

        let mut rows: *mut crate::dumps::MoraineTableRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut dump_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_tables(
                handle,
                &raw mut rows,
                &raw mut len,
                &raw mut dump_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 1);
        // SAFETY: just populated above.
        let name = unsafe { std::ffi::CStr::from_ptr((*rows).table_name) }
            .to_str()
            .unwrap();
        assert_eq!(name, "t");

        // The schema-version row folded into the snapshot record and
        // flattens back out of the dump verbatim.
        let mut versions: *mut crate::dumps::MoraineSchemaVersionRow = ptr::null_mut();
        let mut versions_len: usize = 0;
        let mut versions_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_schema_versions(
                handle,
                &raw mut versions,
                &raw mut versions_len,
                &raw mut versions_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(versions_len, 1);
        // SAFETY: just populated above.
        unsafe {
            assert_eq!((*versions).begin_snapshot, 1);
            assert_eq!((*versions).schema_version, 1);
            assert_eq!((*versions).table_id, 1);
        }

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            crate::dumps::moraine_dump_tables_free(rows, len);
            crate::dumps::moraine_dump_schema_versions_free(versions, versions_len);
            moraine_detach(handle);
        }
    }

    /// The other two op kinds over the wire: an `update_set_end` row
    /// (`op_kind` 2 — the C++ UPDATE operator's staging for a rename/drop)
    /// moves the old table version to history, and a raw `delete` row
    /// (`op_kind` 1) removes an unversioned statistics row. Cell layouts
    /// here are exactly what `cpp/staged_write.cpp`'s Sinks emit: key
    /// cells in decoder order, plus (for `update_set_end`) the new
    /// `end_snapshot`.
    #[test]
    fn update_set_end_and_stats_delete_over_the_abi() {
        let dir = TempDir::new("end-delete");
        let handle = attach_ok(&dir);

        // Snapshot 1: table `t` (id 1, in bootstrap's `main`) plus its
        // stats row.
        let setup = begin(handle);
        let mut arena = StrArena::new();
        stage_table_row(setup, &mut arena, 1, (1, None), 0, "t", "t/");
        // ducklake_table_stats: table_id, record_count, next_row_id,
        // file_size_bytes.
        stage(
            setup,
            8,
            0,
            &[u64_cell(1), u64_cell(0), u64_cell(0), u64_cell(0)],
        );
        stage_snapshot_and_changes(setup, &mut arena, 1, 1, 2, r#"created_table:"main"."t""#);
        let mut id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `setup` is live; outputs are valid local slots.
        let code = unsafe { moraine_txn_commit(setup, &raw mut id, &raw mut err) };
        assert_eq!(code, codes::OK);

        // Snapshot 2: end `t`'s live version (rename shape: end + new
        // version) and delete its stats row.
        let txn = begin(handle);
        // update_set_end on ducklake_table: [table_id, new end_snapshot].
        stage(txn, 3, 2, &[u64_cell(1), u64_cell(2)]);
        stage_table_row(txn, &mut arena, 1, (2, None), 0, "t2", "t/");
        // delete on ducklake_table_stats: [table_id].
        stage(txn, 8, 1, &[u64_cell(1)]);
        stage_snapshot_and_changes(txn, &mut arena, 2, 2, 2, "altered_table:1");
        let mut id2: u64 = 0;
        let mut err2 = MoraineError::default();
        // SAFETY: `txn` is live; outputs are valid local slots.
        let code = unsafe { moraine_txn_commit(txn, &raw mut id2, &raw mut err2) };
        // SAFETY: `err2.message` is null or was just written.
        assert_eq!(code, codes::OK, "commit failed: {:?}", unsafe {
            err2.message.as_ref()
        });
        assert_eq!(id2, 2);

        // The dump serves both versions: history `t` ended at 2, current `t2`.
        let mut rows: *mut crate::dumps::MoraineTableRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut dump_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_tables(
                handle,
                &raw mut rows,
                &raw mut len,
                &raw mut dump_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(len, 2);
        // SAFETY: just populated above with `len` live elements.
        let table_rows = unsafe { std::slice::from_raw_parts(rows, len) };
        let ended = table_rows.iter().find(|r| r.has_end_snapshot).unwrap();
        let live = table_rows.iter().find(|r| !r.has_end_snapshot).unwrap();
        assert_eq!(ended.end_snapshot, 2);
        // SAFETY: owned C strings written above, not yet freed.
        let live_name = unsafe { std::ffi::CStr::from_ptr(live.table_name) }
            .to_str()
            .unwrap();
        assert_eq!(live_name, "t2");

        // The stats row is gone (unversioned raw delete, no history mirror).
        let mut stats: *mut crate::dumps::MoraineTableStatsRow = ptr::null_mut();
        let mut stats_len: usize = 0;
        let mut stats_err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_table_stats(
                handle,
                &raw mut stats,
                &raw mut stats_len,
                &raw mut stats_err,
            )
        };
        assert_eq!(code, codes::OK);
        assert_eq!(stats_len, 0);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            crate::dumps::moraine_dump_tables_free(rows, len);
            crate::dumps::moraine_dump_table_stats_free(stats, stats_len);
            moraine_detach(handle);
        }
    }

    /// A lost race at commit is never retried: the loser's error carries
    /// the literal substring `conflict`, and the store reflects only the
    /// winner.
    #[test]
    fn lost_race_at_commit_is_not_retried_and_carries_conflict_text() {
        let dir = TempDir::new("race");
        let handle = attach_ok(&dir);

        let txn_a = begin(handle);
        let txn_b = begin(handle);

        for (txn, name) in [(txn_a, "a"), (txn_b, "b")] {
            let mut arena = StrArena::new();
            // ducklake_schema: schema_id, schema_uuid, begin_snapshot,
            // end_snapshot, schema_name, path, path_is_relative.
            stage(
                txn,
                2,
                0,
                &[
                    u64_cell(1),
                    arena.cell(&format!("uuid-{name}")),
                    u64_cell(1),
                    null_cell(),
                    arena.cell(name),
                    arena.cell(&format!("{name}/")),
                    bool_cell(true),
                ],
            );
            stage(
                txn,
                0,
                0,
                &[
                    u64_cell(1),
                    i64_cell(1),
                    u64_cell(1),
                    u64_cell(2),
                    u64_cell(0),
                ],
            );
            stage(
                txn,
                1,
                0,
                &[
                    u64_cell(1),
                    arena.cell(&format!(r#"created_schema:"{name}""#)),
                    null_cell(),
                    null_cell(),
                    null_cell(),
                ],
            );
            // Leak `arena` to keep its `CString`s alive past every `stage`
            // call's borrowed pointers.
            std::mem::forget(arena);
        }

        let mut id_a: u64 = 0;
        let mut err_a = MoraineError::default();
        // SAFETY: `txn_a` is live; outputs are valid local slots.
        let code_a = unsafe { moraine_txn_commit(txn_a, &raw mut id_a, &raw mut err_a) };
        assert_eq!(code_a, codes::OK);

        let mut id_b: u64 = 0;
        let mut err_b = MoraineError::default();
        // SAFETY: `txn_b` is live; outputs are valid local slots.
        let code_b = unsafe { moraine_txn_commit(txn_b, &raw mut id_b, &raw mut err_b) };
        assert_eq!(code_b, codes::COMMIT_CONFLICT);
        assert_eq!(err_b.code, codes::COMMIT_CONFLICT);
        assert!(!err_b.message.is_null());
        // SAFETY: just populated above.
        let msg = unsafe { std::ffi::CStr::from_ptr(err_b.message) }
            .to_str()
            .unwrap();
        assert!(msg.contains("conflict"), "message: {msg}");

        // SAFETY: `err_b.message`/`handle` came from the calls above and
        // are each freed exactly once.
        unsafe {
            crate::abi::moraine_error_free(err_b.message);
            moraine_detach(handle);
        }
    }

    /// `moraine_txn_rollback` discards every staged row: nothing lands.
    #[test]
    fn rollback_discards_staged_rows() {
        let dir = TempDir::new("rollback");
        let handle = attach_ok(&dir);
        let txn = begin(handle);

        let mut arena = StrArena::new();
        stage(
            txn,
            2,
            0,
            &[
                u64_cell(1),
                arena.cell("uuid-r"),
                u64_cell(1),
                null_cell(),
                arena.cell("rolled_back"),
                arena.cell("rolled_back/"),
                bool_cell(true),
            ],
        );

        // SAFETY: `txn` is live, not yet committed or rolled back.
        unsafe { moraine_txn_rollback(txn) };

        let mut rows: *mut crate::dumps::MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            crate::dumps::moraine_dump_schemas(handle, &raw mut rows, &raw mut len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        // Only bootstrap's `main` schema — the rolled-back row never
        // landed.
        assert_eq!(len, 1);

        // SAFETY: each from its matching allocator; freed exactly once.
        unsafe {
            crate::dumps::moraine_dump_schemas_free(rows, len);
            moraine_detach(handle);
        }
    }

    /// A malformed staged row (wrong cell count) fails loudly as a
    /// corruption error at commit, not a panic.
    #[test]
    fn malformed_row_reports_corruption_not_a_panic() {
        let dir = TempDir::new("malformed");
        let handle = attach_ok(&dir);
        let txn = begin(handle);

        // Far too few cells for `ducklake_schema`.
        stage(txn, 2, 0, &[u64_cell(1)]);
        stage(
            txn,
            0,
            0,
            &[
                u64_cell(1),
                i64_cell(1),
                u64_cell(1),
                u64_cell(2),
                u64_cell(0),
            ],
        );
        let mut arena = StrArena::new();
        stage(
            txn,
            1,
            0,
            &[
                u64_cell(1),
                arena.cell(""),
                null_cell(),
                null_cell(),
                null_cell(),
            ],
        );

        let mut snapshot_id: u64 = 0;
        let mut err = MoraineError::default();
        // SAFETY: `txn` is live; outputs are valid local slots.
        let code = unsafe { moraine_txn_commit(txn, &raw mut snapshot_id, &raw mut err) };
        assert_eq!(code, codes::CORRUPTION);

        // SAFETY: `err.message`/`handle` came from the calls above and are
        // each freed exactly once.
        unsafe {
            crate::abi::moraine_error_free(err.message);
            moraine_detach(handle);
        }
    }

    #[test]
    fn begin_on_null_handle_reports_invalid_argument() {
        let mut txn: *mut MoraineTxnHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: a null `handle` is exactly the input this test exercises;
        // outputs are valid local slots.
        let code = unsafe { moraine_txn_begin(ptr::null_mut(), &raw mut txn, &raw mut err) };
        assert_eq!(code, codes::INVALID_ARGUMENT);
        assert!(txn.is_null());
        // SAFETY: `err.message` was just populated above and not yet freed.
        unsafe { crate::abi::moraine_error_free(err.message) };
    }

    #[test]
    fn rollback_on_null_txn_is_a_no_op() {
        // SAFETY: a null `txn` is exactly the input this test exercises,
        // documented as a no-op.
        unsafe { moraine_txn_rollback(ptr::null_mut()) };
    }

    #[test]
    fn stage_rejects_unknown_table_kind() {
        let dir = TempDir::new("bad-kind");
        let handle = attach_ok(&dir);
        let txn = begin(handle);

        let mut err = MoraineError::default();
        // SAFETY: `txn` is live; empty cells slice; outputs are valid.
        let code = unsafe { moraine_txn_stage(txn, 99, 0, ptr::null(), 0, &raw mut err) };
        assert_eq!(code, codes::INVALID_ARGUMENT);

        // SAFETY: `err.message`/`txn`/`handle` came from the calls above.
        unsafe {
            crate::abi::moraine_error_free(err.message);
            moraine_txn_rollback(txn);
            moraine_detach(handle);
        }
    }

    /// `cpp/moraine_abi.h` is a hand-written C mirror, kept in lockstep by
    /// hand (see `crate::abi`'s identical test). Checks textual presence
    /// of each symbol/struct name only.
    #[test]
    fn header_declares_every_staged_txn_symbol() {
        let header = include_str!("../cpp/moraine_abi.h");
        let names = [
            "moraine_txn_begin",
            "moraine_txn_stage",
            "moraine_txn_commit",
            "moraine_txn_rollback",
            "MoraineTxnHandle",
            "MoraineCell",
            "moraine_txn_stage_inline_schema",
            "moraine_txn_stage_inline_insert",
            "moraine_txn_stage_inline_inline_delete",
            "moraine_txn_stage_inline_file_delete",
            "moraine_txn_stage_inline_flush_delete",
            "moraine_txn_stage_inline_drop",
            "moraine_txn_stage_inline_schema_drop",
        ];
        for name in names {
            assert!(
                header.contains(name),
                "cpp/moraine_abi.h is missing `{name}`, declared in src/staged.rs — \
                 the two must be kept in lockstep by hand"
            );
        }
    }
}
