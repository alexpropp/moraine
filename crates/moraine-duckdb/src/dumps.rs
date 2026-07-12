//! Row-faithful `ducklake_*` dumps: one C array per table kind the store
//! models, carrying **every** cur and hist row with every lifecycle
//! column verbatim. This is the data feed the coming metadata-table shim
//! scans — DuckLake itself filters lifecycles (`begin_snapshot`,
//! `end_snapshot`) in SQL, so moraine serves everything unfiltered.
//!
//! Split from [`crate::abi`] (already the crate's largest module) but
//! sharing its conventions: `catch_unwind`/null/UTF-8 discipline via
//! [`guard`](crate::abi), owned-first `CString` construction, one
//! `_free` per dump. Every function opens its own fresh transaction
//! against [`moraine::ffi_support`] — no snapshot handle is involved, and
//! no two dump calls are guaranteed to observe the same head (see that
//! module's docs).
//!
//! Two nullability conventions cross the C boundary:
//! - an optional **string** is a null pointer for `None`, exactly like
//!   every non-optional string field already crossing this boundary;
//! - an optional **scalar** (`u64`/`bool`) has no sentinel value that
//!   cannot also be a legitimate id, count, or flag, so it is carried as
//!   a `has_<field>` companion flag next to the raw field, which is
//!   meaningless when the flag is `false`.

use std::ffi::{CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use crate::abi::{free_array, free_c_string, guard, to_c_string, write_array};
use crate::error::{AbiError, MoraineError, codes};
use crate::runtime::MoraineCatalogHandle;

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
    /// Whether `end_snapshot` is present (`false` for a live/cur row).
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

/// Dumps every `ducklake_schema` row — cur and hist — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// `handle` must be a pointer previously returned by
/// [`moraine_attach`](crate::abi::moraine_attach) and not yet detached.
/// `out_items`/`out_len` must be valid, writable pointers. `err`, if
/// non-null, must be a valid, writable [`MoraineError`]. All for the
/// duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_schemas(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSchemaRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSchemaRow>, AbiError> {
        struct Owned {
            schema_id: u64,
            schema_uuid: CString,
            begin_snapshot: u64,
            has_end_snapshot: bool,
            end_snapshot: u64,
            schema_name: CString,
            path: CString,
            path_is_relative: bool,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_schemas(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first: every string in the whole batch converts before
        // any raw pointer is minted, so a partial failure (one row's
        // string fails to convert) leaks none of the rows already
        // processed.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_end, end) = opt_u64(v.end_snapshot);
                Ok(Owned {
                    schema_id: v.schema_id,
                    schema_uuid: to_c_string(&v.schema_uuid)?,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    schema_name: to_c_string(&v.schema_name)?,
                    path: to_c_string(&v.path)?,
                    path_is_relative: v.path_is_relative,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineSchemaRow {
                schema_id: o.schema_id,
                schema_uuid: o.schema_uuid.into_raw(),
                begin_snapshot: o.begin_snapshot,
                has_end_snapshot: o.has_end_snapshot,
                end_snapshot: o.end_snapshot,
                schema_name: o.schema_name.into_raw(),
                path: o.path.into_raw(),
                path_is_relative: o.path_is_relative,
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

/// Dumps every `ducklake_table` row — cur and hist — into
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
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineTableRow>, AbiError> {
        struct Owned {
            table_id: u64,
            table_uuid: CString,
            begin_snapshot: u64,
            has_end_snapshot: bool,
            end_snapshot: u64,
            schema_id: u64,
            table_name: CString,
            path: CString,
            path_is_relative: bool,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_tables(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_end, end) = opt_u64(v.end_snapshot);
                Ok(Owned {
                    table_id: v.table_id,
                    table_uuid: to_c_string(&v.table_uuid)?,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    schema_id: v.schema_id,
                    table_name: to_c_string(&v.table_name)?,
                    path: to_c_string(&v.path)?,
                    path_is_relative: v.path_is_relative,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineTableRow {
                table_id: o.table_id,
                table_uuid: o.table_uuid.into_raw(),
                begin_snapshot: o.begin_snapshot,
                has_end_snapshot: o.has_end_snapshot,
                end_snapshot: o.end_snapshot,
                schema_id: o.schema_id,
                table_name: o.table_name.into_raw(),
                path: o.path.into_raw(),
                path_is_relative: o.path_is_relative,
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

/// Dumps every `ducklake_view` row — cur and hist — into
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
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineViewRow>, AbiError> {
        struct Owned {
            view_id: u64,
            view_uuid: CString,
            begin_snapshot: u64,
            has_end_snapshot: bool,
            end_snapshot: u64,
            schema_id: u64,
            view_name: CString,
            dialect: CString,
            sql: CString,
            column_aliases: Option<CString>,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_views(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_end, end) = opt_u64(v.end_snapshot);
                Ok(Owned {
                    view_id: v.view_id,
                    view_uuid: to_c_string(&v.view_uuid)?,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    schema_id: v.schema_id,
                    view_name: to_c_string(&v.view_name)?,
                    dialect: to_c_string(&v.dialect)?,
                    sql: to_c_string(&v.sql)?,
                    column_aliases: opt_c_string(v.column_aliases.as_deref())?,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineViewRow {
                view_id: o.view_id,
                view_uuid: o.view_uuid.into_raw(),
                begin_snapshot: o.begin_snapshot,
                has_end_snapshot: o.has_end_snapshot,
                end_snapshot: o.end_snapshot,
                schema_id: o.schema_id,
                view_name: o.view_name.into_raw(),
                dialect: o.dialect.into_raw(),
                sql: o.sql.into_raw(),
                column_aliases: opt_into_raw(o.column_aliases),
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

/// Dumps every `ducklake_column` row — cur and hist — into
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
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineColumnRow>, AbiError> {
        struct Owned {
            column_id: u64,
            begin_snapshot: u64,
            has_end_snapshot: bool,
            end_snapshot: u64,
            table_id: u64,
            column_order: u64,
            column_name: CString,
            column_type: CString,
            initial_default: Option<CString>,
            default_value: Option<CString>,
            nulls_allowed: bool,
            has_parent_column: bool,
            parent_column: u64,
            default_value_type: Option<CString>,
            default_value_dialect: Option<CString>,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_columns(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_end, end) = opt_u64(v.end_snapshot);
                let (has_parent, parent) = opt_u64(v.parent_column);
                Ok(Owned {
                    column_id: v.column_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    table_id: v.table_id,
                    column_order: v.column_order,
                    column_name: to_c_string(&v.column_name)?,
                    column_type: to_c_string(&v.column_type)?,
                    initial_default: opt_c_string(v.initial_default.as_deref())?,
                    default_value: opt_c_string(v.default_value.as_deref())?,
                    nulls_allowed: v.nulls_allowed,
                    has_parent_column: has_parent,
                    parent_column: parent,
                    default_value_type: opt_c_string(v.default_value_type.as_deref())?,
                    default_value_dialect: opt_c_string(v.default_value_dialect.as_deref())?,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineColumnRow {
                column_id: o.column_id,
                begin_snapshot: o.begin_snapshot,
                has_end_snapshot: o.has_end_snapshot,
                end_snapshot: o.end_snapshot,
                table_id: o.table_id,
                column_order: o.column_order,
                column_name: o.column_name.into_raw(),
                column_type: o.column_type.into_raw(),
                initial_default: opt_into_raw(o.initial_default),
                default_value: opt_into_raw(o.default_value),
                nulls_allowed: o.nulls_allowed,
                has_parent_column: o.has_parent_column,
                parent_column: o.parent_column,
                default_value_type: opt_into_raw(o.default_value_type),
                default_value_dialect: opt_into_raw(o.default_value_dialect),
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
    /// `row_id_start`.
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

/// Dumps every `ducklake_data_file` row — cur and hist — into
/// `*out_items`/`*out_len`.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
// `ducklake_data_file` is the widest row this module dumps (21 columns,
// 6 of them `has_*` optional-scalar flags mirroring `MoraineDataFileRow`
// field-for-field) — length and bool count are inherent to the row
// shape, not a design smell.
#[allow(clippy::too_many_lines)]
pub unsafe extern "C" fn moraine_dump_data_files(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineDataFileRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineDataFileRow>, AbiError> {
        // `Owned` mirrors `MoraineDataFileRow` field-for-field (see that
        // struct's docs on the `has_*` optional-scalar convention); its
        // bool count is inherent to the row shape.
        #[allow(clippy::struct_excessive_bools)]
        struct Owned {
            data_file_id: u64,
            table_id: u64,
            begin_snapshot: u64,
            has_end_snapshot: bool,
            end_snapshot: u64,
            has_file_order: bool,
            file_order: u64,
            path: CString,
            path_is_relative: bool,
            file_format: CString,
            record_count: u64,
            file_size_bytes: u64,
            footer_size: u64,
            row_id_start: u64,
            has_partition_id: bool,
            partition_id: u64,
            encryption_key: Option<CString>,
            has_mapping_id: bool,
            mapping_id: u64,
            has_partial_max: bool,
            partial_max: u64,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_data_files(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_end, end) = opt_u64(v.end_snapshot);
                let (has_order, order) = opt_u64(v.file_order);
                let (has_partition, partition) = opt_u64(v.partition_id);
                let (has_mapping, mapping) = opt_u64(v.mapping_id);
                let (has_partial_max, partial_max) = opt_u64(v.partial_max);
                Ok(Owned {
                    data_file_id: v.data_file_id,
                    table_id: v.table_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    has_file_order: has_order,
                    file_order: order,
                    path: to_c_string(&v.path)?,
                    path_is_relative: v.path_is_relative,
                    file_format: to_c_string(&v.file_format)?,
                    record_count: v.record_count,
                    file_size_bytes: v.file_size_bytes,
                    footer_size: v.footer_size,
                    row_id_start: v.row_id_start,
                    has_partition_id: has_partition,
                    partition_id: partition,
                    encryption_key: opt_c_string(v.encryption_key.as_deref())?,
                    has_mapping_id: has_mapping,
                    mapping_id: mapping,
                    has_partial_max,
                    partial_max,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineDataFileRow {
                data_file_id: o.data_file_id,
                table_id: o.table_id,
                begin_snapshot: o.begin_snapshot,
                has_end_snapshot: o.has_end_snapshot,
                end_snapshot: o.end_snapshot,
                has_file_order: o.has_file_order,
                file_order: o.file_order,
                path: o.path.into_raw(),
                path_is_relative: o.path_is_relative,
                file_format: o.file_format.into_raw(),
                record_count: o.record_count,
                file_size_bytes: o.file_size_bytes,
                footer_size: o.footer_size,
                row_id_start: o.row_id_start,
                has_partition_id: o.has_partition_id,
                partition_id: o.partition_id,
                encryption_key: opt_into_raw(o.encryption_key),
                has_mapping_id: o.has_mapping_id,
                mapping_id: o.mapping_id,
                has_partial_max: o.has_partial_max,
                partial_max: o.partial_max,
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

/// Dumps every `ducklake_delete_file` row — cur and hist — into
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
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineDeleteFileRow>, AbiError> {
        struct Owned {
            delete_file_id: u64,
            table_id: u64,
            begin_snapshot: u64,
            has_end_snapshot: bool,
            end_snapshot: u64,
            data_file_id: u64,
            path: CString,
            path_is_relative: bool,
            format: CString,
            delete_count: u64,
            file_size_bytes: u64,
            footer_size: u64,
            encryption_key: Option<CString>,
            has_partial_max: bool,
            partial_max: u64,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_delete_files(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_end, end) = opt_u64(v.end_snapshot);
                let (has_partial_max, partial_max) = opt_u64(v.partial_max);
                Ok(Owned {
                    delete_file_id: v.delete_file_id,
                    table_id: v.table_id,
                    begin_snapshot: v.begin_snapshot,
                    has_end_snapshot: has_end,
                    end_snapshot: end,
                    data_file_id: v.data_file_id,
                    path: to_c_string(&v.path)?,
                    path_is_relative: v.path_is_relative,
                    format: to_c_string(&v.format)?,
                    delete_count: v.delete_count,
                    file_size_bytes: v.file_size_bytes,
                    footer_size: v.footer_size,
                    encryption_key: opt_c_string(v.encryption_key.as_deref())?,
                    has_partial_max,
                    partial_max,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineDeleteFileRow {
                delete_file_id: o.delete_file_id,
                table_id: o.table_id,
                begin_snapshot: o.begin_snapshot,
                has_end_snapshot: o.has_end_snapshot,
                end_snapshot: o.end_snapshot,
                data_file_id: o.data_file_id,
                path: o.path.into_raw(),
                path_is_relative: o.path_is_relative,
                format: o.format.into_raw(),
                delete_count: o.delete_count,
                file_size_bytes: o.file_size_bytes,
                footer_size: o.footer_size,
                encryption_key: opt_into_raw(o.encryption_key),
                has_partial_max: o.has_partial_max,
                partial_max: o.partial_max,
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
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_table_stats(&handle_ref.catalog))
            .map_err(AbiError::from)?;
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
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineTableColumnStatsRow>, AbiError> {
        // `Owned` mirrors `MoraineTableColumnStatsRow` field-for-field; its
        // bool count is inherent to the row's optional-scalar columns.
        #[allow(clippy::struct_excessive_bools)]
        struct Owned {
            table_id: u64,
            column_id: u64,
            has_contains_null: bool,
            contains_null: bool,
            has_contains_nan: bool,
            contains_nan: bool,
            min_value: Option<CString>,
            max_value: Option<CString>,
            extra_stats: Option<CString>,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_table_column_stats(
                &handle_ref.catalog,
            ))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_null, contains_null) = opt_bool(v.contains_null);
                let (has_nan, contains_nan) = opt_bool(v.contains_nan);
                Ok(Owned {
                    table_id: v.table_id,
                    column_id: v.column_id,
                    has_contains_null: has_null,
                    contains_null,
                    has_contains_nan: has_nan,
                    contains_nan,
                    min_value: opt_c_string(v.min_value.as_deref())?,
                    max_value: opt_c_string(v.max_value.as_deref())?,
                    extra_stats: opt_c_string(v.extra_stats.as_deref())?,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineTableColumnStatsRow {
                table_id: o.table_id,
                column_id: o.column_id,
                has_contains_null: o.has_contains_null,
                contains_null: o.contains_null,
                has_contains_nan: o.has_contains_nan,
                contains_nan: o.contains_nan,
                min_value: opt_into_raw(o.min_value),
                max_value: opt_into_raw(o.max_value),
                extra_stats: opt_into_raw(o.extra_stats),
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
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineFileColumnStatsRow>, AbiError> {
        struct Owned {
            data_file_id: u64,
            table_id: u64,
            column_id: u64,
            column_size_bytes: u64,
            value_count: u64,
            null_count: u64,
            min_value: Option<CString>,
            max_value: Option<CString>,
            has_contains_nan: bool,
            contains_nan: bool,
            extra_stats: Option<CString>,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_file_column_stats(
                &handle_ref.catalog,
            ))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                let (has_nan, contains_nan) = opt_bool(v.contains_nan);
                Ok(Owned {
                    data_file_id: v.data_file_id,
                    table_id: v.table_id,
                    column_id: v.column_id,
                    column_size_bytes: v.column_size_bytes,
                    value_count: v.value_count,
                    null_count: v.null_count,
                    min_value: opt_c_string(v.min_value.as_deref())?,
                    max_value: opt_c_string(v.max_value.as_deref())?,
                    has_contains_nan: has_nan,
                    contains_nan,
                    extra_stats: opt_c_string(v.extra_stats.as_deref())?,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineFileColumnStatsRow {
                data_file_id: o.data_file_id,
                table_id: o.table_id,
                column_id: o.column_id,
                column_size_bytes: o.column_size_bytes,
                value_count: o.value_count,
                null_count: o.null_count,
                min_value: opt_into_raw(o.min_value),
                max_value: opt_into_raw(o.max_value),
                has_contains_nan: o.has_contains_nan,
                contains_nan: o.contains_nan,
                extra_stats: opt_into_raw(o.extra_stats),
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

/// Dumps every `ducklake_snapshot` row into `*out_items`/`*out_len`.
/// Snapshots carry no begin/end lifecycle of their own — this is the
/// full committed history, not a cur/hist split.
///
/// # Safety
///
/// Same pointer contract as [`moraine_dump_schemas`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_dump_snapshots(
    handle: *mut MoraineCatalogHandle,
    out_items: *mut *mut MoraineSnapshotRow,
    out_len: *mut usize,
    err: *mut MoraineError,
) -> i32 {
    let attempt = || -> Result<Vec<MoraineSnapshotRow>, AbiError> {
        struct Owned {
            snapshot_id: u64,
            snapshot_time_micros: i64,
            schema_version: u64,
            next_catalog_id: u64,
            next_file_id: u64,
            changes_made: CString,
            author: Option<CString>,
            commit_message: Option<CString>,
            commit_extra_info: Option<CString>,
        }
        if handle.is_null() {
            return Err(AbiError::invalid_argument("`handle` is null"));
        }
        if out_items.is_null() || out_len.is_null() {
            return Err(AbiError::invalid_argument("output pointer is null"));
        }
        // SAFETY: caller contract for `handle`.
        let handle_ref = unsafe { &*handle };
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_snapshots(&handle_ref.catalog))
            .map_err(AbiError::from)?;
        // Owned-first (see `moraine_dump_schemas`): every string in the
        // whole batch converts before any raw pointer is minted.
        let owned: Vec<Owned> = rows
            .into_iter()
            .map(|v| -> Result<Owned, AbiError> {
                Ok(Owned {
                    snapshot_id: v.snapshot_id,
                    snapshot_time_micros: v.snapshot_time_micros,
                    schema_version: v.schema_version,
                    next_catalog_id: v.next_catalog_id,
                    next_file_id: v.next_file_id,
                    changes_made: to_c_string(&v.changes_made)?,
                    author: opt_c_string(v.author.as_deref())?,
                    commit_message: opt_c_string(v.commit_message.as_deref())?,
                    commit_extra_info: opt_c_string(v.commit_extra_info.as_deref())?,
                })
            })
            .collect::<Result<_, AbiError>>()?;
        Ok(owned
            .into_iter()
            .map(|o| MoraineSnapshotRow {
                snapshot_id: o.snapshot_id,
                snapshot_time_micros: o.snapshot_time_micros,
                schema_version: o.schema_version,
                next_catalog_id: o.next_catalog_id,
                next_file_id: o.next_file_id,
                changes_made: o.changes_made.into_raw(),
                author: opt_into_raw(o.author),
                commit_message: opt_into_raw(o.commit_message),
                commit_extra_info: opt_into_raw(o.commit_extra_info),
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
        let rows = handle_ref
            .runtime
            .block_on(moraine::ffi_support::dump_schema_versions(
                &handle_ref.catalog,
            ))
            .map_err(AbiError::from)?;
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

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

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
    /// so `ducklake_table` carries one hist row (the old name, ended)
    /// alongside its new cur row — the fixture every dump test below
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

    fn attach_ok(dir: &Path) -> *mut MoraineCatalogHandle {
        let c_path =
            CString::new(dir.to_str().expect("test path is UTF-8")).expect("test path has no NUL");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
        let code =
            unsafe { moraine_attach(c_path.as_ptr(), ptr::null(), &raw mut handle, &raw mut err) };
        // SAFETY: `err.message` is null or was just written by the call above.
        let err_message = unsafe { err.message.as_ref() };
        assert_eq!(code, codes::OK, "attach failed: {err_message:?}");
        assert!(!handle.is_null());
        handle
    }

    #[test]
    fn dump_schemas_and_tables_return_cur_and_hist_rows() {
        let dir = TempDir::new("schemas-tables");
        seed(dir.path());
        let handle = attach_ok(dir.path());

        let mut schemas: *mut MoraineSchemaRow = ptr::null_mut();
        let mut schemas_len: usize = 0;
        let mut err = MoraineError::default();
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_schemas(handle, &raw mut schemas, &raw mut schemas_len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        // `main` (bootstrap) + `sales`; the rename never touched a
        // schema, so neither carries a hist row.
        assert_eq!(schemas_len, 2);
        // SAFETY: just populated above with `schemas_len` live elements.
        let schema_slice = unsafe { std::slice::from_raw_parts(schemas, schemas_len) };
        assert!(schema_slice.iter().all(|s| !s.has_end_snapshot));

        let mut tables: *mut MoraineTableRow = ptr::null_mut();
        let mut tables_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_tables(handle, &raw mut tables, &raw mut tables_len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        assert_eq!(
            tables_len, 2,
            "rename must yield one cur row + one hist row"
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
    /// `CORRUPTION`, leaving the outputs untouched. The dumped views are
    /// ordered by id, so the earlier, clean view's string converts
    /// successfully before the later, poisoned one fails — a regression
    /// test for the owned-first discipline: `moraine_dump_views` must
    /// finish converting every row's strings before minting any raw
    /// pointer, or the clean view's already-converted `CString`s would
    /// leak when the whole call fails.
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
        let code =
            unsafe { moraine_dump_views(handle, &raw mut views, &raw mut views_len, &raw mut err) };
        assert_eq!(code, codes::CORRUPTION);
        assert_eq!(err.code, codes::CORRUPTION);
        // Nothing was handed to the caller, so there is nothing to free —
        // this is what makes leak-freedom hold by construction rather
        // than by this test's observation alone.
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
            moraine_dump_columns(handle, &raw mut columns, &raw mut columns_len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        assert_eq!(columns_len, 2);
        // SAFETY: just populated above.
        let col_slice = unsafe { std::slice::from_raw_parts(columns, columns_len) };
        assert!(col_slice.iter().all(|c| !c.has_end_snapshot));
        assert!(!col_slice[0].nulls_allowed || !col_slice[1].nulls_allowed);

        let mut views: *mut MoraineViewRow = ptr::null_mut();
        let mut views_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code =
            unsafe { moraine_dump_views(handle, &raw mut views, &raw mut views_len, &raw mut err) };
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
            moraine_dump_data_files(handle, &raw mut files, &raw mut files_len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        assert_eq!(files_len, 1);
        // SAFETY: just populated above.
        let file_path = unsafe { CStr::from_ptr((*files).path) }.to_str().unwrap();
        assert_eq!(file_path, "orders/data-1.parquet");
        // SAFETY: same as above.
        assert_eq!(unsafe { (*files).record_count }, 10);
        // SAFETY: same as above.
        assert!(!unsafe { (*files).has_partition_id });
        // SAFETY: same as above.
        let data_file_id = unsafe { (*files).data_file_id };

        let mut deletes: *mut MoraineDeleteFileRow = ptr::null_mut();
        let mut deletes_len: usize = 0;
        // SAFETY: `handle` is attached; outputs are valid local slots.
        let code = unsafe {
            moraine_dump_delete_files(handle, &raw mut deletes, &raw mut deletes_len, &raw mut err)
        };
        assert_eq!(code, codes::OK);
        assert_eq!(deletes_len, 1);
        // SAFETY: just populated above.
        assert_eq!(unsafe { (*deletes).data_file_id }, data_file_id);
        // SAFETY: same as above.
        assert_eq!(unsafe { (*deletes).delete_count }, 2);

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
        // SAFETY: bootstrap's `changes_made` is a non-null, empty string.
        let bootstrap_changes = unsafe { CStr::from_ptr(snap_slice[0].changes_made) }
            .to_str()
            .unwrap();
        assert_eq!(bootstrap_changes, "");
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
    fn dump_on_null_handle_reports_invalid_argument() {
        let mut err = MoraineError::default();
        let mut out: *mut MoraineSchemaRow = ptr::null_mut();
        let mut len: usize = 0;
        // SAFETY: a null `handle` is exactly the input this test exercises.
        let code = unsafe {
            moraine_dump_schemas(ptr::null_mut(), &raw mut out, &raw mut len, &raw mut err)
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
