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

mod catalog;
mod files;
mod macros;
mod mappings;
mod partitions;
mod snapshots;
mod statistics;
mod tags;

use std::{
    ffi::{CString, c_char},
    ptr,
};

pub use catalog::*;
pub use files::*;
pub use macros::*;
pub use mappings::*;
pub use partitions::*;
pub use snapshots::*;
pub use statistics::*;
pub use tags::*;

use crate::{abi::to_c_string, error::AbiError};

/// Splits an optional `u64` into the `(has, value)` pair the dump row
/// structs carry.
pub(crate) fn opt_u64(v: Option<u64>) -> (bool, u64) {
    v.map_or((false, 0), |x| (true, x))
}

/// Splits an optional `bool` into the `(has, value)` pair the dump row
/// structs carry.
pub(crate) fn opt_bool(v: Option<bool>) -> (bool, bool) {
    v.map_or((false, false), |x| (true, x))
}

/// Converts an optional string to an owned, possibly-null `CString`.
pub(crate) fn opt_c_string(s: Option<&str>) -> Result<Option<CString>, AbiError> {
    s.map(to_c_string).transpose()
}

/// The raw pointer for an optional owned `CString`: null for `None`.
pub(crate) fn opt_into_raw(s: Option<CString>) -> *mut c_char {
    s.map_or(ptr::null_mut(), CString::into_raw)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared fixtures for the dump tests, reused by the per-family
    //! submodule tests.

    use std::{
        ffi::CString,
        path::{Path, PathBuf},
        ptr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use moraine::{ColumnDef, ColumnStats, DataFile, DeleteFile, FileColumnStats};
    use object_store::local::LocalFileSystem;

    use crate::{
        abi::moraine_attach,
        error::{MoraineError, codes},
        runtime::MoraineCatalogHandle,
    };

    /// A directory under the OS temp dir, unique per call, removed on
    /// drop.
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "moraine-duckdb-dumps-{tag}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
            Self(dir)
        }

        pub(crate) fn path(&self) -> &Path {
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
    pub(crate) fn seed(dir: &Path) {
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
                        &[],
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
                        &[],
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
    pub(crate) fn seed_with_tags(dir: &Path) {
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
            let mut tx = staged_begin(&catalog, None, String::new())
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

    pub(crate) fn attach_ok(dir: &Path) -> *mut MoraineCatalogHandle {
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
                0,
                ptr::null(),
                ptr::null(),
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
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_void};

    use super::{
        test_support::{TempDir, attach_ok, seed},
        *,
    };
    use crate::{
        abi::{free_c_string, moraine_detach, moraine_error_free},
        error::{MoraineError, codes},
    };

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
            moraine_dump_macros_free(ptr::null_mut(), 0);
            moraine_dump_macro_impls_free(ptr::null_mut(), 0);
            moraine_dump_macro_parameters_free(ptr::null_mut(), 0);
            moraine_dump_column_mappings_free(ptr::null_mut(), 0);
            moraine_dump_name_mappings_free(ptr::null_mut(), 0);
            moraine_dump_tags_free(ptr::null_mut(), 0);
            moraine_dump_column_tags_free(ptr::null_mut(), 0);
            moraine_dump_scheduled_deletions_free(ptr::null_mut(), 0);
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
            "moraine_dump_macros",
            "moraine_dump_macros_free",
            "moraine_dump_macro_impls",
            "moraine_dump_macro_impls_free",
            "moraine_dump_macro_parameters",
            "moraine_dump_macro_parameters_free",
            "moraine_dump_column_mappings",
            "moraine_dump_column_mappings_free",
            "moraine_dump_name_mappings",
            "moraine_dump_name_mappings_free",
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
            "MoraineMacroRow",
            "MoraineMacroImplRow",
            "MoraineMacroParameterRow",
            "MoraineColumnMappingRow",
            "MoraineNameMappingRow",
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
