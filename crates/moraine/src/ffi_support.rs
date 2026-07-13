//! Internal, unstable seam for the `moraine-duckdb` ABI crate.
//!
//! `#[doc(hidden)]` despite the `pub` visibility: not part of the crate's
//! semver contract — shape and presence may change without notice.
//!
//! Each `dump_*` function returns every record of one kind, current and history
//! together, as the wire value type that kind encodes to
//! (`crate::store::proto`) — row-faithful and unfiltered; unversioned
//! kinds yield only their single current row since they are never mirrored
//! to history. DuckLake filters lifecycles in SQL over these rows.
//!
//! Every function opens one fresh read-only transaction, scans, and rolls
//! back. Views spanning several `dump_*` calls are not snapshot-consistent:
//! each call reads at whatever the current head is when it runs.

use crate::{
    catalog::Catalog,
    error::Result,
    store::{
        proto::{
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, SchemaValue,
            SnapshotValue, TableColumnStatsValue, TableStatsValue, TableValue, ViewValue,
        },
        read::{EntityRecord, scan_current_entities, scan_history_entities, scan_snapshots},
    },
};

#[doc(hidden)]
pub mod inline;
#[doc(hidden)]
pub mod staged;

/// Scans `current` then `history` in one transaction, keeping only the records
/// `extract` maps to `Some` — the shared engine every entity-kind
/// `dump_*` function below is a thin, concretely typed wrapper over.
async fn dump_entities<T>(
    catalog: &Catalog,
    extract: impl Fn(EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    let session = catalog.begin_read().await?;
    let current = scan_current_entities(session.handle()).await;
    let history = scan_history_entities(session.handle()).await;
    session.finish();
    let mut records = current?;
    records.extend(history?);
    Ok(records.into_iter().filter_map(extract).collect())
}

/// Every `ducklake_schema` row, current and history.
#[doc(hidden)]
pub async fn dump_schemas(catalog: &Catalog) -> Result<Vec<SchemaValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Schema(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_table` row, current and history.
#[doc(hidden)]
pub async fn dump_tables(catalog: &Catalog) -> Result<Vec<TableValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Table(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_view` row, current and history.
#[doc(hidden)]
pub async fn dump_views(catalog: &Catalog) -> Result<Vec<ViewValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::View(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_column` row, current and history.
#[doc(hidden)]
pub async fn dump_columns(catalog: &Catalog) -> Result<Vec<ColumnValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Column(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_data_file` row, current and history.
#[doc(hidden)]
pub async fn dump_data_files(catalog: &Catalog) -> Result<Vec<DataFileValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::File(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_delete_file` row, current and history.
#[doc(hidden)]
pub async fn dump_delete_files(catalog: &Catalog) -> Result<Vec<DeleteFileValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::DeleteFile(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_table_stats` row. Unversioned (overwritten in place,
/// never mirrored to history), so this is always exactly the live rows.
#[doc(hidden)]
pub async fn dump_table_stats(catalog: &Catalog) -> Result<Vec<TableStatsValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::TableStats(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_table_column_stats` row. Unversioned, as
/// [`dump_table_stats`].
#[doc(hidden)]
pub async fn dump_table_column_stats(catalog: &Catalog) -> Result<Vec<TableColumnStatsValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::TableColumnStats(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_file_column_stats` row. Unversioned, as
/// [`dump_table_stats`].
#[doc(hidden)]
pub async fn dump_file_column_stats(catalog: &Catalog) -> Result<Vec<FileColumnStatsValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::FileColumnStats(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_snapshot`/`ducklake_snapshot_changes` row (merged).
/// Snapshots are append-only and carry no begin/end lifecycle of their
/// own — this is the full history, not a current/history split.
#[doc(hidden)]
pub async fn dump_snapshots(catalog: &Catalog) -> Result<Vec<SnapshotValue>> {
    let session = catalog.begin_read().await?;
    let result = scan_snapshots(session.handle()).await;
    session.finish();
    result
}

/// One `ducklake_schema_versions` row: `(begin_snapshot, schema_version,
/// table_id)`, flattened from a snapshot record; the first two values are
/// the snapshot's own.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersionRow {
    /// The snapshot the schema change landed in.
    pub begin_snapshot: u64,
    /// That snapshot's `schema_version`.
    pub schema_version: u64,
    /// The created-or-schema-altered table.
    pub table_id: u64,
}

/// Every `ducklake_schema_versions` row, flattened from the snapshot
/// history: one row per `(snapshot, schema-changed table)` pair, in
/// snapshot order.
#[doc(hidden)]
pub async fn dump_schema_versions(catalog: &Catalog) -> Result<Vec<SchemaVersionRow>> {
    let snapshots = dump_snapshots(catalog).await?;
    Ok(snapshots
        .into_iter()
        .flat_map(|snapshot| {
            let begin_snapshot = snapshot.snapshot_id;
            let schema_version = snapshot.schema_version;
            snapshot
                .schema_changed_table_ids
                .into_iter()
                .map(move |table_id| SchemaVersionRow {
                    begin_snapshot,
                    schema_version,
                    table_id,
                })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::catalog::{
        CatalogOptions, ColumnDef, ColumnStats, DataFile, DeleteFile, FileColumnStats,
    };

    /// Seeds a store whose second commit renames a table — the fixture
    /// every assertion below reads from, so a table, a schema, a view, a
    /// data file, a delete file, and every statistics kind all carry both
    /// a current row and (for the versioned kinds) a history row with exact
    /// lifecycle values.
    async fn seed() -> Catalog {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();

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
            .unwrap();

        catalog
            .commit(|tx| {
                let table = tx.tables_in(tx.schemas()[1].id)[0].id;
                tx.rename_table(table, "orders2")
            })
            .await
            .unwrap();

        catalog
    }

    #[tokio::test]
    async fn dump_schemas_returns_bootstrap_and_seeded_schemas_with_no_history() {
        let catalog = seed().await;
        let rows = dump_schemas(&catalog).await.unwrap();
        // `main` (bootstrap) and `sales`; the rename touched only the
        // table, so neither schema ever moved to history.
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.end_snapshot.is_none()));
        let names: Vec<&str> = rows.iter().map(|r| r.schema_name.as_str()).collect();
        assert_eq!(names, vec!["main", "sales"]);
    }

    #[tokio::test]
    async fn dump_tables_returns_both_versions_of_a_renamed_table() {
        let catalog = seed().await;
        let rows = dump_tables(&catalog).await.unwrap();
        assert_eq!(
            rows.len(),
            2,
            "rename must yield exactly one current + one history row"
        );

        let ended = rows.iter().find(|r| r.end_snapshot.is_some()).unwrap();
        let live = rows.iter().find(|r| r.end_snapshot.is_none()).unwrap();
        assert_eq!(ended.table_name, "orders");
        assert_eq!(live.table_name, "orders2");
        // Same entity, same uuid, exact lifecycle stitching: the history
        // row's end_snapshot is the live row's (new) begin_snapshot.
        assert_eq!(ended.table_id, live.table_id);
        assert_eq!(ended.table_uuid, live.table_uuid);
        assert_eq!(ended.end_snapshot, Some(live.begin_snapshot));
        assert!(live.begin_snapshot > ended.begin_snapshot);
    }

    #[tokio::test]
    async fn dump_columns_and_views_are_row_faithful() {
        let catalog = seed().await;

        let columns = dump_columns(&catalog).await.unwrap();
        assert_eq!(columns.len(), 2);
        assert!(columns.iter().all(|c| c.end_snapshot.is_none()));
        let mut names: Vec<&str> = columns.iter().map(|c| c.column_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["amount", "id"]);

        let views = dump_views(&catalog).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].sql, "select * from orders");
        assert!(views[0].end_snapshot.is_none());
    }

    #[tokio::test]
    async fn dump_data_and_delete_files_carry_registration_values_verbatim() {
        let catalog = seed().await;

        let files = dump_data_files(&catalog).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "orders/data-1.parquet");
        assert_eq!(files[0].record_count, 10);
        assert_eq!(files[0].row_id_start, 0);
        assert!(files[0].end_snapshot.is_none());

        let deletes = dump_delete_files(&catalog).await.unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].path, "orders/delete-1.parquet");
        assert_eq!(deletes[0].data_file_id, files[0].data_file_id);
        assert_eq!(deletes[0].delete_count, 2);
    }

    #[tokio::test]
    async fn dump_statistics_kinds_are_unversioned_single_rows() {
        let catalog = seed().await;

        // The rename commit did not touch statistics, and stats are
        // never mirrored to history regardless — one row each, live values.
        let table_rows = dump_table_stats(&catalog).await.unwrap();
        assert_eq!(table_rows.len(), 1);
        assert_eq!(table_rows[0].record_count, 10);

        let table_col_rows = dump_table_column_stats(&catalog).await.unwrap();
        assert_eq!(table_col_rows.len(), 1);
        assert_eq!(table_col_rows[0].contains_null, Some(false));

        let file_col_rows = dump_file_column_stats(&catalog).await.unwrap();
        assert_eq!(file_col_rows.len(), 1);
        assert_eq!(file_col_rows[0].min_value.as_deref(), Some("1"));
        assert_eq!(file_col_rows[0].max_value.as_deref(), Some("10"));
    }

    #[tokio::test]
    async fn dump_snapshots_returns_every_committed_snapshot_in_order() {
        let catalog = seed().await;
        let rows = dump_snapshots(&catalog).await.unwrap();
        // Bootstrap (0) + the two commits `seed` makes.
        assert_eq!(rows.len(), 3);
        let ids: Vec<u64> = rows.iter().map(|r| r.snapshot_id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
        // Bootstrap's changes_made is the empty string; both real commits
        // record something.
        assert_eq!(rows[0].changes_made, "");
        assert!(!rows[1].changes_made.is_empty());
        assert!(!rows[2].changes_made.is_empty());
    }
}
