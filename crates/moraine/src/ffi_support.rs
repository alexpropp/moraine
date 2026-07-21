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
            ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, GcFileValue,
            MacroValue, MappingValue, PartitionValue, SchemaValue, SnapshotValue, SortValue,
            TableColumnStatsValue, TableStatsValue, TableValue, ViewValue,
        },
        read::{
            EntityRecord, read_head, scan_current_entities, scan_history_entities, scan_snapshots,
        },
    },
};

/// The head snapshot id inside an open read session, or `None` on a
/// store that has no head yet (mid-bootstrap).
async fn session_head(session: &crate::store::handle::ReadSession) -> Result<Option<u64>> {
    Ok(read_head(session.handle()).await?.map(|h| h.snapshot_id))
}

/// Locks the shared projection state for reading, recovering a poisoned
/// lock (folds never panic mid-flight, so the state is whole).
fn projections_read(
    catalog: &Catalog,
) -> std::sync::RwLockReadGuard<'_, crate::catalog::projection::ProjectionCache> {
    catalog
        .projections()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// As [`projections_read`], for writing (installs).
fn projections_write(
    catalog: &Catalog,
) -> std::sync::RwLockWriteGuard<'_, crate::catalog::projection::ProjectionCache> {
    catalog
        .projections()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[doc(hidden)]
pub mod inline;
#[doc(hidden)]
pub mod staged;

/// Scans `current` then `history` in one transaction, keeping only the records
/// `extract` maps to `Some` — the shared engine every *versioned*
/// entity-kind `dump_*` function below is a thin, concretely typed
/// wrapper over.
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

/// As [`dump_entities`], but `current` only — for the unversioned kinds
/// (statistics, tags, scheduled deletions) that are overwritten in place
/// and never mirrored to `history`, where that scan is pure waste.
async fn dump_current_entities<T>(
    catalog: &Catalog,
    extract: impl Fn(EntityRecord) -> Option<T>,
) -> Result<Vec<T>> {
    let session = catalog.begin_read().await?;
    let current = scan_current_entities(session.handle()).await;
    session.finish();
    Ok(current?.into_iter().filter_map(extract).collect())
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

/// Every `ducklake_macro` row, current and history, implementations and
/// their parameters embedded in `impl_id`/`column_id` order.
#[doc(hidden)]
pub async fn dump_macros(catalog: &Catalog) -> Result<Vec<MacroValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Macro(m) => Some(m),
        _ => None,
    })
    .await
}

/// Every `ducklake_column_mapping` row with its embedded
/// `ducklake_name_mapping` rows in `column_id` order. Unversioned
/// (create-only, never mirrored), so this is always exactly the live
/// rows.
#[doc(hidden)]
pub async fn dump_mappings(catalog: &Catalog) -> Result<Vec<MappingValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Mapping(m) => Some(m),
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

/// Every `ducklake_partition_info` row (with its embedded partition
/// columns), current and history.
#[doc(hidden)]
pub async fn dump_partition_info(catalog: &Catalog) -> Result<Vec<PartitionValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Partition(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_sort_info` row (with its embedded sort expressions),
/// current and history.
#[doc(hidden)]
pub async fn dump_sort_info(catalog: &Catalog) -> Result<Vec<SortValue>> {
    dump_entities(catalog, |r| match r {
        EntityRecord::Sort(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_table_stats` row. Unversioned (overwritten in place,
/// never mirrored to history), so this is always exactly the live rows.
/// Served from the maintained projection when its head matches; a fresh
/// scan installs it otherwise.
#[doc(hidden)]
pub async fn dump_table_stats(catalog: &Catalog) -> Result<Vec<TableStatsValue>> {
    let session = catalog.begin_read().await?;
    let head = session_head(&session).await?;
    if let (true, Some(head)) = (catalog.maintains_projections(), head) {
        if let Some(rows) = projections_read(catalog).table_stats_at(head) {
            session.finish();
            return Ok(rows);
        }
        let current = scan_current_entities(session.handle()).await;
        session.finish();
        let rows: Vec<TableStatsValue> = current?
            .into_iter()
            .filter_map(|r| match r {
                EntityRecord::TableStats(v) => Some(v),
                _ => None,
            })
            .collect();
        projections_write(catalog).install_table_stats(head, rows.clone());
        return Ok(rows);
    }
    let current = scan_current_entities(session.handle()).await;
    session.finish();
    Ok(current?
        .into_iter()
        .filter_map(|r| match r {
            EntityRecord::TableStats(v) => Some(v),
            _ => None,
        })
        .collect())
}

/// Every `ducklake_table_column_stats` row. Unversioned, as
/// [`dump_table_stats`], and served from the maintained projection the
/// same way.
#[doc(hidden)]
pub async fn dump_table_column_stats(catalog: &Catalog) -> Result<Vec<TableColumnStatsValue>> {
    let session = catalog.begin_read().await?;
    let head = session_head(&session).await?;
    if let (true, Some(head)) = (catalog.maintains_projections(), head) {
        if let Some(rows) = projections_read(catalog).table_column_stats_at(head) {
            session.finish();
            return Ok(rows);
        }
        let current = scan_current_entities(session.handle()).await;
        session.finish();
        let rows: Vec<TableColumnStatsValue> = current?
            .into_iter()
            .filter_map(|r| match r {
                EntityRecord::TableColumnStats(v) => Some(v),
                _ => None,
            })
            .collect();
        projections_write(catalog).install_table_column_stats(head, rows.clone());
        return Ok(rows);
    }
    let current = scan_current_entities(session.handle()).await;
    session.finish();
    Ok(current?
        .into_iter()
        .filter_map(|r| match r {
            EntityRecord::TableColumnStats(v) => Some(v),
            _ => None,
        })
        .collect())
}

/// Every `ducklake_file_column_stats` row. Unversioned, as
/// [`dump_table_stats`].
#[doc(hidden)]
pub async fn dump_file_column_stats(catalog: &Catalog) -> Result<Vec<FileColumnStatsValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::FileColumnStats(v) => Some(v),
        _ => None,
    })
    .await
}

/// Every `ducklake_snapshot`/`ducklake_snapshot_changes` row (merged).
/// Snapshots are append-only and carry no begin/end lifecycle of their
/// own — this is the full history, not a current/history split. Served
/// from the maintained projection when its head matches; a fresh scan
/// installs it otherwise.
#[doc(hidden)]
pub async fn dump_snapshots(catalog: &Catalog) -> Result<Vec<SnapshotValue>> {
    let session = catalog.begin_read().await?;
    let head = session_head(&session).await?;
    if let (true, Some(head)) = (catalog.maintains_projections(), head) {
        if let Some(rows) = projections_read(catalog).snapshots_at(head) {
            session.finish();
            return Ok(rows);
        }
        let result = scan_snapshots(session.handle()).await;
        session.finish();
        let rows = result?;
        projections_write(catalog).install_snapshots(head, rows.clone());
        return Ok(rows);
    }
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

/// Every `ducklake_files_scheduled_for_deletion` row. Live bookkeeping
/// with no temporal lifecycle: always exactly the rows awaiting physical
/// deletion.
#[doc(hidden)]
pub async fn dump_scheduled_deletions(catalog: &Catalog) -> Result<Vec<GcFileValue>> {
    dump_current_entities(catalog, |r| match r {
        EntityRecord::GcFile(v) => Some(v),
        _ => None,
    })
    .await
}

/// One `ducklake_tag` row, flattened from its object's container record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagRow {
    /// The tagged object (a schema/table/view id).
    pub object_id: u64,
    /// Snapshot at which this tag value became visible.
    pub begin_snapshot: u64,
    /// Snapshot at which it was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// Every `ducklake_tag` row: one per embedded entry, ended entries
/// included — each row carries its lifecycle verbatim and DuckLake
/// filters in SQL.
#[doc(hidden)]
pub async fn dump_tags(catalog: &Catalog) -> Result<Vec<TagRow>> {
    let containers = dump_current_entities(catalog, |r| match r {
        EntityRecord::Tag(v) => Some(v),
        _ => None,
    })
    .await?;
    Ok(containers
        .into_iter()
        .flat_map(|container| {
            let object_id = container.object_id;
            container.entries.into_iter().map(move |e| TagRow {
                object_id,
                begin_snapshot: e.begin_snapshot,
                end_snapshot: e.end_snapshot,
                key: e.key,
                value: e.value,
            })
        })
        .collect())
}

/// One `ducklake_column_tag` row, flattened from its column's record.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTagRow {
    /// The tagged column's table.
    pub table_id: u64,
    /// The tagged column.
    pub column_id: u64,
    /// Snapshot at which this tag value became visible.
    pub begin_snapshot: u64,
    /// Snapshot at which it was superseded, if it has been.
    pub end_snapshot: Option<u64>,
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// Every `ducklake_column_tag` row. Entries are authoritative on each
/// column's latest record (a version transition carries them forward),
/// so rows are emitted from that record only — emitting from every
/// version would duplicate them.
#[doc(hidden)]
pub async fn dump_column_tags(catalog: &Catalog) -> Result<Vec<ColumnTagRow>> {
    let columns = dump_columns(catalog).await?;

    let mut latest: std::collections::BTreeMap<(u64, u64), &ColumnValue> =
        std::collections::BTreeMap::new();
    for column in &columns {
        let entry = latest
            .entry((column.table_id, column.column_id))
            .or_insert(column);
        // Later than the incumbent: live beats ended, higher end beats
        // lower.
        let newer = match (column.end_snapshot, entry.end_snapshot) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a > b,
        };
        if newer {
            *entry = column;
        }
    }

    Ok(latest
        .into_values()
        .flat_map(|column| {
            column.tags.iter().map(|t| ColumnTagRow {
                table_id: column.table_id,
                column_id: column.column_id,
                begin_snapshot: t.begin_snapshot,
                end_snapshot: t.end_snapshot,
                key: t.key.clone(),
                value: t.value.clone(),
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
        MacroImplementationDef, MacroParameterDef,
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
                        encryption_key: None,
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
                        encryption_key: None,
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

    /// Unversioned kinds (statistics, tags, scheduled deletions) live only
    /// in `current`; their dumps must serve exactly the live rows on a
    /// catalog whose history is non-empty.
    #[tokio::test]
    async fn unversioned_dumps_serve_live_rows_on_a_history_bearing_catalog() {
        let catalog = seed().await;

        let stats = dump_table_stats(&catalog).await.unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].record_count, 10);

        let column_stats = dump_table_column_stats(&catalog).await.unwrap();
        assert_eq!(column_stats.len(), 1);
        assert_eq!(column_stats[0].min_value.as_deref(), Some("1"));

        let file_stats = dump_file_column_stats(&catalog).await.unwrap();
        assert_eq!(file_stats.len(), 1);

        let deletions = dump_scheduled_deletions(&catalog).await.unwrap();
        assert!(deletions.is_empty());
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

    /// A mapping staged through the DuckLake row path dumps back
    /// row-faithfully, embedded rows in `column_id` order.
    #[tokio::test]
    async fn dump_mappings_serves_embedded_rows() {
        use crate::transaction::staged::{Cell, RowOperation, StagedTransaction, TableKind};

        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        let db_tx = catalog.begin_write_tx().await.unwrap();
        let mut tx = StagedTransaction::begin_detached(db_tx);
        tx.stage(RowOperation::Insert {
            table: TableKind::ColumnMapping,
            cells: vec![Cell::U64(21), Cell::U64(1), Cell::Str("map_by_name".into())],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::NameMapping,
            cells: vec![
                Cell::U64(21),
                Cell::U64(0),
                Cell::Str("id".into()),
                Cell::U64(1),
                Cell::Null,
                Cell::Bool(false),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: vec![
                Cell::U64(1),
                Cell::I64(1),
                Cell::U64(1),
                Cell::U64(11),
                Cell::U64(22),
            ],
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: vec![
                Cell::U64(1),
                Cell::Str("inserted_into_table:1".into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
            ],
        });
        tx.commit().await.unwrap();

        let mappings = dump_mappings(&catalog).await.unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].mapping_id, 21);
        assert_eq!(mappings[0].table_id, 1);
        assert_eq!(mappings[0].map_type, "map_by_name");
        assert_eq!(mappings[0].name_mappings.len(), 1);
        assert_eq!(mappings[0].name_mappings[0].source_name, "id");
    }

    /// An ended macro keeps serving its implementation and parameter
    /// rows: the whole record — children included — mirrors to history,
    /// where time travel still reads it.
    #[tokio::test]
    async fn dump_macros_serves_children_current_and_history() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        catalog
            .commit(|tx| {
                let schema = tx.create_schema("s")?;
                tx.create_macro(
                    schema,
                    "add",
                    &[
                        MacroImplementationDef {
                            dialect: "duckdb".into(),
                            sql: "(a + 1)".into(),
                            macro_type: "scalar".into(),
                            parameters: vec![MacroParameterDef {
                                name: "a".into(),
                                parameter_type: "unknown".into(),
                                default_value: None,
                                default_value_type: "unknown".into(),
                            }],
                        },
                        MacroImplementationDef {
                            dialect: "duckdb".into(),
                            sql: "(a + b)".into(),
                            macro_type: "scalar".into(),
                            parameters: vec![
                                MacroParameterDef {
                                    name: "a".into(),
                                    parameter_type: "unknown".into(),
                                    default_value: None,
                                    default_value_type: "unknown".into(),
                                },
                                MacroParameterDef {
                                    name: "b".into(),
                                    parameter_type: "unknown".into(),
                                    default_value: Some("5".into()),
                                    default_value_type: "int32".into(),
                                },
                            ],
                        },
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let head = catalog.snapshot().await.unwrap();
        let schema = head.schema_by_name("s").unwrap();
        let created = head.macro_by_name(schema.id, "add").unwrap();
        catalog
            .commit(move |tx| tx.drop_macro(created.id))
            .await
            .unwrap();

        let macros = dump_macros(&catalog).await.unwrap();
        assert_eq!(macros.len(), 1);
        let ended = &macros[0];
        assert!(ended.end_snapshot.is_some());
        assert_eq!(ended.implementations.len(), 2);
        assert_eq!(ended.implementations[0].impl_id, 0);
        assert_eq!(ended.implementations[1].parameters[1].parameter_name, "b");
        assert_eq!(
            ended.implementations[1].parameters[1]
                .default_value
                .as_deref(),
            Some("5")
        );
    }

    #[tokio::test]
    async fn dump_data_and_delete_files_carry_registration_values_verbatim() {
        let catalog = seed().await;

        let files = dump_data_files(&catalog).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "orders/data-1.parquet");
        assert_eq!(files[0].record_count, 10);
        assert_eq!(files[0].row_id_start, Some(0));
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
        // Bootstrap records minting `main`, exactly as DuckLake's own
        // initialization writes it; both real commits record something.
        assert_eq!(rows[0].changes_made, "created_schema:\"main\"");
        assert!(!rows[1].changes_made.is_empty());
        assert!(!rows[2].changes_made.is_empty());
    }
}
