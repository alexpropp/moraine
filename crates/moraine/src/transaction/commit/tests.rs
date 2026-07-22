use std::sync::Arc;

use object_store::memory::InMemory;

use super::*;

/// A store stamped with a newer structural format must be refused,
/// not misread.
#[tokio::test]
async fn unknown_format_is_refused() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        &Key::Sys(SysKey::Format).encode(),
        &value::encode_value(&proto::FormatValue {
            format_version: MAX_FORMAT_VERSION + 1,
            writer_version: "future".into(),
        }),
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    // `Result::unwrap_err` needs `T: Debug`, and `slatedb::Db` has no
    // `Debug` impl; `err().unwrap()` only needs it on the error side.
    let err = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::Corruption(_)));
}

/// A mid-migration marker refuses the open outright.
#[tokio::test]
async fn migration_marker_is_refused() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = StoreBuilder::new("", object_store.clone())
        .open_writer()
        .await
        .unwrap();
    db.put(
        &Key::Sys(SysKey::Migration).encode(),
        &value::encode_value(&proto::MigrationValue {
            from_format: 1,
            to_format: 2,
            cursor: vec![],
        }),
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    let err = open_initialized(StoreBuilder::new("", object_store), false, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, Error::Corruption(_)));
}

/// A file registered and expired within one commit exists in neither
/// `base` nor `state`'s `data_files`: its per-file column stats are
/// orphaned and must never be staged as a write.
#[test]
fn register_then_expire_in_one_commit_stages_no_orphaned_file_column_stats() {
    use crate::{
        catalog::{ColumnDef, DataFile, FileColumnStats},
        store::key::{CurrentKey, HistoryKey},
    };

    let snap0 = proto::SnapshotValue {
        snapshot_id: 0,
        snapshot_time_micros: 1,
        schema_version: 0,
        next_catalog_id: 0,
        next_file_id: 0,
        changes_made: String::new(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
        schema_changed_table_ids: Vec::new(),
    };
    let empty = CatalogSnapshot::build(snap0, vec![], vec![], None);
    let mut setup = Transaction::new(empty, 1);
    let schema = setup.create_schema("s").unwrap();
    let table = setup
        .create_table(
            schema,
            "t",
            &[ColumnDef {
                name: "a".into(),
                column_type: "BIGINT".into(),
                nulls_allowed: true,
                default_value: None,
            }],
        )
        .unwrap();
    let column = setup.columns_of(table)[0].id;
    let base = setup.into_parts().state;

    // Register a file with column stats, then expire it — all inside
    // this one commit's transaction.
    let mut tx = Transaction::new(base.clone(), 2);
    let file = tx
        .register_data_file(
            table,
            DataFile {
                path: "f.parquet".into(),
                path_is_relative: true,
                file_format: "parquet".into(),
                record_count: 10,
                file_size_bytes: 100,
                footer_size: 4,
                encryption_key: None,
                column_stats: vec![FileColumnStats {
                    column_id: column,
                    column_size_bytes: 10,
                    value_count: 10,
                    null_count: 0,
                    min_value: Some("1".into()),
                    max_value: Some("2".into()),
                    contains_nan: None,
                    extra_stats: None,
                }],
            },
            &[],
        )
        .unwrap();
    tx.expire_data_file(table, file).unwrap();
    let state = tx.into_parts().state;

    let writes = diff_writes(&base, &state, 2);
    for (key_bytes, _) in &writes {
        let key = Key::decode(key_bytes).unwrap();
        let is_file_column_stats = matches!(
            key,
            Key::Current(CurrentKey::Entity(EntityKey::FileColumnStats { .. }))
                | Key::History(HistoryKey {
                    entity: EntityKey::FileColumnStats { .. },
                    ..
                })
        );
        assert!(
            !is_file_column_stats,
            "orphaned file_column_stats write staged: {key:?}"
        );
    }
}

/// A fresh reader opened after commit returns resolves the new head:
/// commit durability must imply visibility to subsequently opened
/// handles.
#[tokio::test]
async fn fresh_reader_sees_committed_head() {
    use slatedb::DbReader;

    use crate::catalog::{Catalog, CatalogOptions};

    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("visible").map(|_| ()))
        .await
        .unwrap();

    let reader = DbReader::builder("", object_store)
        .with_segment_extractor(Arc::new(crate::store::segment::TagSegmentExtractor))
        .build()
        .await
        .unwrap();
    let head_bytes = reader
        .get(Key::Sys(SysKey::Head).encode())
        .await
        .unwrap()
        .expect("fresh reader must see the head");
    let head: proto::HeadValue = value::decode_value(&head_bytes).unwrap();
    assert_eq!(head.snapshot_id, 1);
    reader.close().await.unwrap();
    catalog.close().await.unwrap();
}

/// Verb-path DDL records the shape-changed table ids on its snapshot,
/// one per changed table or view — the id set `ducklake_schema_versions`
/// rows are served from. Data-only commits record none.
#[tokio::test]
async fn verb_ddl_records_schema_changed_table_ids() {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef, DataFile};

    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();

    // One commit creating a table and altering it twice: the id set
    // dedups to that one table.
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let table = tx.create_table(
                schema,
                "t",
                &[ColumnDef {
                    name: "a".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                }],
            )?;
            tx.rename_table(table, "t2")?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    let table = created.get().unwrap();

    // A data-only commit changes no table's shape.
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 1,
                    file_size_bytes: 10,
                    footer_size: 4,
                    encryption_key: None,
                    column_stats: vec![],
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let ddl = read::read_snapshot(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ddl.schema_changed_table_ids, vec![table.get()]);
    let data_only = read::read_snapshot(ReadHandle::Tx(&tx), 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data_only.schema_changed_table_ids, Vec::<u64>::new());
    tx.rollback();
    catalog.close().await.unwrap();
}

async fn catalog_with_two_column_table() -> (crate::catalog::Catalog, crate::catalog::TableId) {
    use crate::catalog::{Catalog, CatalogOptions, ColumnDef};
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let table = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            let column = |name: &str| ColumnDef {
                name: name.into(),
                column_type: "BIGINT".into(),
                nulls_allowed: true,
                default_value: None,
            };
            let created = tx.create_table(schema, "t", &[column("a"), column("b")])?;
            table.set(Some(created));
            Ok(())
        })
        .await
        .unwrap();
    (catalog, table.get().unwrap())
}

fn entry(row_id: u64, value: i128) -> crate::catalog::IndexEntry {
    crate::catalog::IndexEntry {
        row_id,
        values: vec![Some(crate::store::index_encoding::IndexKeyValue::Int {
            value,
            width: crate::store::index_encoding::IntWidth::I64,
        })],
    }
}

/// An index entry for a row whose single indexed column is NULL.
fn null_entry(row_id: u64) -> crate::catalog::IndexEntry {
    crate::catalog::IndexEntry {
        row_id,
        values: vec![None],
    }
}

async fn read_format_version(catalog: &crate::catalog::Catalog) -> u64 {
    let tx = catalog.begin_write_tx().await.unwrap();
    let format = read::read_format(ReadHandle::Tx(&tx)).await.unwrap();
    tx.rollback();
    format.map_or(FORMAT_VERSION, |f| f.format_version)
}

#[tokio::test]
async fn create_index_persists_definition_stamps_format_and_lands_entries() {
    use crate::{
        catalog::{ColumnId, IndexDef, IndexState},
        store::key::{IdxKind, idx_index_prefix},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    assert_eq!(read_format_version(&catalog).await, FORMAT_VERSION);

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index_id = index.get().unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    let infos = snapshot.indexes_of(table);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].id, index_id);
    assert_eq!(infos[0].columns, vec![ColumnId::new(1)]);
    assert!(infos[0].unique);
    assert_eq!(infos[0].state, IndexState::Ready);
    assert_eq!(read_format_version(&catalog).await, FORMAT_WITH_INDEX);

    // Both backfill rows produced a stored entry.
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(idx_index_prefix(IdxKind::Unique, index_id.get()), ..)
        .await
        .unwrap();
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2);
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_unique_value_in_backfill_aborts_create() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let err = catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 10)],
            )
            .map(|_| ())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    // The aborted commit left no index and did not stamp the format.
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty()
    );
    assert_eq!(read_format_version(&catalog).await, FORMAT_VERSION);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn non_unique_index_accepts_duplicate_values() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[entry(0, 10), entry(1, 10)],
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    assert_eq!(catalog.snapshot().await.unwrap().indexes_of(table).len(), 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn null_indexed_value_gets_no_entry_so_unique_admits_many() {
    use crate::catalog::{ColumnId, IndexDef, IndexEntry};
    let (catalog, table) = catalog_with_two_column_table().await;
    let null_entry = |row_id| IndexEntry {
        row_id,
        values: vec![None],
    };
    // Two NULL rows under a unique index: NULLs get no entry, so no
    // collision.
    catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[null_entry(0), null_entry(1)],
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    assert_eq!(catalog.snapshot().await.unwrap().indexes_of(table).len(), 1);
    catalog.close().await.unwrap();
}

async fn register_three_row_file(
    catalog: &crate::catalog::Catalog,
    table: crate::catalog::TableId,
) -> crate::catalog::DataFileId {
    use crate::catalog::DataFile;
    let file = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 3,
                    file_size_bytes: 30,
                    footer_size: 4,
                    encryption_key: None,
                    column_stats: vec![],
                },
                &[],
            )?;
            file.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    file.get().unwrap()
}

#[tokio::test]
async fn index_lookup_resolves_unique_value_to_its_data_file_row() {
    use crate::catalog::{ColumnId, IndexDef, RowHolder};
    let (catalog, table) = catalog_with_two_column_table().await;
    // Rows 0,1,2 land in this file (row_id_start = 0).
    let file = register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let value = |v: i128| crate::store::index_encoding::IndexKeyValue::Int {
        value: v,
        width: crate::store::index_encoding::IntWidth::I64,
    };
    let hits = catalog
        .index_lookup(table, index, &[value(20)])
        .await
        .unwrap();
    assert_eq!(
        hits,
        vec![crate::catalog::RowLocation {
            row_id: 1,
            holder: RowHolder::DataFile(file),
        }]
    );
    // A value no row holds resolves to nothing.
    assert!(
        catalog
            .index_lookup(table, index, &[value(99)])
            .await
            .unwrap()
            .is_empty()
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_lookup_returns_all_rows_for_a_non_unique_value() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Rows 0 and 2 share value 10; row 1 is 20.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 10)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    let value = crate::store::index_encoding::IndexKeyValue::Int {
        value: 10,
        width: crate::store::index_encoding::IntWidth::I64,
    };
    let mut rows: Vec<u64> = catalog
        .index_lookup(table, index.get().unwrap(), &[value])
        .await
        .unwrap()
        .into_iter()
        .map(|location| location.row_id)
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![0, 2]);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_selects_unique_values_in_a_bounded_interval() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef, RowHolder};
    let (catalog, table) = catalog_with_two_column_table().await;
    let file = register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Rows 0,1,2 hold ascending values 10,20,30.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let value = |v: i128| {
        vec![crate::store::index_encoding::IndexKeyValue::Int {
            value: v,
            width: crate::store::index_encoding::IntWidth::I64,
        }]
    };
    let ids = |hits: Vec<crate::catalog::RowLocation>| {
        let mut ids: Vec<u64> = hits.into_iter().map(|hit| hit.row_id).collect();
        ids.sort_unstable();
        ids
    };

    // BETWEEN 15 AND 25 — only value 20 (row 1).
    let between = catalog
        .index_range(
            table,
            index,
            Bound::Included(value(15)),
            Bound::Included(value(25)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        between,
        vec![crate::catalog::RowLocation {
            row_id: 1,
            holder: RowHolder::DataFile(file),
        }]
    );

    // > 20 (half-open) — value 30 (row 2).
    let above = catalog
        .index_range(
            table,
            index,
            Bound::Excluded(value(20)),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(ids(above), vec![2]);

    // <= 20 — values 10 and 20 (rows 0, 1).
    let below = catalog
        .index_range(
            table,
            index,
            Bound::Unbounded,
            Bound::Included(value(20)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(ids(below), vec![0, 1]);

    // Closed [10, 30] covers every row.
    let all = catalog
        .index_range(
            table,
            index,
            Bound::Included(value(10)),
            Bound::Included(value(30)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(ids(all), vec![0, 1, 2]);

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_reverse_serves_the_opposite_order() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let value = |v: i128| {
        vec![crate::store::index_encoding::IndexKeyValue::Int {
            value: v,
            width: crate::store::index_encoding::IntWidth::I64,
        }]
    };
    let order = |query: Vec<crate::catalog::RowLocation>| {
        query.into_iter().map(|hit| hit.row_id).collect::<Vec<_>>()
    };

    // Ascending index: default order is low-to-high, `reverse` is high-to-low.
    let ascending = catalog
        .index_range(
            table,
            index,
            Bound::Included(value(10)),
            Bound::Included(value(30)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(order(ascending), vec![0, 1, 2]);
    let reversed = catalog
        .index_range(
            table,
            index,
            Bound::Included(value(10)),
            Bound::Included(value(30)),
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        order(reversed),
        vec![2, 1, 0],
        "reverse serves the exact opposite order"
    );

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_range_over_a_non_unique_index_returns_every_matching_row() {
    use std::ops::Bound;

    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Rows 0 and 2 share value 10; row 1 is 20.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 10)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let value = |v: i128| {
        vec![crate::store::index_encoding::IndexKeyValue::Int {
            value: v,
            width: crate::store::index_encoding::IntWidth::I64,
        }]
    };
    let ids = |hits: Vec<crate::catalog::RowLocation>| {
        let mut ids: Vec<u64> = hits.into_iter().map(|hit| hit.row_id).collect();
        ids.sort_unstable();
        ids
    };

    // < 20 — both rows holding value 10.
    let low = catalog
        .index_range(
            table,
            index,
            Bound::Unbounded,
            Bound::Excluded(value(20)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(ids(low), vec![0, 2]);

    // >= 20 — only row 1.
    let high = catalog
        .index_range(
            table,
            index,
            Bound::Included(value(20)),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(ids(high), vec![1]);

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn descending_index_scans_high_value_first() {
    use std::ops::Bound;

    use crate::{
        catalog::{ColumnId, ColumnOrder, IndexDef},
        store::index_encoding::{Direction, NullOrder},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_ordered(
                table,
                &IndexDef {
                    name: "by_a_desc".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[ColumnOrder {
                    direction: Direction::Descending,
                    nulls: NullOrder::Last,
                }],
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let value = |v: i128| {
        vec![crate::store::index_encoding::IndexKeyValue::Int {
            value: v,
            width: crate::store::index_encoding::IntWidth::I64,
        }]
    };
    // Results come back in the index's stored order — descending by value.
    let order = |hits: Vec<crate::catalog::RowLocation>| {
        hits.into_iter().map(|hit| hit.row_id).collect::<Vec<u64>>()
    };

    // Closed [10, 30]: 30, 20, 10 -> rows 2, 1, 0.
    let all = catalog
        .index_range(
            table,
            index,
            Bound::Included(value(10)),
            Bound::Included(value(30)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        order(all),
        vec![2, 1, 0],
        "descending index scans high first"
    );

    // a > 15 (half-open): 30, 20 -> rows 2, 1.
    let above = catalog
        .index_range(
            table,
            index,
            Bound::Excluded(value(15)),
            Bound::Unbounded,
            false,
        )
        .await
        .unwrap();
    assert_eq!(order(above), vec![2, 1]);

    // a <= 20: 20, 10 -> rows 1, 0.
    let below = catalog
        .index_range(
            table,
            index,
            Bound::Unbounded,
            Bound::Included(value(20)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(order(below), vec![1, 0]);

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn unique_index_admits_null_rows_and_index_nulls_finds_them() {
    use crate::catalog::{ColumnId, IndexDef, RowHolder};
    let (catalog, table) = catalog_with_two_column_table().await;
    let file = register_three_row_file(&catalog, table).await;

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // Row 0 holds a=10; rows 1 and 2 are both a=NULL. A unique index
            // must accept two NULL rows — SQL treats NULLs as distinct.
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), null_entry(1), null_entry(2)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .expect("a unique index must admit multiple NULL rows");
    let index = index.get().unwrap();

    // `a IS NULL` resolves to exactly the two NULL rows.
    let mut nulls: Vec<u64> = catalog
        .index_nulls(table, index, vec![None], false)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.row_id)
        .collect();
    nulls.sort_unstable();
    assert_eq!(nulls, vec![1, 2], "IS NULL finds both NULL rows");

    // The non-null value is still uniquely resolvable, unaffected.
    let value = vec![crate::store::index_encoding::IndexKeyValue::Int {
        value: 10,
        width: crate::store::index_encoding::IntWidth::I64,
    }];
    assert_eq!(
        catalog.index_lookup(table, index, &value).await.unwrap(),
        vec![crate::catalog::RowLocation {
            row_id: 0,
            holder: RowHolder::DataFile(file),
        }]
    );
    // A pure-equality prefix through index_nulls is refused.
    let err = catalog
        .index_nulls(
            table,
            index,
            vec![Some(crate::store::index_encoding::IndexKeyValue::Int {
                value: 10,
                width: crate::store::index_encoding::IntWidth::I64,
            })],
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn composite_index_nulls_matches_a_leading_prefix() {
    use crate::{
        catalog::{ColumnId, IndexDef, IndexEntry},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;

    let int = |v: i128| {
        Some(IndexKeyValue::Int {
            value: v,
            width: IntWidth::I64,
        })
    };
    let ent = |row_id, a: Option<IndexKeyValue>, b: Option<IndexKeyValue>| IndexEntry {
        row_id,
        values: vec![a, b],
    };
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            // row 0: (5, NULL); row 1: (5, 3); row 2: (7, NULL).
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_ab".into(),
                    columns: vec![ColumnId::new(1), ColumnId::new(2)],
                    unique: false,
                },
                &[
                    ent(0, int(5), None),
                    ent(1, int(5), int(3)),
                    ent(2, int(7), None),
                ],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let ids = |mut v: Vec<crate::catalog::RowLocation>| {
        v.sort_by_key(|hit| hit.row_id);
        v.into_iter().map(|hit| hit.row_id).collect::<Vec<_>>()
    };

    // a = 5 AND b IS NULL -> only row 0 (row 1 has b=3, row 2 has a=7).
    assert_eq!(
        ids(catalog
            .index_nulls(table, index, vec![int(5), None], false)
            .await
            .unwrap()),
        vec![0]
    );
    // b IS NULL across all a -> rows 0 and 2 is a gap pattern (leading a
    // free) and is not expressible; a IS NULL (leading) matches no row.
    assert!(
        catalog
            .index_nulls(table, index, vec![None], false)
            .await
            .unwrap()
            .is_empty()
    );

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn index_lookup_on_missing_index_is_not_found() {
    use crate::catalog::IndexId;
    let (catalog, table) = catalog_with_two_column_table().await;
    let value = crate::store::index_encoding::IndexKeyValue::Int {
        value: 1,
        width: crate::store::index_encoding::IntWidth::I64,
    };
    let err = catalog
        .index_lookup(table, IndexId::new(999), &[value])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn register_data_file_must_supply_index_entries_and_they_are_looked_up() {
    use crate::{
        catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let file = || DataFile {
        path: "f.parquet".into(),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: 2,
        file_size_bytes: 20,
        footer_size: 4,
        encryption_key: None,
        column_stats: vec![],
    };
    let int = |value: i128| IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    };

    // A non-empty file on an indexed table with no entries is refused.
    let refused = catalog
        .commit(|tx| tx.register_data_file(table, file(), &[]).map(|_| ()))
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");

    // With entries it lands; ordinals map to row ids 0 and 1.
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                file(),
                &[
                    FileIndexEntry {
                        index,
                        ordinal: 0,
                        values: vec![Some(int(10))],
                    },
                    FileIndexEntry {
                        index,
                        ordinal: 1,
                        values: vec![Some(int(20))],
                    },
                ],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let hits = catalog
        .index_lookup(table, index, &[int(20)])
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].row_id, 1);
    catalog.close().await.unwrap();
}

/// Sets up an indexed table holding one two-row data file (values 10 and
/// 20 at row ids 0 and 1), returning the catalog, table, index and file.
async fn catalog_with_indexed_data_file() -> (
    crate::catalog::Catalog,
    crate::catalog::TableId,
    crate::catalog::IndexId,
    crate::catalog::DataFileId,
) {
    use crate::{
        catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    let file = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            let int = |value: i128| IndexKeyValue::Int {
                value,
                width: IntWidth::I64,
            };
            let registered = tx.register_data_file(
                table,
                DataFile {
                    path: "f.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 2,
                    file_size_bytes: 20,
                    footer_size: 4,
                    encryption_key: None,
                    column_stats: vec![],
                },
                &[
                    FileIndexEntry {
                        index: id,
                        ordinal: 0,
                        values: vec![Some(int(10))],
                    },
                    FileIndexEntry {
                        index: id,
                        ordinal: 1,
                        values: vec![Some(int(20))],
                    },
                ],
            )?;
            file.set(Some(registered));
            Ok(())
        })
        .await
        .unwrap();
    (catalog, table, index.get().unwrap(), file.get().unwrap())
}

fn delete_file(data_file: crate::catalog::DataFileId) -> crate::catalog::DeleteFile {
    crate::catalog::DeleteFile {
        data_file_id: data_file,
        path: "d.parquet".into(),
        path_is_relative: true,
        format: "parquet".into(),
        delete_count: 1,
        file_size_bytes: 10,
        footer_size: 4,
        encryption_key: None,
    }
}

/// A delete file names the rows it kills, so their entries go with them
/// and the value is free to be indexed again.
#[tokio::test]
async fn register_delete_file_removes_the_entries_it_names() {
    use crate::{
        catalog::FileIndexRemoval,
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
    let int = |value: i128| IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    };

    catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                delete_file(file),
                &[FileIndexRemoval {
                    index,
                    row_id: 1,
                    values: vec![Some(int(20))],
                }],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    assert!(
        catalog
            .index_lookup(table, index, &[int(20)])
            .await
            .unwrap()
            .is_empty(),
        "the killed row's entry is gone"
    );
    assert_eq!(
        catalog
            .index_lookup(table, index, &[int(10)])
            .await
            .unwrap()
            .len(),
        1,
        "the surviving row is still indexed"
    );
    catalog.close().await.unwrap();
}

/// Supplying no entries on an indexed table is refused, exactly as it is
/// on the register side — a silently under-covered index is a lie.
#[tokio::test]
async fn register_delete_file_must_supply_index_entries() {
    let (catalog, table, _, file) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_delete_file(table, delete_file(file), &[])
                .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

/// Entries without deletes would strip the index of rows the catalog
/// still counts as live.
#[tokio::test]
async fn register_delete_file_rejects_index_entries_without_deletes() {
    use crate::{
        catalog::FileIndexRemoval,
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                crate::catalog::DeleteFile {
                    delete_count: 0,
                    ..delete_file(file)
                },
                &[FileIndexRemoval {
                    index,
                    row_id: 1,
                    values: vec![Some(IndexKeyValue::Int {
                        value: 20,
                        width: IntWidth::I64,
                    })],
                }],
            )
            .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

/// A row id past a dense target's range would name a row it does not hold.
#[tokio::test]
async fn register_delete_file_rejects_an_out_of_range_row_id() {
    use crate::{
        catalog::FileIndexRemoval,
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table, index, file) = catalog_with_indexed_data_file().await;
    let refused = catalog
        .commit(|tx| {
            tx.register_delete_file(
                table,
                delete_file(file),
                &[FileIndexRemoval {
                    index,
                    row_id: 2,
                    values: vec![Some(IndexKeyValue::Int {
                        value: 30,
                        width: IntWidth::I64,
                    })],
                }],
            )
            .map(|_| ())
        })
        .await;
    assert!(matches!(refused, Err(Error::Constraint(_))), "{refused:?}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn unique_index_rejects_a_duplicate_value_across_commits() {
    use crate::{
        catalog::{ColumnId, DataFile, FileIndexEntry, IndexDef},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    let one_row_with = |value: i128| {
        let file = DataFile {
            path: "f.parquet".into(),
            path_is_relative: true,
            file_format: "parquet".into(),
            record_count: 1,
            file_size_bytes: 10,
            footer_size: 4,
            encryption_key: None,
            column_stats: vec![],
        };
        (
            file,
            FileIndexEntry {
                index,
                ordinal: 0,
                values: vec![Some(IndexKeyValue::Int {
                    value,
                    width: IntWidth::I64,
                })],
            },
        )
    };

    // First value 10 lands.
    catalog
        .commit(|tx| {
            let (file, entry) = one_row_with(10);
            tx.register_data_file(table, file, &[entry]).map(|_| ())
        })
        .await
        .unwrap();
    // A later commit inserting the same value 10 (different row) is
    // rejected by the point-get against the winner's entry.
    let dup = catalog
        .commit(|tx| {
            let (file, entry) = one_row_with(10);
            tx.register_data_file(table, file, &[entry]).map(|_| ())
        })
        .await;
    assert!(matches!(dup, Err(Error::Constraint(_))), "{dup:?}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn scoped_read_covers_a_registration_end_to_end() {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;

    use crate::{
        catalog::{ColumnId, DataFile, IndexDef},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };

    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // A DATA_PATH object store holds a Parquet file with the indexed
    // column "a" at physical position 0.
    let data = Arc::new(InMemory::new());
    let path = Path::from("t/data-1.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))]).unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    data.put(&path, buffer.into()).await.unwrap();

    // moraine derives coverage entries by the scoped read (column "a"
    // at position 0), then registration lands them — DuckLake supplied
    // none, and the read stands in for the refusal.
    let entries = catalog
        .scoped_file_index_entries(data.clone(), &path, index, &[0])
        .await
        .unwrap();
    assert_eq!(entries.len(), 3);
    catalog
        .commit(|tx| {
            tx.register_data_file(
                table,
                DataFile {
                    path: "t/data-1.parquet".into(),
                    path_is_relative: true,
                    file_format: "parquet".into(),
                    record_count: 3,
                    file_size_bytes: 30,
                    footer_size: 4,
                    encryption_key: None,
                    column_stats: vec![],
                },
                &entries,
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let value = IndexKeyValue::Int {
        value: 20,
        width: IntWidth::I64,
    };
    let hits = catalog.index_lookup(table, index, &[value]).await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].row_id, 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn ddl_on_an_indexed_column_is_guarded() {
    use crate::catalog::{ColumnAlteration, ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    catalog
        .commit(|tx| {
            tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10)],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    // Dropping or retyping the indexed column is refused.
    let dropped = catalog
        .commit(|tx| tx.drop_column(table, ColumnId::new(1)))
        .await;
    assert!(matches!(dropped, Err(Error::Constraint(_))), "{dropped:?}");
    let retyped = catalog
        .commit(|tx| {
            tx.alter_column(
                table,
                ColumnId::new(1),
                ColumnAlteration {
                    column_type: Some("INTEGER".into()),
                    ..ColumnAlteration::default()
                },
            )
        })
        .await;
    assert!(matches!(retyped, Err(Error::Constraint(_))), "{retyped:?}");

    // Renaming the indexed column, and retyping a non-indexed column,
    // are unaffected.
    catalog
        .commit(|tx| tx.rename_column(table, ColumnId::new(1), "a2"))
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            tx.alter_column(
                table,
                ColumnId::new(2),
                ColumnAlteration {
                    column_type: Some("INTEGER".into()),
                    ..ColumnAlteration::default()
                },
            )
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
}

async fn scan_idx_entries(
    catalog: &crate::catalog::Catalog,
    index: crate::catalog::IndexId,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    use crate::store::key::{IdxKind, idx_index_prefix};
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut entries = Vec::new();
    for kind in [IdxKind::Unique, IdxKind::Multi] {
        let mut iter = ReadHandle::Tx(&tx)
            .scan_prefix(idx_index_prefix(kind, index.get()), ..)
            .await
            .unwrap();
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push((entry.key.to_vec(), entry.value.to_vec()));
        }
    }
    tx.rollback();
    entries.sort();
    entries
}

#[tokio::test]
async fn staged_build_gates_lookups_flips_ready_and_matches_single_commit() {
    use crate::{
        catalog::{ColumnId, IndexDef, IndexState},
        store::index_encoding::{IndexKeyValue, IntWidth},
    };
    let def = || IndexDef {
        name: "by_a".into(),
        columns: vec![ColumnId::new(1)],
        unique: true,
    };
    let value = |v: i128| IndexKeyValue::Int {
        value: v,
        width: IntWidth::I64,
    };

    // Reference: a single-commit build over rows 0,1,2.
    let (single, table_single) = catalog_with_two_column_table().await;
    register_three_row_file(&single, table_single).await;
    let single_index = std::cell::Cell::new(None);
    single
        .commit(|tx| {
            let id = tx.create_index(
                table_single,
                &def(),
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            single_index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let single_index = single_index.get().unwrap();

    // Staged: same table shape, same rows, built in two batches.
    let (staged, table_staged) = catalog_with_two_column_table().await;
    register_three_row_file(&staged, table_staged).await;
    let staged_index = std::cell::Cell::new(None);
    staged
        .commit(|tx| {
            let id = tx.create_index_staged(table_staged, &def())?;
            staged_index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let staged_index = staged_index.get().unwrap();
    // Identical allocation sequence → identical index id, so the idx
    // keys can be compared directly.
    assert_eq!(single_index, staged_index);

    // While building: format 3, lookups fail typed.
    assert_eq!(read_format_version(&staged).await, FORMAT_WITH_STAGED_INDEX);
    assert!(matches!(
        staged
            .index_lookup(table_staged, staged_index, &[value(20)])
            .await,
        Err(Error::IndexBuilding(_))
    ));

    // Two batches, the second final.
    staged
        .commit(|tx| {
            tx.build_index_step(staged_index, &[entry(0, 10), entry(1, 20)], false)
                .map(|_| ())
        })
        .await
        .unwrap();
    let final_state = std::cell::Cell::new(None);
    staged
        .commit(|tx| {
            let state = tx.build_index_step(staged_index, &[entry(2, 30)], true)?;
            final_state.set(Some(state));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(final_state.get().unwrap(), IndexState::Ready);

    // After the flip: lookups serve, and the idx range is byte-identical
    // to the single-commit build over the same rows.
    let hits = staged
        .index_lookup(table_staged, staged_index, &[value(20)])
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].row_id, 1);
    assert_eq!(
        scan_idx_entries(&single, single_index).await,
        scan_idx_entries(&staged, staged_index).await
    );

    single.close().await.unwrap();
    staged.close().await.unwrap();
}

#[tokio::test]
async fn staged_build_step_rejects_a_duplicate_and_a_ready_index() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_staged(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // A duplicate value within a batch fails the step.
    let dup = catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(0, 10), entry(1, 10)], false)
                .map(|_| ())
        })
        .await;
    assert!(matches!(dup, Err(Error::Constraint(_))), "{dup:?}");

    // Complete the build, then a further step on the ready index is
    // refused.
    catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(0, 10)], true)
                .map(|_| ())
        })
        .await
        .unwrap();
    let after_ready = catalog
        .commit(|tx| {
            tx.build_index_step(index, &[entry(1, 20)], false)
                .map(|_| ())
        })
        .await;
    assert!(
        matches!(after_ready, Err(Error::Constraint(_))),
        "{after_ready:?}"
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn reclaiming_a_dropped_index_deletes_its_orphaned_entries() {
    use crate::{
        catalog::{ColumnId, IndexDef},
        store::key::{IdxKind, idx_index_prefix},
    };
    let (catalog, table) = catalog_with_two_column_table().await;
    register_three_row_file(&catalog, table).await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10), entry(1, 20), entry(2, 30)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();

    // Reclaiming a live index is refused.
    assert!(matches!(
        catalog.reclaim_index_entries(index, 100).await,
        Err(Error::Constraint(_))
    ));

    catalog.commit(|tx| tx.drop_index(index)).await.unwrap();

    // A bounded sweep deletes the three orphaned entries, then reports
    // nothing left.
    let first = catalog.reclaim_index_entries(index, 2).await.unwrap();
    assert_eq!(first, 2);
    let second = catalog.reclaim_index_entries(index, 100).await.unwrap();
    assert_eq!(second, 1);
    assert_eq!(catalog.reclaim_index_entries(index, 100).await.unwrap(), 0);

    // The idx range is empty afterward.
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(idx_index_prefix(IdxKind::Unique, index.get()), ..)
        .await
        .unwrap();
    assert!(iter.next().await.unwrap().is_none());
    tx.rollback();
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn drop_index_ends_definition_and_keeps_format() {
    use crate::catalog::{ColumnId, IndexDef};
    let (catalog, table) = catalog_with_two_column_table().await;
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                table,
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[entry(0, 10)],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    catalog
        .commit(|tx| tx.drop_index(index.get().unwrap()))
        .await
        .unwrap();
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty()
    );
    // Dropping the last index does not downgrade the stamp.
    assert_eq!(read_format_version(&catalog).await, FORMAT_WITH_INDEX);
    catalog.close().await.unwrap();
}
