use std::sync::Arc;

use object_store::memory::InMemory;

use super::*;
use crate::catalog::{Catalog, CatalogOptions};

fn schema_row(id: u64, name: &str, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(format!("uuid-{id}")),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(name.to_string()),
        Cell::Str(format!("{name}/")),
        Cell::Bool(true),
    ]
}

fn table_row(id: u64, schema_id: u64, name: &str, begin: u64, end: Option<u64>) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(format!("uuid-t{id}")),
        Cell::U64(begin),
        end.map_or(Cell::Null, Cell::U64),
        Cell::U64(schema_id),
        Cell::Str(name.to_string()),
        Cell::Str(format!("{name}/")),
        Cell::Bool(true),
    ]
}

fn column_row(table_id: u64, column_id: u64, name: &str, order: u64) -> Vec<Cell> {
    vec![
        Cell::U64(column_id),
        Cell::U64(0),
        Cell::Null,
        Cell::U64(table_id),
        Cell::U64(order),
        Cell::Str(name.to_string()),
        Cell::Str("BIGINT".to_string()),
        Cell::Null,
        Cell::Null,
        Cell::Bool(true),
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

fn snapshot_row(id: u64, schema_version: u64, next_catalog_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::I64(1),
        Cell::U64(schema_version),
        Cell::U64(next_catalog_id),
        Cell::U64(0),
    ]
}

fn snapshot_changes_row(id: u64, changes_made: &str) -> Vec<Cell> {
    vec![
        Cell::U64(id),
        Cell::Str(changes_made.to_string()),
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

async fn open() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

/// A DuckLake-shaped snapshot bump plus table create: table `t` (id
/// 1, schema 0 = bootstrap's `main`) with one column, staged and
/// committed as one batch, then verified through the ordinary
/// snapshot read (the same view the dump ABI serves).
#[tokio::test]
async fn stages_table_create_and_snapshot_bump() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });

    let id = tx.commit().await.unwrap();
    assert_eq!(id.get(), 1);

    let snapshot = catalog.snapshot().await.unwrap();
    let tables = snapshot.tables_in(crate::catalog::SchemaId::new(0));
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "t");
    let cols = snapshot.columns_of(tables[0].id);
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].name, "a");
}

/// Staged column inserts advance the table's field-id counter: a later
/// verb `add_column` — even after the highest staged column is dropped
/// — must never re-allocate a DuckLake-authored field id.
#[tokio::test]
async fn staged_columns_advance_the_field_id_counter() {
    use crate::catalog::{ColumnDef, ColumnId, SchemaId, TableId};

    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 2, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 5, "b", 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    // Drop the max-id column, then add: the freed id must not return.
    let table = TableId::new(1);
    catalog
        .commit(|tx| tx.drop_column(table, ColumnId::new(5)))
        .await
        .unwrap();
    let added = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            added.set(Some(tx.add_column(
                table,
                &ColumnDef {
                    name: "c".into(),
                    column_type: "BIGINT".into(),
                    nulls_allowed: true,
                    default_value: None,
                },
            )?));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        added.get(),
        Some(ColumnId::new(6)),
        "field id 5 must not be reused"
    );

    // The counter-only table update stayed in place: one live table
    // row for snapshot 1's create, no counter-minted history version.
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.tables_in(SchemaId::new(0)).len(), 1);
}

/// DuckLake's UPDATE only names rows it read: an `UpdateSetEnd` for a
/// row that is not live is drift and must fail the commit loudly, not
/// pass as a silent no-op that drops the authored end.
#[tokio::test]
async fn ending_an_absent_row_is_rejected() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    // No table 7 exists; end it at this commit's snapshot id (1).
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(7), Cell::U64(1)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "dropped_table:7"),
    });

    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, crate::error::Error::Corruption(_)), "{err}");
}

/// DuckLake-authored data-file and delete-file rows carry
/// `encryption_key` through commit and back out of the snapshot read
/// verbatim — the faithful-conduit guarantee for key material.
#[tokio::test]
async fn encryption_keys_round_trip_through_staged_rows() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: vec![
            Cell::U64(1),                     // data_file_id
            Cell::U64(1),                     // table_id
            Cell::U64(1),                     // begin_snapshot
            Cell::Null,                       // end_snapshot
            Cell::Null,                       // file_order
            Cell::Str("data.parquet".into()), // path
            Cell::Bool(true),                 // path_is_relative
            Cell::Str("parquet".into()),      // file_format
            Cell::U64(10),                    // record_count
            Cell::U64(1024),                  // file_size_bytes
            Cell::U64(64),                    // footer_size
            Cell::U64(0),                     // row_id_start
            Cell::Null,                       // partition_id
            Cell::Str("ZGF0YS1rZXk=".into()), // encryption_key
            Cell::Null,                       // mapping_id
            Cell::Null,                       // partial_max
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),                         // delete_file_id
            Cell::U64(1),                         // table_id
            Cell::U64(1),                         // begin_snapshot
            Cell::Null,                           // end_snapshot
            Cell::U64(1),                         // data_file_id
            Cell::Str("delete.parquet".into()),   // path
            Cell::Bool(true),                     // path_is_relative
            Cell::Str("parquet".into()),          // format
            Cell::U64(2),                         // delete_count
            Cell::U64(128),                       // file_size_bytes
            Cell::U64(32),                        // footer_size
            Cell::Str("ZGVsZXRlLWtleQ==".into()), // encryption_key
            Cell::Null,                           // partial_max
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let table = head.tables_in(crate::catalog::SchemaId::new(0))[0].id;
    assert_eq!(
        head.data_files_of(table)[0].encryption_key.as_deref(),
        Some("ZGF0YS1rZXk=")
    );
    assert_eq!(
        head.delete_files_of(table)[0].encryption_key.as_deref(),
        Some("ZGVsZXRlLWtleQ==")
    );
}

/// An `UPDATE ... SET end_snapshot` row ends a live table version:
/// the old row moves to `history`, the new one lands in `current`, exactly
/// the lifecycle convention this path interprets.
#[tokio::test]
async fn update_set_end_moves_the_old_version_to_history() {
    let catalog = open().await;

    // Seed schema `s` (id 1) and table `t` (id 1) via a plain insert.
    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: schema_row(1, "s", 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_old", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_schema:"s""#),
    });
    setup.commit().await.unwrap();

    // Rename: end the old table version, insert the renamed one.
    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut rename = StagedTransaction::begin_detached(db_tx2);
    rename.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(1), Cell::U64(2)],
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_new", 2, None),
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    rename.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.tables_in(crate::catalog::SchemaId::new(1))[0].name,
        "t_new"
    );

    let past = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap();
    assert_eq!(
        past.tables_in(crate::catalog::SchemaId::new(1))[0].name,
        "t_old"
    );
}

/// A rename staged in DuckLake's live order — the new version's
/// insert *before* the old version's end — keeps the new version
/// live. Translation applies ends before inserts, so the shared `current`
/// key resolves to the insert regardless of stage order.
#[tokio::test]
async fn rename_survives_insert_before_end_order() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: schema_row(1, "s", 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_old", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_schema:"s""#),
    });
    setup.commit().await.unwrap();

    // Insert the renamed version first, then end the old one — the
    // reverse of the safe order, matching what DuckLake emits.
    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut rename = StagedTransaction::begin_detached(db_tx2);
    rename.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 1, "t_new", 2, None),
    });
    rename.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Table,
        cells: vec![Cell::U64(1), Cell::U64(2)],
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    rename.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    rename.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let live = head.tables_in(crate::catalog::SchemaId::new(1));
    assert_eq!(live.len(), 1, "exactly one live table after rename");
    assert_eq!(live[0].name, "t_new");
}

/// A lost race at commit is never retried: the loser's error carries
/// the literal substring `conflict`, and the store is left exactly as
/// the winner left it.
#[tokio::test]
async fn lost_race_is_not_retried_and_carries_conflict_text() {
    let catalog = open().await;

    let tx_a = catalog.begin_write_tx().await.unwrap();
    let tx_b = catalog.begin_write_tx().await.unwrap();
    let mut a = StagedTransaction::begin_detached(tx_a);
    let mut b = StagedTransaction::begin_detached(tx_b);

    for (tx, name) in [(&mut a, "a"), (&mut b, "b")] {
        tx.stage(RowOperation::Insert {
            table: TableKind::Schema,
            cells: schema_row(1, name, 1),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::Snapshot,
            cells: snapshot_row(1, 1, 2),
        });
        tx.stage(RowOperation::Insert {
            table: TableKind::SnapshotChanges,
            cells: snapshot_changes_row(1, format!(r#"created_schema:"{name}""#).as_str()),
        });
    }

    a.commit().await.unwrap();
    let err = b.commit().await.unwrap_err();
    assert!(
        err.to_string().contains("conflict"),
        "error must carry the literal substring `conflict`: {err}"
    );

    // The store reflects only the winner: schema `a`, not `b`.
    let head = catalog.snapshot().await.unwrap();
    assert!(head.schema_by_name("a").is_some());
    assert!(head.schema_by_name("b").is_none());
}

/// A malformed staged row (wrong cell count) fails loudly as
/// `Corruption` rather than panicking or silently truncating.
#[tokio::test]
async fn malformed_row_is_corruption_not_a_panic() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Schema,
        cells: vec![Cell::U64(1)], // far too few cells
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, ""),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Corruption(_)));
}

/// Stages an inline schema plus two inserts against the same
/// `(table_id, schema_version, begin_snapshot)` in one commit: the
/// chunks land with sequential `chunk_seq` (stage order), and the
/// schema is readable back verbatim.
#[tokio::test]
async fn stages_inline_schema_and_sequential_inserts() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);

    tx.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: b"schema".to_vec(),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 2,
        arrow_body: b"chunk-a".to_vec(),
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 2,
        row_count: 1,
        arrow_body: b"chunk-b".to_vec(),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        chunks[0].0,
        InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 0,
        }
    );
    assert_eq!(chunks[0].1.body, b"chunk-a");
    assert_eq!(chunks[0].1.row_id_start, 0);
    assert_eq!(chunks[0].1.row_count, 2);
    assert_eq!(
        chunks[1].0,
        InlineOperation::Insert {
            table_id: 1,
            schema_version: 0,
            begin_snapshot: 1,
            chunk_seq: 1,
        }
    );
    assert_eq!(chunks[1].1.body, b"chunk-b");

    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    assert_eq!(
        schemas,
        vec![(
            0,
            proto::InlineSchemaValue {
                arrow_schema: b"schema".to_vec(),
            }
        )]
    );
    tx.rollback();
}

/// An `InlineIdel` tombstones a row: the row is absent from a
/// `Table`-kind materialization at or after its `end_snapshot`.
#[tokio::test]
async fn stages_inline_idel_and_row_disappears_from_table_scan_after_it() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 2,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut inline_delete = StagedTransaction::begin_detached(db_tx2);
    inline_delete.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 2,
    });
    inline_delete.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    inline_delete.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "inlined_delete:1"),
    });
    inline_delete.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let inline_deletes = store_inline::scan_inline_inline_deletes(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert_eq!(
        inline_deletes,
        vec![(0, proto::InlineInlineDeleteValue { end_snapshot: 2 })]
    );

    let rows = materialize_inline_rows(&chunks, &inline_deletes);
    assert_eq!(
        InlineScanKind::Table
            .select(&rows, 1, 0)
            .iter()
            .map(|r| r.row_id)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        InlineScanKind::Table
            .select(&rows, 2, 0)
            .iter()
            .map(|r| r.row_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

/// The schema-only Arrow IPC stream stored once per inline schema
/// version, matching what the extension's encoder produces.
fn inline_schema_ipc(schema: &arrow::datatypes::Schema) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, schema).unwrap();
        writer.finish().unwrap();
    }
    buffer
}

/// One inline chunk body: a little-endian `u32` message length, the
/// record-batch message, then the arrow data buffers.
fn inline_body(batch: &arrow::array::RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };

    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let options = IpcWriteOptions::default();
    let mut context = IpcWriteContext::default();
    let (dictionaries, encoded) = generator
        .encode(batch, &mut tracker, &options, &mut context)
        .unwrap();
    assert!(dictionaries.is_empty(), "test bodies carry no dictionaries");

    let mut buffer = Vec::new();
    buffer.extend_from_slice(
        &u32::try_from(encoded.ipc_message.len())
            .unwrap()
            .to_le_bytes(),
    );
    buffer.extend_from_slice(&encoded.ipc_message);
    buffer.extend_from_slice(&encoded.arrow_data);
    buffer
}

/// A one-column `BIGINT` batch, the shape the inline index tests insert.
fn bigint_batch(values: &[i64]) -> (arrow::datatypes::Schema, arrow::array::RecordBatch) {
    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    (schema, batch)
}

/// Creates table 1 with one `BIGINT` column and an equality index over
/// it, returning the catalog and the index id.
async fn catalog_with_indexed_inline_table(unique: bool) -> (Catalog, u64) {
    use crate::catalog::{IndexDef, TableId};

    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx);
    setup.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "created_table:1"),
    });
    setup.commit().await.unwrap();

    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index(
                TableId::new(1),
                &IndexDef {
                    name: "by_a".into(),
                    columns: vec![crate::catalog::ColumnId::new(1)],
                    unique,
                },
                &[],
            )?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();

    (catalog, index.get().unwrap().get())
}

/// The stored entry count for one index.
async fn index_entry_count(catalog: &Catalog, unique: bool, index_id: u64) -> usize {
    use crate::store::key::{IdxKind, idx_index_prefix};

    let kind = if unique {
        IdxKind::Unique
    } else {
        IdxKind::Multi
    };
    let tx = catalog.begin_write_tx().await.unwrap();
    let mut iter = ReadHandle::Tx(&tx)
        .scan_prefix(idx_index_prefix(kind, index_id), ..)
        .await
        .unwrap();
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    tx.rollback();
    count
}

/// Stages one inline chunk of `values` starting at `row_id_start`,
/// registering the schema on the first call.
async fn inline_insert(
    catalog: &Catalog,
    snapshot_id: u64,
    row_id_start: u64,
    values: &[i64],
    with_schema: bool,
) {
    let (schema, batch) = bigint_batch(values);
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    if with_schema {
        tx.stage(RowOperation::InlineSchema {
            table_id: 1,
            schema_version: 0,
            arrow_schema: inline_schema_ipc(&schema),
        });
    }
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: snapshot_id,
        row_id_start,
        row_count: u64::try_from(values.len()).unwrap(),
        arrow_body: inline_body(&batch),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();
}

/// Tombstones one inlined row in its own commit.
async fn inline_row_delete(catalog: &Catalog, snapshot_id: u64, row_id: u64) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id,
        end_snapshot: snapshot_id,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "inlined_delete:1"),
    });
    tx.commit().await.map(|_| ())
}

/// Deleting an inlined row removes its unique index entry, so the value
/// is free to be inserted again — entries are live-only.
#[tokio::test]
async fn inline_row_delete_removes_its_unique_index_entry() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    inline_insert(&catalog, 3, 0, &[7], true).await;
    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the insert lands one entry"
    );

    inline_row_delete(&catalog, 4, 0).await.unwrap();
    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        0,
        "the delete removes the entry it killed"
    );
}

/// The replace pattern a writer depends on: delete a row, then insert
/// the same unique value again in a later commit.
#[tokio::test]
async fn inline_delete_then_reinsert_admits_the_same_unique_value() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    inline_insert(&catalog, 3, 0, &[7], true).await;
    inline_row_delete(&catalog, 4, 0).await.unwrap();
    inline_insert(&catalog, 5, 1, &[7], false).await;

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the value is indexed again, held by the new row"
    );
}

/// Delete and reinsert of one unique value inside a single commit: the
/// removal is staged before the put, so the value reads as absent.
#[tokio::test]
async fn inline_delete_and_reinsert_in_one_commit_admits_the_same_unique_value() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;

    inline_insert(&catalog, 3, 0, &[7], true).await;

    let (_, batch) = bigint_batch(&[7]);
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 4,
    });
    tx.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 4,
        row_id_start: 1,
        row_count: 1,
        arrow_body: inline_body(&batch),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inlined_insert:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "the reinserted value holds exactly one entry"
    );
}

/// A non-unique index leaks differently: a stale entry does not block
/// writes, it makes a lookup resolve a row id that no longer exists.
#[tokio::test]
async fn inline_row_delete_removes_its_non_unique_index_entry() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(false).await;

    inline_insert(&catalog, 3, 0, &[7, 7], true).await;
    assert_eq!(
        index_entry_count(&catalog, false, index_id).await,
        2,
        "both rows share the value under a non-unique index"
    );

    inline_row_delete(&catalog, 4, 0).await.unwrap();
    assert_eq!(
        index_entry_count(&catalog, false, index_id).await,
        1,
        "only the surviving row is still indexed"
    );
}

/// Writes `batch` to `path` on `store` as Parquet, returning the
/// written object's size — the maintenance read locates the footer by
/// the recorded `file_size_bytes`, so fixtures must record the truth,
/// exactly as DuckLake records the real written size.
async fn write_parquet(store: &InMemory, path: &str, batch: &arrow::array::RecordBatch) -> u64 {
    use object_store::ObjectStoreExt;

    let mut buffer = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    let object_len = u64::try_from(buffer.len()).unwrap();
    store
        .put(&object_store::path::Path::from(path), buffer.into())
        .await
        .unwrap();
    object_len
}

/// A `ducklake_data_file` row for a file of `record_count` rows and
/// `file_size_bytes` bytes on the store.
fn indexed_data_file_row(record_count: u64, file_size_bytes: u64) -> Vec<Cell> {
    vec![
        Cell::U64(1),
        Cell::U64(1),
        Cell::U64(3),
        Cell::Null,
        Cell::Null,
        Cell::Str("data.parquet".into()),
        Cell::Bool(true),
        Cell::Str("parquet".into()),
        Cell::U64(record_count),
        Cell::U64(file_size_bytes),
        Cell::U64(64),
        Cell::U64(0), // row_id_start
        Cell::Null,
        Cell::Null,
        Cell::Null,
        Cell::Null,
    ]
}

/// Registers a Parquet data file of `values` on the indexed table,
/// returning the store it lives on.
async fn register_indexed_data_file(catalog: &Catalog, values: &[i64]) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let (_, batch) = bigint_batch(values);
    // `s/` and `t/` are the bootstrap schema and table path prefixes.
    let file_size = write_parquet(&store, "main/t/data.parquet", &batch).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store.clone());
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: indexed_data_file_row(u64::try_from(values.len()).unwrap(), file_size),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();
    store
}

/// An inlined delete against a Parquet-file row removes that row's
/// entry, read back out of the target file.
#[tokio::test]
async fn inlined_file_delete_removes_the_killed_rows_index_entry() {
    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;
    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        3,
        "the registered file lands one entry per row"
    );

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 1,
        row_id: 1,
        begin_snapshot: 4,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "inlined_delete:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        2,
        "the deleted row's entry is gone, the other two remain"
    );
}

/// A registered delete file names positions in its target; the commit
/// reads those positions' values and removes exactly their entries.
#[tokio::test]
async fn registered_delete_file_removes_the_killed_rows_index_entries() {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    let (catalog, index_id) = catalog_with_indexed_inline_table(true).await;
    let store = register_indexed_data_file(&catalog, &[10, 20, 30]).await;

    // A DuckLake delete file: `file_path` plus the killed positions.
    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet", "data.parquet"])),
            Arc::new(Int64Array::from(vec![0, 2])),
        ],
    )
    .unwrap();
    write_parquet(&store, "main/t/deletes.parquet", &deletes).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached_with_store(db_tx, store);
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(2),
            Cell::U64(1),
            Cell::U64(4),
            Cell::Null,
            Cell::U64(1), // data_file_id
            Cell::Str("deletes.parquet".into()),
            Cell::Bool(true),
            Cell::Str("parquet".into()),
            Cell::U64(2), // delete_count
            Cell::U64(512),
            Cell::U64(64),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();

    assert_eq!(
        index_entry_count(&catalog, true, index_id).await,
        1,
        "positions 0 and 2 are unindexed; only row 1 survives"
    );
}

/// `InlineFlushDelete` removes chunks begun at or before the flush
/// snapshot for the named schema version, plus the `inline/inline_delete`
/// tombstones those chunks' rows consumed — a later schema version's
/// chunk (begun after the flush point) survives untouched.
#[tokio::test]
async fn stages_inline_flush_delete_removes_flushed_chunks_and_their_idels() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 2,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    // A later commit tombstones one row (a tombstone only ever ends a
    // version begun before it — DuckLake's writer never stamps a row
    // with its own insertion snapshot).
    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut delete = StagedTransaction::begin_detached(db_tx2);
    delete.stage(RowOperation::InlineInlineDelete {
        table_id: 1,
        row_id: 0,
        end_snapshot: 2,
    });
    delete.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    delete.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "inlined_delete:1"),
    });
    delete.commit().await.unwrap();

    let db_tx3 = catalog.begin_write_tx().await.unwrap();
    let mut flush = StagedTransaction::begin_detached(db_tx3);
    flush.stage(RowOperation::InlineFlushDelete {
        table_id: 1,
        schema_version: 0,
        flush_snapshot: 2,
    });
    flush.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 0, 1),
    });
    flush.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "flushed_inlined_data:1"),
    });
    flush.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let inline_deletes = store_inline::scan_inline_inline_deletes(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert!(chunks.is_empty(), "flushed chunk must be gone: {chunks:?}");
    assert!(
        inline_deletes.is_empty(),
        "consumed inline_delete must be gone: {inline_deletes:?}"
    );
}

/// `InlineDrop` removes every `inline/*` record for the table:
/// schema, chunks, and tombstones.
#[tokio::test]
async fn stages_inline_drop_removes_every_record_for_the_table() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: b"schema".to_vec(),
    });
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 0,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 1,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::InlineFileDelete {
        table_id: 1,
        data_file_id: 9,
        row_id: 5,
        begin_snapshot: 1,
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut drop_tx = StagedTransaction::begin_detached(db_tx2);
    drop_tx.stage(RowOperation::InlineDrop { table_id: 1 });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, r#"dropped_table:"main"."t""#),
    });
    drop_tx.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let file_deletes = store_inline::scan_inline_file_deletes(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert!(chunks.is_empty());
    assert!(file_deletes.is_empty());
    assert!(schemas.is_empty());
}

/// `InlineSchemaDrop` removes only the named schema version's
/// `inline/schema` record, leaving a different schema version's
/// record (and its chunks) untouched — the scoped cleanup a
/// superseded-inlined-table flush needs, as opposed to `InlineDrop`'s
/// whole-table sweep.
#[tokio::test]
async fn stages_inline_schema_drop_removes_only_the_named_schema_version() {
    let catalog = open().await;

    let db_tx1 = catalog.begin_write_tx().await.unwrap();
    let mut setup = StagedTransaction::begin_detached(db_tx1);
    setup.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 0,
        arrow_schema: b"schema-v0".to_vec(),
    });
    setup.stage(RowOperation::InlineSchema {
        table_id: 1,
        schema_version: 1,
        arrow_schema: b"schema-v1".to_vec(),
    });
    setup.stage(RowOperation::InlineInsert {
        table_id: 1,
        schema_version: 1,
        begin_snapshot: 1,
        row_id_start: 0,
        row_count: 1,
        arrow_body: b"chunk".to_vec(),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 0, 1),
    });
    setup.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "inlined_insert:1"),
    });
    setup.commit().await.unwrap();

    let db_tx2 = catalog.begin_write_tx().await.unwrap();
    let mut drop_tx = StagedTransaction::begin_detached(db_tx2);
    drop_tx.stage(RowOperation::InlineSchemaDrop {
        table_id: 1,
        schema_version: 0,
    });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 0, 1),
    });
    drop_tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "flushed_inlined_data:1"),
    });
    drop_tx.commit().await.unwrap();

    let tx = catalog.begin_write_tx().await.unwrap();
    let schemas = store_inline::scan_inline_schemas(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    let chunks = store_inline::scan_inline_chunks(ReadHandle::Tx(&tx), 1)
        .await
        .unwrap();
    tx.rollback();
    assert_eq!(
        schemas,
        vec![(
            1,
            proto::InlineSchemaValue {
                arrow_schema: b"schema-v1".to_vec()
            }
        )]
    );
    assert_eq!(chunks.len(), 1, "schema_version 1's chunk must survive");
}

fn partition_info_row(partition_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(partition_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,
    ]
}

fn partition_column_row(partition_id: u64, table_id: u64, index: u64, column_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(partition_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::U64(column_id),
        Cell::Str("identity".to_string()),
    ]
}

fn file_partition_value_row(
    data_file_id: u64,
    table_id: u64,
    index: u64,
    value: &str,
) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::Str(value.to_string()),
    ]
}

fn data_file_row(data_file_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(data_file_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,                       // end_snapshot
        Cell::Null,                       // file_order
        Cell::Str("data.parquet".into()), // path
        Cell::Bool(true),                 // path_is_relative
        Cell::Str("parquet".into()),      // file_format
        Cell::U64(10),                    // record_count
        Cell::U64(1024),                  // file_size_bytes
        Cell::U64(64),                    // footer_size
        Cell::U64(0),                     // row_id_start
        Cell::Null,                       // partition_id
        Cell::Null,                       // encryption_key
        Cell::Null,                       // mapping_id
        Cell::Null,                       // partial_max
    ]
}

/// A partition spec, its columns, and a file's partition values land
/// verbatim; repartitioning ends the old spec; time travel
/// reconstructs the spec-in-force, and every file still reports the
/// spec it was written under.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn partition_spec_rows_land_fold_and_time_travel() {
    let catalog = open().await;

    // Commit 1: table + column + spec 10 (identity on column 1) + one
    // data file written under it, carrying one partition value.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "part_key", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionInfo,
        cells: partition_info_row(10, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionColumn,
        cells: partition_column_row(10, 1, 0, 1),
    });
    let mut file = data_file_row(1, 1, 1);
    file[12] = Cell::U64(10); // partition_id
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: file,
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilePartitionValue,
        cells: file_partition_value_row(1, 1, 0, "7"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let spec = &head.partitions[&1][&10];
    assert_eq!(spec.begin_snapshot, 1);
    assert_eq!(spec.columns.len(), 1);
    assert_eq!(spec.columns[0].column_id, 1);
    assert_eq!(spec.columns[0].transform, "identity");
    let stored = &head.data_files[&1][&1];
    assert_eq!(stored.partition_id, Some(10));
    assert_eq!(stored.partition_values.len(), 1);
    assert_eq!(stored.partition_values[0].partition_value, "7");

    // Commit 2: repartition — end spec 10, insert spec 11.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::PartitionInfo,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionInfo,
        cells: partition_info_row(11, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionColumn,
        cells: partition_column_row(11, 1, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 12),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(!head.partitions[&1].contains_key(&10));
    assert!(head.partitions[&1].contains_key(&11));
    // The file still names the spec it was written under.
    assert_eq!(head.data_files[&1][&1].partition_id, Some(10));

    // Time travel reconstructs spec 10 at snapshot 1.
    let at_one = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap();
    assert!(at_one.partitions[&1].contains_key(&10));
    assert!(!at_one.partitions[&1].contains_key(&11));

    // The dump surface serves current and history rows unfiltered.
    let specs = crate::ffi_support::dump_partition_info(&catalog)
        .await
        .unwrap();
    assert!(
        specs
            .iter()
            .any(|p| p.partition_id == 10 && p.end_snapshot == Some(2))
    );
    assert!(
        specs
            .iter()
            .any(|p| p.partition_id == 11 && p.end_snapshot.is_none())
    );

    // Commit 3: clear — end spec 11, insert nothing.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::PartitionInfo,
        cells: vec![Cell::U64(1), Cell::U64(11), Cell::U64(3)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 12),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(
        head.partitions
            .get(&1)
            .is_none_or(std::collections::BTreeMap::is_empty)
    );

    catalog.close().await.unwrap();
}

fn sort_info_row(sort_id: u64, table_id: u64, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(sort_id),
        Cell::U64(table_id),
        Cell::U64(begin),
        Cell::Null,
    ]
}

fn sort_expression_row(sort_id: u64, table_id: u64, index: u64, expression: &str) -> Vec<Cell> {
    vec![
        Cell::U64(sort_id),
        Cell::U64(table_id),
        Cell::U64(index),
        Cell::Str(expression.to_string()),
        Cell::Str("duckdb".to_string()),
        Cell::Str("DESC".to_string()),
        Cell::Str("NULLS_FIRST".to_string()),
    ]
}

/// A sort spec and its expressions land verbatim — direction, null
/// order, and dialect untouched; re-sorting ends the old spec; time
/// travel reconstructs the spec-in-force.
#[tokio::test]
async fn sort_spec_rows_land_fold_and_time_travel() {
    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "v", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortInfo,
        cells: sort_info_row(20, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortExpression,
        cells: sort_expression_row(20, 1, 0, "v"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 21),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let spec = &head.sorts[&1][&20];
    assert_eq!(spec.begin_snapshot, 1);
    assert_eq!(spec.expressions.len(), 1);
    assert_eq!(spec.expressions[0].expression, "v");
    assert_eq!(spec.expressions[0].dialect, "duckdb");
    assert_eq!(spec.expressions[0].sort_direction, "DESC");
    assert_eq!(spec.expressions[0].null_order, "NULLS_FIRST");

    // End spec 20, insert spec 21 — the snapshot row keeps the same
    // schema_version: DuckLake does not bump it for sort changes.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::SortInfo,
        cells: vec![Cell::U64(1), Cell::U64(20), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortInfo,
        cells: sort_info_row(21, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SortExpression,
        cells: sort_expression_row(21, 1, 0, "v || 'x'"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 22),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(!head.sorts[&1].contains_key(&20));
    assert_eq!(head.sorts[&1][&21].expressions[0].expression, "v || 'x'");

    let at_one = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap();
    assert!(at_one.sorts[&1].contains_key(&20));
    assert!(!at_one.sorts[&1].contains_key(&21));

    // The dump surface serves current and history rows unfiltered.
    let specs = crate::ffi_support::dump_sort_info(&catalog).await.unwrap();
    assert!(
        specs
            .iter()
            .any(|s| s.sort_id == 20 && s.end_snapshot == Some(2))
    );
    assert!(
        specs
            .iter()
            .any(|s| s.sort_id == 21 && s.end_snapshot.is_none())
    );

    catalog.close().await.unwrap();
}

/// A `partition_column` row whose spec is not inserted in the same
/// commit is a shape error, not a silent drop.
#[tokio::test]
async fn orphaned_partition_column_row_is_rejected() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::PartitionColumn,
        cells: partition_column_row(99, 1, 0, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "none"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("partition"), "{err}");
    catalog.close().await.unwrap();
}

fn tag_row(object_id: u64, begin: u64, key: &str, value: &str) -> Vec<Cell> {
    vec![
        Cell::U64(object_id),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(key.to_string()),
        Cell::Str(value.to_string()),
    ]
}

fn column_tag_row(table_id: u64, column_id: u64, begin: u64, key: &str, value: &str) -> Vec<Cell> {
    vec![
        Cell::U64(table_id),
        Cell::U64(column_id),
        Cell::U64(begin),
        Cell::Null,
        Cell::Str(key.to_string()),
        Cell::Str(value.to_string()),
    ]
}

async fn seed_table(catalog: &Catalog) {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();
}

/// A re-comment (`COMMENT ON` twice) is DuckLake's set-end + insert
/// pair: the old entry ends, the new one lands live, both kept in the
/// object's container for time travel.
#[tokio::test]
async fn tag_rows_land_and_a_recomment_ends_the_old_entry() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Tag,
        cells: tag_row(1, 2, "comment", "first"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Tag,
        cells: vec![Cell::U64(1), Cell::Str("comment".into()), Cell::U64(3)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Tag,
        cells: tag_row(1, 3, "comment", "second"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let tags = head.tags_of(1);
    assert_eq!(tags.len(), 2);
    assert_eq!(
        (tags[0].value.as_str(), tags[0].end_snapshot),
        ("first", Some(3))
    );
    assert_eq!(
        (tags[1].value.as_str(), tags[1].end_snapshot),
        ("second", None)
    );
    catalog.close().await.unwrap();
}

/// A column tag rides its column's record without minting a column
/// version: after tagging, the column still has exactly one row on
/// the dump surface, and the tag ends in place on a re-comment.
#[tokio::test]
async fn column_tags_ride_the_column_record_without_a_version_transition() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::ColumnTag,
        cells: column_tag_row(1, 1, 2, "comment", "col comment"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    let columns = crate::ffi_support::dump_columns(&catalog).await.unwrap();
    let rows: Vec<_> = columns.iter().filter(|c| c.column_id == 1).collect();
    assert_eq!(rows.len(), 1, "a tag change must not mint a column version");
    assert_eq!(rows[0].begin_snapshot, 0);
    assert!(rows[0].end_snapshot.is_none());
    assert_eq!(rows[0].tags.len(), 1);
    assert_eq!(rows[0].tags[0].value, "col comment");
    catalog.close().await.unwrap();
}

/// A column version transition (rename) carries the prior version's
/// tag entries onto the new current record — DuckLake keys column
/// tags by (table, column) with their own lifecycle, so an alter
/// never re-authors them.
#[tokio::test]
async fn column_alter_carries_tags_forward() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::ColumnTag,
        cells: column_tag_row(1, 1, 2, "comment", "kept"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    tx.commit().await.unwrap();

    // Rename the column: end the old version, insert the new one.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Column,
        cells: vec![Cell::U64(1), Cell::U64(1), Cell::U64(3)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: {
            let mut cells = column_row(1, 1, "renamed", 0);
            cells[1] = Cell::U64(3); // begin_snapshot
            cells
        },
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 2, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "altered_table:1"),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SchemaVersions,
        cells: vec![Cell::U64(3), Cell::U64(2), Cell::U64(1)],
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let column = &head.columns[&1][&1];
    assert_eq!(column.column_name, "renamed");
    assert_eq!(column.tags.len(), 1);
    assert_eq!(column.tags[0].value, "kept");
    catalog.close().await.unwrap();
}

/// Seeds `t` (table 1) with data file 9 (snapshot 1), then expires the
/// file's live version at snapshot 2 — leaving one dead history row,
/// the fixture the expiry tests prune.
async fn seed_expired_file(catalog: &Catalog) {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(9, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(9), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "deleted_from_table:1"),
    });
    tx.commit().await.unwrap();
}

/// The expiry cascade's staged shape: delete a dead snapshot, prune
/// the history row it alone saw, and schedule the file's bytes — all
/// in one head-preserving maintenance commit.
#[tokio::test]
async fn expiry_prunes_history_and_schedules_files_without_advancing_head() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(1)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::SnapshotChanges,
        cells: vec![Cell::U64(1)],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(9), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(9),
            Cell::Str("f9.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    let id = tx.commit().await.unwrap();
    assert_eq!(id.get(), 2, "maintenance must not advance head");

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id.get(), 2);
    let schedule = head.scheduled_deletions();
    assert_eq!(schedule.len(), 1);
    assert_eq!(schedule[0].data_file_id, 9);
    assert_eq!(schedule[0].path, "f9.parquet");

    // The dead snapshot no longer resolves; the survivor does, and
    // the pruned history row is gone from the dump surface.
    let expired = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(1))
        .await
        .unwrap_err();
    assert!(matches!(expired, Error::NotFound(_)), "{expired}");
    let surviving = catalog
        .snapshot_at(crate::catalog::SnapshotId::new(2))
        .await
        .unwrap();
    assert!(
        surviving
            .data_files_of(crate::catalog::TableId::new(1))
            .is_empty()
    );
    let files = crate::ffi_support::dump_data_files(&catalog).await.unwrap();
    assert!(files.is_empty(), "history row must be pruned: {files:?}");

    catalog.close().await.unwrap();
}

/// Cleanup's staged shape: after DuckDB deletes the bytes, the
/// schedule row is forgotten in a head-preserving commit.
#[tokio::test]
async fn cleanup_forgets_the_schedule() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(9),
            Cell::Str("f9.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![Cell::U64(9)],
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.scheduled_deletions().is_empty());
    assert_eq!(head.current_snapshot().id.get(), 2);
    catalog.close().await.unwrap();
}

/// The head snapshot can never be expired.
#[tokio::test]
async fn deleting_the_head_snapshot_is_rejected() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(2)],
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
    catalog.close().await.unwrap();
}

/// A commit that mutates entities without minting a snapshot is not a
/// maintenance commit — it is a malformed write.
#[tokio::test]
async fn maintenance_commit_rejects_entity_inserts() {
    let catalog = open().await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
    catalog.close().await.unwrap();
}

/// A merge-shaped compaction commit: the merged file lands backdated,
/// the source rows (current and history alike) are hard-deleted, the
/// source bytes are scheduled, and `next_row_id` is untouched — all
/// in one ordinary snapshot-minting commit.
#[tokio::test]
async fn merge_shaped_commit_replaces_files_and_schedules_sources() {
    let catalog = open().await;

    // Seed: table 1 with files 9 and 10, both live.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::Table,
        cells: table_row(1, 0, "t", 1, None),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Column,
        cells: column_row(1, 1, "a", 0),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(9, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(10, 1, 1),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::TableStats,
        cells: vec![Cell::U64(1), Cell::U64(20), Cell::U64(20), Cell::U64(2048)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, r#"created_table:"main"."t""#),
    });
    tx.commit().await.unwrap();

    // The merge: insert file 11 backdated to the sources' begin,
    // hard-delete both sources, schedule their bytes.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(11, 1, 1),
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(9), Cell::Null],
    });
    tx.stage(RowOperation::Delete {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::Null],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(9),
            Cell::Str("data.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::FilesScheduledForDeletion,
        cells: vec![
            Cell::U64(10),
            Cell::Str("data.parquet".to_string()),
            Cell::Bool(true),
            Cell::I64(1_000),
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "merge_adjacent:1"),
    });
    let id = tx.commit().await.unwrap();
    assert_eq!(id.get(), 2, "compaction mints an ordinary snapshot");

    let head = catalog.snapshot().await.unwrap();
    let files = head.data_files_of(crate::catalog::TableId::new(1));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id.get(), 11);
    assert_eq!(
        head.scheduled_deletions()
            .iter()
            .map(|d| d.data_file_id)
            .collect::<Vec<_>>(),
        vec![9, 10]
    );
    assert_eq!(
        head.table_stats(crate::catalog::TableId::new(1))
            .unwrap()
            .next_row_id,
        20,
        "compaction never allocates row ids"
    );

    // The sources are gone outright — no history mirror.
    let rows = crate::ffi_support::dump_data_files(&catalog).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data_file_id, 11);
    assert_eq!(rows[0].begin_snapshot, 1, "the merged file is backdated");
    catalog.close().await.unwrap();
}

/// A rewrite-shaped commit ends the source file and its delete file
/// into history and rebases the replacement's `begin_snapshot` in
/// place; nothing is scheduled.
#[tokio::test]
async fn rewrite_shaped_commit_ends_rows_and_rebases_the_new_file() {
    let catalog = open().await;
    seed_expired_file(&catalog).await; // file 9 ended at snapshot 2

    // Re-seed a live file 10 with a delete file 11 over it.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(10, 1, 3),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::DeleteFile,
        cells: vec![
            Cell::U64(11),
            Cell::U64(1),
            Cell::U64(3),
            Cell::Null,
            Cell::U64(10),
            Cell::Str("delete.parquet".to_string()),
            Cell::Bool(true),
            Cell::Str("parquet".to_string()),
            Cell::U64(2),
            Cell::U64(128),
            Cell::U64(32),
            Cell::Null,
            Cell::Null,
        ],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    // The rewrite: new file 12, end file 10 and delete file 11,
    // rebase 12's begin to this commit's snapshot.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(12, 1, 4),
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::DeleteFile,
        cells: vec![Cell::U64(1), Cell::U64(11), Cell::U64(4)],
    });
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(12), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "rewrite_delete:1"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    let files = head.data_files_of(crate::catalog::TableId::new(1));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id.get(), 12);
    assert!(
        head.delete_files_of(crate::catalog::TableId::new(1))
            .is_empty()
    );
    assert!(head.scheduled_deletions().is_empty());

    // The ended rows survive in history; the replacement is rebased.
    let rows = crate::ffi_support::dump_data_files(&catalog).await.unwrap();
    assert!(
        rows.iter()
            .any(|f| f.data_file_id == 10 && f.end_snapshot == Some(4))
    );
    assert!(
        rows.iter()
            .any(|f| f.data_file_id == 12 && f.begin_snapshot == 4 && f.end_snapshot.is_none())
    );
    catalog.close().await.unwrap();
}

/// Rebasing a file that predates the commit is a shape error —
/// DuckLake only rebases the replacement it just inserted.
#[tokio::test]
async fn set_begin_on_a_preexisting_file_is_rejected() {
    let catalog = open().await;
    seed_expired_file(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::Insert {
        table: TableKind::DataFile,
        cells: data_file_row(10, 1, 3),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(3, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(3, "inserted_into_table:1"),
    });
    tx.commit().await.unwrap();

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetBegin {
        table: TableKind::DataFile,
        cells: vec![Cell::U64(1), Cell::U64(10), Cell::U64(4)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(4, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(4, "rewrite_delete:1"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("predates this commit"), "{err}");
    catalog.close().await.unwrap();
}

/// Ending a tag entry that does not exist is a shape error — DuckLake
/// only updates rows it just read, so a miss means drift.
#[tokio::test]
async fn ending_an_absent_tag_entry_is_rejected() {
    let catalog = open().await;
    seed_table(&catalog).await;

    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Tag,
        cells: vec![Cell::U64(1), Cell::Str("comment".into()), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 2),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "altered_table:1"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("tag"), "{err}");
    catalog.close().await.unwrap();
}

fn macro_row(schema_id: u64, macro_id: u64, name: &str, begin: u64) -> Vec<Cell> {
    vec![
        Cell::U64(schema_id),
        Cell::U64(macro_id),
        Cell::Str(name.to_string()),
        Cell::U64(begin),
        Cell::Null, // end_snapshot
    ]
}

fn macro_impl_row(macro_id: u64, impl_id: u64, sql: &str, macro_type: &str) -> Vec<Cell> {
    vec![
        Cell::U64(macro_id),
        Cell::U64(impl_id),
        Cell::Str("duckdb".into()),
        Cell::Str(sql.to_string()),
        Cell::Str(macro_type.to_string()),
    ]
}

fn macro_parameter_row(
    macro_id: u64,
    impl_id: u64,
    column_id: u64,
    name: &str,
    default: Option<&str>,
) -> Vec<Cell> {
    vec![
        Cell::U64(macro_id),
        Cell::U64(impl_id),
        Cell::U64(column_id),
        Cell::Str(name.to_string()),
        Cell::Str("unknown".into()),
        default.map_or(Cell::Null, |d| Cell::Str(d.to_string())),
        Cell::Str(default.map_or("unknown", |_| "int32").to_string()),
    ]
}

/// Stages one commit's rows plus the snapshot pair and returns the
/// commit error, if any.
async fn stage_macro_batch(
    catalog: &Catalog,
    snapshot_id: u64,
    next_catalog_id: u64,
    rows: Vec<(TableKind, Vec<Cell>)>,
) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    for (table, cells) in rows {
        tx.stage(RowOperation::Insert { table, cells });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, next_catalog_id),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, r#"created_scalar_macro:"main"."m""#),
    });
    tx.commit().await.map(|_| ())
}

/// A macro insert with its impl and parameter rows folds into one
/// record (children ordered by their ordinals regardless of emit
/// order); a later drop ends the whole record into history, children
/// intact, and time travel still reads it.
#[tokio::test]
async fn macro_rows_land_fold_and_drop() {
    let catalog = open().await;

    // Rows deliberately emitted out of ordinal order.
    stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 1, 1, "b", Some("5")),
            ),
            (
                TableKind::MacroImpl,
                macro_impl_row(10, 1, "(a + b)", "scalar"),
            ),
            (TableKind::Macro, macro_row(0, 10, "add", 1)),
            (
                TableKind::MacroImpl,
                macro_impl_row(10, 0, "(a + 1)", "scalar"),
            ),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 0, 0, "a", None),
            ),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 1, 0, "a", None),
            ),
        ],
    )
    .await
    .unwrap();

    let head = catalog.snapshot().await.unwrap();
    let stored = &head.macros[&10];
    assert_eq!(stored.begin_snapshot, 1);
    assert_eq!(stored.implementations.len(), 2);
    assert_eq!(stored.implementations[0].impl_id, 0);
    assert_eq!(stored.implementations[0].sql, "(a + 1)");
    assert_eq!(stored.implementations[1].parameters.len(), 2);
    assert_eq!(stored.implementations[1].parameters[1].parameter_name, "b");
    assert_eq!(
        stored.implementations[1].parameters[1]
            .default_value
            .as_deref(),
        Some("5")
    );

    // Drop: the one UPDATE DuckLake issues, nothing touching children.
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::Macro,
        cells: vec![Cell::U64(10), Cell::U64(2)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(2, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(2, "dropped_scalar_macro:10"),
    });
    tx.commit().await.unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.macros.is_empty());
    let past = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap();
    let past_macro = &past.macros[&10];
    assert_eq!(past_macro.end_snapshot, Some(2));
    assert_eq!(past_macro.implementations.len(), 2);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn macro_insert_without_impl_rows_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![(TableKind::Macro, macro_row(0, 10, "m", 1))],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("macro_impl"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn orphaned_macro_impl_row_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![(TableKind::MacroImpl, macro_impl_row(99, 0, "1", "scalar"))],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("macro_impl"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn orphaned_macro_parameter_row_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 7, 0, "a", None),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("macro_parameters"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn macro_impl_id_gap_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (TableKind::MacroImpl, macro_impl_row(10, 2, "2", "scalar")),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("contiguous"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn macro_parameter_column_id_gap_is_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (
                TableKind::MacroParameters,
                macro_parameter_row(10, 0, 1, "a", None),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("contiguous"), "{err}");
    catalog.close().await.unwrap();
}

fn column_mapping_row(mapping_id: u64, table_id: u64) -> Vec<Cell> {
    vec![
        Cell::U64(mapping_id),
        Cell::U64(table_id),
        Cell::Str("map_by_name".into()),
    ]
}

fn name_mapping_row(
    mapping_id: u64,
    column_id: u64,
    source_name: &str,
    target_field_id: u64,
    parent_column: Option<u64>,
    is_partition: bool,
) -> Vec<Cell> {
    vec![
        Cell::U64(mapping_id),
        Cell::U64(column_id),
        Cell::Str(source_name.to_string()),
        Cell::U64(target_field_id),
        parent_column.map_or(Cell::Null, Cell::U64),
        Cell::Bool(is_partition),
    ]
}

/// Stages one commit's rows plus the snapshot pair and returns the
/// commit result.
async fn stage_mapping_batch(
    catalog: &Catalog,
    snapshot_id: u64,
    rows: Vec<(TableKind, Vec<Cell>)>,
) -> Result<()> {
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    for (table, cells) in rows {
        tx.stage(RowOperation::Insert { table, cells });
    }
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(snapshot_id, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(snapshot_id, "inserted_into_table:1"),
    });
    tx.commit().await.map(|_| ())
}

/// A column mapping folds its name-mapping rows (emitted out of
/// ordinal order, one nested child, one hive-partition virtual
/// column) and the added file carries its `mapping_id`; the record
/// is served at any time-travel target.
#[tokio::test]
async fn mapping_rows_land_fold_and_serve_time_travel() {
    let catalog = open().await;

    let mut file = data_file_row(7, 1, 1);
    file[14] = Cell::U64(21); // mapping_id
    stage_mapping_batch(
        &catalog,
        1,
        vec![
            (
                TableKind::NameMapping,
                name_mapping_row(21, 2, "region", 3, None, true),
            ),
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 1, "id", 2, Some(0), false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "payload", 1, None, false),
            ),
            (TableKind::DataFile, file),
        ],
    )
    .await
    .unwrap();

    let head = catalog.snapshot().await.unwrap();
    let stored = &head.mappings[&1][&21];
    assert_eq!(stored.map_type, "map_by_name");
    assert_eq!(stored.name_mappings.len(), 3);
    assert_eq!(stored.name_mappings[0].source_name, "payload");
    assert_eq!(stored.name_mappings[1].parent_column, Some(0));
    assert!(stored.name_mappings[2].is_partition);
    assert_eq!(head.data_files[&1][&7].mapping_id, Some(21));

    // Unversioned: a time-travel view still serves the mapping.
    let past = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap();
    assert_eq!(past.mappings[&1][&21].name_mappings.len(), 3);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn column_mapping_without_name_rows_is_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![(TableKind::ColumnMapping, column_mapping_row(21, 1))],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("name_mapping"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn orphaned_name_mapping_row_is_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![(
            TableKind::NameMapping,
            name_mapping_row(99, 0, "id", 1, None, false),
        )],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("name_mapping"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_name_mapping_ordinals_are_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "a", 1, None, false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "b", 2, None, false),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("unique"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn name_mapping_parent_after_child_is_rejected() {
    let catalog = open().await;
    let err = stage_mapping_batch(
        &catalog,
        1,
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "a", 1, Some(1), false),
            ),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 1, "b", 2, None, false),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("earlier"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_mapping_id_against_base_is_rejected() {
    let catalog = open().await;
    let mapping = || {
        vec![
            (TableKind::ColumnMapping, column_mapping_row(21, 1)),
            (
                TableKind::NameMapping,
                name_mapping_row(21, 0, "id", 1, None, false),
            ),
        ]
    };
    stage_mapping_batch(&catalog, 1, mapping()).await.unwrap();
    let err = stage_mapping_batch(&catalog, 2, mapping())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn update_set_end_on_column_mapping_is_rejected() {
    let catalog = open().await;
    let db_tx = catalog.begin_write_tx().await.unwrap();
    let mut tx = StagedTransaction::begin_detached(db_tx);
    tx.stage(RowOperation::UpdateSetEnd {
        table: TableKind::ColumnMapping,
        cells: vec![Cell::U64(21), Cell::U64(1)],
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::Snapshot,
        cells: snapshot_row(1, 1, 11),
    });
    tx.stage(RowOperation::Insert {
        table: TableKind::SnapshotChanges,
        cells: snapshot_changes_row(1, "none"),
    });
    let err = tx.commit().await.unwrap_err();
    assert!(err.to_string().contains("not defined"), "{err}");
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn mixed_macro_impl_types_are_rejected() {
    let catalog = open().await;
    let err = stage_macro_batch(
        &catalog,
        1,
        11,
        vec![
            (TableKind::Macro, macro_row(0, 10, "m", 1)),
            (TableKind::MacroImpl, macro_impl_row(10, 0, "1", "scalar")),
            (
                TableKind::MacroImpl,
                macro_impl_row(10, 1, "SELECT 1", "table"),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("share a type"), "{err}");
    catalog.close().await.unwrap();
}

/// `ALL` is the wire-decode table: each kind's discriminant is its
/// index, and out-of-range values are refused. The exhaustive match
/// makes adding a variant a compile error here until `ALL` (and the
/// ABI doc) cover it.
#[test]
fn table_kind_wire_order_is_pinned() {
    for (index, kind) in TableKind::ALL.iter().enumerate() {
        assert_eq!(*kind as usize, index, "{kind:?}");
        assert_eq!(TableKind::try_from(*kind as i32), Ok(*kind));
    }
    assert_eq!(TableKind::try_from(25), Err(25));
    assert_eq!(TableKind::try_from(-1), Err(-1));

    for kind in TableKind::ALL {
        match kind {
            TableKind::Snapshot
            | TableKind::SnapshotChanges
            | TableKind::Schema
            | TableKind::Table
            | TableKind::View
            | TableKind::Column
            | TableKind::DataFile
            | TableKind::DeleteFile
            | TableKind::TableStats
            | TableKind::TableColumnStats
            | TableKind::FileColumnStats
            | TableKind::SchemaVersions
            | TableKind::PartitionInfo
            | TableKind::PartitionColumn
            | TableKind::FilePartitionValue
            | TableKind::SortInfo
            | TableKind::SortExpression
            | TableKind::Tag
            | TableKind::ColumnTag
            | TableKind::FilesScheduledForDeletion
            | TableKind::Macro
            | TableKind::MacroImpl
            | TableKind::MacroParameters
            | TableKind::ColumnMapping
            | TableKind::NameMapping => {}
        }
    }
}
