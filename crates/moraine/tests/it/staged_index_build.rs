//! `Catalog::create_index_staged`: driving a multi-commit index build to
//! `ready` over a table whose rows live in registered Parquet files.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use moraine::{Catalog, ColumnId, Error, IndexDef, IndexKeyValue, IndexState, IntWidth, TableId};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;

use crate::fixtures::{col, datafile, open_memory};

/// A table holding one registered file of `values`, plus the data store
/// that file lives in.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn table_with_file(values: Vec<i64>) -> (Catalog, TableId, Arc<dyn ObjectStore>) {
    let catalog = open_memory().await;
    let rows = u64::try_from(values.len()).expect("row count fits");
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.register_data_file(table, datafile(rows), &[])?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();

    let data = Arc::new(InMemory::new());
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int64Array::from(values.clone()))],
    )
    .unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    data.put(
        &Path::from(format!("main/orders/data-{rows}.parquet")),
        buffer.into(),
    )
    .await
    .unwrap();

    (catalog, created.get().unwrap(), data)
}

fn def(unique: bool) -> IndexDef {
    IndexDef {
        name: "by_a".into(),
        columns: vec![ColumnId::new(1)],
        unique,
    }
}

fn int(value: i128) -> IndexKeyValue {
    IndexKeyValue::Int {
        value,
        width: IntWidth::I64,
    }
}

/// The driver builds across several bounded steps and flips the index
/// ready; every backfilled row is then resolvable.
#[tokio::test]
async fn staged_build_spans_steps_and_finishes_ready() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;

    let index = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(2))
        .await
        .unwrap();

    let info = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(info.state, IndexState::Ready, "the build flipped ready");
    assert_eq!(info.id, index);

    for value in 0..7 {
        let hits = catalog
            .index_lookup(table, index, &[int(i128::from(value))])
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "value {value} is indexed");
        assert_eq!(hits[0].row_id, u64::try_from(value).unwrap());
    }
    catalog.close().await.unwrap();
}

/// A build interrupted partway resumes from its persisted cursor: the
/// re-derived entries below the watermark land as idempotent puts, and the
/// finished index covers every row exactly once.
#[tokio::test]
async fn interrupted_staged_build_resumes_from_its_cursor() {
    let (catalog, table, data) = table_with_file((0..7).collect()).await;

    // Begin the definition and advance one step by hand, leaving it
    // building — the state a cancelled or crashed driver leaves behind.
    let index = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let id = tx.create_index_staged(table, &def(true))?;
            index.set(Some(id));
            Ok(())
        })
        .await
        .unwrap();
    let index = index.get().unwrap();
    catalog
        .commit(|tx| {
            tx.build_index_step(
                index,
                &[
                    moraine::IndexEntry {
                        row_id: 0,
                        values: vec![Some(int(0))],
                    },
                    moraine::IndexEntry {
                        row_id: 1,
                        values: vec![Some(int(1))],
                    },
                ],
                false,
            )
            .map(|_| ())
        })
        .await
        .unwrap();
    let building = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(building.state, IndexState::Building);
    assert_eq!(building.build_cursor, Some(1), "the cursor persisted");

    // Re-issuing the create resumes rather than starting over or colliding
    // on the name.
    let resumed = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(2))
        .await
        .unwrap();
    assert_eq!(resumed, index, "resumed the same definition");

    let info = catalog
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(info.state, IndexState::Ready);
    for value in 0..7 {
        assert_eq!(
            catalog
                .index_lookup(table, index, &[int(i128::from(value))])
                .await
                .unwrap()
                .len(),
            1,
            "value {value} is indexed exactly once"
        );
    }
    catalog.close().await.unwrap();
}

/// A duplicate among the backfilled rows fails the build: the caller gets
/// `Constraint` and the definition is gone, exactly as a single-commit
/// create over the same rows would leave things.
#[tokio::test]
async fn duplicate_in_backfill_fails_the_build_and_drops_the_definition() {
    // The duplicate (4) sits past the first step, so it is only discovered
    // once the build is already several commits in.
    let (catalog, table, data) = table_with_file(vec![0, 1, 2, 3, 4, 4, 6]).await;

    let err = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(2))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err}");
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .indexes_of(table)
            .is_empty(),
        "the failed build left no definition behind"
    );
    catalog.close().await.unwrap();
}

/// A non-unique staged build admits the duplicates a unique one refuses.
#[tokio::test]
async fn non_unique_staged_build_admits_duplicates() {
    let (catalog, table, data) = table_with_file(vec![0, 1, 2, 3, 4, 4, 6]).await;

    let index = catalog
        .create_index_staged(table, &def(false), &[], Some(data), "", Some(2))
        .await
        .unwrap();

    let hits = catalog.index_lookup(table, index, &[int(4)]).await.unwrap();
    assert_eq!(hits.len(), 2, "both rows holding 4 are indexed");
    catalog.close().await.unwrap();
}

/// Resuming adopts the definition already on disk, so a create describing a
/// different index is refused rather than silently handed the stored one —
/// its entries are already encoded under the stored orders.
#[tokio::test]
async fn resuming_with_a_different_definition_is_refused() {
    use moraine::{ColumnOrder, Direction, NullOrder};
    let (catalog, table, data) = table_with_file((0..7).collect()).await;
    catalog
        .commit(|tx| tx.create_index_staged(table, &def(true)).map(|_| ()))
        .await
        .unwrap();

    // Same name and columns, opposite uniqueness.
    let unique_differs = catalog
        .create_index_staged(
            table,
            &def(false),
            &[],
            Some(Arc::clone(&data)),
            "",
            Some(2),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(unique_differs, Error::Constraint(_)),
        "{unique_differs}"
    );

    // Same name, columns and uniqueness, different declared order.
    let orders_differ = catalog
        .create_index_staged(
            table,
            &def(true),
            &[ColumnOrder {
                direction: Direction::Descending,
                nulls: NullOrder::First,
            }],
            Some(data),
            "",
            Some(2),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(orders_differ, Error::Constraint(_)),
        "{orders_differ}"
    );
    catalog.close().await.unwrap();
}

/// A staged create naming an index that is already ready is refused, like
/// any duplicate name.
#[tokio::test]
async fn staged_create_over_a_ready_index_is_refused() {
    let (catalog, table, data) = table_with_file((0..3).collect()).await;
    catalog
        .create_index_staged(table, &def(true), &[], Some(Arc::clone(&data)), "", Some(2))
        .await
        .unwrap();

    let err = catalog
        .create_index_staged(table, &def(true), &[], Some(data), "", Some(2))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "{err}");
    catalog.close().await.unwrap();
}
