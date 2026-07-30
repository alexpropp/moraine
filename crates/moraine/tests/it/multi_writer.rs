use std::sync::Arc;

use futures::StreamExt;
use moraine::{
    Catalog, CatalogOptions, ColumnId, Error, FileIndexEntry, IndexDef, IndexKeyValue, IntWidth,
    SnapshotId,
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use crate::fixtures::{col, datafile};

fn multi_writer_options() -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.multi_writer = true;
    options
}

#[allow(clippy::unwrap_used)]
async fn open_multi_writer(store: &Arc<InMemory>) -> Catalog {
    Catalog::open(
        store.clone() as Arc<dyn ObjectStore>,
        multi_writer_options(),
    )
    .await
    .unwrap()
}

/// How many objects sit under `prefix`.
async fn objects_under(store: &Arc<InMemory>, prefix: &str) -> usize {
    let mut listing = store.list(Some(&Path::from(prefix)));
    let mut count = 0;
    while listing.next().await.is_some() {
        count += 1;
    }
    count
}

#[tokio::test]
async fn multi_writer_open_bootstraps_and_serves_the_empty_catalog() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id, moraine::SnapshotId::new(0));
    // A second full open finds the initialized store rather than
    // re-bootstrapping, and does not fence anything (there is no writer).
    let second = open_multi_writer(&store).await;
    second.snapshot().await.unwrap();
    catalog.snapshot().await.unwrap();
}

/// Time travel over a slot-backed attach: the bootstrap snapshot resolves
/// from the folded store, and a snapshot no slot has minted does not.
#[tokio::test]
async fn multi_writer_time_travel_spans_the_folded_head() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;

    let bootstrapped = catalog
        .snapshot_at(moraine::SnapshotId::new(0))
        .await
        .unwrap();
    assert_eq!(bootstrapped.schemas().len(), 1);

    let err = catalog
        .snapshot_at(moraine::SnapshotId::new(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
}

/// A prefix holding objects but no readable manifest is a damaged store, not
/// a fresh one: the attach refuses instead of stamping a new catalog over
/// whatever is there.
#[tokio::test]
async fn multi_writer_open_refuses_a_store_it_cannot_read_but_is_not_empty() {
    let store = Arc::new(InMemory::new());
    store
        .put(&Path::from("cat/leftover"), "not a slatedb object".into())
        .await
        .unwrap();

    let mut options = multi_writer_options();
    options.path = "cat".to_string();
    let err = Catalog::open(store.clone() as Arc<dyn ObjectStore>, options)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Store(_)), "got {err:?}");

    // The refusal wrote nothing: the planted object is still all there is.
    assert_eq!(objects_under(&store, "cat").await, 1);
}

#[tokio::test]
async fn a_multi_writer_commit_lands_in_a_slot_and_is_readable_unfolded() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    let id = catalog
        .commit(|tx| tx.create_schema("analytics").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(1));

    // A second, independent attach sees it purely through tail replay.
    let other = open_multi_writer(&store).await;
    let snapshot = other.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("analytics").is_some());
}

/// Genuinely disjoint DDL — a column added to each of two different tables —
/// never conflicts, so the loser of a slot race rebases onto the winner and
/// both land in adjacent slots. (Two `create_schema`s would collide under the
/// coarse schema-list rule, so the disjoint case is DDL on distinct tables,
/// as `commit_concurrency::disjoint_table_ddl_both_succeed` establishes.)
#[tokio::test(flavor = "multi_thread")]
async fn disjoint_racing_commits_both_land_in_adjacent_slots() {
    let store = Arc::new(InMemory::new());
    let setup = open_multi_writer(&store).await;
    setup
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "a", &[col("x")])?;
            tx.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = setup.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap().id;
    let ta = snapshot.table_by_name(s, "a").unwrap().id;
    let tb = snapshot.table_by_name(s, "b").unwrap().id;

    let a = open_multi_writer(&store).await;
    let b = open_multi_writer(&store).await;
    let (ra, rb) = tokio::join!(
        a.commit(move |tx| tx.add_column(ta, &col("a1")).map(|_| ())),
        b.commit(move |tx| tx.add_column(tb, &col("b1")).map(|_| ())),
    );
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    assert_ne!(ra, rb, "dense, distinct snapshot ids");

    let head = a.snapshot().await.unwrap();
    assert!(head.columns_of(ta).iter().any(|c| c.name == "a1"));
    assert!(head.columns_of(tb).iter().any(|c| c.name == "b1"));
}

/// Both drop the same schema: one wins, the loser's re-validation fails typed
/// — either `CommitConflict` from classification, or the closure's own
/// `NotFound` once the winner's drop replays into the loser's head.
#[tokio::test(flavor = "multi_thread")]
async fn conflicting_racing_commits_surface_one_typed_conflict() {
    let store = Arc::new(InMemory::new());
    let a = open_multi_writer(&store).await;
    a.commit(|tx| tx.create_schema("doomed").map(|_| ()))
        .await
        .unwrap();
    let doomed = a
        .snapshot()
        .await
        .unwrap()
        .schema_by_name("doomed")
        .unwrap()
        .id;

    let b = open_multi_writer(&store).await;
    let (ra, rb) = tokio::join!(
        a.commit(move |tx| tx.drop_schema(doomed)),
        b.commit(move |tx| tx.drop_schema(doomed)),
    );
    assert_eq!(u8::from(ra.is_ok()) + u8::from(rb.is_ok()), 1);
    let loser = if ra.is_err() { ra } else { rb };
    assert!(
        matches!(loser, Err(Error::CommitConflict(_) | Error::NotFound(_))),
        "{loser:?}"
    );
}

/// Two commits insert the same unique value concurrently: exactly one lands,
/// and the loser is rejected by the uniqueness probe reading the winner's
/// unfolded entry through the overlay — proof the `Overlaid` probe sees tail
/// writes no folder has applied.
#[tokio::test(flavor = "multi_thread")]
async fn racing_unique_inserts_reject_the_duplicate_through_the_overlay() {
    let store = Arc::new(InMemory::new());
    let setup = open_multi_writer(&store).await;
    setup
        .commit(|tx| {
            let s = tx.schema_by_name("main").unwrap().id;
            let t = tx.create_table(s, "orders", &[col("k")])?;
            tx.create_index(
                t,
                &IndexDef {
                    name: "by_k".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = setup.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("main").unwrap().id;
    let t = snapshot.table_by_name(s, "orders").unwrap().id;
    let index = snapshot.indexes_of(t).remove(0).id;

    let insert = move |tx: &mut moraine::Transaction| {
        tx.register_data_file(
            t,
            datafile(1),
            &[FileIndexEntry {
                index,
                ordinal: 0,
                values: vec![Some(IndexKeyValue::Int {
                    value: 42,
                    width: IntWidth::I64,
                })],
            }],
        )
        .map(|_| ())
    };

    let a = open_multi_writer(&store).await;
    let b = open_multi_writer(&store).await;
    let (ra, rb) = tokio::join!(a.commit(insert), b.commit(insert));
    assert_eq!(u8::from(ra.is_ok()) + u8::from(rb.is_ok()), 1);

    let loser = if ra.is_err() { ra } else { rb };
    assert!(matches!(loser, Err(Error::Constraint(_))), "{loser:?}");
}
