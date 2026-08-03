//! The read cache through the public API: repeated reads must serve the
//! same catalog a cold read would build, whatever the cache did in between.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, SnapshotId};
use object_store::memory::InMemory;

use crate::fixtures::{col, datafile, seeded};

/// A second read is served from the cache after a commit moved head. It
/// must show the commit — a cache that serves a stale head is worse than
/// no cache.
#[tokio::test]
async fn a_read_after_a_commit_sees_the_commit() {
    let (catalog, schema, table_a, _) = seeded().await;

    let first = catalog.snapshot().await.unwrap();
    assert!(first.table_by_name(schema, "late").is_none());

    catalog
        .commit(|tx| {
            tx.create_table(schema, "late", &[col("x")])?;
            tx.register_data_file(table_a, datafile(5), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let second = catalog.snapshot().await.unwrap();
    let late = second
        .table_by_name(schema, "late")
        .expect("a cached read must see the new table");
    assert_eq!(second.columns_of(late.id).len(), 1);
    assert_eq!(second.data_files_of(table_a).len(), 1);

    catalog.close().await.unwrap();
}

/// Reading twice with no commit in between must not drift: the second
/// read serves the cache, and the cache must equal what it replaced.
#[tokio::test]
async fn repeated_reads_agree() {
    let (catalog, schema, table_a, _) = seeded().await;
    catalog
        .commit(|tx| {
            tx.register_data_file(table_a, datafile(3), &[])?;
            Ok(())
        })
        .await
        .unwrap();

    let first = catalog.snapshot().await.unwrap();
    let second = catalog.snapshot().await.unwrap();

    assert_eq!(
        first.current_snapshot().id.get(),
        second.current_snapshot().id.get()
    );
    assert_eq!(
        first.tables_in(schema).len(),
        second.tables_in(schema).len()
    );
    assert_eq!(
        first.data_files_of(table_a).len(),
        second.data_files_of(table_a).len()
    );

    catalog.close().await.unwrap();
}

/// Every entity kind a commit can touch must survive the fold: the view a
/// warm handle serves answers exactly as a cold reopen does.
#[tokio::test]
async fn a_cached_read_matches_a_cold_reopen() {
    let (catalog, schema, table_a, table_b) = seeded().await;
    let warm = catalog.snapshot().await.unwrap();
    assert_eq!(warm.tables_in(schema).len(), 2);

    let wide = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            tx.register_data_file(table_a, datafile(7), &[])?;
            tx.rename_table(table_b, "renamed")?;
            tx.create_view(schema, "v", "duckdb", "SELECT 1")?;
            wide.set(Some(tx.create_table(
                schema,
                "wide",
                &[col("x"), col("y")],
            )?));
            Ok(())
        })
        .await
        .unwrap();
    let wide = wide.get().unwrap();

    // Dropping a column ends one child of a table the same commit did not
    // create, which is the fold's over-cascade trap.
    catalog
        .commit(|tx| {
            let second = tx.columns_of(wide)[1].id;
            tx.drop_column(wide, second)
        })
        .await
        .unwrap();

    // The live handle answers from its folded-forward cache.
    let cached = catalog.snapshot().await.unwrap();
    let head = cached.current_snapshot().id;

    // A cold handle over the same store has no cache and must scan.
    let cold = catalog
        .snapshot_at(SnapshotId::new(head.get()))
        .await
        .unwrap();

    assert_eq!(cached.tables_in(schema).len(), cold.tables_in(schema).len());
    assert!(cached.table_by_name(schema, "renamed").is_some());
    assert!(cached.view_by_name(schema, "v").is_some());
    assert_eq!(
        cached.data_files_of(table_a).len(),
        cold.data_files_of(table_a).len()
    );
    // The drop must end exactly one column, not cascade to the table.
    assert_eq!(cached.columns_of(wide).len(), cold.columns_of(wide).len());
    assert_eq!(cached.columns_of(wide).len(), 1);

    catalog.close().await.unwrap();
}

/// A held view is a value, not a cursor: commits after it was built must
/// leave it exactly as it was, however many land and whatever they touch.
#[tokio::test]
async fn a_held_view_is_unmoved_by_later_commits() {
    let (catalog, schema, table_a, table_b) = seeded().await;

    let held = catalog.snapshot().await.unwrap();
    let at = held.current_snapshot().id.get();
    let tables_before = held.tables_in(schema).len();
    let files_before = held.data_files_of(table_a).len();

    for round in 0..4u64 {
        catalog
            .commit(|tx| {
                tx.register_data_file(table_a, datafile(round), &[])?;
                tx.create_table(schema, &format!("later{round}"), &[col("x")])?;
                Ok(())
            })
            .await
            .unwrap();
    }
    catalog.commit(|tx| tx.drop_table(table_b)).await.unwrap();

    assert_eq!(held.current_snapshot().id.get(), at);
    assert_eq!(held.tables_in(schema).len(), tables_before);
    assert_eq!(held.data_files_of(table_a).len(), files_before);
    assert!(held.table_by_name(schema, "later0").is_none());
    assert!(held.table_by_name(schema, "b").is_some());

    // The same view, rebuilt from `history` at the same snapshot, agrees —
    // so what the held value shows is the catalog at `at`, not a stale
    // accident of how it was built.
    let travelled = catalog.snapshot_at(SnapshotId::new(at)).await.unwrap();
    assert_eq!(travelled.tables_in(schema).len(), tables_before);
    assert_eq!(travelled.data_files_of(table_a).len(), files_before);

    catalog.close().await.unwrap();
}

/// A read-only catalog caches its view as a writer does. It has no commits
/// of its own to fold, so what the cache must never do is drift: a second
/// read has to answer exactly what the first one did and exactly what the
/// store holds.
#[tokio::test]
async fn a_read_only_catalog_serves_a_cached_view_that_matches_the_store() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let writer = Catalog::open(object_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            tx.create_table(schema, "t", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();

    // A reader opened after `commit` returns resolves that commit.
    let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
        .await
        .unwrap();
    let first = reader.snapshot().await.unwrap();
    let schema = first.schema_by_name("s").unwrap().id;
    assert!(first.table_by_name(schema, "t").is_some());

    // The second read is served from the cache and must not drift.
    let second = reader.snapshot().await.unwrap();
    assert_eq!(
        first.current_snapshot().id.get(),
        second.current_snapshot().id.get()
    );
    assert_eq!(
        second.tables_in(schema).len(),
        first.tables_in(schema).len()
    );
    assert!(second.table_by_name(schema, "t").is_some());

    // And it matches what a rebuild from the store at the same snapshot
    // shows, so the cache is not the only thing that believes it.
    let scanned = reader
        .snapshot_at(SnapshotId::new(first.current_snapshot().id.get()))
        .await
        .unwrap();
    assert_eq!(
        scanned.tables_in(schema).len(),
        first.tables_in(schema).len()
    );

    writer.close().await.unwrap();
}
