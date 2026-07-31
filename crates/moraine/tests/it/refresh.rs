//! Reader refresh through the public API: repeated reads must serve the
//! same catalog a cold read would build, whatever the cache did in between.

use moraine::SnapshotId;

use crate::fixtures::{col, datafile, open_memory, seeded};

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
        .expect("a refreshed read must see the new table");
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

/// Every entity kind a refresh can touch must survive one: a view built by
/// advancing through the changelog answers exactly as a cold reopen does.
#[tokio::test]
async fn a_refreshed_read_matches_a_cold_reopen() {
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

    // The live handle answers from a refreshed cache.
    let refreshed = catalog.snapshot().await.unwrap();
    let head = refreshed.current_snapshot().id;

    // A cold handle over the same store has no cache and must scan.
    let cold = catalog
        .snapshot_at(SnapshotId::new(head.get()))
        .await
        .unwrap();

    assert_eq!(
        refreshed.tables_in(schema).len(),
        cold.tables_in(schema).len()
    );
    assert!(refreshed.table_by_name(schema, "renamed").is_some());
    assert!(refreshed.view_by_name(schema, "v").is_some());
    assert_eq!(
        refreshed.data_files_of(table_a).len(),
        cold.data_files_of(table_a).len()
    );
    // The drop must end exactly one column, not cascade to the table.
    assert_eq!(
        refreshed.columns_of(wide).len(),
        cold.columns_of(wide).len()
    );
    assert_eq!(refreshed.columns_of(wide).len(), 1);

    catalog.close().await.unwrap();
}

/// A read-only catalog keeps no cache, so its reads must still resolve the
/// writer's commits from the store on every call.
#[tokio::test]
async fn a_read_only_catalog_reads_through() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let schema = tx.create_schema("s")?;
            tx.create_table(schema, "t", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();

    let snapshot = catalog.snapshot().await.unwrap();
    let schema = snapshot.schema_by_name("s").unwrap().id;
    assert!(snapshot.table_by_name(schema, "t").is_some());

    catalog.close().await.unwrap();
}
