//! Integration tests: exercise the public API only, against real SlateDB
//! on in-memory object storage.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, ColumnId, Error, SnapshotId};
use object_store::memory::InMemory;

#[tokio::test]
async fn bootstrap_creates_snapshot_zero() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let snap = catalog.snapshot().await.unwrap();
    assert_eq!(snap.current_snapshot().id, SnapshotId::new(0));
    assert_eq!(snap.current_snapshot().schema_version, 0);
    assert!(snap.schemas().is_empty());
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn reopen_finds_the_initialized_store() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let first = catalog.snapshot().await.unwrap().current_snapshot();
    catalog.close().await.unwrap();

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let second = catalog.snapshot().await.unwrap().current_snapshot();
    // Same snapshot 0, same commit time: opened, not re-initialized.
    assert_eq!(first, second);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_at_beyond_head_is_not_found() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let err = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn committed_state_survives_reopen() {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|txn| {
            let s = txn.create_schema("durable")?;
            txn.create_table(s, "t", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();

    let catalog = Catalog::open(store, CatalogOptions::default())
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(1));
    let s = head.schema_by_name("durable").unwrap();
    assert!(head.table_by_name(s.id, "t").is_some());
    catalog.close().await.unwrap();
}

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
    }
}

// Test-only helper: `unwrap_used` is a library-code lint, not exempted
// automatically for a plain (non-`#[test]`) function even in an
// integration-test crate.
#[allow(clippy::unwrap_used)]
async fn open_memory() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn ddl_commits_are_visible_and_time_travelable() {
    let catalog = open_memory().await;

    let s1 = catalog
        .commit(|txn| {
            let s = txn.create_schema("sales")?;
            txn.create_table(s, "orders", &[col("id"), col("qty")])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s1, SnapshotId::new(1));

    let s2 = catalog
        .commit(|txn| {
            let schema = txn.schema_by_name("sales").expect("committed above");
            let table = txn
                .table_by_name(schema.id, "orders")
                .expect("committed above");
            txn.rename_table(table.id, "orders_v2")?;
            txn.add_column(table.id, &col("note"))?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s2, SnapshotId::new(2));

    // Head sees the final shape.
    let head = catalog.snapshot().await.unwrap();
    let schema = head.schema_by_name("sales").unwrap();
    let table = head.table_by_name(schema.id, "orders_v2").unwrap();
    assert_eq!(head.columns_of(table.id).len(), 3);
    assert!(head.table_by_name(schema.id, "orders").is_none());

    // Snapshot 1 still sees the original shape.
    let past = catalog.snapshot_at(s1).await.unwrap();
    let old = past.table_by_name(schema.id, "orders").unwrap();
    assert_eq!(old.id, table.id);
    assert_eq!(past.columns_of(old.id).len(), 2);

    // Snapshot 0 sees an empty catalog.
    let zero = catalog.snapshot_at(SnapshotId::new(0)).await.unwrap();
    assert!(zero.schemas().is_empty());

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn drop_ends_versions_and_schema_version_tracks_ddl() {
    let catalog = open_memory().await;
    let s1 = catalog
        .commit(|txn| {
            let s = txn.create_schema("tmp")?;
            txn.create_table(s, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    let s2 = catalog
        .commit(|txn| {
            let schema = txn.schema_by_name("tmp").expect("committed above");
            let table = txn.table_by_name(schema.id, "t").expect("committed above");
            txn.drop_table(table.id)?;
            txn.drop_schema(schema.id)?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert!(head.schemas().is_empty());
    // Every DDL commit advanced the schema version.
    assert_eq!(head.current_snapshot().schema_version, 2);

    // The dropped entities are still visible at their snapshot.
    let past = catalog.snapshot_at(s1).await.unwrap();
    assert_eq!(past.schemas().len(), 1);
    let _ = s2;
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn empty_commit_mints_no_snapshot() {
    let catalog = open_memory().await;
    let id = catalog.commit(|_txn| Ok(())).await.unwrap();
    assert_eq!(id, SnapshotId::new(0));
    let err = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn logical_errors_abort_the_commit() {
    let catalog = open_memory().await;
    catalog
        .commit(|txn| {
            txn.create_schema("sales")?;
            Ok(())
        })
        .await
        .unwrap();
    let err = catalog
        .commit(|txn| {
            txn.create_schema("sales")?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    // The failed commit left no snapshot behind.
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(1));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn dropped_column_field_ids_are_not_reused_across_commits() {
    let catalog = open_memory().await;
    catalog
        .commit(|txn| {
            let s = txn.create_schema("s")?;
            txn.create_table(s, "t", &[col("a"), col("b")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|txn| {
            let schema = txn.schema_by_name("s").expect("committed above");
            let table = txn.table_by_name(schema.id, "t").expect("committed above");
            txn.drop_column(table.id, ColumnId::new(2))?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|txn| {
            let schema = txn.schema_by_name("s").expect("committed above");
            let table = txn.table_by_name(schema.id, "t").expect("committed above");
            let id = txn.add_column(table.id, &col("c"))?;
            assert_eq!(id, ColumnId::new(3), "field id 2 must not be reused");
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
}
