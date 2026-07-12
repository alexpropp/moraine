//! Integration tests: exercise the public API only, against real SlateDB
//! on in-memory object storage.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, ColumnId, Error, SchemaId, SnapshotId};
use object_store::memory::InMemory;

#[tokio::test]
async fn bootstrap_creates_snapshot_zero() {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    let snap = catalog.snapshot().await.unwrap();
    assert_eq!(snap.current_snapshot().id, SnapshotId::new(0));
    assert_eq!(snap.current_snapshot().schema_version, 0);
    let schemas = snap.schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "main");

    // `main` consumed catalog id 0; the first user-created schema follows.
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let sales = head.schema_by_name("sales").unwrap();
    assert_eq!(sales.id, SchemaId::new(1));

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
        .commit(|tx| {
            let s = tx.create_schema("durable")?;
            tx.create_table(s, "t", &[col("x")])?;
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
        .commit(|tx| {
            let s = tx.create_schema("sales")?;
            tx.create_table(s, "orders", &[col("id"), col("qty")])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s1, SnapshotId::new(1));

    let s2 = catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("sales").expect("committed above");
            let table = tx
                .table_by_name(schema.id, "orders")
                .expect("committed above");
            tx.rename_table(table.id, "orders_v2")?;
            tx.add_column(table.id, &col("note"))?;
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

    // Snapshot 0 sees only the bootstrap-minted `main` schema.
    let zero = catalog.snapshot_at(SnapshotId::new(0)).await.unwrap();
    assert_eq!(zero.schemas().len(), 1);
    assert_eq!(zero.schemas()[0].name, "main");

    catalog.close().await.unwrap();
}

#[tokio::test]
async fn drop_ends_versions_and_schema_version_tracks_ddl() {
    let catalog = open_memory().await;
    let s1 = catalog
        .commit(|tx| {
            let s = tx.create_schema("tmp")?;
            tx.create_table(s, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();
    let s2 = catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("tmp").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            tx.drop_table(table.id)?;
            tx.drop_schema(schema.id)?;
            Ok(())
        })
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    // Only the bootstrap-minted `main` schema remains live.
    assert_eq!(head.schemas().len(), 1);
    assert_eq!(head.schemas()[0].name, "main");
    // Every DDL commit advanced the schema version.
    assert_eq!(head.current_snapshot().schema_version, 2);

    // The dropped entities are still visible at their snapshot.
    let past = catalog.snapshot_at(s1).await.unwrap();
    assert_eq!(past.schemas().len(), 2);
    let _ = s2;
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn empty_commit_mints_no_snapshot() {
    let catalog = open_memory().await;
    let id = catalog.commit(|_tx| Ok(())).await.unwrap();
    assert_eq!(id, SnapshotId::new(0));
    let err = catalog.snapshot_at(SnapshotId::new(1)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn logical_errors_abort_the_commit() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            tx.create_schema("sales")?;
            Ok(())
        })
        .await
        .unwrap();
    let err = catalog
        .commit(|tx| {
            tx.create_schema("sales")?;
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
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "t", &[col("a"), col("b")])?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            tx.drop_column(table.id, ColumnId::new(2))?;
            Ok(())
        })
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("s").expect("committed above");
            let table = tx.table_by_name(schema.id, "t").expect("committed above");
            let id = tx.add_column(table.id, &col("c"))?;
            assert_eq!(id, ColumnId::new(3), "field id 2 must not be reused");
            Ok(())
        })
        .await
        .unwrap();
    catalog.close().await.unwrap();
}
