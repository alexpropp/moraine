//! Views and options through the public API: versioned view lifecycle,
//! the shared relation namespace, and options committed outside the
//! snapshot protocol — real SlateDB on in-memory object storage.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, Error, OptionScope, SnapshotId};
use object_store::memory::InMemory;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
    }
}

#[allow(clippy::unwrap_used)]
async fn open_memory() -> Catalog {
    Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn views_commit_version_and_time_travel() {
    let catalog = open_memory().await;
    let created = catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "t", &[col("a")])?;
            tx.create_view(s, "v", "duckdb", "SELECT 1")?;
            Ok(())
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let s = head.schema_by_name("s").unwrap();
    let v = head.view_by_name(s.id, "v").unwrap();
    assert_eq!(v.sql, "SELECT 1");
    let version_after_create = head.current_snapshot().schema_version;

    let altered = catalog
        .commit(move |tx| tx.alter_view(v.id, "duckdb", "SELECT 2"))
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.view_by_id(v.id).unwrap().sql, "SELECT 2");
    // The old definition is still visible at the pre-alter snapshot.
    let past = catalog.snapshot_at(created).await.unwrap();
    assert_eq!(past.view_by_id(v.id).unwrap().sql, "SELECT 1");
    // View DDL is schema-changing.
    assert_eq!(
        head.current_snapshot().schema_version,
        version_after_create + 1
    );

    catalog.commit(move |tx| tx.drop_view(v.id)).await.unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert!(head.view_by_id(v.id).is_none());
    assert!(head.views_in(s.id).is_empty());
    let past = catalog.snapshot_at(altered).await.unwrap();
    assert_eq!(past.view_by_id(v.id).unwrap().sql, "SELECT 2");
    assert_eq!(
        head.current_snapshot().schema_version,
        version_after_create + 2
    );
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn options_commit_without_minting_snapshots() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "t", &[col("a")])?;
            Ok(())
        })
        .await
        .unwrap();

    // Options-only: no snapshot minted, head unchanged.
    let id = catalog
        .commit(|tx| tx.set_option(OptionScope::Global, "k", "v"))
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(1));
    let err = catalog.snapshot_at(SnapshotId::new(2)).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.option(OptionScope::Global, "k").as_deref(), Some("v"));

    // Unsetting in another options-only commit: still head 1, option gone.
    let id = catalog
        .commit(|tx| tx.unset_option(OptionScope::Global, "k"))
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(1));
    let head = catalog.snapshot().await.unwrap();
    assert!(head.option(OptionScope::Global, "k").is_none());

    // Mixed commit: option rides the snapshot-minting batch.
    let version_before = head.current_snapshot().schema_version;
    let id = catalog
        .commit(|tx| {
            let s = tx
                .schema_by_name("s")
                .ok_or_else(|| Error::NotFound("schema s".to_string()))?;
            tx.create_table(s.id, "t2", &[col("a")])?;
            tx.set_option(OptionScope::Global, "k2", "v2")
        })
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(2));
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Global, "k2").as_deref(),
        Some("v2")
    );
    assert_eq!(head.current_snapshot().schema_version, version_before + 1);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn option_resolution_reads_through_the_catalog() {
    let catalog = open_memory().await;
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "t", &[col("a")])?;
            let other = tx.create_schema("other")?;
            tx.create_table(other, "u", &[col("a")])?;
            tx.set_option(OptionScope::Global, "k", "g")?;
            tx.set_option(OptionScope::Schema(s), "k", "s")
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    let s = head.schema_by_name("s").unwrap();
    let t = head.table_by_name(s.id, "t").unwrap();
    assert_eq!(
        head.option(OptionScope::Table(t.id), "k").as_deref(),
        Some("s")
    );
    assert_eq!(head.option(OptionScope::Global, "k").as_deref(), Some("g"));
    // A table in a schema with no override falls through to global.
    let other = head.schema_by_name("other").unwrap();
    let u = head.table_by_name(other.id, "u").unwrap();
    assert_eq!(
        head.option(OptionScope::Table(u.id), "k").as_deref(),
        Some("g")
    );

    // The schema's option record dies with the schema.
    catalog
        .commit(move |tx| {
            tx.drop_table(t.id)?;
            tx.drop_schema(s.id)
        })
        .await
        .unwrap();
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.option(OptionScope::Table(u.id), "k").as_deref(),
        Some("g")
    );
    assert!(head.schema_by_name("s").is_none());
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_option_writes_are_last_write_wins() {
    let catalog = open_memory().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(|tx| tx.set_option(OptionScope::Global, "k", "a"))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(|tx| tx.set_option(OptionScope::Global, "k", "b"))
            .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();
    let head = catalog.snapshot().await.unwrap();
    let value = head.option(OptionScope::Global, "k");
    assert!(matches!(value.as_deref(), Some("a" | "b")));
    // Neither write minted a snapshot.
    assert_eq!(head.current_snapshot().id, SnapshotId::new(0));
    catalog.close().await.unwrap();
}
