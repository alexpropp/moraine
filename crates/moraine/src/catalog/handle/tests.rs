use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory};
use slatedb::IsolationLevel;

use super::*;
use crate::{
    catalog::IndexId,
    store::{
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, IntWidth, NullOrder, encode_ordered_values,
        },
        key::{IndexKey, Key},
        open::StoreBuilder,
    },
    transaction::commit,
};

fn multi_writer_options() -> CatalogOptions {
    CatalogOptions {
        multi_writer: true,
        // Flush continuously so a per-batch durable commit waits only on the
        // in-memory object store, not a 100ms flush timer.
        flush_interval: std::time::Duration::ZERO,
        ..CatalogOptions::default()
    }
}

/// A one-column ascending key over `value`.
fn value_key(value: u128) -> CanonicalKey {
    encode_ordered_values(
        &[Some(IndexKeyValue::UInt {
            value,
            width: IntWidth::I64,
        })],
        &[Direction::Ascending],
        &[NullOrder::Last],
    )
    .unwrap()
}

/// Bootstraps a fresh slot-backed store through the writer and closes it, so a
/// later attach opens a reader that already sees everything seeded below.
async fn bootstrap_multi(object_store: &Arc<dyn ObjectStore>) {
    let db = commit::open_initialized(
        StoreBuilder::new("", Arc::clone(object_store)),
        false,
        None,
        true,
    )
    .await
    .unwrap();
    db.close().await.unwrap();
}

/// Seeds `rows` non-unique entries of one index directly into the folded
/// store, as a completed fold would have left them. The index id is never made
/// live, so the entries are exactly what a dropped index orphans.
async fn seed_orphaned_entries(object_store: &Arc<dyn ObjectStore>, index_id: u64, rows: u64) {
    let db = StoreBuilder::new("", Arc::clone(object_store))
        .open_writer()
        .await
        .unwrap();
    let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for row in 0..rows {
        let key = Key::Index(IndexKey::Multi {
            index_id,
            key: value_key(u128::from(row)),
            row_id: row,
        })
        .encode();
        tx.put(key, Vec::new()).unwrap();
    }
    tx.commit_with_options(&commit::durable()).await.unwrap();
    db.close().await.unwrap();
}

/// Maintenance on a slot-backed catalog reclaims a dropped index's entries
/// through the folder role — the folder is the one process allowed to write
/// the store directly, and a sweep touches only ids no live index holds.
#[tokio::test]
async fn maintain_sweeps_orphaned_entries_on_a_slot_backed_catalog() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    bootstrap_multi(&object_store).await;
    seed_orphaned_entries(&object_store, 42, 5).await;
    let catalog = Catalog::open(Arc::clone(&object_store), multi_writer_options())
        .await
        .unwrap();

    let report = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(report.indexes_swept, 1);
    assert_eq!(report.index_entries_reclaimed, 5);

    // A second pass finds the range already empty.
    let again = catalog
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(again.index_entries_reclaimed, 0);
    assert_eq!(again.indexes_swept, 0);
}

/// `reclaim_index_entries` on a slot-backed catalog deletes a not-live index's
/// entries through the folder role, one bounded batch at a time.
#[tokio::test]
async fn reclaim_index_entries_runs_under_the_folder_role() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    bootstrap_multi(&object_store).await;
    seed_orphaned_entries(&object_store, 7, 5).await;
    let catalog = Catalog::open(Arc::clone(&object_store), multi_writer_options())
        .await
        .unwrap();

    let deleted = catalog
        .reclaim_index_entries(IndexId::new(7), 3)
        .await
        .unwrap();
    assert_eq!(deleted, 3);
    let rest = catalog
        .reclaim_index_entries(IndexId::new(7), 10)
        .await
        .unwrap();
    assert_eq!(rest, 2);
    let none = catalog
        .reclaim_index_entries(IndexId::new(7), 10)
        .await
        .unwrap();
    assert_eq!(none, 0);
}

/// The load-bearing property: a verb commit races the slot log while a
/// maintenance sweep holds the fenced folder writer, and lands unimpeded.
/// Commits never touch the folder writer — they race the object-store log and
/// read through the reader — so the folder is availability-optional and a
/// commit never waits on it. A small batch size makes the sweep span many
/// folder transactions, so the commit overlaps a live folder session.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_lands_unimpeded_during_a_maintenance_sweep() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    bootstrap_multi(&object_store).await;
    seed_orphaned_entries(&object_store, 42, 500).await;
    let catalog = Catalog::open(Arc::clone(&object_store), multi_writer_options())
        .await
        .unwrap();

    let (swept, committed) = tokio::join!(
        catalog.maintain(MaintenanceRequest {
            sweep_orphaned_index_entries: true,
            batch_size: 1,
        }),
        catalog.commit(|tx| tx.create_schema("live").map(|_| ())),
    );

    let report = swept.unwrap();
    assert_eq!(report.index_entries_reclaimed, 500);
    let id = committed.unwrap();
    assert_eq!(id, SnapshotId::new(1));

    let snapshot = catalog.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("live").is_some());
}

/// Staged index builds are not yet wired for the folder role, so a slot-backed
/// attach refuses them typed rather than routing entry batches through slots.
#[tokio::test]
async fn create_index_staged_refuses_on_a_slot_backed_catalog() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::open(Arc::clone(&object_store), multi_writer_options())
        .await
        .unwrap();
    let table = {
        let created = std::cell::Cell::new(None);
        catalog
            .commit(|tx| {
                let schema = tx.schema_by_name("main").unwrap().id;
                let t = tx.create_table(
                    schema,
                    "orders",
                    &[crate::catalog::ColumnDef {
                        name: "a".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: true,
                        default_value: None,
                    }],
                )?;
                created.set(Some(t));
                Ok(())
            })
            .await
            .unwrap();
        created.get().unwrap()
    };

    let err = catalog
        .create_index_staged(
            table,
            &crate::catalog::IndexDef {
                name: "by_a".into(),
                columns: vec![crate::catalog::ColumnId::new(1)],
                unique: false,
            },
            &[],
            None,
            "",
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Constraint(_)), "{err:?}");
}
