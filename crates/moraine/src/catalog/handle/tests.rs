use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory};
use slatedb::IsolationLevel;

use super::*;
use crate::{
    catalog::IndexId,
    store::{
        handle::ReadHandle,
        index_encoding::{
            CanonicalKey, Direction, IndexKeyValue, IntWidth, NullOrder, encode_ordered_values,
        },
        key::{IndexKey, Key},
        open::StoreBuilder,
        read,
    },
    transaction::commit,
};

/// The stored structural format and fold cursor of a store (fold absent reads
/// as 0), read through a fresh reader.
async fn stored_format_and_fold(object_store: &Arc<dyn ObjectStore>) -> (u64, u64) {
    let reader = StoreBuilder::new("", Arc::clone(object_store))
        .open_reader()
        .await
        .unwrap();
    let handle = ReadHandle::Reader(&reader);
    let format = read::read_format(handle)
        .await
        .unwrap()
        .unwrap()
        .format_version;
    let fold = read::read_fold(handle)
        .await
        .unwrap()
        .map_or(0, |fold| fold.folded_sequence);
    reader.close().await.unwrap();
    (format, fold)
}

/// Creates a legacy (format 1) store carrying a `legacy` schema, as the
/// pre-flip single-writer binary left it, then closes its writer.
async fn legacy_store_with_schema(object_store: Arc<dyn ObjectStore>) {
    let legacy = Catalog::open_single_writer(object_store, CatalogOptions::default())
        .await
        .unwrap();
    legacy
        .commit(|tx| tx.create_schema("legacy").map(|_| ()))
        .await
        .unwrap();
    legacy.close().await.unwrap();
}

/// A legacy store migrates to the slot-log topology on its first read-write
/// attach, serving its pre-migration data; the new commit lands in a slot a
/// fresh reader replays, and reopening is an idempotent no-op migration.
#[tokio::test]
async fn a_legacy_store_migrates_on_first_write_attach_and_serves_its_data() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some(),
        "the pre-migration schema survives"
    );
    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_MULTI_WRITER, 0),
        "one atomic stamp: format 4 and fold 0"
    );

    catalog
        .commit(|tx| tx.create_schema("post").map(|_| ()))
        .await
        .unwrap();

    // The new commit rode a slot: a fresh reader replays it over the folded,
    // migrated store.
    let fresh = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    let view = fresh.snapshot().await.unwrap();
    assert!(view.schema_by_name("legacy").is_some());
    assert!(view.schema_by_name("post").is_some());

    // Reopening migrates nothing: still format 4, fold 0.
    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_MULTI_WRITER, 0),
    );
}

/// A read-only attach of a legacy store serves it unmigrated: it writes
/// nothing, and its absent fold cursor reads as 0 with an empty tail.
#[tokio::test]
async fn read_only_attach_of_a_legacy_store_serves_it_unmigrated() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    let reader = Catalog::open_read_only(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        reader
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );

    // Nothing was written: the store is still the legacy format.
    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_VERSION, 0),
    );
}

/// Two new-binary opens racing the same legacy store converge: at least one
/// migrates, a fenced migration re-probes and finds the store converted, and
/// both attach cleanly onto exactly format 4 / fold 0.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_migrations_converge_on_one_stamp() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    legacy_store_with_schema(Arc::clone(&object_store)).await;

    let (ra, rb) = tokio::join!(
        Catalog::open(Arc::clone(&object_store), CatalogOptions::default()),
        Catalog::open(Arc::clone(&object_store), CatalogOptions::default()),
    );
    let a = ra.expect("first open attaches");
    let b = rb.expect("second open attaches");

    assert_eq!(
        stored_format_and_fold(&object_store).await,
        (commit::FORMAT_MULTI_WRITER, 0),
    );
    assert!(
        a.snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );
    assert!(
        b.snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );
}

/// Migrating a legacy store fences an incumbent old-binary writer: the
/// migration opens its own writer, and the live legacy writer's next commit
/// fails typed — never corruption, never a silent success.
#[tokio::test]
async fn migration_fences_a_live_legacy_writer_with_a_typed_error() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let legacy = Catalog::open_single_writer(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    legacy
        .commit(|tx| tx.create_schema("legacy").map(|_| ()))
        .await
        .unwrap();

    let migrated = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    assert!(
        migrated
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("legacy")
            .is_some()
    );

    let err = legacy
        .commit(|tx| tx.create_schema("doomed").map(|_| ()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Fenced(_)), "{err:?}");
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
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
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
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
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
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
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

/// A staged index build over a slot-backed catalog drives to ready: its
/// definition and final flip ride the log, so a fresh attach replays the
/// finished index. An empty table exercises the plumbing without a backfill.
#[tokio::test]
async fn create_index_staged_lands_ready_over_the_slot_log() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
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

    let index = catalog
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
        .unwrap();

    // A fresh attach replays the finished index through the log.
    let other = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
        .await
        .unwrap();
    let info = other.snapshot().await.unwrap().indexes_of(table).remove(0);
    assert_eq!(info.id, index);
    assert_eq!(info.state, crate::catalog::IndexState::Ready);
}
