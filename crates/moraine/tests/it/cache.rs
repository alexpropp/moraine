//! The read cache through the public API: repeated reads must serve the
//! same catalog a cold read would build, whatever the cache did in between.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, SnapshotId};
use object_store::memory::InMemory;

use crate::{
    counting_store::CountingStore,
    fixtures::{col, datafile, seeded},
};

/// A read-only handle materializes the catalog **once** and serves every
/// later read from the cache.
///
/// This is the incident's shape as a test. A handle that rebuilds per read
/// returns the right answer every time and differs only in traffic, so the
/// assertion counts object-store reads rather than timing anything: after
/// the first view, repeated reads must cost a bounded handful of reads —
/// the head point read and its neighbours — not another scan of `current`.
#[tokio::test]
async fn a_read_only_handle_materializes_once() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..64 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // The first view is the materialization, and is allowed to read.
    let first = reader.snapshot().await.unwrap();
    let cold = counting.take_reads();
    assert!(cold > 0, "a cold view read nothing");

    // Every later view serves the same catalog for a small, constant cost.
    for _ in 0..8 {
        let view = reader.snapshot().await.unwrap();
        assert_eq!(view.schemas().len(), first.schemas().len());
    }
    let warm = counting.take_reads();

    assert!(
        warm < cold,
        "warm reads cost as much as the cold one ({warm} vs {cold}): the handle is \
         rebuilding the catalog per read"
    );
}

/// A whole-subspace scan reads ahead rather than paying a round trip per
/// block.
///
/// SlateDB's scan default is one block, fetched serially — invisible on
/// local storage and ruinous on remote, where a 12.8 MB subspace measured
/// 276 s at ~46 KB/s, which is 3 200 sequential fetches and nothing else.
/// The assertion counts reads rather than timing them, because on an
/// in-memory store the defect costs nothing observable.
#[tokio::test]
async fn a_materialization_reads_ahead_rather_than_block_by_block() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // Enough live rows that `current` spans many blocks: a scan that
    // fetches one at a time issues an order more reads than one that does
    // not, whatever the block size turns out to be.
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..4_000 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    counting.take_reads();

    let view = reader.snapshot().await.unwrap();
    let reads = counting.take_reads();
    let files = view
        .tables_in(view.schemas()[0].id)
        .first()
        .map(|table| view.data_files_of(table.id).len())
        .unwrap_or_default();

    assert!(files >= 4_000, "seed did not land: {files} files");
    // One round trip per 4 KiB block would be in the thousands here. The
    // bound is deliberately loose: it catches the defect's order of
    // magnitude without pinning SlateDB's block size or layout.
    // Measured: 5 reads with read-ahead, 89 without, on this seed. The
    // bound sits between them with headroom, so it catches the defect's
    // order of magnitude without pinning SlateDB's block size or layout.
    assert!(
        reads < 20,
        "materialization issued {reads} reads for {files} files — scanning block by block"
    );
}

/// A read-only handle scans once for a whole population of DuckLake's
/// metadata tables, not once per `dump_*` call.
///
/// DuckLake issues roughly two dozen dumps to populate its metadata, and
/// each one used to rescan `current` *and* `history` on a reader — the
/// entity projection was gated on holding the writer. That is the cost a
/// query pays on every execution, not just at attach.
#[tokio::test]
async fn a_read_only_handle_scans_once_for_many_dumps() {
    let object_store = Arc::new(InMemory::new());
    let writer = Catalog::open(
        Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();
    writer
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap").id;
            let table = tx.create_table(schema, "t", &[col("a")])?;
            for _ in 0..2_000 {
                tx.register_data_file(table, datafile(100), &[])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let counting = Arc::new(CountingStore::new(object_store));
    let reader = Catalog::open_read_only(
        Arc::clone(&counting) as Arc<dyn object_store::ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    // The first dump scans; it is the one that installs the projection.
    let first = moraine::ffi_support::dump_data_files(&reader)
        .await
        .unwrap();
    let cold = counting.take_reads();
    assert!(!first.is_empty(), "seed did not land");
    assert!(cold > 0, "a cold dump read nothing");

    // A population's worth of further dumps must not rescan.
    for _ in 0..12 {
        let again = moraine::ffi_support::dump_data_files(&reader)
            .await
            .unwrap();
        assert_eq!(again.len(), first.len());
    }
    let warm = counting.take_reads();

    assert!(
        warm < cold,
        "twelve further dumps cost {warm} reads against {cold} for one: the reader is \
         rescanning per dump"
    );
}

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
