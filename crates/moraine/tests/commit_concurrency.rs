//! Concurrent-commit behavior through the public API: benign races
//! retry internally; true conflicts surface typed.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, Error, SchemaId, TableId};
use object_store::memory::InMemory;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
    }
}

fn datafile(rows: u64) -> DataFile {
    DataFile {
        path: format!("data-{rows}.parquet"),
        path_is_relative: true,
        file_format: "parquet".into(),
        record_count: rows,
        file_size_bytes: rows * 10,
        footer_size: 4,
        encryption_key: None,
        column_stats: vec![],
    }
}

/// Opens a catalog pre-seeded with two tables in one schema.
///
/// Test-only helper: `unwrap_used` is a library-code lint, not exempted
/// automatically for a plain (non-`#[test]`) function even in an
/// integration-test crate.
#[allow(clippy::unwrap_used)]
async fn seeded() -> (Catalog, SchemaId, TableId, TableId) {
    let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "a", &[col("x")])?;
            tx.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = catalog.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap().id;
    let a = snapshot.table_by_name(s, "a").unwrap().id;
    let b = snapshot.table_by_name(s, "b").unwrap().id;
    (catalog, s, a, b)
}

#[tokio::test(flavor = "multi_thread")]
async fn disjoint_table_ddl_both_succeed() {
    let (catalog, _s, a, b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(move |tx| tx.add_column(a, &col("a1")).map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(move |tx| tx.add_column(b, &col("b1")).map(|_| ()))
            .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.columns_of(a).len(), 2);
    assert_eq!(head.columns_of(b).len(), 2);
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_table_ddl_races_serialize_or_conflict() {
    let (catalog, _s, a, _b) = seeded().await;
    // Whether a round serializes or conflicts depends on scheduling; the
    // classifier itself is pinned by the `transaction::operations` unit tests. This test
    // only requires that races never corrupt state.
    let mut added = Vec::new();
    for round in 0..20 {
        let c1 = catalog.clone();
        let c2 = catalog.clone();
        let n1 = format!("c1_{round}");
        let n2 = format!("c2_{round}");
        let t1 = tokio::spawn({
            let n1 = n1.clone();
            async move {
                c1.commit(move |tx| tx.add_column(a, &col(&n1)).map(|_| ()))
                    .await
            }
        });
        let t2 = tokio::spawn({
            let n2 = n2.clone();
            async move {
                c2.commit(move |tx| tx.add_column(a, &col(&n2)).map(|_| ()))
                    .await
            }
        });
        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();
        for (r, name) in [(r1, n1), (r2, n2)] {
            match r {
                Ok(_) => added.push(name),
                Err(Error::CommitConflict(_)) => {}
                Err(other) => panic!("unexpected error: {other}"),
            }
        }
    }
    let head = catalog.snapshot().await.unwrap();
    let names: std::collections::HashSet<String> =
        head.columns_of(a).into_iter().map(|c| c.name).collect();
    for name in &added {
        assert!(
            names.contains(name),
            "successfully committed column {name} missing from table a"
        );
    }
    assert_eq!(
        names.len(),
        added.len() + 1,
        "every successful add present exactly once, plus the seeded column"
    );
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_name_create_race_yields_one_table() {
    let (catalog, s, _a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(move |tx| tx.create_table(s, "orders", &[col("x")]).map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(move |tx| tx.create_table(s, "orders", &[col("x")]).map(|_| ()))
            .await
    });
    let results = [t1.await.unwrap(), t2.await.unwrap()];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(oks, 1);
    assert!(
        results
            .iter()
            .any(|r| matches!(r, Err(Error::AlreadyExists(_) | Error::CommitConflict(_))))
    );
    // Exactly one live table named "orders" either way.
    let head = catalog.snapshot().await.unwrap();
    let orders: Vec<_> = head
        .tables_in(s)
        .into_iter()
        .filter(|t| t.name == "orders")
        .collect();
    assert_eq!(orders.len(), 1);
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn counters_never_regress_or_collide_under_concurrency() {
    let (catalog, s, _a, _b) = seeded().await;
    let mut handles = Vec::new();
    for i in 0..8 {
        let c = catalog.clone();
        let name = format!("t{i}");
        handles.push(tokio::spawn(async move {
            c.commit(move |tx| tx.create_table(s, &name, &[col("x")]).map(|_| ()))
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    let head = catalog.snapshot().await.unwrap();
    let mut ids: Vec<u64> = head.tables_in(s).iter().map(|t| t.id.get()).collect();
    ids.sort_unstable();
    let before = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), before, "table ids must be unique");
    assert_eq!(before, 10, "2 seeded + 8 concurrent");
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn same_table_appends_both_land_with_dense_row_ids() {
    let (catalog, _s, a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(move |tx| tx.register_data_file(a, datafile(100)).map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(move |tx| tx.register_data_file(a, datafile(50)).map(|_| ()))
            .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let head = catalog.snapshot().await.unwrap();
    let files = head.data_files_of(a);
    assert_eq!(files.len(), 2);
    let mut starts: Vec<(u64, u64)> = files
        .iter()
        .map(|f| {
            (
                f.row_id_start.expect("verb-registered files carry a start"),
                f.record_count,
            )
        })
        .collect();
    starts.sort_unstable();

    // Dense, disjoint ranges regardless of which commit won the race.
    assert_eq!(starts[0].0, 0);
    assert_eq!(starts[1].0, starts[0].1);
    assert_eq!(head.table_stats(a).unwrap().next_row_id, 150);
    catalog.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn append_vs_drop_is_a_real_error() {
    let (catalog, s, a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(move |tx| tx.register_data_file(a, datafile(10)).map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move { c2.commit(move |tx| tx.drop_table(a)).await });
    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();

    // Serialized is fine; a genuine race surfaces CommitConflict or
    // NotFound on the loser.
    for r in [&r1, &r2] {
        match r {
            Ok(_) | Err(Error::CommitConflict(_) | Error::NotFound(_)) => {}
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    // Whatever the outcome mix, the head must be consistent with it.
    let head = catalog.snapshot().await.unwrap();
    let live = head.tables_in(s).into_iter().any(|t| t.id == a);
    if r2.is_ok() {
        assert!(!live, "drop committed, so the table must be gone");
    } else {
        assert!(live, "drop failed, so the table must survive");
        if r1.is_ok() {
            assert_eq!(head.data_files_of(a).len(), 1);
        }
    }
    catalog.close().await.unwrap();
}

/// An options-only commit re-validates its scope against a racing drop:
/// whichever order the two land in, a dropped table never keeps a live
/// option record (which nothing could ever remove again).
#[tokio::test(flavor = "multi_thread")]
async fn option_set_vs_drop_leaves_no_orphaned_option() {
    use moraine::OptionScope;

    for _ in 0..10 {
        let (catalog, s, a, _b) = seeded().await;
        let c1 = catalog.clone();
        let c2 = catalog.clone();
        let t1 = tokio::spawn(async move {
            c1.commit(move |tx| tx.set_option(OptionScope::Table(a), "k", "v"))
                .await
        });
        let t2 = tokio::spawn(async move { c2.commit(move |tx| tx.drop_table(a)).await });
        let set = t1.await.unwrap();
        let dropped = t2.await.unwrap();

        for r in [&set, &dropped] {
            match r {
                Ok(_) | Err(Error::CommitConflict(_) | Error::NotFound(_)) => {}
                Err(other) => panic!("unexpected error: {other}"),
            }
        }

        let head = catalog.snapshot().await.unwrap();
        let live = head.tables_in(s).into_iter().any(|t| t.id == a);
        if !live {
            assert_eq!(
                head.option(OptionScope::Table(a), "k"),
                None,
                "dropped table kept an orphaned option record"
            );
        }
        catalog.close().await.unwrap();
    }
}
