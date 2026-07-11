//! Concurrent-commit behavior through the public API: benign races
//! retry internally; true conflicts surface typed.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, ColumnDef, Error, SchemaId, TableId};
use object_store::memory::InMemory;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: "BIGINT".into(),
        nulls_allowed: true,
        default_value: None,
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
        .commit(|txn| {
            let s = txn.create_schema("s")?;
            txn.create_table(s, "a", &[col("x")])?;
            txn.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snap = catalog.snapshot().await.unwrap();
    let s = snap.schema_by_name("s").unwrap().id;
    let a = snap.table_by_name(s, "a").unwrap().id;
    let b = snap.table_by_name(s, "b").unwrap().id;
    (catalog, s, a, b)
}

#[tokio::test]
async fn disjoint_table_ddl_both_succeed() {
    let (catalog, _s, a, b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(move |txn| txn.add_column(a, &col("a1")).map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(move |txn| txn.add_column(b, &col("b1")).map(|_| ()))
            .await
    });
    t1.await.unwrap().unwrap();
    t2.await.unwrap().unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.columns_of(a).len(), 2);
    assert_eq!(head.columns_of(b).len(), 2);
    catalog.close().await.unwrap();
}

#[tokio::test]
async fn same_table_ddl_races_serialize_or_conflict() {
    let (catalog, _s, a, _b) = seeded().await;
    // Race two column adds against the same table many times over. Which
    // way a given round resolves — both succeed (serialized by the
    // store's write-write detection) or one surfaces `CommitConflict` —
    // depends on scheduler interleaving and is not pinned here: the
    // deterministic classifier behavior (that two adds to the same table
    // conflict) is pinned by the `txn::ops` unit tests
    // (`overlapping_tables_conflict`). This test only requires that races
    // never corrupt state: every successful add lands exactly once, and
    // the table stays intact.
    let mut added = Vec::new();
    for round in 0..20 {
        let c1 = catalog.clone();
        let c2 = catalog.clone();
        let n1 = format!("c1_{round}");
        let n2 = format!("c2_{round}");
        let t1 = tokio::spawn({
            let n1 = n1.clone();
            async move {
                c1.commit(move |txn| txn.add_column(a, &col(&n1)).map(|_| ()))
                    .await
            }
        });
        let t2 = tokio::spawn({
            let n2 = n2.clone();
            async move {
                c2.commit(move |txn| txn.add_column(a, &col(&n2)).map(|_| ()))
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

#[tokio::test]
async fn same_name_create_race_yields_already_exists() {
    let (catalog, s, _a, _b) = seeded().await;
    let c1 = catalog.clone();
    let c2 = catalog.clone();
    let t1 = tokio::spawn(async move {
        c1.commit(move |txn| txn.create_table(s, "orders", &[col("x")]).map(|_| ()))
            .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(move |txn| txn.create_table(s, "orders", &[col("x")]).map(|_| ()))
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

#[tokio::test]
async fn counters_never_regress_or_collide_under_concurrency() {
    let (catalog, s, _a, _b) = seeded().await;
    let mut handles = Vec::new();
    for i in 0..8 {
        let c = catalog.clone();
        let name = format!("t{i}");
        handles.push(tokio::spawn(async move {
            c.commit(move |txn| txn.create_table(s, &name, &[col("x")]).map(|_| ()))
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
