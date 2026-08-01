//! Group commit through the public API: several logical commits staged
//! into one batch and made durable by one flush.
//!
//! Each member is its own snapshot with its own id — a group is a batching
//! of commits, not a merging of them — and each stages against the state
//! the members before it left behind.

use std::sync::Arc;

use moraine::{Catalog, CatalogOptions, CommitMember, Error, Transaction};
use object_store::memory::InMemory;

use crate::{
    crash_recovery::freezing_store::FreezingStore,
    fixtures::{col, open_memory, seeded},
};

/// A catalog over a store that counts the writes it serves.
#[allow(clippy::unwrap_used)]
async fn counted() -> (Catalog, Arc<FreezingStore>) {
    let store = Arc::new(FreezingStore::thawed(Arc::new(InMemory::new())));
    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    (catalog, store)
}

/// The headline property: three logical commits cost the object-store
/// writes of one, because they share a batch and a flush.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_group_costs_one_flush_where_separate_commits_cost_one_each() {
    let (catalog, store) = counted().await;

    let before_separate = store.writes_attempted();
    for name in ["s1", "s2", "s3"] {
        catalog
            .commit(move |tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    let separate = store.writes_attempted() - before_separate;

    let before_group = store.writes_attempted();
    catalog
        .commit_group(&[
            &|tx: &mut Transaction| tx.create_schema("g1").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("g2").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("g3").map(|_| ()),
        ])
        .await
        .unwrap();
    let grouped = store.writes_attempted() - before_group;

    assert!(
        grouped > 0 && grouped < separate,
        "three grouped commits wrote {grouped} objects, three separate ones {separate}; \
         grouping is supposed to amortize the flush"
    );
    catalog.close().await.unwrap();
}

/// Every member mints its own snapshot, numbered consecutively from the
/// head the group started at, and the returned ids are in member order.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn every_member_mints_its_own_consecutive_snapshot() {
    let catalog = open_memory().await;
    let start = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();

    let ids = catalog
        .commit_group(&[
            &|tx: &mut Transaction| tx.create_schema("one").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("two").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("three").map(|_| ()),
        ])
        .await
        .unwrap();

    assert_eq!(
        ids.iter().map(|id| id.get()).collect::<Vec<_>>(),
        vec![start + 1, start + 2, start + 3]
    );
    let head = catalog.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id.get(), start + 3);
    for name in ["one", "two", "three"] {
        assert!(head.schema_by_name(name).is_some(), "{name} is missing");
    }
    catalog.close().await.unwrap();
}

/// A member stages against what the members before it left, not against
/// the head the group started at.
#[tokio::test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn a_member_sees_the_state_the_previous_member_committed() {
    let catalog = open_memory().await;

    catalog
        .commit_group(&[
            &|tx: &mut Transaction| {
                let schema = tx.create_schema("shop")?;
                tx.create_table(schema, "orders", &[col("id")]).map(|_| ())
            },
            &|tx: &mut Transaction| {
                let schema = tx
                    .schema_by_name("shop")
                    .expect("the first member's schema");
                let table = tx
                    .table_by_name(schema.id, "orders")
                    .expect("the first member's table");
                tx.add_column(table.id, &col("total")).map(|_| ())
            },
        ])
        .await
        .unwrap();

    let head = catalog.snapshot().await.unwrap();
    let schema = head.schema_by_name("shop").unwrap();
    let table = head.table_by_name(schema.id, "orders").unwrap();
    assert_eq!(head.columns_of(table.id).len(), 2);
    catalog.close().await.unwrap();
}

/// One batch, but not one snapshot: time travel resolves each member's id
/// to exactly the state that member left.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn time_travel_resolves_each_member_separately() {
    let catalog = open_memory().await;

    let ids = catalog
        .commit_group(&[
            &|tx: &mut Transaction| tx.create_schema("first").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("second").map(|_| ()),
        ])
        .await
        .unwrap();

    let after_first = catalog.snapshot_at(ids[0]).await.unwrap();
    assert!(after_first.schema_by_name("first").is_some());
    assert!(
        after_first.schema_by_name("second").is_none(),
        "the second member's schema must not exist at the first member's snapshot"
    );

    let after_second = catalog.snapshot_at(ids[1]).await.unwrap();
    assert!(after_second.schema_by_name("first").is_some());
    assert!(after_second.schema_by_name("second").is_some());
    catalog.close().await.unwrap();
}

/// A member that stages nothing mints nothing, exactly as a lone commit of
/// the same closure would: it reports the head standing at its turn.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_member_that_stages_nothing_mints_no_snapshot() {
    let catalog = open_memory().await;
    let start = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();

    let ids = catalog
        .commit_group(&[
            &|tx: &mut Transaction| tx.create_schema("before").map(|_| ()),
            &|_tx: &mut Transaction| Ok(()),
            &|tx: &mut Transaction| tx.create_schema("after").map(|_| ()),
        ])
        .await
        .unwrap();

    assert_eq!(
        ids.iter().map(|id| id.get()).collect::<Vec<_>>(),
        vec![start + 1, start + 1, start + 2],
        "an empty member reports the head standing at its turn"
    );
    catalog.close().await.unwrap();
}

/// A group of no members commits nothing and reports nothing.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn an_empty_group_commits_nothing() {
    let catalog = open_memory().await;
    let start = catalog.snapshot().await.unwrap().current_snapshot().id;

    let members: [CommitMember<'_>; 0] = [];
    assert!(catalog.commit_group(&members).await.unwrap().is_empty());

    assert_eq!(
        catalog.snapshot().await.unwrap().current_snapshot().id,
        start
    );
    catalog.close().await.unwrap();
}

/// A group is one batch, so a member that fails takes the whole group with
/// it — including the members that already staged. The failure here is one
/// only the folded premise can produce: the second member sees the first
/// member's schema and refuses the duplicate.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_failing_member_aborts_the_whole_group() {
    let catalog = open_memory().await;
    let start = catalog.snapshot().await.unwrap().current_snapshot().id;

    let err = catalog
        .commit_group(&[
            &|tx: &mut Transaction| tx.create_schema("dup").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("dup").map(|_| ()),
            &|tx: &mut Transaction| tx.create_schema("never").map(|_| ()),
        ])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "{err:?}");

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.current_snapshot().id,
        start,
        "an aborted group must leave the head where it was"
    );
    assert!(head.schema_by_name("dup").is_none());
    assert!(head.schema_by_name("never").is_none());
    catalog.close().await.unwrap();
}

/// A group of one is the ordinary commit: same snapshot, same effect.
#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_group_of_one_is_an_ordinary_commit() {
    let catalog = open_memory().await;
    let start = catalog
        .snapshot()
        .await
        .unwrap()
        .current_snapshot()
        .id
        .get();

    let ids = catalog
        .commit_group(&[&|tx: &mut Transaction| tx.create_schema("alone").map(|_| ())])
        .await
        .unwrap();

    assert_eq!(
        ids.iter().map(|id| id.get()).collect::<Vec<_>>(),
        vec![start + 1]
    );
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("alone")
            .is_some()
    );
    catalog.close().await.unwrap();
}

/// Grouping does not need asking for: concurrent commits meet in whichever
/// batch is forming, so a burst of them costs far fewer flushes than the
/// same commits one at a time — while each still mints its own snapshot.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::unwrap_used)]
async fn concurrent_commits_coalesce_without_being_asked() {
    const COMMITS: usize = 8;
    let (catalog, store) = counted().await;

    // The baseline: the same commits one at a time, each its own flush.
    let before_separate = store.writes_attempted();
    for round in 0..COMMITS {
        let name = format!("alone_{round}");
        catalog
            .commit(move |tx| tx.create_schema(&name).map(|_| ()))
            .await
            .unwrap();
    }
    let separate = store.writes_attempted() - before_separate;

    let before_together = store.writes_attempted();
    let mut running = Vec::new();
    for round in 0..COMMITS {
        let catalog = catalog.clone();
        let name = format!("together_{round}");
        running.push(tokio::spawn(async move {
            catalog
                .commit(move |tx| tx.create_schema(&name).map(|_| ()))
                .await
        }));
    }
    let mut ids = Vec::new();
    for commit in running {
        ids.push(commit.await.unwrap().unwrap().get());
    }
    let together = store.writes_attempted() - before_together;

    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids.len(),
        COMMITS,
        "every coalesced commit still mints a snapshot of its own"
    );
    assert!(
        together < separate,
        "{COMMITS} concurrent commits wrote {together} objects, the same {COMMITS} one at a \
         time wrote {separate}; concurrent commits are supposed to share batches"
    );
    catalog.close().await.unwrap();
}

/// A commit whose future is dropped — the shape a host interrupt takes —
/// must not leave the batch it was joining waiting for a member that will
/// never arrive. The deadline sweeps across the commit's life so the drop
/// lands before staging, during it, and while waiting for the flush.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::unwrap_used)]
async fn a_cancelled_commit_never_strands_the_batch_it_left() {
    let catalog = open_memory().await;

    for round in 0..16_u64 {
        let abandoning = catalog.clone();
        let doomed = format!("doomed_{round}");
        let giving_up = tokio::spawn(async move {
            let deadline = std::time::Duration::from_micros(round * 200 + 1);
            let _ = tokio::time::timeout(
                deadline,
                abandoning.commit(move |tx| tx.create_schema(&doomed).map(|_| ())),
            )
            .await;
        });

        let surviving = catalog.clone();
        let name = format!("survivor_{round}");
        let lands = tokio::spawn(async move {
            surviving
                .commit(move |tx| tx.create_schema(&name).map(|_| ()))
                .await
        });

        giving_up.await.unwrap();
        // A strand would hang here rather than fail, so it is bounded.
        let landed = tokio::time::timeout(std::time::Duration::from_secs(10), lands)
            .await
            .unwrap_or_else(|_| panic!("round {round}: a commit was stranded by a cancelled one"));
        landed.unwrap().unwrap();
    }

    let head = catalog.snapshot().await.unwrap();
    for round in 0..16 {
        assert!(
            head.schema_by_name(&format!("survivor_{round}")).is_some(),
            "round {round}'s surviving commit is missing"
        );
    }
    catalog.close().await.unwrap();
}

/// A group races a concurrent commit exactly as a lone commit does: a
/// disjoint loser re-runs every member against the winner's state and
/// lands, with no member left behind.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::unwrap_used)]
async fn a_group_losing_a_disjoint_race_retries_every_member() {
    let (catalog, _s, a, b) = seeded().await;
    let grouped = catalog.clone();
    let lone = catalog.clone();

    let group = tokio::spawn(async move {
        grouped
            .commit_group(&[
                &|tx: &mut Transaction| tx.add_column(a, &col("g1")).map(|_| ()),
                &|tx: &mut Transaction| tx.add_column(a, &col("g2")).map(|_| ()),
            ])
            .await
    });
    let single = tokio::spawn(async move {
        lone.commit(move |tx| tx.add_column(b, &col("s1")).map(|_| ()))
            .await
    });
    group.await.unwrap().unwrap();
    single.await.unwrap().unwrap();

    let head = catalog.snapshot().await.unwrap();
    assert_eq!(
        head.columns_of(a).len(),
        3,
        "both grouped members must land"
    );
    assert_eq!(head.columns_of(b).len(), 2);
    catalog.close().await.unwrap();
}
