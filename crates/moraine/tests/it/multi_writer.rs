use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use moraine::{
    Catalog, CatalogOptions, ColumnId, Error, FileIndexEntry, IndexDef, IndexKeyValue, IntWidth,
    MaintenanceRequest, SnapshotId,
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use crate::fixtures::{CountingStore, col, datafile};

fn multi_writer_options() -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.multi_writer = true;
    options
}

#[allow(clippy::unwrap_used)]
async fn open_multi_writer(store: &Arc<InMemory>) -> Catalog {
    Catalog::open(
        store.clone() as Arc<dyn ObjectStore>,
        multi_writer_options(),
    )
    .await
    .unwrap()
}

#[allow(clippy::unwrap_used)]
async fn open_multi_writer_over(store: Arc<dyn ObjectStore>, options: CatalogOptions) -> Catalog {
    Catalog::open(store, options).await.unwrap()
}

/// How many objects sit under `prefix`.
async fn objects_under(store: &Arc<InMemory>, prefix: &str) -> usize {
    let mut listing = store.list(Some(&Path::from(prefix)));
    let mut count = 0;
    while listing.next().await.is_some() {
        count += 1;
    }
    count
}

/// Maintenance is available on a slot-backed catalog rather than refused like
/// a read-only attach: it runs under the folder role. A fresh catalog has
/// nothing to reclaim, and a concurrent commit is unaffected by the pass.
#[tokio::test(flavor = "multi_thread")]
async fn maintain_is_available_on_a_multi_writer_catalog() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;

    let (report, committed) = tokio::join!(
        catalog.maintain(MaintenanceRequest::default()),
        catalog.commit(|tx| tx.create_schema("live").map(|_| ())),
    );
    let report = report.unwrap();
    assert_eq!(report.index_entries_reclaimed, 0);
    assert_eq!(report.indexes_swept, 0);
    assert_eq!(committed.unwrap(), SnapshotId::new(1));
}

#[tokio::test]
async fn multi_writer_open_bootstraps_and_serves_the_empty_catalog() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id, moraine::SnapshotId::new(0));
    // A second full open finds the initialized store rather than
    // re-bootstrapping, and does not fence anything (there is no writer).
    let second = open_multi_writer(&store).await;
    second.snapshot().await.unwrap();
    catalog.snapshot().await.unwrap();
}

/// Time travel over a slot-backed attach: the bootstrap snapshot resolves
/// from the folded store, and a snapshot no slot has minted does not.
#[tokio::test]
async fn multi_writer_time_travel_spans_the_folded_head() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;

    let bootstrapped = catalog
        .snapshot_at(moraine::SnapshotId::new(0))
        .await
        .unwrap();
    assert_eq!(bootstrapped.schemas().len(), 1);

    let err = catalog
        .snapshot_at(moraine::SnapshotId::new(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
}

/// A prefix holding objects but no readable manifest is a damaged store, not
/// a fresh one: the attach refuses instead of stamping a new catalog over
/// whatever is there.
#[tokio::test]
async fn multi_writer_open_refuses_a_store_it_cannot_read_but_is_not_empty() {
    let store = Arc::new(InMemory::new());
    store
        .put(&Path::from("cat/leftover"), "not a slatedb object".into())
        .await
        .unwrap();

    let mut options = multi_writer_options();
    options.path = "cat".to_string();
    let err = Catalog::open(store.clone() as Arc<dyn ObjectStore>, options)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Store(_)), "got {err:?}");

    // The refusal wrote nothing: the planted object is still all there is.
    assert_eq!(objects_under(&store, "cat").await, 1);
}

#[tokio::test]
async fn a_multi_writer_commit_lands_in_a_slot_and_is_readable_unfolded() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    let id = catalog
        .commit(|tx| tx.create_schema("analytics").map(|_| ()))
        .await
        .unwrap();
    assert_eq!(id, SnapshotId::new(1));

    // A second, independent attach sees it purely through tail replay.
    let other = open_multi_writer(&store).await;
    let snapshot = other.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("analytics").is_some());
}

/// Genuinely disjoint DDL — a column added to each of two different tables —
/// never conflicts, so the loser of a slot race rebases onto the winner and
/// both land in adjacent slots. (Two `create_schema`s would collide under the
/// coarse schema-list rule, so the disjoint case is DDL on distinct tables,
/// as `commit_concurrency::disjoint_table_ddl_both_succeed` establishes.)
#[tokio::test(flavor = "multi_thread")]
async fn disjoint_racing_commits_both_land_in_adjacent_slots() {
    let store = Arc::new(InMemory::new());
    let setup = open_multi_writer(&store).await;
    setup
        .commit(|tx| {
            let s = tx.create_schema("s")?;
            tx.create_table(s, "a", &[col("x")])?;
            tx.create_table(s, "b", &[col("x")])?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = setup.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("s").unwrap().id;
    let ta = snapshot.table_by_name(s, "a").unwrap().id;
    let tb = snapshot.table_by_name(s, "b").unwrap().id;

    let a = open_multi_writer(&store).await;
    let b = open_multi_writer(&store).await;
    let (ra, rb) = tokio::join!(
        a.commit(move |tx| tx.add_column(ta, &col("a1")).map(|_| ())),
        b.commit(move |tx| tx.add_column(tb, &col("b1")).map(|_| ())),
    );
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    assert_ne!(ra, rb, "dense, distinct snapshot ids");

    let head = a.snapshot().await.unwrap();
    assert!(head.columns_of(ta).iter().any(|c| c.name == "a1"));
    assert!(head.columns_of(tb).iter().any(|c| c.name == "b1"));
}

/// Both drop the same schema: one wins, the loser's re-validation fails typed
/// — either `CommitConflict` from classification, or the closure's own
/// `NotFound` once the winner's drop replays into the loser's head.
#[tokio::test(flavor = "multi_thread")]
async fn conflicting_racing_commits_surface_one_typed_conflict() {
    let store = Arc::new(InMemory::new());
    let a = open_multi_writer(&store).await;
    a.commit(|tx| tx.create_schema("doomed").map(|_| ()))
        .await
        .unwrap();
    let doomed = a
        .snapshot()
        .await
        .unwrap()
        .schema_by_name("doomed")
        .unwrap()
        .id;

    let b = open_multi_writer(&store).await;
    let (ra, rb) = tokio::join!(
        a.commit(move |tx| tx.drop_schema(doomed)),
        b.commit(move |tx| tx.drop_schema(doomed)),
    );
    assert_eq!(u8::from(ra.is_ok()) + u8::from(rb.is_ok()), 1);
    let loser = if ra.is_err() { ra } else { rb };
    assert!(
        matches!(loser, Err(Error::CommitConflict(_) | Error::NotFound(_))),
        "{loser:?}"
    );
}

/// Two commits insert the same unique value concurrently: exactly one lands,
/// and the loser is rejected by the uniqueness probe reading the winner's
/// unfolded entry through the overlay — proof the `Overlaid` probe sees tail
/// writes no folder has applied.
#[tokio::test(flavor = "multi_thread")]
async fn racing_unique_inserts_reject_the_duplicate_through_the_overlay() {
    let store = Arc::new(InMemory::new());
    let setup = open_multi_writer(&store).await;
    setup
        .commit(|tx| {
            let s = tx.schema_by_name("main").unwrap().id;
            let t = tx.create_table(s, "orders", &[col("k")])?;
            tx.create_index(
                t,
                &IndexDef {
                    name: "by_k".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: true,
                },
                &[],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let snapshot = setup.snapshot().await.unwrap();
    let s = snapshot.schema_by_name("main").unwrap().id;
    let t = snapshot.table_by_name(s, "orders").unwrap().id;
    let index = snapshot.indexes_of(t).remove(0).id;

    let insert = move |tx: &mut moraine::Transaction| {
        tx.register_data_file(
            t,
            datafile(1),
            &[FileIndexEntry {
                index,
                ordinal: 0,
                values: vec![Some(IndexKeyValue::Int {
                    value: 42,
                    width: IntWidth::I64,
                })],
            }],
        )
        .map(|_| ())
    };

    let a = open_multi_writer(&store).await;
    let b = open_multi_writer(&store).await;
    let (ra, rb) = tokio::join!(a.commit(insert), b.commit(insert));
    assert_eq!(u8::from(ra.is_ok()) + u8::from(rb.is_ok()), 1);

    let loser = if ra.is_err() { ra } else { rb };
    assert!(matches!(loser, Err(Error::Constraint(_))), "{loser:?}");
}

/// The task's reason to exist: many commits through one handle coalesce into
/// a handful of envelopes, so the slot PUTs stay far below the commit count.
/// Without the coalescer this is O(n^2) — each loser retries the next
/// sequence, and N commits cost ~N^2/2 PUTs.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_commits_coalesce_into_few_slots() {
    let store = Arc::new(CountingStore::new());
    let catalog = open_multi_writer_over(
        store.clone() as Arc<dyn ObjectStore>,
        multi_writer_options(),
    )
    .await;

    // Every PUT the open itself cost is already counted; the commits' cost is
    // the delta from here.
    let before = store.put_count();
    let commits = 50;
    let results = futures::future::join_all(
        (0..commits)
            .map(|i| catalog.commit(move |tx| tx.create_schema(&format!("s{i}")).map(|_| ()))),
    )
    .await;
    let commit_puts = store.put_count() - before;

    assert!(results.iter().all(Result::is_ok), "every commit lands");
    let snapshot = catalog.snapshot().await.unwrap();
    assert_eq!(
        snapshot.schemas().len(),
        commits + 1,
        "all visible, plus main"
    );

    // One envelope per batch, one PUT per envelope: a handful, never one per
    // commit and never the quadratic blow-up.
    assert!(
        commit_puts < commits as u64,
        "coalesced {commits} commits into {commit_puts} slot PUTs"
    );
}

/// The default (ZERO) window declines to *wait*: a lone commit issues its PUT
/// without sleeping for a batching window. Under a paused clock a fixed
/// batching delay would advance virtual time; an opportunistic batch does not.
#[tokio::test(start_paused = true)]
async fn an_uncontended_commit_waits_for_nothing() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;

    let started = tokio::time::Instant::now();
    catalog
        .commit(|tx| tx.create_schema("solo").map(|_| ()))
        .await
        .unwrap();

    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "the default window pays no batching delay"
    );
}

/// A member whose closure fails when re-run against the accumulating head is
/// dropped from the envelope with its own error; the rest of the batch
/// commits. Two callers create the same schema and a third creates a distinct
/// one: exactly one of the colliding pair succeeds, the unrelated member is
/// untouched, and both surviving schemas land. A window forces the three into
/// one batch so the intra-batch drop is what is exercised.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_batch_member_does_not_poison_its_batch() {
    let store = Arc::new(InMemory::new());
    let mut options = multi_writer_options();
    options.commit_batch_window = Duration::from_millis(80);
    let catalog = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options).await;

    let (dup_a, dup_b, other) = tokio::join!(
        catalog.commit(|tx| tx.create_schema("dup").map(|_| ())),
        catalog.commit(|tx| tx.create_schema("dup").map(|_| ())),
        catalog.commit(|tx| tx.create_schema("other").map(|_| ())),
    );

    // Exactly one of the colliding pair commits; the other gets its own error
    // (the closure's `AlreadyExists`, or a `CommitConflict` if the collision
    // landed as a lost slot race) — never a poisoned neighbour.
    assert_eq!(
        u8::from(dup_a.is_ok()) + u8::from(dup_b.is_ok()),
        1,
        "{dup_a:?} / {dup_b:?}"
    );
    let loser = if dup_a.is_err() { dup_a } else { dup_b };
    assert!(
        matches!(
            loser,
            Err(Error::AlreadyExists(_) | Error::CommitConflict(_))
        ),
        "{loser:?}"
    );
    assert!(
        other.is_ok(),
        "the unrelated member is untouched: {other:?}"
    );

    let snapshot = catalog.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("dup").is_some());
    assert!(snapshot.schema_by_name("other").is_some());
}

/// A cancelled participant — a caller whose `commit` future is dropped
/// mid-batch (an ordinary `timeout` or a lost `select!` branch) — must not
/// wedge the handle. The cancelled commit does not land, every other member of
/// the batch still commits, and a *subsequent* commit on the same handle
/// completes. That last assertion is the anti-wedge one: without the fix the
/// leader admits the cancelled member, awaits its reply forever, never clears
/// `driving`, and every later commit parks behind it. `start_paused` makes the
/// drop land while the leader still holds the batch open in its window; the
/// 5-second timeouts turn a wedge into a failure rather than a hung test.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_cancelled_participant_does_not_wedge_the_handle() {
    let store = Arc::new(InMemory::new());
    let mut options = multi_writer_options();
    options.commit_batch_window = Duration::from_millis(100);
    let catalog = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options).await;

    // The leader holds the batch open for its window, so the others queue.
    let leader = {
        let catalog = catalog.clone();
        tokio::spawn(async move {
            catalog
                .commit(|tx| tx.create_schema("leader").map(|_| ()))
                .await
        })
    };
    tokio::task::yield_now().await;

    let victim = {
        let catalog = catalog.clone();
        tokio::spawn(async move {
            catalog
                .commit(|tx| tx.create_schema("victim").map(|_| ()))
                .await
        })
    };
    let survivor = {
        let catalog = catalog.clone();
        tokio::spawn(async move {
            catalog
                .commit(|tx| tx.create_schema("survivor").map(|_| ()))
                .await
        })
    };
    tokio::task::yield_now().await;

    // Cancel the victim while it is queued in the batch.
    victim.abort();
    tokio::task::yield_now().await;
    assert!(victim.await.unwrap_err().is_cancelled());

    let leader_out = tokio::time::timeout(Duration::from_secs(5), leader)
        .await
        .expect("the leader must not wedge behind the cancelled participant")
        .expect("the leader task must not panic");
    let survivor_out = tokio::time::timeout(Duration::from_secs(5), survivor)
        .await
        .expect("the survivor must not wedge behind the cancelled participant")
        .expect("the survivor task must not panic");
    assert!(leader_out.is_ok(), "{leader_out:?}");
    assert!(survivor_out.is_ok(), "{survivor_out:?}");

    // The anti-wedge assertion: a fresh commit on the same handle still lands.
    let after = tokio::time::timeout(
        Duration::from_secs(5),
        catalog.commit(|tx| tx.create_schema("after").map(|_| ())),
    )
    .await
    .expect("the handle must not be wedged behind the cancelled commit")
    .expect("the follow-up commit must succeed");
    assert!(after > SnapshotId::new(0));

    let snapshot = catalog.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("leader").is_some());
    assert!(snapshot.schema_by_name("survivor").is_some());
    assert!(snapshot.schema_by_name("after").is_some());
    assert!(snapshot.schema_by_name("victim").is_none());
}

/// The leader-drop case: if the *leader's* own `commit` future is dropped
/// mid-batch, its followers must not be stranded and the handle must stay live.
/// The drop guard hands the baton to a waiting follower (promotes it to lead)
/// rather than leaving `driving` set behind a vanished leader. Without the fix
/// the follower parks on a `resume` that never comes and every later commit
/// parks behind a leaked `driving`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_cancelled_leader_hands_off_and_keeps_the_handle_live() {
    let store = Arc::new(InMemory::new());
    let mut options = multi_writer_options();
    options.commit_batch_window = Duration::from_millis(100);
    let catalog = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options).await;

    let leader = {
        let catalog = catalog.clone();
        tokio::spawn(async move {
            catalog
                .commit(|tx| tx.create_schema("leader").map(|_| ()))
                .await
        })
    };
    tokio::task::yield_now().await;

    let follower = {
        let catalog = catalog.clone();
        tokio::spawn(async move {
            catalog
                .commit(|tx| tx.create_schema("follower").map(|_| ()))
                .await
        })
    };
    tokio::task::yield_now().await;

    // Cancel the leader while it holds the batch open in its window.
    leader.abort();
    tokio::task::yield_now().await;
    assert!(leader.await.unwrap_err().is_cancelled());

    // The follower is promoted rather than stranded.
    let follower_out = tokio::time::timeout(Duration::from_secs(5), follower)
        .await
        .expect("the follower must not be stranded behind the cancelled leader")
        .expect("the follower task must not panic");
    assert!(follower_out.is_ok(), "{follower_out:?}");

    // And the handle stays live for new commits.
    let after = tokio::time::timeout(
        Duration::from_secs(5),
        catalog.commit(|tx| tx.create_schema("after").map(|_| ())),
    )
    .await
    .expect("the handle must not be wedged behind the cancelled leader")
    .expect("the follow-up commit must succeed");
    assert!(after > SnapshotId::new(0));

    let snapshot = catalog.snapshot().await.unwrap();
    assert!(snapshot.schema_by_name("follower").is_some());
    assert!(snapshot.schema_by_name("after").is_some());
    assert!(snapshot.schema_by_name("leader").is_none());
}
