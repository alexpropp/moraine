use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use moraine::{
    Catalog, CatalogOptions, ColumnId, Error, FileIndexEntry, IndexDef, IndexKeyValue, IntWidth,
    MaintenanceRequest, OptionScope, SnapshotId,
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use crate::fixtures::{CountingStore, col, datafile};

#[allow(clippy::unwrap_used)]
async fn open_multi_writer(store: &Arc<InMemory>) -> Catalog {
    Catalog::open(
        store.clone() as Arc<dyn ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap()
}

#[allow(clippy::unwrap_used)]
async fn open_multi_writer_over(store: Arc<dyn ObjectStore>, options: CatalogOptions) -> Catalog {
    Catalog::open(store, options).await.unwrap()
}

/// Options whose zero refresh window makes every read revalidate its cached
/// head, so a peer's commit is seen without waiting out the window.
fn zero_refresh() -> CatalogOptions {
    let mut options = CatalogOptions::default();
    options.refresh_interval = Duration::ZERO;
    options
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

    let mut options = CatalogOptions::default();
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

    // A zero refresh window makes each read revalidate its cached head, so the
    // peer's commit is seen at once rather than after the window elapses.
    let a = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, zero_refresh()).await;
    let b = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, zero_refresh()).await;
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
        CatalogOptions::default(),
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
    let mut options = CatalogOptions::default();
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
    let mut options = CatalogOptions::default();
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
    let mut options = CatalogOptions::default();
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

/// A coalesced commit's ambiguous outcome resolves through the public
/// recovery surface: its transaction id, read back from the shared multi-commit
/// envelope, resolves to the snapshot it minted — the exactly-once answer a
/// crashed committer needs — while a transaction that never committed resolves
/// to a definitive `None`.
#[tokio::test(flavor = "multi_thread")]
async fn a_coalesced_commits_id_resolves_via_transaction_outcome() {
    use moraine_wal::SlotLog;
    use uuid::Uuid;

    let store = Arc::new(InMemory::new());
    let mut options = CatalogOptions::default();
    options.commit_batch_window = Duration::from_millis(80);
    let catalog = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options).await;

    // Three commits joined into one batch coalesce into a single shared
    // envelope.
    let (r0, r1, r2) = tokio::join!(
        catalog.commit(|tx| tx.create_schema("c0").map(|_| ())),
        catalog.commit(|tx| tx.create_schema("c1").map(|_| ())),
        catalog.commit(|tx| tx.create_schema("c2").map(|_| ())),
    );
    r0.unwrap();
    r1.unwrap();
    r2.unwrap();

    // The shared envelope carries every member's transaction id.
    let slots = SlotLog::new(store.clone() as Arc<dyn ObjectStore>, "");
    let envelope = slots.read_slot(1).await.unwrap().expect("the batch's slot");
    assert!(
        envelope.commits.len() > 1,
        "the commits coalesced into one envelope"
    );

    let member = &envelope.commits[1];
    let id = Uuid::from_bytes(member.transaction_id);
    let expected = SnapshotId::new(member.payload.validated_head + 1);
    let resolved = catalog
        .transaction_outcome(id, SnapshotId::new(0))
        .await
        .unwrap();
    assert_eq!(resolved, Some(expected));

    let unknown = catalog
        .transaction_outcome(Uuid::from_bytes([0xAB; 16]), SnapshotId::new(0))
        .await
        .unwrap();
    assert_eq!(unknown, None);
}

/// Once a read has materialized the head, further reads within the refresh
/// window serve the cached head: no `current`-scan GETs and no slot listing.
/// This is the whole point of the cache — a repeated `snapshot()` used to pay
/// a full materialization every time.
#[tokio::test]
async fn repeated_snapshots_within_the_window_serve_the_cached_head() {
    let store = Arc::new(CountingStore::new());
    let catalog = open_multi_writer_over(
        store.clone() as Arc<dyn ObjectStore>,
        CatalogOptions::default(),
    )
    .await;
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    // The first read materializes the head and warms the cache.
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );

    // Every subsequent read within the window is served from the cache: it
    // touches the store for neither a scan nor a freshness listing.
    let before = (store.get_count(), store.list_count());
    let second = catalog.snapshot().await.unwrap();
    let third = catalog.snapshot().await.unwrap();
    let (gets, lists) = (store.get_count() - before.0, store.list_count() - before.1);

    assert!(second.schema_by_name("sales").is_some());
    assert!(third.schema_by_name("sales").is_some());
    assert_eq!(gets, 0, "a cached read runs no current-scan GETs");
    assert_eq!(lists, 0, "a cached read runs no freshness listing");
}

/// A handle's own commit is visible to its next read regardless of the refresh
/// window: the commit updates the cache directly. Set an hour-long window so a
/// stale cache could never be revalidated by the clock; only the direct update
/// makes the write visible.
#[tokio::test]
async fn a_handles_own_commit_is_visible_within_the_window() {
    let store = Arc::new(InMemory::new());
    let mut options = CatalogOptions::default();
    options.refresh_interval = Duration::from_secs(3600);
    let catalog = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options).await;

    // Warm the cache at the bootstrap head.
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_none()
    );

    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    // The window has not elapsed, yet the handle sees its own write.
    assert!(
        catalog
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
}

/// A peer's commit becomes visible once the refresh window elapses: the read
/// lists from the cached frontier, finds the peer's slot, and absorbs it
/// incrementally. A zero window revalidates on every read.
#[tokio::test]
async fn a_peer_commit_becomes_visible_after_the_window() {
    let store = Arc::new(InMemory::new());
    let mut options = CatalogOptions::default();
    options.refresh_interval = Duration::ZERO;
    let a = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options.clone()).await;
    let b = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, options).await;

    // Warm A's cache before the peer commits.
    assert!(
        a.snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_none()
    );

    b.commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    // With no window, A's next read revalidates and absorbs the peer's slot.
    assert!(
        a.snapshot()
            .await
            .unwrap()
            .schema_by_name("sales")
            .is_some()
    );
}

/// A bounded fold drains the tail into the store and leaves the served state
/// byte-for-byte where it was: folding is invisible to readers. A second sprint
/// finds nothing left to fold.
#[tokio::test]
async fn fold_sprint_drains_the_tail_and_preserves_the_served_state() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    for name in ["a", "b", "c"] {
        catalog
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    let before = catalog.snapshot().await.unwrap();
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 3);

    let report = catalog.fold_sprint(u64::MAX).await.unwrap();
    assert_eq!(report.slots_folded, 3);
    assert_eq!(report.folded_through, 3);
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 0);

    // Folding is invisible to readers: same state, now served from the store.
    let after = open_multi_writer(&store).await.snapshot().await.unwrap();
    assert_eq!(after.current_snapshot().id, before.current_snapshot().id);
    assert!(after.schema_by_name("c").is_some());

    // Idempotent: a second sprint folds nothing.
    assert_eq!(catalog.fold_sprint(u64::MAX).await.unwrap().slots_folded, 0);
}

/// A partial sprint advances the durable cursor atomically with its applies, so
/// the next sprint resumes from it and never re-applies a folded slot.
#[tokio::test]
async fn an_interrupted_sprint_resumes_from_the_durable_cursor() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    for name in ["a", "b", "c", "d"] {
        catalog
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }

    catalog.fold_sprint(2).await.unwrap(); // partial: folds slots 1-2
    let report = catalog.fold_sprint(u64::MAX).await.unwrap();
    assert_eq!(report.slots_folded, 2, "resumes at 3, no re-apply");
    assert_eq!(report.folded_through, 4);
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 0);

    // The full head survives across the two sprints unchanged.
    let head = open_multi_writer(&store).await.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(4));
    for name in ["a", "b", "c", "d"] {
        assert!(head.schema_by_name(name).is_some(), "{name} survived");
    }
}

/// After a full sprint, a committed transaction still resolves to the snapshot
/// it minted — now from the folded snapshot record rather than the tail scan.
#[tokio::test]
async fn transaction_outcome_resolves_from_a_folded_snapshot_after_a_sprint() {
    use moraine_wal::SlotLog;
    use uuid::Uuid;

    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();

    let slots = SlotLog::new(store.clone() as Arc<dyn ObjectStore>, "");
    let envelope = slots
        .read_slot(1)
        .await
        .unwrap()
        .expect("the commit's slot");
    let id = Uuid::from_bytes(envelope.commits[0].transaction_id);

    catalog.fold_sprint(u64::MAX).await.unwrap();
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 0);

    let resolved = catalog
        .transaction_outcome(id, SnapshotId::new(0))
        .await
        .unwrap();
    assert_eq!(resolved, Some(SnapshotId::new(1)));
}

/// A read-only attach refuses every folder surface with a typed error: folding
/// is the writer's monopoly.
#[tokio::test]
async fn folder_surfaces_refuse_a_read_only_attach() {
    let store = Arc::new(InMemory::new());
    open_multi_writer(&store).await; // bootstrap so the read-only open succeeds

    let read_only = Catalog::open_read_only(
        store.clone() as Arc<dyn ObjectStore>,
        CatalogOptions::default(),
    )
    .await
    .unwrap();

    assert!(matches!(
        read_only.fold_sprint(u64::MAX).await,
        Err(Error::Constraint(_))
    ));
    assert!(matches!(
        read_only.unfolded_tail().await,
        Err(Error::Constraint(_))
    ));
    assert!(matches!(
        read_only.fold_if_stalled(0, Duration::ZERO, u64::MAX).await,
        Err(Error::Constraint(_))
    ));
}

/// The self-appointment rule wired over the real log: a stalled tail past the
/// threshold appoints this handle, which folds it; a tail at or below the
/// threshold appoints no one. A real (short) delay is used rather than a paused
/// clock, since folding drives a live SlateDB writer whose flush loop needs
/// real time to make progress.
#[tokio::test]
async fn fold_if_stalled_appoints_only_past_the_threshold() {
    let store = Arc::new(InMemory::new());
    let catalog =
        open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, zero_refresh()).await;
    for name in ["a", "b", "c"] {
        catalog
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }

    // A tail of 3 at or below the threshold appoints no folder.
    assert!(
        catalog
            .fold_if_stalled(3, Duration::from_millis(1), u64::MAX)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 3);

    // Past the threshold it appoints this handle, which folds the tail.
    let report = catalog
        .fold_if_stalled(2, Duration::from_millis(1), u64::MAX)
        .await
        .unwrap()
        .expect("a stalled tail appoints a folder");
    assert_eq!(report.slots_folded, 3);
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 0);
}

/// The full logical catalog, read through a fresh attach so the folded store
/// (not a lagging reader) is what the dumps reconstruct over. Every dump merges
/// the folded store with the unfolded tail, so this is invariant to how far a
/// fold has advanced — the byte-level statement of "the log is truth, the store
/// is a derived index."
#[allow(clippy::unwrap_used)]
async fn logical_dump(store: &Arc<InMemory>) -> Vec<String> {
    use moraine::ffi_support::{
        dump_columns, dump_data_files, dump_schemas, dump_snapshots, dump_tables,
    };
    let catalog = open_multi_writer(store).await;
    let dump = vec![
        format!("{:?}", dump_snapshots(&catalog).await.unwrap()),
        format!("{:?}", dump_schemas(&catalog).await.unwrap()),
        format!("{:?}", dump_tables(&catalog).await.unwrap()),
        format!("{:?}", dump_columns(&catalog).await.unwrap()),
        format!("{:?}", dump_data_files(&catalog).await.unwrap()),
    ];
    catalog.close().await.unwrap();
    dump
}

/// Step 4b — the design's central proof. For a store with k slots,
/// materializing after folding to **any** prefix `0..=k` yields byte-identical
/// catalog state. A varied workload (schema and table DDL, an append, an
/// options-only head-preserving commit, and an index definition) commits into k
/// slots; then each successive fold is dumped through a fresh attach and
/// compared to the fully-unfolded materialization. Any divergence would mean
/// the folded and replayed paths disagree — the one bug class that makes
/// folding observable.
#[tokio::test]
async fn folding_to_any_prefix_yields_byte_identical_catalog_state() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;

    // Sequential commits, one slot each: schema, table, a column, an append, an
    // options-only commit, and an index definition.
    catalog
        .commit(|tx| tx.create_schema("sales").map(|_| ()))
        .await
        .unwrap();
    let sales = catalog
        .snapshot()
        .await
        .unwrap()
        .schema_by_name("sales")
        .unwrap()
        .id;
    catalog
        .commit(move |tx| tx.create_table(sales, "orders", &[col("id")]).map(|_| ()))
        .await
        .unwrap();
    let orders = catalog
        .snapshot()
        .await
        .unwrap()
        .table_by_name(sales, "orders")
        .unwrap()
        .id;
    catalog
        .commit(move |tx| tx.add_column(orders, &col("amount")).map(|_| ()))
        .await
        .unwrap();
    catalog
        .commit(move |tx| tx.register_data_file(orders, datafile(1), &[]).map(|_| ()))
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.set_option(OptionScope::Global, "answer", "42"))
        .await
        .unwrap();
    catalog
        .commit(move |tx| {
            tx.create_index(
                orders,
                &IndexDef {
                    name: "by_id".into(),
                    columns: vec![ColumnId::new(1)],
                    unique: false,
                },
                &[],
            )
            .map(|_| ())
        })
        .await
        .unwrap();

    let k = catalog.unfolded_tail().await.unwrap();
    assert_eq!(k, 6, "the workload committed one slot each");

    // Prefix 0: the fully-unfolded materialization is the reference.
    let reference = logical_dump(&store).await;

    // Every prefix 1..=k must materialize byte-for-byte the same state.
    for prefix in 1..=k {
        catalog.fold_sprint(1).await.unwrap();
        assert_eq!(catalog.unfolded_tail().await.unwrap(), k - prefix);
        assert_eq!(
            logical_dump(&store).await,
            reference,
            "folding to prefix {prefix} diverged from the unfolded materialization"
        );
    }
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 0);

    // The materialized head, now fully folded, still carries the whole workload.
    let head = open_multi_writer(&store).await.snapshot().await.unwrap();
    let orders = head.table_by_name(sales, "orders").unwrap().id;
    assert!(head.schema_by_name("sales").is_some());
    assert!(head.columns_of(orders).iter().any(|c| c.name == "amount"));
    assert_eq!(head.data_files_of(orders).len(), 1);
    assert!(head.index_by_name(orders, "by_id").is_some());
    assert_eq!(
        head.option(OptionScope::Global, "answer").as_deref(),
        Some("42")
    );
}

/// A drop is fold-invisible: dropping a schema removes it from the head and
/// ends its history record, and folding those writes into the store reconciles
/// exactly — a fresh attach reading the folded store agrees with the unfolded
/// tail on both the head (the schema gone) and time travel (the schema live at
/// the snapshot before its drop).
#[tokio::test]
async fn folding_is_invisible_across_a_dropped_schema() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    catalog
        .commit(|tx| tx.create_schema("doomed").map(|_| ()))
        .await
        .unwrap();
    let doomed = catalog
        .snapshot()
        .await
        .unwrap()
        .schema_by_name("doomed")
        .unwrap()
        .id;
    catalog
        .commit(move |tx| tx.drop_schema(doomed))
        .await
        .unwrap();

    // Before folding: doomed gone from the head, live at snapshot 1 (the drop
    // is snapshot 2), read through the unfolded tail.
    let before = open_multi_writer(&store).await;
    assert!(
        before
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("doomed")
            .is_none()
    );
    assert!(
        before
            .snapshot_at(SnapshotId::new(1))
            .await
            .unwrap()
            .schema_by_name("doomed")
            .is_some()
    );

    catalog.fold_sprint(u64::MAX).await.unwrap();

    // After folding: identical semantics, now from the folded store's stored
    // end_snapshot plus lifecycle filtering.
    let after = open_multi_writer(&store).await;
    assert!(
        after
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("doomed")
            .is_none(),
        "the drop folded: doomed is gone from the head"
    );
    assert!(
        after
            .snapshot_at(SnapshotId::new(1))
            .await
            .unwrap()
            .schema_by_name("doomed")
            .is_some(),
        "the drop is fold-invisible: doomed is still live at snapshot 1"
    );
}

/// A pruned snapshot is time-travelable until a folder applies the prune to the
/// store; folding reconciles the divergence. An expiry deletes snapshot 1's
/// record in a head-preserving commit: the unfolded tail still reconstructs the
/// state as of snapshot 1, but once folded the store no longer holds that
/// record and time travel to it resolves to `NotFound`.
#[tokio::test]
async fn folding_reconciles_an_expired_snapshot_to_not_found() {
    use moraine::ffi_support::staged::{Cell, RowOperation, TableKind, staged_begin};

    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    catalog
        .commit(|tx| tx.create_schema("keep").map(|_| ()))
        .await
        .unwrap(); // snapshot 1
    catalog
        .commit(|tx| tx.create_schema("extra").map(|_| ()))
        .await
        .unwrap(); // snapshot 2

    // Snapshot 1 is time-travelable before the expiry.
    open_multi_writer(&store)
        .await
        .snapshot_at(SnapshotId::new(1))
        .await
        .unwrap();

    // A head-preserving maintenance commit expires snapshot 1's record.
    let mut expire = staged_begin(&catalog, None, String::new()).await.unwrap();
    expire.stage(RowOperation::Delete {
        table: TableKind::Snapshot,
        cells: vec![Cell::U64(1)],
    });
    expire.stage(RowOperation::Delete {
        table: TableKind::SnapshotChanges,
        cells: vec![Cell::U64(1)],
    });
    let head_after = expire.commit().await.unwrap();
    assert_eq!(
        head_after,
        SnapshotId::new(2),
        "expiry must not advance the head"
    );

    // Pruned but unfolded: the tail replay still reconstructs snapshot 1.
    open_multi_writer(&store)
        .await
        .snapshot_at(SnapshotId::new(1))
        .await
        .expect("a pruned-but-unfolded snapshot stays time-travelable");

    catalog.fold_sprint(u64::MAX).await.unwrap();
    assert_eq!(catalog.unfolded_tail().await.unwrap(), 0);

    // Folding reconciles: the store no longer holds snapshot 1, so time travel
    // to it resolves to NotFound — the folded/single-writer semantics.
    let err = open_multi_writer(&store)
        .await
        .snapshot_at(SnapshotId::new(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");

    // The surviving snapshot and its schemas are unaffected.
    let head = open_multi_writer(&store).await.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(2));
    assert!(head.schema_by_name("keep").is_some());
    assert!(head.schema_by_name("extra").is_some());
}

/// Truncation deletes only durably folded slots, and never so far that the
/// store stops materializing completely. Nothing folded means nothing deleted;
/// after a fold the count stays at or below the durable cursor (a flush or poll
/// interval can move it, so the exact count is not asserted), and the head
/// still carries the unfolded remainder.
#[tokio::test]
async fn truncation_removes_only_durably_folded_slots() {
    let store = Arc::new(InMemory::new());
    let catalog = open_multi_writer(&store).await;
    for name in ["a", "b", "c"] {
        catalog
            .commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    // Nothing folded: nothing may be deleted.
    assert_eq!(catalog.truncate_folded_slots().await.unwrap(), 0);

    catalog.fold_sprint(2).await.unwrap();
    let removed = catalog.truncate_folded_slots().await.unwrap();
    assert!(removed <= 2, "never past the durable cursor, got {removed}");

    // The store still materializes completely: folded state plus slot 3.
    let fresh = open_multi_writer(&store).await.snapshot().await.unwrap();
    assert!(fresh.schema_by_name("c").is_some());
    assert_eq!(fresh.current_snapshot().id, SnapshotId::new(3));
}

/// The healthy-fleet case bound 2 exists for. A reader opened before a peer
/// folds and truncates lags the fold, so the slots it still must replay are the
/// only copies of those commits it can see. Bound 2 keeps them: the stale
/// reader materializes the full catalog — no hole, no `Corruption`, no missing
/// commit. A fold-cursor-only horizon would delete them and this would fail.
#[tokio::test]
async fn a_stale_reader_survives_a_peer_fold_and_truncation() {
    let store = Arc::new(InMemory::new());

    // B's reader will not poll the fold for the test's duration, and its head
    // cache stays cold so the first read materializes straight from the tail.
    let mut stale = CatalogOptions::default();
    stale.refresh_interval = Duration::from_secs(3600);
    let b = open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, stale).await;

    let a = open_multi_writer(&store).await;
    for name in ["a", "b", "c", "d"] {
        a.commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    a.fold_sprint(u64::MAX).await.unwrap();
    a.truncate_folded_slots().await.unwrap();

    // B still sits at the bootstrap manifest, yet reaches the full head: bound 2
    // kept every slot B has not folded past.
    let head = b.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(4));
    for name in ["a", "b", "c", "d"] {
        assert!(head.schema_by_name(name).is_some(), "{name} survived");
    }
}

/// The point-probe trap made concrete: a peer commits, folds, and truncates
/// between two of this handle's reads. The cached handle revalidates by LISTING
/// from its cached `next_sequence`, so it absorbs the peer's slots (or
/// re-materializes past a hole) and resolves the full head — never the stale
/// cached view a point probe on a now-absent frontier slot would serve. The
/// retention margin guarantees a truncated run always leaves newer slots
/// visible for that LIST to find.
#[tokio::test]
async fn a_cached_head_survives_a_peer_fold_and_truncation() {
    let store = Arc::new(InMemory::new());
    let cached =
        open_multi_writer_over(store.clone() as Arc<dyn ObjectStore>, zero_refresh()).await;
    let peer = open_multi_writer(&store).await;

    // Warm the cached handle at the frontier past its own commit.
    cached
        .commit(|tx| tx.create_schema("a").map(|_| ()))
        .await
        .unwrap();
    assert!(
        cached
            .snapshot()
            .await
            .unwrap()
            .schema_by_name("a")
            .is_some()
    );

    // The peer commits more, folds everything, and truncates.
    for name in ["b", "c"] {
        peer.commit(|tx| tx.create_schema(name).map(|_| ()))
            .await
            .unwrap();
    }
    peer.fold_sprint(u64::MAX).await.unwrap();
    peer.truncate_folded_slots().await.unwrap();

    // The cached handle's next read resolves the full head rather than the
    // stale cached view.
    let head = cached.snapshot().await.unwrap();
    assert_eq!(head.current_snapshot().id, SnapshotId::new(3));
    for name in ["a", "b", "c"] {
        assert!(head.schema_by_name(name).is_some(), "{name} resolved");
    }
}
