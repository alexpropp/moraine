//! What survives a process death: the places a crash can interrupt a
//! state-changing operation, and what must hold once a fresh handle opens
//! the store again.
//!
//! "Reopen" always means constructing a new catalog handle over the same
//! object store, with no in-memory carryover from the process that died.
//! Cases run against real SlateDB on in-memory `object_store`.

mod freezing_store;

use std::sync::Arc;

use futures::TryStreamExt;
use moraine::{
    Catalog, CatalogOptions, ColumnId, Error, IndexDef, IndexEntry, IndexKeyValue, IndexState,
    IntWidth, MaintenanceRequest, TableId,
};
use object_store::{ObjectStore, memory::InMemory};

use crate::{
    crash_recovery::freezing_store::FreezingStore,
    fixtures::{col, datafile},
};

/// Where a crash can interrupt a state-changing operation. Each variant
/// names what the process was doing when it died; [`guarantee`] says which
/// guarantee makes that survivable, and its exhaustive match makes adding
/// a case without deciding one a compile error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashCase {
    /// Batch staged, its WAL flush never reached object storage.
    CommitNotDurable,
    /// Flush landed; the process died before the caller was told.
    CommitDurableNotAcknowledged,
    /// A drop ending many records at once, crashed at every WAL boundary.
    MultiTombstoneDrop,
    /// Several catalog commits staged into one batch.
    GroupCommit,
    /// A writer died mid-commit and another took the store over.
    TakeoverMidCommit,
    /// The displaced writer woke and tried to commit anyway.
    FencedWriterResumes,
    /// An initializer died partway through creating the catalog.
    GenesisInterrupted,
    /// Two processes created the same empty catalog at once.
    ConcurrentGenesis,
    /// A staged index build died between two of its steps.
    StagedBuildInterrupted,
    /// A batched entry reclamation died between two of its batches.
    ReclamationInterrupted,
}

/// Every case.
const CASES: [CrashCase; 10] = [
    CrashCase::CommitNotDurable,
    CrashCase::CommitDurableNotAcknowledged,
    CrashCase::MultiTombstoneDrop,
    CrashCase::GroupCommit,
    CrashCase::TakeoverMidCommit,
    CrashCase::FencedWriterResumes,
    CrashCase::GenesisInterrupted,
    CrashCase::ConcurrentGenesis,
    CrashCase::StagedBuildInterrupted,
    CrashCase::ReclamationInterrupted,
];

/// Why a case is survivable. Which one applies follows from the path: a
/// path is either one batch or several, never both.
#[derive(Debug, PartialEq, Eq)]
enum Guarantee {
    /// One commit is one batch, so a crash leaves the whole commit or none
    /// of it. There is no torn intermediate to find — for the drop and
    /// genesis cases, proving that absence is the whole point.
    Atomicity,
    /// A long operation runs as several batches and is *not* atomic as a
    /// whole. Each batch is, and the operation persists how far it got, so
    /// a crash leaves a partial state that a re-run continues from —
    /// never one that restarts, double-counts, or serves reads early.
    Resumability,
}

fn guarantee(case: CrashCase) -> Guarantee {
    match case {
        CrashCase::CommitNotDurable
        | CrashCase::CommitDurableNotAcknowledged
        | CrashCase::MultiTombstoneDrop
        | CrashCase::GroupCommit
        | CrashCase::TakeoverMidCommit
        | CrashCase::FencedWriterResumes
        | CrashCase::GenesisInterrupted
        | CrashCase::ConcurrentGenesis => Guarantee::Atomicity,

        CrashCase::StagedBuildInterrupted | CrashCase::ReclamationInterrupted => {
            Guarantee::Resumability
        }
    }
}

/// Whether a case is driven by a test here.
#[derive(Debug, PartialEq, Eq)]
enum Coverage {
    /// A test below builds the pre-crash state, crashes, reopens, and
    /// asserts what must hold.
    Driven,
    /// Nothing can crash the operation at that point yet; carries what
    /// must be built first.
    Blocked(&'static str),
}

fn coverage(case: CrashCase) -> Coverage {
    match case {
        CrashCase::CommitNotDurable
        | CrashCase::CommitDurableNotAcknowledged
        | CrashCase::MultiTombstoneDrop
        | CrashCase::TakeoverMidCommit
        | CrashCase::FencedWriterResumes
        | CrashCase::GenesisInterrupted
        | CrashCase::ConcurrentGenesis
        | CrashCase::StagedBuildInterrupted
        | CrashCase::ReclamationInterrupted => Coverage::Driven,

        CrashCase::GroupCommit => {
            Coverage::Blocked("group commit is unbuilt: a batch of one is the only path today")
        }
    }
}

/// Pins which cases are driven and requires the rest to name what blocks
/// them, so a case cannot be quietly stubbed out. The exhaustive matches
/// in [`guarantee`] and [`coverage`] cover the other half: a new case
/// fails to compile until both decisions are made.
///
/// Every case is driven but one, and that one is blocked on a feature
/// rather than on the harness: group commit is unbuilt, so no batch
/// carries several commits for a crash to catch half-applied.
#[test]
fn every_case_declares_its_guarantee_and_coverage() {
    let driven: Vec<CrashCase> = CASES
        .into_iter()
        .filter(|case| coverage(*case) == Coverage::Driven)
        .collect();
    assert_eq!(
        driven,
        vec![
            CrashCase::CommitNotDurable,
            CrashCase::CommitDurableNotAcknowledged,
            CrashCase::MultiTombstoneDrop,
            CrashCase::TakeoverMidCommit,
            CrashCase::FencedWriterResumes,
            CrashCase::GenesisInterrupted,
            CrashCase::ConcurrentGenesis,
            CrashCase::StagedBuildInterrupted,
            CrashCase::ReclamationInterrupted,
        ],
        "driven cases changed; update this list as cases land"
    );
    for expected in [Guarantee::Atomicity, Guarantee::Resumability] {
        assert!(
            driven.iter().any(|case| guarantee(*case) == expected),
            "{expected:?} has no driven case; a guarantee nothing exercises is a claim, not a test"
        );
    }

    for case in CASES {
        if let Coverage::Blocked(reason) = coverage(case) {
            assert!(!reason.is_empty(), "{case:?} is blocked without a reason");
        }
    }
}

/// Every object in the store as sorted `(path, size)` pairs — what a
/// "the store is unchanged" assertion compares.
#[allow(clippy::unwrap_used)]
async fn inventory(store: &Arc<InMemory>) -> Vec<(String, u64)> {
    let mut objects: Vec<(String, u64)> = store
        .list(None)
        .map_ok(|meta| (meta.location.to_string(), meta.size))
        .try_collect()
        .await
        .unwrap();
    objects.sort();
    objects
}

/// `CommitDurableNotAcknowledged` — the flush landed, then the process
/// died before the caller heard back. The commit is durable, so a caller
/// that re-drives the same logical operation runs against the *advanced*
/// head: ids never collide, and a guarded operation surfaces its guard.
/// The scope is deliberate — a data-only re-drive is a fresh commit and
/// lands a second time, because nothing in the protocol dedups it.
#[tokio::test]
async fn durable_commit_survives_a_crash_before_its_acknowledgement() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());

    let writer = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let ids = std::cell::Cell::new(None);
    writer
        .commit(|tx| {
            let schema = tx.create_schema("sales")?;
            let table = tx.create_table(schema, "orders", &[col("id")])?;
            ids.set(Some((schema, table)));
            Ok(())
        })
        .await
        .unwrap();
    let (schema, table) = ids.get().unwrap();
    writer
        .commit(move |tx| tx.register_data_file(table, datafile(100), &[]).map(|_| ()))
        .await
        .unwrap();
    let head = writer.snapshot().await.unwrap().current_snapshot().id;

    // Death between the durable flush and the caller's ack: drop the
    // handle without closing it, so nothing is flushed on the way out.
    drop(writer);

    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let recovered = reopened.snapshot().await.unwrap();
    assert_eq!(
        recovered.current_snapshot().id,
        head,
        "the landed commit is durable across the crash"
    );
    assert!(
        recovered.schema_by_name("sales").is_some(),
        "and the snapshot resolves fully, not partially"
    );
    assert_eq!(recovered.data_files_of(table).len(), 1);

    // The caller re-drives the guarded operation it never got an ack for:
    // the guard surfaces instead of a duplicate or a corrupted commit.
    let err = reopened
        .commit(move |tx| tx.create_table(schema, "orders", &[col("id")]).map(|_| ()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "got {err:?}");

    // A data-only re-drive is a fresh commit and lands a second time. The
    // duplicate is the caller's to prevent; what matters here is that it
    // lands *cleanly* — row id ranges stay dense and disjoint rather than
    // colliding with the pre-crash commit's.
    reopened
        .commit(move |tx| tx.register_data_file(table, datafile(100), &[]).map(|_| ()))
        .await
        .unwrap();
    let after = reopened.snapshot().await.unwrap();
    let mut starts: Vec<u64> = after
        .data_files_of(table)
        .iter()
        .filter_map(|file| file.row_id_start)
        .collect();
    starts.sort_unstable();
    assert_eq!(
        starts,
        vec![0, 100],
        "counters advanced with the landed commit, so ids never collide"
    );
    reopened.close().await.unwrap();
}

/// `ConcurrentGenesis` — two processes create the same empty catalog at
/// once. There is no create-if-absent primitive to lean on; the guards are
/// the real ones: SlateDB's writer epoch across processes (the second
/// `Db::open` fences the first, so the fenced initializer's genesis batch
/// writes nothing) and write-write conflict detection on `sys/format`
/// within one writer. Exactly one wins, and reopen shows a single coherent
/// genesis — never a second `sys/format`, a divergent genesis snapshot, or
/// a conflicting head.
///
/// The race runs repeatedly because its two guards trip at different
/// points and one round samples only one of them.
///
/// The loser's *error shape* is deliberately not pinned. It is sometimes
/// [`Error::Fenced`] and sometimes an untyped [`Error::Store`] carrying
/// SlateDB's manifest-CAS collision, which "adopts it, or returns a typed
/// error" does not admit. Telling that collision from real corruption
/// needs a condition SlateDB keeps `pub(crate)`, so the taxonomy decision
/// is open; asserted below is the part that is guaranteed — the loser
/// never half-creates the catalog.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_genesis_leaves_exactly_one_catalog() {
    for round in 0..25 {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());

        let first = backing.clone();
        let second = backing.clone();
        let left =
            tokio::spawn(async move { Catalog::open(first, CatalogOptions::default()).await });
        let right =
            tokio::spawn(async move { Catalog::open(second, CatalogOptions::default()).await });
        let results = [left.await.unwrap(), right.await.unwrap()];

        assert!(
            results.iter().any(Result::is_ok),
            "round {round}: at least one initializer must win"
        );
        // A fresh store is not a true conflict, so the loser's failure is
        // benign. It must never be a *catalog* error: reporting corruption,
        // a duplicate, or a missing entity would mean genesis itself tore.
        for result in &results {
            match result {
                Ok(_) | Err(Error::Fenced(_) | Error::CommitConflict(_) | Error::Store(_)) => {}
                Err(other) => panic!("round {round}: the race tore the catalog: {other:?}"),
            }
        }

        // Reopen with no carryover from either initializer.
        drop(results);
        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let snapshot = reopened.snapshot().await.unwrap();
        assert_eq!(
            snapshot.current_snapshot().id.get(),
            0,
            "round {round}: genesis leaves head at snapshot 0; a second genesis would advance it"
        );
        let schemas = snapshot.schemas();
        assert_eq!(
            schemas.len(),
            1,
            "round {round}: exactly one bootstrap `main` schema, not one per initializer"
        );
        assert_eq!(schemas[0].name, "main");
        reopened.close().await.unwrap();
    }
}

/// `TakeoverMidCommit` — a second writer opens the store while the first
/// is live and takes it over. The takeover adds no torn state of its own:
/// it changes only *who* observes the all-or-none result. Here the first
/// writer's commit landed durably before the takeover, so the second must
/// read the advanced head and continue from it. (The unlanded branch is
/// `CommitNotDurable`, which needs the write freeze.)
#[tokio::test]
async fn takeover_reads_the_durable_head_and_continues_from_it() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());

    let first = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    first
        .commit(|tx| tx.create_schema("landed").map(|_| ()))
        .await
        .unwrap();
    let head_at_first = first.snapshot().await.unwrap().current_snapshot().id;

    // The second writer never saw the first's memory, only the store.
    let second = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let taken_over = second.snapshot().await.unwrap();
    assert_eq!(
        taken_over.current_snapshot().id,
        head_at_first,
        "a durable commit survives the takeover intact"
    );
    assert!(
        taken_over.schema_by_name("landed").is_some(),
        "the new writer sees the landed commit, not a partial view of it"
    );

    second
        .commit(|tx| tx.create_schema("after_takeover").map(|_| ()))
        .await
        .unwrap();
    let advanced = second.snapshot().await.unwrap();
    assert_eq!(
        advanced.current_snapshot().id.get(),
        head_at_first.get() + 1,
        "the new writer advances the inherited head by one"
    );
    assert!(advanced.schema_by_name("landed").is_some());
    assert!(advanced.schema_by_name("after_takeover").is_some());
    second.close().await.unwrap();
}

/// `FencedWriterResumes` — the displaced writer wakes and tries to commit
/// after the takeover bumped the writer epoch. Takeover is only safe if a
/// writer that resumes cannot still land its stale batch: it is fenced,
/// writes nothing, and leaves the store as the timeline in which it never
/// woke would have left it.
#[tokio::test]
async fn fenced_writer_that_resumes_writes_nothing() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());

    let displaced = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    displaced
        .commit(|tx| tx.create_schema("before_takeover").map(|_| ()))
        .await
        .unwrap();

    let successor = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let head_after_takeover = successor.snapshot().await.unwrap().current_snapshot().id;

    let before = inventory(&backing).await;

    // The displaced writer resumes and tries to commit against a premise
    // the takeover already invalidated.
    let err = displaced
        .commit(|tx| tx.create_schema("stale").map(|_| ()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Fenced(_)), "got {err:?}");

    assert_eq!(
        inventory(&backing).await,
        before,
        "a fenced writer must leave the store byte-identical"
    );

    let snapshot = successor.snapshot().await.unwrap();
    assert_eq!(snapshot.current_snapshot().id, head_after_takeover);
    assert!(
        snapshot.schema_by_name("stale").is_none(),
        "the fenced writer's commit must be invisible"
    );
    assert!(snapshot.schema_by_name("before_takeover").is_some());
    successor.close().await.unwrap();
}

/// A unique index over the table's one column.
fn index_def() -> IndexDef {
    IndexDef {
        name: "by_a".into(),
        columns: vec![ColumnId::new(1)],
        unique: true,
    }
}

/// The index key for `value`.
fn key(value: u64) -> IndexKeyValue {
    IndexKeyValue::Int {
        value: i128::from(value),
        width: IntWidth::I64,
    }
}

/// The backfill entry pairing row `row_id` with key `row_id`.
fn entry(row_id: u64) -> IndexEntry {
    IndexEntry {
        row_id,
        values: vec![Some(key(row_id))],
    }
}

/// A catalog on `backing` holding one empty table, ready to index.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn catalog_with_table(backing: &Arc<InMemory>) -> (Catalog, TableId) {
    let catalog = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a")])?;
            tx.register_data_file(table, datafile(7), &[])?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    (catalog, created.get().unwrap())
}

/// `StagedBuildInterrupted` — a staged index build runs as several
/// commits, so unlike a commit it *is* interruptible partway. What carries
/// it is the cursor persisted with each step: after a crash the index is
/// still building, serves no reads, and resumes from where it stopped
/// rather than restarting or double-counting the rows below the
/// watermark.
#[tokio::test]
async fn interrupted_index_build_resumes_from_its_persisted_cursor() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let (catalog, table) = catalog_with_table(&backing).await;

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            created.set(Some(tx.create_index_staged(table, &index_def())?));
            Ok(())
        })
        .await
        .unwrap();
    let index = created.get().unwrap();

    // One step lands durably, then the process dies mid-build.
    catalog
        .commit(move |tx| {
            tx.build_index_step(index, &[entry(0), entry(1), entry(2)], false)
                .map(|_| ())
        })
        .await
        .unwrap();
    drop(catalog);

    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let building = reopened
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(
        building.state,
        IndexState::Building,
        "a half-built index must not present itself as ready"
    );
    assert_eq!(
        building.build_cursor,
        Some(2),
        "the cursor is durable, so the resume knows where to start"
    );

    // Building means unavailable, not empty: serving the rows it happens
    // to have would be a silently partial answer.
    let err = reopened
        .index_lookup(table, index, &[key(0)])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::IndexBuilding(_)), "got {err:?}");

    // Resume past the cursor and finish.
    reopened
        .commit(move |tx| {
            tx.build_index_step(index, &[entry(3), entry(4), entry(5), entry(6)], true)
                .map(|_| ())
        })
        .await
        .unwrap();

    let finished = reopened
        .snapshot()
        .await
        .unwrap()
        .indexes_of(table)
        .remove(0);
    assert_eq!(finished.state, IndexState::Ready);
    for row_id in 0..7 {
        assert_eq!(
            reopened
                .index_lookup(table, index, &[key(row_id)])
                .await
                .unwrap()
                .len(),
            1,
            "row {row_id} is indexed exactly once across the crash"
        );
    }
    reopened.close().await.unwrap();
}

/// `ReclamationInterrupted` — reclaiming a dead index's entries runs one
/// batch per commit so a large sweep never holds the writer, which means a
/// crash can land between batches. The batches already committed stay
/// reclaimed, the rest survive to be found again, and a re-run converges
/// without re-reclaiming what is already gone.
#[tokio::test]
async fn interrupted_entry_reclamation_converges_on_re_run() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let (catalog, table) = catalog_with_table(&backing).await;

    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            created.set(Some(tx.create_index_staged(table, &index_def())?));
            Ok(())
        })
        .await
        .unwrap();
    let index = created.get().unwrap();
    let entries: Vec<IndexEntry> = (0..7).map(entry).collect();
    catalog
        .commit(move |tx| tx.build_index_step(index, &entries, true).map(|_| ()))
        .await
        .unwrap();
    catalog
        .commit(move |tx| tx.drop_index(index))
        .await
        .unwrap();

    // One batch of the sweep lands, then the process dies.
    let first = catalog.reclaim_index_entries(index, 3).await.unwrap();
    assert_eq!(first, 3, "the interrupted pass reclaimed one batch");
    drop(catalog);

    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let resumed = reopened
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(
        resumed.index_entries_reclaimed, 4,
        "the re-run finishes the remaining entries, never re-reclaiming the first batch"
    );
    assert_eq!(resumed.indexes_swept, 1);

    // Converged: nothing is left to do, and a further pass is a no-op.
    let again = reopened
        .maintain(MaintenanceRequest::default())
        .await
        .unwrap();
    assert_eq!(again.index_entries_reclaimed, 0);
    assert_eq!(again.indexes_swept, 0);

    // The sweep touched only the dead index's entries.
    let snapshot = reopened.snapshot().await.unwrap();
    assert_eq!(snapshot.data_files_of(table).len(), 1);
    assert!(snapshot.table_by_id(table).is_some());
    reopened.close().await.unwrap();
}

/// Asserts an operation stopped short of success once the store froze. It
/// may hang retrying — a dying process never learns its outcome either —
/// or surface the write failure; both are a crash. Only success is
/// disqualifying, because it would mean the batch became durable after
/// all and the case tested nothing.
#[allow(clippy::panic)]
fn assert_never_landed<T: std::fmt::Debug>(
    outcome: Result<Result<T, Error>, tokio::time::error::Elapsed>,
) {
    if let Ok(Ok(value)) = outcome {
        panic!("operation reported success ({value:?}) though its writes could not land");
    }
}

/// How long to let a doomed operation run before calling it a crash.
const UNTIL_CRASH: std::time::Duration = std::time::Duration::from_millis(300);

/// `CommitNotDurable` — the batch was staged but its WAL flush never
/// reached object storage. All-or-none resolves to none: the head does not
/// move, and a handle opened afterwards sees no trace of the commit.
#[tokio::test]
async fn commit_whose_wal_never_landed_is_invisible() {
    let backing: Arc<InMemory> = Arc::new(InMemory::new());
    let store = Arc::new(FreezingStore::thawed(backing.clone()));

    let catalog = Catalog::open(store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    catalog
        .commit(|tx| tx.create_schema("durable").map(|_| ()))
        .await
        .unwrap();
    let head_before = catalog.snapshot().await.unwrap().current_snapshot().id;

    // The process dies: the batch can still be staged in memory, but
    // nothing it writes will ever reach object storage.
    store.freeze_after(0);
    assert_never_landed(
        tokio::time::timeout(
            UNTIL_CRASH,
            catalog.commit(|tx| tx.create_schema("lost").map(|_| ())),
        )
        .await,
    );
    drop(catalog);

    // A fresh handle over the same bytes, with no freeze in the way.
    let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let recovered = reopened.snapshot().await.unwrap();
    assert_eq!(
        recovered.current_snapshot().id,
        head_before,
        "the head must not move for a commit that never became durable"
    );
    assert!(
        recovered.schema_by_name("lost").is_none(),
        "no record of the undurable commit may be visible"
    );
    assert!(
        recovered.schema_by_name("durable").is_some(),
        "the commit that did land is untouched"
    );
    reopened.close().await.unwrap();
}

/// Builds a table with three columns and two registered files, and
/// returns it with the number of writes the store has served so far.
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn table_to_drop(catalog: &Catalog) -> TableId {
    let created = std::cell::Cell::new(None);
    catalog
        .commit(|tx| {
            let schema = tx.schema_by_name("main").expect("bootstrap schema").id;
            let table = tx.create_table(schema, "orders", &[col("a"), col("b"), col("c")])?;
            tx.register_data_file(table, datafile(10), &[])?;
            tx.register_data_file(table, datafile(20), &[])?;
            created.set(Some(table));
            Ok(())
        })
        .await
        .unwrap();
    created.get().unwrap()
}

/// `MultiTombstoneDrop` — a drop ends the table row, all three columns and
/// both files at once. Its job is to prove there is no reachable point
/// "after the first tombstone, before the last": the whole drop is one
/// batch, so stopping the store at *every* write the drop issues must
/// still leave the table wholly present or wholly gone. It fails if a drop
/// is ever split across batches.
#[tokio::test]
async fn multi_tombstone_drop_is_never_observed_half_done() {
    // How many writes the drop issues, measured on an unfrozen run: the
    // boundaries the sweep below has to cover.
    let probe_backing: Arc<InMemory> = Arc::new(InMemory::new());
    let probe_store = Arc::new(FreezingStore::thawed(probe_backing.clone()));
    let probe = Catalog::open(probe_store.clone(), CatalogOptions::default())
        .await
        .unwrap();
    let table = table_to_drop(&probe).await;
    let before_drop = probe_store.writes_attempted();
    probe.commit(move |tx| tx.drop_table(table)).await.unwrap();
    let boundaries = probe_store.writes_attempted() - before_drop;
    probe.close().await.unwrap();
    assert!(
        boundaries > 0,
        "the drop must write something to be worth stopping"
    );

    let mut interrupted = 0;
    for stop_after in 0..=boundaries {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());
        let store = Arc::new(FreezingStore::thawed(backing.clone()));
        let catalog = Catalog::open(store.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let table = table_to_drop(&catalog).await;

        // Die partway through the drop, one write later each round. Once
        // the allowance covers every write the drop makes, it commits
        // normally — that is the end of the sweep, not a failure.
        store.freeze_after(i64::try_from(stop_after).unwrap());
        let outcome =
            tokio::time::timeout(UNTIL_CRASH, catalog.commit(move |tx| tx.drop_table(table))).await;
        if !matches!(outcome, Ok(Ok(_))) {
            interrupted += 1;
        }
        drop(catalog);

        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap();
        let recovered = reopened.snapshot().await.unwrap();
        if recovered.table_by_id(table).is_some() {
            assert_eq!(
                recovered.columns_of(table).len(),
                3,
                "stopping after {stop_after} writes left a table missing columns"
            );
            assert_eq!(
                recovered.data_files_of(table).len(),
                2,
                "stopping after {stop_after} writes left a table missing files"
            );
        } else {
            assert!(
                recovered.columns_of(table).is_empty(),
                "stopping after {stop_after} writes dropped a table but kept its columns"
            );
            assert!(
                recovered.data_files_of(table).is_empty(),
                "stopping after {stop_after} writes dropped a table but kept its files"
            );
        }
        reopened.close().await.unwrap();
    }
    assert!(
        interrupted > 0,
        "no round actually stopped the drop, so the sweep proved nothing"
    );
}

/// `GenesisInterrupted` — an initializer dies partway through creating the
/// catalog. Genesis is one batch like any commit, so stopping the store at
/// every write it issues must leave the store either untouched (the next
/// open creates it from scratch) or wholly created. A `sys/format` with no
/// head would survive validation on the next open and then fail to
/// materialize, so this sweep is what forces genesis to stay one batch.
#[tokio::test]
async fn interrupted_genesis_is_never_observed_half_created() {
    let probe_backing: Arc<InMemory> = Arc::new(InMemory::new());
    let probe_store = Arc::new(FreezingStore::thawed(probe_backing.clone()));
    Catalog::open(probe_store.clone(), CatalogOptions::default())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let boundaries = probe_store.writes_attempted();
    assert!(boundaries > 0, "genesis must write something");

    let mut interrupted = 0;
    for stop_after in 0..=boundaries {
        let backing: Arc<InMemory> = Arc::new(InMemory::new());
        let store = Arc::new(FreezingStore::thawed(backing.clone()));
        store.freeze_after(i64::try_from(stop_after).unwrap());

        // The initializer dies partway: it may fail outright, never
        // return, or — once the allowance covers all of genesis — finish.
        let outcome = tokio::time::timeout(
            UNTIL_CRASH,
            Catalog::open(store.clone(), CatalogOptions::default()),
        )
        .await;
        if !matches!(outcome, Ok(Ok(_))) {
            interrupted += 1;
        }
        drop(outcome);

        // Whatever it left, the next open must produce a coherent catalog
        // — either by finding one or by creating it now.
        let reopened = Catalog::open(backing.clone(), CatalogOptions::default())
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "stopping genesis after {stop_after} writes left an unopenable store: {err:?}"
                )
            });
        let snapshot = reopened.snapshot().await.unwrap_or_else(|err| {
            panic!(
                "stopping genesis after {stop_after} writes left a half-created catalog: {err:?}"
            )
        });
        assert_eq!(snapshot.current_snapshot().id.get(), 0);
        let schemas = snapshot.schemas();
        assert_eq!(schemas.len(), 1, "stopping after {stop_after} writes");
        assert_eq!(schemas[0].name, "main");
        reopened.close().await.unwrap();
    }
    assert!(
        interrupted > 0,
        "no round actually stopped genesis, so the sweep proved nothing"
    );
}
