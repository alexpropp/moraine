//! The multi-writer head: the folded store view with every slot above the
//! fold cursor replayed onto it.

use std::{cmp::Ordering, sync::Arc};

use moraine_wal::{Commit, Envelope, Overlay, Race, SlotLog, SlotPayload, SlotWrite};
use slatedb::DbReader;
use tracing::warn;

use crate::{
    catalog::{CatalogSnapshot, MultiWriterStore},
    error::{Error, Result},
    store::{handle::ReadHandle, open::StoreBuilder, read},
    transaction::{
        commit::{self, StagedWrite, fold},
        operations::{ChangeSet, conflicts},
    },
};

mod coalesce;
pub(crate) use coalesce::CommitCoalescer;

/// The multi-writer head: folded store state plus replay of every slot
/// past the fold cursor.
pub(crate) struct SlotHead {
    pub(crate) view: CatalogSnapshot,
    /// The tail's writes, keyed by encoded store key: read-your-tail for
    /// probes the projection does not model (index entries above all).
    pub(crate) overlay: Overlay,
    /// The next unwritten slot sequence — the one a commit races for.
    pub(crate) next_sequence: u64,
    /// Set when the view came from a reader this materialization opened rather
    /// than the handle's. Any read that must match the view — an index entry
    /// scan above all — goes through it, and [`release_reader`] closes it.
    pub(crate) reader: Option<DbReader>,
}

impl std::fmt::Debug for SlotHead {
    // `DbReader` carries no `Debug`, so the reader is reported by presence.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotHead")
            .field("view", &self.view)
            .field("overlay", &self.overlay)
            .field("next_sequence", &self.next_sequence)
            .field("has_own_reader", &self.reader.is_some())
            .finish()
    }
}

impl SlotHead {
    /// A read handle over the reader this head's view came from: the one it
    /// opened for itself, else `folded` — the handle's own.
    pub(crate) fn handle<'a>(&'a self, folded: ReadHandle<'a>) -> ReadHandle<'a> {
        match &self.reader {
            Some(reader) => ReadHandle::Reader(reader),
            None => folded,
        }
    }
}

/// The head as of now.
pub(crate) async fn materialize_slot_head(store: &MultiWriterStore) -> Result<SlotHead> {
    slot_head(store, None).await
}

/// Closes a reader a hole retry opened. A handle's own reader is left alone; a
/// close that fails is logged, never substituted for the caller's outcome.
pub(crate) async fn release_reader(reader: Option<&DbReader>) {
    if let Some(reader) = reader
        && let Err(err) = reader.close().await
    {
        warn!(error = %err, "could not close the reader opened past a truncated prefix");
    }
}

/// The view at `snapshot`: a target at or below the folded head is history the
/// store still holds; above it, only the tail carries it.
pub(crate) async fn materialize_slot_view_at(
    store: &MultiWriterStore,
    snapshot: u64,
) -> Result<CatalogSnapshot> {
    let handle = ReadHandle::Reader(&store.reader);
    // The head pointer routes between two reads that each establish their own
    // consistency, so it needs no cursor read before it. A head read stale low
    // routes to the replay, which resolves the target below.
    if snapshot <= folded_head(handle).await? {
        return commit::materialize(handle, Some(snapshot)).await;
    }

    let head = slot_head(store, Some(snapshot)).await?;
    let reached = head.view.snapshot.snapshot_id;
    if reached == snapshot {
        release_reader(head.reader.as_ref()).await;
        return Ok(head.view);
    }

    let outcome = if reached > snapshot {
        // The reader followed the manifest past the target while the tail was
        // read, so the store now holds the target as history.
        commit::materialize(head.handle(handle), Some(snapshot)).await
    } else {
        Err(Error::NotFound(format!(
            "snapshot {snapshot} (head is {reached})"
        )))
    };
    release_reader(head.reader.as_ref()).await;

    outcome
}

/// Replays the tail, telling a truncated prefix this reader is behind from a
/// destroyed slot. A hole is never served as a head: continuing past it would
/// hide committed state and let the next committer re-win that sequence.
async fn slot_head(store: &MultiWriterStore, until: Option<u64>) -> Result<SlotHead> {
    if let Replayed::Head(head) = replay(&store.reader, &store.slots, until).await? {
        return Ok(*head);
    }

    // A peer that folded past the hole truncated it, which leaves this
    // reader's cursor stale by up to its manifest poll interval. A live
    // `DbReader` cannot be refreshed, so only a freshly opened one can tell
    // that from a slot destroyed outside the protocol: replaying from a cursor
    // at or past the hole never inspects it.
    let fresh = reopen_reader(store).await?;
    let retried = replay(&fresh, &store.slots, until).await;

    match retried {
        // The store this view came from is ahead of the handle's, so the reader
        // travels with the head: an entry scan through the handle's reader
        // would miss the writes the truncated slots left.
        Ok(Replayed::Head(mut head)) => {
            head.reader = Some(fresh);
            Ok(*head)
        }
        Ok(Replayed::Hole { gap_at, folded }) => {
            release_reader(Some(&fresh)).await;
            Err(Error::Corruption(format!(
                "slot {gap_at} is absent while higher slots are present, and the fold cursor \
                 {folded} is below it: nothing folded that commit, so no truncation could have \
                 removed it"
            )))
        }
        Err(err) => {
            release_reader(Some(&fresh)).await;
            Err(err)
        }
    }
}

/// One replay's outcome. A hole never reaches a caller as a head.
enum Replayed {
    /// The tail replayed onto the folded view.
    Head(Box<SlotHead>),
    /// A sequence absent below a present one, with the fold cursor the tail
    /// was read from.
    Hole { gap_at: u64, folded: u64 },
}

/// Replays the tail onto the folded view, stopping once `until` is reached. A
/// truncated replay's overlay and `next_sequence` cover only the slots it
/// applied, so only an `until` of `None` describes the head.
async fn replay(reader: &DbReader, slots: &SlotLog, until: Option<u64>) -> Result<Replayed> {
    let handle = ReadHandle::Reader(reader);

    // The cursor is read before the view, both through this reader. A
    // `DbReader` follows the manifest on its own interval, so a refresh can
    // land between the two reads: a cursor stale against fresher data replays
    // slots the admission rule below skips, while a cursor ahead of the data
    // would drop the slots between them and serve a gapped catalog.
    let folded = fold_cursor(handle).await?;
    let mut view = commit::materialize(handle, None).await?;

    // A fold cursor of `n` means slots `1..=n` are applied, so the tail starts
    // at `n + 1`.
    let from = folded.saturating_add(1);
    let tail = slots.read_tail(from).await?;
    if let Some(gap_at) = tail.gap_at {
        return Ok(Replayed::Hole { gap_at, folded });
    }

    let mut overlay = Overlay::default();
    let mut next_sequence = from;
    // Folding advances in log order under a prefix cursor, so the commits this
    // view already reflects are a leading prefix of the tail. Past the first
    // commit that applies, one that does not is a broken chain, not a fold this
    // cursor had not caught up to.
    let mut applied = false;
    'tail: for (sequence, envelope) in &tail.slots {
        for commit in &envelope.commits {
            if until.is_some_and(|target| view.snapshot.snapshot_id >= target) {
                break 'tail;
            }
            match admit(&view, commit, *sequence)? {
                Admission::Apply => {
                    apply(&mut view, commit, *sequence)?;
                    applied = true;
                }
                Admission::Skip if applied => {
                    return Err(Error::Corruption(format!(
                        "slot {sequence} was validated against snapshot {} after replay had \
                         already reached {}; its commit does not chain onto the one before it",
                        commit.payload.validated_head, view.snapshot.snapshot_id
                    )));
                }
                Admission::Skip => {}
            }
        }
        overlay.absorb(envelope);
        next_sequence = sequence.saturating_add(1);
    }

    Ok(Replayed::Head(Box::new(SlotHead {
        view,
        overlay,
        next_sequence,
        reader: None,
    })))
}

/// Whether one commit's writes belong on the replaying view.
enum Admission {
    /// The commit staged against exactly this head.
    Apply,
    /// The commit is already reflected in the folded view.
    Skip,
}

/// Admits one commit by its validated head: equal applies, less is already
/// folded in, and greater means the slots between the view and this commit are
/// missing — a substituted or reordered slot never applies its writes.
fn admit(view: &CatalogSnapshot, commit: &Commit, sequence: u64) -> Result<Admission> {
    let head = view.snapshot.snapshot_id;
    let validated_head = commit.payload.validated_head;
    match validated_head.cmp(&head) {
        Ordering::Equal => Ok(Admission::Apply),
        Ordering::Less => Ok(Admission::Skip),
        Ordering::Greater => Err(Error::Corruption(format!(
            "slot {sequence} was validated against snapshot {validated_head} but replay \
             reached only {head}; the slots between them are missing"
        ))),
    }
}

/// Applies one commit's staged batch. The log is authoritative, so a batch
/// that cannot replay is a broken store rather than a stale view.
fn apply(view: &mut CatalogSnapshot, commit: &Commit, sequence: u64) -> Result<()> {
    let writes: Vec<StagedWrite> = commit
        .payload
        .writes
        .iter()
        .map(|write| (write.key.clone(), write.value.clone()))
        .collect();

    fold::fold_batch(view, &writes)
        .map_err(|err| Error::Corruption(format!("slot {sequence} could not be replayed: {err}")))
}

/// Builds one commit from its arbitration-independent parts: the batch every
/// backing computes the same way, wrapped with the transaction id and the
/// validated head a replay chains on. Shared by the verb cycle's coalescer and
/// the staged-row path.
pub(crate) fn commit_from(
    transaction_id: [u8; 16],
    validated_head: u64,
    changes_made: String,
    writes: &[StagedWrite],
) -> Commit {
    Commit {
        transaction_id,
        payload: SlotPayload {
            validated_head,
            changes_made,
            writes: writes
                .iter()
                .map(|(key, value)| SlotWrite {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        },
    }
}

/// Judges a lost race by comparing this commit's change set against every
/// commit the winner carries. A parse yielding an unknown kind classifies as a
/// conflict through [`conflicts`], so an unparseable winner is never benign.
fn classify_lost_race(ours: Option<&ChangeSet>, winner: &Envelope) -> Race {
    let Some(ours) = ours else {
        return Race::Conflict;
    };
    for commit in &winner.commits {
        let theirs = ChangeSet::parse(&commit.payload.changes_made);
        if conflicts(ours, &theirs) {
            return Race::Conflict;
        }
    }

    Race::Benign
}

/// The fold cursor; absent reads as 0, since a store with no cursor has no
/// folded slots.
async fn fold_cursor(handle: ReadHandle<'_>) -> Result<u64> {
    Ok(read::read_fold(handle)
        .await?
        .map_or(0, |fold| fold.folded_sequence))
}

/// The head snapshot id the folded store carries.
async fn folded_head(handle: ReadHandle<'_>) -> Result<u64> {
    Ok(read::read_head(handle)
        .await?
        .ok_or_else(|| Error::Corruption("store has no head pointer".to_string()))?
        .snapshot_id)
}

/// A reader at the manifest as it stands now.
async fn reopen_reader(store: &MultiWriterStore) -> Result<DbReader> {
    StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_reader()
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use moraine_wal::{Commit, Envelope, SlotLog, SlotPayload, SlotWrite};
    use object_store::{ObjectStore, memory::InMemory};
    use uuid::Uuid;

    use super::*;
    use crate::{
        catalog::{Catalog, CatalogOptions, MultiWriterStore, SnapshotId},
        store::{
            key::{EntityKey, Key, SysKey},
            open::StoreBuilder,
            proto, value,
        },
        transaction::{commit, operations::ChangeSet},
    };

    fn multi_writer_options() -> CatalogOptions {
        CatalogOptions {
            multi_writer: true,
            ..CatalogOptions::default()
        }
    }

    /// A bootstrapped slot-backed store, attached as the catalog attaches it:
    /// bootstrap through the writer, then read through a `DbReader`.
    async fn bootstrap(object_store: Arc<dyn ObjectStore>) -> MultiWriterStore {
        let options = multi_writer_options();
        let db = commit::open_initialized(
            StoreBuilder::new(&options.path, Arc::clone(&object_store)),
            false,
            None,
            true,
        )
        .await
        .unwrap();
        db.close().await.unwrap();

        let reader = StoreBuilder::new(&options.path, Arc::clone(&object_store))
            .open_reader()
            .await
            .unwrap();
        let slots = SlotLog::new(Arc::clone(&object_store), &options.path);
        let coalescer = CommitCoalescer::new(options.commit_batch_window);
        MultiWriterStore {
            reader: Arc::new(reader),
            slots,
            object_store,
            options,
            read_only: false,
            coalescer,
        }
    }

    /// The writes a commit creating one schema stages: the schema record, the
    /// minted snapshot record, and the head advance — the shapes
    /// `stage_bootstrap` produces, under the real key and value codecs.
    fn schema_writes(schema_id: u64, name: &str, snapshot_id: u64) -> Vec<SlotWrite> {
        let mut changes = ChangeSet::default();
        changes.created_schemas.insert(name.to_string());

        vec![
            SlotWrite {
                key: Key::current(EntityKey::Schema { schema_id }).encode(),
                value: Some(value::encode_value(&proto::SchemaValue {
                    schema_id,
                    schema_uuid: Uuid::new_v4().to_string(),
                    begin_snapshot: snapshot_id,
                    end_snapshot: None,
                    schema_name: name.to_string(),
                    path: format!("{name}/"),
                    path_is_relative: true,
                })),
            },
            SlotWrite {
                key: Key::Snapshot { snapshot_id }.encode(),
                value: Some(value::encode_value(&proto::SnapshotValue {
                    snapshot_id,
                    schema_version: snapshot_id,
                    next_catalog_id: schema_id + 1,
                    changes_made: changes.to_changes_made(),
                    ..proto::SnapshotValue::default()
                })),
            },
            SlotWrite {
                key: Key::Sys(SysKey::Head).encode(),
                value: Some(value::encode_value(&proto::HeadValue { snapshot_id })),
            },
        ]
    }

    /// One commit that creates a schema, minting the snapshot above
    /// `validated_head`.
    fn schema_commit(
        transaction_id: u8,
        schema_id: u64,
        name: &str,
        validated_head: u64,
    ) -> Commit {
        Commit {
            transaction_id: [transaction_id; 16],
            payload: SlotPayload {
                validated_head,
                changes_made: format!("created_schema:\"{name}\""),
                writes: schema_writes(schema_id, name, validated_head + 1),
            },
        }
    }

    /// One envelope holding one commit that creates a schema.
    fn schema_slot(
        transaction_id: u8,
        schema_id: u64,
        name: &str,
        validated_head: u64,
    ) -> Envelope {
        Envelope {
            commits: vec![schema_commit(
                transaction_id,
                schema_id,
                name,
                validated_head,
            )],
        }
    }

    /// A commit carrying only a classification string, for judging a race
    /// without staging any writes.
    fn classified(changes_made: &str) -> Commit {
        Commit {
            transaction_id: [0; 16],
            payload: SlotPayload {
                validated_head: 0,
                changes_made: changes_made.to_string(),
                writes: vec![],
            },
        }
    }

    /// A lost race is judged against every commit a winner carries, so a
    /// conflict a second commit introduces is not missed; an unparseable
    /// classification is an unknown change and never benign; and an absent
    /// change set cannot be judged benign at all. Multi-commit winners first
    /// exist with the coalescer, so these branches get their own test rather
    /// than only correct-by-construction cover.
    #[test]
    fn classify_lost_race_scans_all_commits_and_refuses_the_unparseable() {
        let mut ours = ChangeSet::default();
        ours.altered_tables.insert(5);

        // Two commits, neither touching table 5: nothing to rebase away from.
        let benign = Envelope {
            commits: vec![classified("altered_table:7"), classified("altered_table:9")],
        };
        assert_eq!(classify_lost_race(Some(&ours), &benign), Race::Benign);

        // The conflict is the winner's *second* commit; a scan that stopped at
        // the first would call this benign and apply over a real conflict.
        let conflicting = Envelope {
            commits: vec![classified("altered_table:7"), classified("altered_table:5")],
        };
        assert_eq!(
            classify_lost_race(Some(&ours), &conflicting),
            Race::Conflict
        );

        // An unparseable classification parses to an unknown change, which
        // conflicts with anything.
        let unparseable = Envelope {
            commits: vec![classified("not a change list at all")],
        };
        assert_eq!(
            classify_lost_race(Some(&ours), &unparseable),
            Race::Conflict
        );

        // No change set to compare cannot be called benign.
        assert_eq!(classify_lost_race(None, &benign), Race::Conflict);
    }

    /// The head is the folded store plus the tail: a slot no folder has
    /// applied is already visible, and its writes are readable as an overlay.
    #[tokio::test]
    async fn the_head_replays_the_tail_over_the_folded_view() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let envelope = schema_slot(1, 1, "sales", 0);
        store.slots.put_slot(1, &envelope).await.unwrap();

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(1));
        assert!(head.view.schema_by_name("sales").is_some());
        assert_eq!(head.next_sequence, 2);
        for write in &envelope.commits[0].payload.writes {
            assert_eq!(head.overlay.get(&write.key), Some(write.value.as_deref()));
        }
    }

    /// Slots apply in order, and every process replays the same order: a
    /// second slot staged against the first's result advances the head again.
    #[tokio::test]
    async fn the_tail_replays_in_slot_order() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert_eq!(head.view.schemas().len(), 3);
        assert_eq!(head.next_sequence, 3);
    }

    /// A commit validated above the replayed head means the slots between them
    /// are missing, so its writes never apply.
    #[tokio::test]
    async fn a_commit_validated_above_the_view_refuses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        // Bootstrap leaves head 0, so a slot staged against head 7 has seven
        // predecessors nothing in this log holds.
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 7))
            .await
            .unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 1"), "{err}");
    }

    /// A slot that does not chain onto the one before it is refused, not
    /// skipped: once a commit has applied in this replay, a following `less`
    /// is a broken chain, never a stale cursor. Skipping it would silently
    /// drop a committer's writes while reporting success.
    #[tokio::test]
    async fn a_slot_that_does_not_chain_after_an_apply_refuses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        // A second slot staged against head 0 as well: whatever assembled it
        // did not stage it against slot 1's result.
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 0))
            .await
            .unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 2"), "{err}");
    }

    /// The legitimate `less` case the skip rule exists for: slot 1 is folded
    /// into the store while the cursor lags at 0, so replay re-reads it, finds
    /// it already reflected, and skips it — then applies slot 2. No commit has
    /// applied when the skip happens, so the latch does not fire.
    #[tokio::test]
    async fn a_slot_already_folded_under_a_lagging_cursor_is_skipped() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let first = schema_slot(1, 1, "sales", 0);
        store.slots.put_slot(1, &first).await.unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        // Slot 1's writes are in the store, but the cursor still reads 0, so
        // replay starts at slot 1 and must skip it rather than re-apply.
        fold_through(&object_store, &store.options, &first, 0).await;

        let store = reopen(store, &object_store).await;
        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert!(head.view.schema_by_name("sales").is_some());
        assert!(head.view.schema_by_name("staging").is_some());
        assert_eq!(head.next_sequence, 3);
    }

    /// A well-chained multi-commit envelope replays commit by commit to
    /// head + 2. Nothing produces these yet; the per-commit apply is what makes
    /// accepting them safe in advance.
    #[tokio::test]
    async fn a_chained_multi_commit_envelope_reaches_head_plus_two() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let envelope = Envelope {
            commits: vec![
                schema_commit(1, 1, "sales", 0),
                schema_commit(2, 2, "staging", 1),
            ],
        };
        store.slots.put_slot(1, &envelope).await.unwrap();

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert!(head.view.schema_by_name("sales").is_some());
        assert!(head.view.schema_by_name("staging").is_some());
        assert_eq!(head.next_sequence, 2);
    }

    /// A multi-commit envelope whose second commit did not stage against the
    /// first's result fails its own replay under the latch, rather than
    /// applying the first and silently dropping the second.
    #[tokio::test]
    async fn a_badly_chained_multi_commit_envelope_refuses() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let envelope = Envelope {
            commits: vec![
                schema_commit(1, 1, "sales", 0),
                // Staged against head 0 as well: it does not chain onto commit 1.
                schema_commit(2, 2, "staging", 0),
            ],
        };
        store.slots.put_slot(1, &envelope).await.unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 1"), "{err}");
    }

    /// A hole the fold cursor is still below is a slot destroyed outside the
    /// protocol: the prefix is never served as the head.
    #[tokio::test]
    async fn a_hole_below_the_fold_cursor_is_corruption() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        // Slot 2 was destroyed: 3 exists above it and nothing folded it.
        store
            .slots
            .put_slot(3, &schema_slot(3, 3, "archive", 2))
            .await
            .unwrap();

        let err = materialize_slot_head(&store).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        assert!(err.to_string().contains("slot 2"), "{err}");
    }

    /// The other cause of a hole: a peer folded past it and truncated it, so
    /// this reader is merely behind. A freshly opened reader sees the advanced
    /// cursor, and the slots above it still serve.
    #[tokio::test]
    async fn a_hole_a_peer_truncated_replays_against_the_fresher_cursor() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        let first = schema_slot(1, 1, "sales", 0);
        store.slots.put_slot(1, &first).await.unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        // A peer folds slot 1 into the store, advances the cursor, and
        // truncates the slot it no longer needs.
        fold_through(&object_store, &store.options, &first, 1).await;
        store.slots.truncate_through(1).await.unwrap();

        // Whether or not this handle's reader has caught up, the head is the
        // fold plus what remains of the tail.
        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(2));
        assert!(head.view.schema_by_name("sales").is_some());
        assert!(head.view.schema_by_name("staging").is_some());
        assert_eq!(head.next_sequence, 3);
    }

    /// After a hole retry the head carries the reader its view came from, so a
    /// probe scanning entries through it sees the writes the truncated slots
    /// left — reads through the handle's stale reader would miss them, and the
    /// view would claim rows the probe cannot find.
    #[tokio::test]
    async fn the_head_reader_travels_past_a_truncation() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;

        let slots = [
            schema_slot(1, 1, "sales", 0),
            schema_slot(2, 2, "staging", 1),
            schema_slot(3, 3, "archive", 2),
            schema_slot(4, 4, "analytics", 3),
        ];
        for (offset, slot) in slots.iter().enumerate() {
            let sequence = offset as u64 + 1;
            store.slots.put_slot(sequence, slot).await.unwrap();
        }

        // A peer folds slots 1..=3 into the store, advancing the cursor, then
        // truncates them. This handle's reader still sees the bootstrap
        // manifest — cursor 0, none of those writes.
        for (offset, slot) in slots[..3].iter().enumerate() {
            let through = offset as u64 + 1;
            fold_through(&object_store, &store.options, slot, through).await;
        }
        store.slots.truncate_through(3).await.unwrap();

        let head = materialize_slot_head(&store).await.unwrap();
        assert!(head.reader.is_some(), "a hole retry adopts a fresh reader");
        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(4));
        assert!(head.view.schema_by_name("sales").is_some());

        // The record slot 1 wrote is now in the store but not in this handle's
        // stale reader. A probe must read it through the head's own reader.
        let sales_key = Key::current(EntityKey::Schema { schema_id: 1 }).encode();
        let stale = ReadHandle::Reader(&store.reader);
        assert!(
            head.handle(stale).get(&sales_key).await.unwrap().is_some(),
            "the head's reader sees the folded record"
        );
        assert!(
            stale.get(&sales_key).await.unwrap().is_none(),
            "the handle's reader is stale and would skew the probe"
        );

        release_reader(head.reader.as_ref()).await;
    }

    /// Time travel below the folded head reads history from the store; above
    /// it, the tail is replayed up to the target and no further.
    #[tokio::test]
    async fn time_travel_spans_the_folded_head_and_the_tail() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = bootstrap(Arc::clone(&object_store)).await;
        store
            .slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();
        store
            .slots
            .put_slot(2, &schema_slot(2, 2, "staging", 1))
            .await
            .unwrap();

        let bootstrapped = materialize_slot_view_at(&store, 0).await.unwrap();
        assert_eq!(bootstrapped.schemas().len(), 1);

        let first = materialize_slot_view_at(&store, 1).await.unwrap();
        assert_eq!(first.current_snapshot().id, SnapshotId::new(1));
        assert!(first.schema_by_name("sales").is_some());
        assert!(first.schema_by_name("staging").is_none());

        let err = materialize_slot_view_at(&store, 3).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
    }

    /// The wired read path: a handle attached to the same bucket serves a slot
    /// no folder has applied, and one opened before the slot landed serves it
    /// too.
    #[tokio::test]
    async fn a_catalog_handle_serves_the_tail_without_a_folder() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = Catalog::open(Arc::clone(&object_store), multi_writer_options())
            .await
            .unwrap();

        let slots = SlotLog::new(Arc::clone(&object_store), "");
        slots
            .put_slot(1, &schema_slot(1, 1, "sales", 0))
            .await
            .unwrap();

        let second = Catalog::open(Arc::clone(&object_store), multi_writer_options())
            .await
            .unwrap();
        let view = second.snapshot().await.unwrap();
        assert_eq!(view.current_snapshot().id, SnapshotId::new(1));
        assert!(view.schema_by_name("sales").is_some());

        assert!(
            first
                .snapshot()
                .await
                .unwrap()
                .schema_by_name("sales")
                .is_some()
        );
        let bootstrapped = second.snapshot_at(SnapshotId::new(0)).await.unwrap();
        assert_eq!(bootstrapped.schemas().len(), 1);
    }

    /// Reopens the store's reader against the current manifest, so a test sees
    /// a fold without waiting on the reader's poll interval.
    async fn reopen(
        store: MultiWriterStore,
        object_store: &Arc<dyn ObjectStore>,
    ) -> MultiWriterStore {
        let reader = StoreBuilder::new(&store.options.path, Arc::clone(object_store))
            .open_reader()
            .await
            .unwrap();
        MultiWriterStore {
            reader: Arc::new(reader),
            ..store
        }
    }

    /// Folds one envelope's writes into the store the way the folder will,
    /// stamping the cursor in the same batch.
    async fn fold_through(
        object_store: &Arc<dyn ObjectStore>,
        options: &CatalogOptions,
        envelope: &Envelope,
        through: u64,
    ) {
        let db = StoreBuilder::new(&options.path, Arc::clone(object_store))
            .open_writer()
            .await
            .unwrap();
        let tx = db.begin(slatedb::IsolationLevel::Snapshot).await.unwrap();
        for commit in &envelope.commits {
            for write in &commit.payload.writes {
                match &write.value {
                    Some(bytes) => tx.put(write.key.clone(), bytes.clone()).unwrap(),
                    None => tx.delete(write.key.clone()).unwrap(),
                }
            }
        }
        tx.put(
            Key::Sys(SysKey::Fold).encode(),
            value::encode_value(&moraine_wal::FoldValue {
                folded_sequence: through,
            }),
        )
        .unwrap();
        tx.commit_with_options(&commit::durable()).await.unwrap();
        db.close().await.unwrap();
    }
}
