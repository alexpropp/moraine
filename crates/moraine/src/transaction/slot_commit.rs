//! The multi-writer head: the folded store view with every slot above the
//! fold cursor replayed onto it.
//!
//! The store is a fold of the slots below the cursor and nothing more, so a
//! process serves the head by reading that fold and applying the slots above
//! it. Every process replays the same total order, so every process arrives at
//! the same view without coordinating.

use std::{cmp::Ordering, sync::Arc};

use moraine_wal::{Commit, Overlay, SlotLog};
use slatedb::DbReader;
use tracing::warn;

use crate::{
    catalog::{CatalogSnapshot, handle::MultiWriterStore},
    error::{Error, Result},
    store::{handle::ReadHandle, open::StoreBuilder, read},
    transaction::commit::{self, StagedWrite, fold},
};

/// The multi-writer head: folded store state plus replay of every slot
/// past the fold cursor.
#[derive(Debug)]
pub(crate) struct SlotHead {
    pub(crate) view: CatalogSnapshot,
    /// The tail's writes, keyed by encoded store key: read-your-tail for
    /// probes the projection does not model (index entries above all).
    pub(crate) overlay: Overlay,
    /// The next unwritten slot sequence — the one a commit races for.
    // dead_code: read by the slot commit cycle, landing in a later task.
    #[allow(dead_code)]
    pub(crate) next_sequence: u64,
}

/// The head as of now.
pub(crate) async fn materialize_slot_head(store: &MultiWriterStore) -> Result<SlotHead> {
    slot_head(store, None).await
}

/// The view at `snapshot`: a target at or below the folded head is history the
/// store still holds; above it, only the tail carries it.
pub(crate) async fn materialize_slot_view_at(
    store: &MultiWriterStore,
    snapshot: u64,
) -> Result<CatalogSnapshot> {
    let handle = ReadHandle::Reader(&store.reader);
    // The head pointer only routes between two self-contained reads, so it
    // needs no cursor read before it: a head read stale low routes to the
    // replay, which reads its own cursor and resolves the target below.
    if snapshot <= folded_head(handle).await? {
        return commit::materialize(handle, Some(snapshot)).await;
    }

    let view = slot_head(store, Some(snapshot)).await?.view;
    let reached = view.snapshot.snapshot_id;
    match reached.cmp(&snapshot) {
        Ordering::Equal => Ok(view),
        // The reader followed the manifest past the target while the tail was
        // read, so the store now holds the target as history.
        Ordering::Greater => commit::materialize(handle, Some(snapshot)).await,
        Ordering::Less => Err(Error::NotFound(format!(
            "snapshot {snapshot} (head is {reached})"
        ))),
    }
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
    if let Err(err) = fresh.close().await {
        warn!(error = %err, "could not close the reader opened to re-read the fold cursor");
    }

    match retried? {
        Replayed::Head(head) => Ok(*head),
        Replayed::Hole { gap_at, folded } => Err(Error::Corruption(format!(
            "slot {gap_at} is absent while higher slots are present, and the fold cursor \
             {folded} is below it: nothing folded that commit, so no truncation could have \
             removed it"
        ))),
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
    'tail: for (sequence, envelope) in &tail.slots {
        for commit in &envelope.commits {
            if until.is_some_and(|target| view.snapshot.snapshot_id >= target) {
                break 'tail;
            }
            if let Admission::Apply = admit(&view, commit, *sequence)? {
                apply(&mut view, commit, *sequence)?;
            }
        }
        overlay.absorb(envelope);
        next_sequence = sequence.saturating_add(1);
    }

    Ok(Replayed::Head(Box::new(SlotHead {
        view,
        overlay,
        next_sequence,
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
        catalog::{Catalog, CatalogOptions, SnapshotId, handle::MultiWriterStore},
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
        MultiWriterStore {
            reader: Arc::new(reader),
            slots,
            object_store,
            options,
            read_only: false,
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

    /// One envelope holding one commit that creates a schema.
    fn schema_slot(
        transaction_id: u8,
        schema_id: u64,
        name: &str,
        validated_head: u64,
    ) -> Envelope {
        Envelope {
            commits: vec![Commit {
                transaction_id: [transaction_id; 16],
                payload: SlotPayload {
                    validated_head,
                    changes_made: format!("created_schema:\"{name}\""),
                    writes: schema_writes(schema_id, name, validated_head + 1),
                },
            }],
        }
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

    /// A commit already reflected in the folded view is skipped rather than
    /// re-applied, which is what makes a cursor stale against fresher data
    /// harmless.
    #[tokio::test]
    async fn a_commit_below_the_view_is_skipped() {
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

        let head = materialize_slot_head(&store).await.unwrap();

        assert_eq!(head.view.current_snapshot().id, SnapshotId::new(1));
        assert!(head.view.schema_by_name("staging").is_none());
        assert_eq!(head.next_sequence, 3);
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
