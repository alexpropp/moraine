//! The folder-role session: the fenced writer that derived-state maintenance
//! and folding run under, so a slot-backed store keeps exactly one direct
//! writer.
//!
//! With commits in the log, nothing may write the store directly except the
//! one fenced writer, or replay and the store diverge. Opening that writer is
//! the whole license: concurrent sessions fence each other, the newest winning,
//! so a loser surfaces [`Error::Fenced`](crate::Error::Fenced) rather than
//! corrupting anything.
//!
//! Folding drains the slot log into that store so the store is an accurate
//! derived index of the log. The fold loop itself is
//! [`moraine_wal::drive_fold`]; this module supplies the [`SlateDbCursorStore`]
//! that gives it a SlateDB target, applying each slot's writes and advancing
//! the `sys/fold` cursor as one atomic [`WriteBatch`].

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use moraine_wal::{
    CursorStore, Envelope, FoldReport, FoldValue, FolderRole, Jitter, drive_fold,
    drive_fold_if_stalled,
};
use slatedb::{Checkpoint, Db, DbReader, WriteBatch, config::WriteOptions};
use uuid::Uuid;

use crate::{
    catalog::SlotStore,
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{Key, SysKey},
        open::StoreBuilder,
        proto::HeadValue,
        read, value,
    },
};

/// Opens the fenced writer over `store`, runs `body` against it, and closes it.
/// The writer is the single direct writer of the slot-backed store; a second
/// session opened concurrently fences this one, which surfaces the fencing as
/// an error from `body` rather than a corrupt store.
pub(crate) async fn with_folder<T, F>(store: &SlotStore, body: F) -> Result<T>
where
    F: AsyncFnOnce(&Db) -> Result<T>,
{
    let db = open_folder_writer(store).await?;

    let outcome = body(&db).await;

    // A close failure surfaces only when the body itself succeeded; a body
    // error is the primary cause and keeps precedence.
    match db.close().await {
        Ok(()) => outcome,
        Err(err) => outcome.and(Err(Error::from(err))),
    }
}

/// Opens the fenced writer `Db` over the store's retained builder shape.
async fn open_folder_writer(store: &SlotStore) -> Result<Db> {
    StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_writer()
        .await
}

/// The SlateDB store as a fold target: the cursor is `sys/fold`, and applying a
/// slot plus advancing the cursor is one [`WriteBatch`]. Applying and advancing
/// as two writes would double-apply on a crash-resume, so they are one unit.
pub(crate) struct SlateDbCursorStore {
    db: Arc<Db>,
    /// The head this session has folded to, tracked across the sprint from each
    /// batch's `sys/head` write; `None` until the first apply reads the folded
    /// store's head.
    head: Option<u64>,
}

impl SlateDbCursorStore {
    fn new(db: Arc<Db>) -> Self {
        Self { db, head: None }
    }

    /// The folded store's current head snapshot id, read through the writer.
    async fn store_head(&self) -> Result<u64> {
        let bytes = self
            .db
            .get(Key::Sys(SysKey::Head).encode())
            .await
            .map_err(Error::from)?;
        match bytes {
            Some(bytes) => Ok(value::decode_value::<HeadValue>(&bytes)?.snapshot_id),
            None => Err(Error::Corruption("store has no head pointer".to_string())),
        }
    }
}

impl CursorStore for SlateDbCursorStore {
    type Error = Error;

    async fn cursor(&mut self) -> Result<u64> {
        let bytes = self
            .db
            .get(Key::Sys(SysKey::Fold).encode())
            .await
            .map_err(Error::from)?;
        Ok(match bytes {
            Some(bytes) => value::decode_value::<FoldValue>(&bytes)?.folded_sequence,
            None => 0,
        })
    }

    async fn apply(&mut self, sequence: u64, envelope: &Envelope) -> Result<()> {
        let head_key = Key::Sys(SysKey::Head).encode();
        let mut head = match self.head {
            Some(head) => head,
            None => self.store_head().await?,
        };

        let mut batch = WriteBatch::new();
        for commit in &envelope.commits {
            // The same continuity check replay makes: a slot must chain onto the
            // head the fold has reached, or its predecessors are missing and
            // folding the prefix would hide committed state.
            if commit.payload.validated_head != head {
                return Err(Error::Corruption(format!(
                    "slot {sequence} was validated against snapshot {} but the fold has reached \
                     {head}; its commit does not chain onto the one before it",
                    commit.payload.validated_head
                )));
            }

            for write in &commit.payload.writes {
                match &write.value {
                    Some(bytes) => batch.put(&write.key, bytes),
                    None => batch.delete(&write.key),
                }
                if write.key == head_key
                    && let Some(bytes) = &write.value
                {
                    head = value::decode_value::<HeadValue>(bytes)?.snapshot_id;
                }
            }
        }

        batch.put(
            Key::Sys(SysKey::Fold).encode(),
            value::encode_value(&FoldValue {
                folded_sequence: sequence,
            }),
        );

        // Apply and advance in one batch: a size-triggered flush might land the
        // memtable mid-sprint, so the two must never split across a batch
        // boundary. Durability is the sprint's end barrier, not each apply's.
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(Error::from)?;

        self.head = Some(head);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        // One durability barrier so the sprint's progress survives the writer's
        // close: the per-apply batches were written non-durable. Truncation
        // stays safe even if this fails, since its horizon is what a reader can
        // see, so a barrier that lost the folds costs retained slots, never a
        // deleted one.
        self.db.flush().await.map_err(Error::from)
    }
}

/// One bounded fold pass: opens the fenced writer, applies up to `limit`
/// unfolded slots (each as one atomic batch advancing the fold cursor), and
/// closes. The attach's reader is undisturbed; resume-after-crash is the
/// driver's contract, honored by the batch atomicity above.
///
/// `limit` bounds the applies, not the listing: `drive_fold` reads the whole
/// contiguous tail every pass, because hole detection needs the full run. A
/// post-migration first fold can therefore hold a long tail's slot bodies in
/// memory even under a small `limit` — the bound paces the writes, not the
/// read.
pub(crate) async fn fold_sprint(store: &SlotStore, limit: u64) -> Result<FoldReport> {
    let db = Arc::new(open_folder_writer(store).await?);
    let mut session = SlateDbCursorStore::new(Arc::clone(&db));
    let report = drive_fold(&store.slots, &mut session, limit).await;
    drop(session);

    match db.close().await {
        Ok(()) => report,
        Err(err) => report.and(Err(Error::from(err))),
    }
}

/// Slots committed but not yet folded — the clock-free staleness signal. One
/// `tail_length` from the fold cursor: existence probes, no slot bodies
/// fetched. The cursor is read through a reader opened at the current manifest,
/// so a sprint this handle just ran is reflected rather than lagged behind the
/// attach reader's poll interval.
pub(crate) async fn unfolded_tail(store: &SlotStore) -> Result<u64> {
    let reader = StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_reader()
        .await?;
    let fold = fold_cursor(ReadHandle::Reader(&reader)).await;
    let unfolded = match fold {
        Ok(fold) => store
            .slots
            .tail_length(fold.saturating_add(1))
            .await
            .map_err(Error::from),
        Err(err) => Err(err),
    };

    if let Err(err) = reader.close().await {
        tracing::warn!(error = %err, "could not close the reader opened for the unfolded-tail probe");
    }
    unfolded
}

/// Slots kept below the horizon regardless of the two bounds, so a reader whose
/// checkpoint advances between the manifest read and the delete still finds the
/// run it is about to replay. Mirrors the min-age SlateDB's own WAL GC keeps
/// alongside checkpoint references; slot envelopes are small, so over-retention
/// costs storage while under-retention costs a committed slot.
const TRUNCATION_RETENTION_MARGIN: u64 = 64;

/// Deletes durably folded slots the fleet no longer needs, oldest first, and
/// returns how many objects were removed. The horizon is the lower of two
/// bounds, held back by [`TRUNCATION_RETENTION_MARGIN`]: what is durably folded
/// (a reader cannot see a memtable, so an unfolded slot — the only copy of its
/// commit — is never past it), and the fold cursor the oldest live reader still
/// sits at (or its next materialization finds the slots it must replay
/// deleted).
pub(crate) async fn truncate_folded_slots(store: &SlotStore) -> Result<u64> {
    let reader = reopen_reader(store).await?;

    // One fresh reader answers both bounds: its own fold cursor is the true
    // durable frontier (not the attach reader's lagged poll), and its manifest
    // lists every live reader checkpoint.
    let horizon = async {
        let durable = fold_cursor(ReadHandle::Reader(&reader)).await?;
        let reader_floor = oldest_reader_fold_cursor(store, &reader, durable).await?;
        Ok::<u64, Error>(
            durable
                .min(reader_floor)
                .saturating_sub(TRUNCATION_RETENTION_MARGIN),
        )
    }
    .await;

    if let Err(err) = reader.close().await {
        tracing::warn!(error = %err, "could not close the reader opened for the truncation horizon");
    }

    store
        .slots
        .truncate_through(horizon?)
        .await
        .map_err(Error::from)
}

/// The fold cursor as of the oldest live reader checkpoint in
/// `manifest_reader`'s manifest, or `durable` when no live checkpoint
/// constrains truncation. Every live reader pins its view as a manifest
/// checkpoint — the mechanism SlateDB's own GC leans on — so the oldest
/// checkpoint is the reader furthest behind, and the fold cursor read through
/// it is the deepest a truncation may reach without deleting a slot that reader
/// still replays. An expired checkpoint (a crashed reader's lapsed pin) does
/// not hold truncation back.
async fn oldest_reader_fold_cursor(
    store: &SlotStore,
    manifest_reader: &DbReader,
    durable: u64,
) -> Result<u64> {
    let Some(checkpoint_id) = oldest_live_checkpoint(manifest_reader.manifest().checkpoints())
    else {
        // No live reader pins anything, so only the durable frontier bounds us.
        return Ok(durable);
    };

    match fold_cursor_as_of(store, checkpoint_id).await {
        Ok(cursor) => Ok(cursor),
        // A reader churning its checkpoint can delete the one just listed before
        // it is opened, and a checkpoint can expire mid-open. Either way this
        // round retains rather than truncate past a reader it could not inspect;
        // the next pass re-reads the manifest and makes progress.
        Err(err) => {
            tracing::warn!(
                error = %err,
                "could not read the oldest reader checkpoint; retaining all slots this pass"
            );
            Ok(0)
        }
    }
}

/// The id of the live checkpoint referencing the oldest manifest, if any.
fn oldest_live_checkpoint(checkpoints: &[Checkpoint]) -> Option<Uuid> {
    let now = unix_seconds_now();
    checkpoints
        .iter()
        .filter(|checkpoint| checkpoint_is_live(checkpoint, now))
        .min_by_key(|checkpoint| checkpoint.manifest_id)
        .map(|checkpoint| checkpoint.id)
}

/// Whether `checkpoint`'s pin still holds: an absent expiry never lapses, and a
/// future one has not yet.
fn checkpoint_is_live(checkpoint: &Checkpoint, now_seconds: i64) -> bool {
    match checkpoint.expire_time {
        Some(expire) => expire.timestamp() > now_seconds,
        None => true,
    }
}

/// Seconds since the Unix epoch; 0 if the clock is unreadable, which retains
/// rather than truncates.
fn unix_seconds_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        .unwrap_or(0)
}

/// The fold cursor read through a reader pinned to `checkpoint_id`: the store
/// view the checkpoint's owner currently holds.
async fn fold_cursor_as_of(store: &SlotStore, checkpoint_id: Uuid) -> Result<u64> {
    let reader = StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_reader_at(checkpoint_id)
        .await?;
    let cursor = fold_cursor(ReadHandle::Reader(&reader)).await;

    if let Err(err) = reader.close().await {
        tracing::warn!(error = %err, "could not close the checkpoint-pinned reader");
    }
    cursor
}

/// A reader at the manifest as it stands now, for discovering live checkpoints.
async fn reopen_reader(store: &SlotStore) -> Result<DbReader> {
    StoreBuilder::new(&store.options.path, Arc::clone(&store.object_store))
        .cache_dir(store.options.cache_dir.clone())
        .open_reader()
        .await
}

/// The self-appointment rule over the real log: if the unfolded tail exceeds
/// `threshold`, wait `delay` plus jitter, re-check fold progress, and sprint
/// only if no other folder advanced the cursor. The stampede logic is
/// [`drive_fold_if_stalled`]'s; moraine supplies the policy values and the
/// [`FolderRole`] whose `peek_cursor` reads the cursor without opening the
/// writer and whose `open` is the fencing writer open.
pub(crate) async fn fold_if_stalled(
    store: &SlotStore,
    threshold: u64,
    delay: Duration,
    limit: u64,
) -> Result<Option<FoldReport>> {
    let role = SlotFolder {
        store,
        opened: Mutex::new(None),
    };
    let jitter = Jitter::from_entropy();
    let outcome =
        drive_fold_if_stalled(&store.slots, &role, threshold, delay, limit, &jitter).await;

    // Close the writer the appointment opened, if it opened one. A close failure
    // surfaces only when the fold itself succeeded.
    let opened = role
        .opened
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    match opened {
        Some(db) => match db.close().await {
            Ok(()) => outcome,
            Err(err) => outcome.and(Err(Error::from(err))),
        },
        None => outcome,
    }
}

/// Appoints and opens the folder role over a slot-backed store: `peek_cursor`
/// reads the cursor through the attach's reader without acquiring anything;
/// `open` acquires the fenced writer. The opened writer is retained so the
/// caller can close it once the appointment's fold completes.
struct SlotFolder<'a> {
    store: &'a SlotStore,
    opened: Mutex<Option<Arc<Db>>>,
}

impl FolderRole for SlotFolder<'_> {
    type Error = Error;
    type Session = SlateDbCursorStore;

    async fn peek_cursor(&self) -> Result<u64> {
        fold_cursor(ReadHandle::Reader(&self.store.reader)).await
    }

    async fn open(&self) -> Result<SlateDbCursorStore> {
        let db = Arc::new(open_folder_writer(self.store).await?);
        *self.opened.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&db));
        Ok(SlateDbCursorStore::new(db))
    }
}

/// The fold cursor read through `handle`; absent reads as 0, since a store with
/// no cursor has no folded slots.
async fn fold_cursor(handle: ReadHandle<'_>) -> Result<u64> {
    Ok(read::read_fold(handle)
        .await?
        .map_or(0, |fold| fold.folded_sequence))
}
