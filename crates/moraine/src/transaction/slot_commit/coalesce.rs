//! Group commit at the slot layer: this process's concurrent commits queue
//! here and coalesce into one multi-commit envelope, so N commits cost one
//! slot PUT rather than the ~N²/2 a race for the same sequence would.
//!
//! The closures stay in their callers' futures — each member runs its own
//! closure against the accumulating head when the batch driver asks it to, so
//! nothing that borrows a caller's stack ever crosses to the driver. The
//! driver owns racing the log; the members own assembling their commits.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use moraine_wal::{
    Commit, CommitDrive, Committer, Envelope, Overlay, Race, RetryPolicy, SlotPayload, SlotWrite,
    drive_commit,
};
use slatedb::DbReader;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

use super::{
    Admission, SlotHead, admit, apply, classify_lost_race, materialize_slot_head, release_reader,
};
use crate::{
    catalog::{CatalogSnapshot, MultiWriterStore, SnapshotId},
    error::{Error, Result},
    store::handle::ReadHandle,
    transaction::{
        commit::{Assembled, Prepared, StagedWrite, assemble_commit, fold},
        index_maintenance::ProbeHandle,
        operations::ChangeSet,
        verbs::Transaction,
    },
};

/// Serializes this process's slot attempts and coalesces whatever is waiting
/// into one envelope. Shared across every clone of a catalog handle, so
/// commits from all of them batch together.
pub(crate) struct CommitCoalescer {
    window: Duration,
    shared: Mutex<Shared>,
}

/// The queue and the one-batch-at-a-time flag.
struct Shared {
    waiting: VecDeque<Arc<Member>>,
    driving: bool,
}

impl CommitCoalescer {
    /// A coalescer whose leader waits `window` to accumulate more before it
    /// races. Zero declines to wait but still batches whatever is queued.
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            shared: Mutex::new(Shared {
                waiting: VecDeque::new(),
                driving: false,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Commits `f` through the batch: leads a new batch if none is running,
    /// else joins the running one and waits for its outcome.
    pub(crate) async fn commit<F>(&self, store: &MultiWriterStore, f: &F) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        if store.read_only {
            return Err(Error::Constraint(
                "catalog attached read-only; writes are unavailable".to_string(),
            ));
        }

        let member = Arc::new(Member::new());
        let lead_now = {
            let mut shared = self.lock();
            shared.waiting.push_back(Arc::clone(&member));
            if shared.driving {
                false
            } else {
                shared.driving = true;
                true
            }
        };

        if lead_now {
            self.lead(store, f, member).await
        } else {
            self.participate(store, f, member).await
        }
    }

    /// Drives one batch to its slot, then hands the baton on. The leader
    /// contributes its own commit inline and orchestrates the followers.
    async fn lead<F>(
        &self,
        store: &MultiWriterStore,
        f: &F,
        leader: Arc<Member>,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        if self.window.is_zero() {
            // Let any already-ready commit register before the batch drains,
            // so an opportunistic batch forms without paying a timer.
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(self.window).await;
        }

        // The leader is its own inline member; drop it from the queue so the
        // follower drain never doubles it.
        {
            let mut shared = self.lock();
            shared.waiting.retain(|m| !Arc::ptr_eq(m, &leader));
        }

        let outcome = self.drive_batch(store, f, leader).await;

        // Hand the baton to the next waiter, else close the batch.
        {
            let mut shared = self.lock();
            match shared.waiting.pop_front() {
                Some(next) => {
                    next.direct(Directive::Lead);
                    next.resume.notify_one();
                }
                None => shared.driving = false,
            }
        }

        outcome
    }

    async fn drive_batch<F>(
        &self,
        store: &MultiWriterStore,
        f: &F,
        leader: Arc<Member>,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        let head = match materialize_slot_head(store).await {
            Ok(head) => head,
            Err(err) => return Err(err),
        };
        let original_head = head.view.snapshot.snapshot_id;
        let start_sequence = head.next_sequence;
        let base = Base::from_head(store, head);

        let mut committer = CoalescingCommitter {
            coalescer: self,
            leader_f: f,
            leader_slot: Slot::new(leader.txid),
            followers: Vec::new(),
            base,
            original_head,
            last_changes: Vec::new(),
        };

        let drive = drive_commit(
            &store.slots,
            &mut committer,
            start_sequence,
            &RetryPolicy::default(),
        )
        .await;

        let outcome = committer.settle(&drive);
        if committer.base.owns_reader {
            release_reader(Some(committer.base.reader.as_ref())).await;
        }
        outcome
    }

    /// A follower's life: assemble on request, then take its settled outcome
    /// — or, if the batch it joined ended before it was reached, lead the
    /// next one itself.
    async fn participate<F>(
        &self,
        store: &MultiWriterStore,
        f: &F,
        member: Arc<Member>,
    ) -> Result<SnapshotId>
    where
        F: Fn(&mut Transaction) -> Result<()>,
    {
        loop {
            let directive = member.take_directive();
            match directive {
                Directive::Idle => member.resume.notified().await,
                Directive::Lead => return self.lead(store, f, member).await,
                Directive::Assemble { accum } => {
                    let product = assemble_member(f, member.txid, &accum).await;
                    member.produce(product);
                    member.reply.notify_one();
                    member.resume.notified().await;
                }
                Directive::Settle => return member.take_outcome(),
            }
        }
    }
}

/// The evolving head a batch assembles against: the folded view plus every
/// slot the driver has absorbed on the way.
struct Base {
    view: CatalogSnapshot,
    overlay: Overlay,
    reader: Arc<DbReader>,
    /// Whether `reader` is one this batch opened past a truncated prefix, so
    /// closing it is the batch's to do.
    owns_reader: bool,
}

impl Base {
    fn from_head(store: &MultiWriterStore, head: SlotHead) -> Self {
        let SlotHead {
            view,
            overlay,
            next_sequence: _,
            reader,
        } = head;
        match reader {
            Some(reader) => Self {
                view,
                overlay,
                reader: Arc::new(reader),
                owns_reader: true,
            },
            None => Self {
                view,
                overlay,
                reader: Arc::clone(&store.reader),
                owns_reader: false,
            },
        }
    }
}

/// One member's per-batch bookkeeping the driver keeps.
struct Slot {
    txid: Uuid,
    /// The snapshot the member's last assembly minted, meaningful only while
    /// `terminal` is `None`.
    minted: u64,
    /// Whether the member contributed a commit to the last round's envelope.
    staged: bool,
    /// Set once the member is out of the envelope for good: nothing to commit,
    /// or its own closure failed on re-run.
    terminal: Option<Result<SnapshotId>>,
}

impl Slot {
    fn new(txid: Uuid) -> Self {
        Self {
            txid,
            minted: 0,
            staged: false,
            terminal: None,
        }
    }
}

/// A follower and the driver's bookkeeping for it.
struct Follower {
    member: Arc<Member>,
    slot: Slot,
}

/// The batch driver plugged into the log's commit loop: each `assemble`
/// re-runs every live member against the accumulating head and returns the
/// whole batch as one envelope.
struct CoalescingCommitter<'a, F> {
    coalescer: &'a CommitCoalescer,
    leader_f: &'a F,
    leader_slot: Slot,
    followers: Vec<Follower>,
    base: Base,
    original_head: u64,
    // The change set of every member that staged this round, for judging a
    // lost race.
    last_changes: Vec<ChangeSet>,
}

impl<F> CoalescingCommitter<'_, F> {
    /// Admits every commit that queued since the last round, so a joiner
    /// inherits the batch's attempt count rather than resetting it.
    fn admit_joiners(&mut self) {
        let mut shared = self.coalescer.lock();
        while let Some(member) = shared.waiting.pop_front() {
            let slot = Slot::new(member.txid);
            self.followers.push(Follower { member, slot });
        }
    }

    /// Distributes the batch's outcome to every follower and returns the
    /// leader's own.
    fn settle(&mut self, drive: &Result<CommitDrive>) -> Result<SnapshotId> {
        let verdict = Verdict::of(drive);
        let leader = outcome_for(&self.leader_slot, &verdict, self.original_head);

        for follower in &mut self.followers {
            let outcome = outcome_for(&follower.slot, &verdict, self.original_head);
            follower.member.settle(outcome);
            follower.member.resume.notify_one();
        }

        leader
    }
}

impl<F> Committer for CoalescingCommitter<'_, F>
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    type Error = Error;

    async fn assemble(&mut self) -> Result<Option<Envelope>> {
        self.admit_joiners();
        self.last_changes.clear();

        let accum = Arc::new(AsyncMutex::new(Accum {
            view: self.base.view.clone(),
            overlay: self.base.overlay.clone(),
            reader: Arc::clone(&self.base.reader),
            commits: Vec::new(),
        }));

        if self.leader_slot.terminal.is_none() {
            let product = assemble_member(self.leader_f, self.leader_slot.txid, &accum).await;
            record(
                &mut self.leader_slot,
                product,
                &mut self.last_changes,
                self.original_head,
            );
        }

        for index in 0..self.followers.len() {
            if self.followers[index].slot.terminal.is_some() {
                continue;
            }
            let member = Arc::clone(&self.followers[index].member);
            let product = request_assemble(&member, Arc::clone(&accum)).await;
            record(
                &mut self.followers[index].slot,
                product,
                &mut self.last_changes,
                self.original_head,
            );
        }

        let commits = accum.lock().await.commits.clone();
        if commits.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Envelope { commits }))
        }
    }

    fn classify(&self, winner: &Envelope) -> Race {
        for ours in &self.last_changes {
            if let Race::Conflict = classify_lost_race(Some(ours), winner) {
                return Race::Conflict;
            }
        }
        Race::Benign
    }

    fn absorb(&mut self, sequence: u64, winner: Envelope) -> Result<()> {
        for commit in &winner.commits {
            match admit(&self.base.view, commit, sequence)? {
                Admission::Apply => apply(&mut self.base.view, commit, sequence)?,
                Admission::Skip => {}
            }
        }
        self.base.overlay.absorb(&winner);
        Ok(())
    }
}

/// Folds one member's product into its slot bookkeeping and the round's change
/// list.
fn record(slot: &mut Slot, product: Product, changes: &mut Vec<ChangeSet>, original_head: u64) {
    match product {
        Product::Staged { minted, ours } => {
            slot.minted = minted;
            slot.staged = true;
            changes.push(*ours);
        }
        Product::Nothing => {
            slot.staged = false;
            slot.terminal = Some(Ok(SnapshotId::new(original_head)));
        }
        Product::Failed(err) => {
            slot.staged = false;
            slot.terminal = Some(Err(err));
        }
    }
}

/// The verdict a batch reached, owned so every member can be mapped from it.
enum Verdict {
    Committed,
    Nothing,
    Conflict {
        sequence: u64,
    },
    Exhausted {
        attempts: usize,
        last_sequence: u64,
    },
    Unavailable {
        attempts: usize,
        last_sequence: u64,
        last_error: String,
    },
    Failed(String),
}

impl Verdict {
    fn of(drive: &Result<CommitDrive>) -> Self {
        match drive {
            Ok(CommitDrive::Committed { .. }) => Self::Committed,
            Ok(CommitDrive::Nothing) => Self::Nothing,
            Ok(CommitDrive::Conflict { sequence, .. }) => Self::Conflict {
                sequence: *sequence,
            },
            Ok(CommitDrive::Exhausted {
                attempts,
                last_sequence,
            }) => Self::Exhausted {
                attempts: *attempts,
                last_sequence: *last_sequence,
            },
            Ok(CommitDrive::Unavailable {
                attempts,
                last_sequence,
                last_error,
            }) => Self::Unavailable {
                attempts: *attempts,
                last_sequence: *last_sequence,
                last_error: last_error.to_string(),
            },
            Err(err) => Self::Failed(err.to_string()),
        }
    }
}

/// One member's outcome: its own terminal result if it left the envelope, else
/// whatever the batch as a whole settled to.
fn outcome_for(slot: &Slot, verdict: &Verdict, original_head: u64) -> Result<SnapshotId> {
    if let Some(terminal) = &slot.terminal {
        return match terminal {
            Ok(id) => Ok(*id),
            Err(err) => Err(clone_error(err)),
        };
    }

    match verdict {
        Verdict::Committed => Ok(SnapshotId::new(slot.minted)),
        Verdict::Nothing => Ok(SnapshotId::new(original_head)),
        Verdict::Conflict { sequence } => Err(Error::CommitConflict(format!(
            "a concurrent commit won slot {sequence} from head snapshot {original_head} and \
             conflicts with this transaction"
        ))),
        Verdict::Exhausted {
            attempts,
            last_sequence,
        } => Err(Error::RetryBudgetExhausted(format!(
            "spent {attempts} attempts from head snapshot {original_head} without settling; \
             last raced slot {last_sequence}"
        ))),
        Verdict::Unavailable {
            attempts,
            last_sequence,
            last_error,
        } => Err(Error::SlotLog(format!(
            "commit-slot log unreachable after {attempts} attempts from head snapshot \
             {original_head} (last raced slot {last_sequence}): {last_error}"
        ))),
        Verdict::Failed(text) => Err(Error::Corruption(text.clone())),
    }
}

/// Reconstructs a terminal error to hand a second member; the batch's members
/// each get their own value of the shared outcome.
fn clone_error(err: &Error) -> Error {
    match err {
        Error::CommitConflict(text) => Error::CommitConflict(text.clone()),
        Error::RetryBudgetExhausted(text) => Error::RetryBudgetExhausted(text.clone()),
        Error::NotFound(text) => Error::NotFound(text.clone()),
        Error::AlreadyExists(text) => Error::AlreadyExists(text.clone()),
        Error::Constraint(text) => Error::Constraint(text.clone()),
        Error::IndexBuilding(text) => Error::IndexBuilding(text.clone()),
        Error::SlotLog(text) => Error::SlotLog(text.clone()),
        other => Error::Corruption(other.to_string()),
    }
}

/// One assembly's result for a member.
enum Product {
    Staged { minted: u64, ours: Box<ChangeSet> },
    Nothing,
    Failed(Error),
}

/// The accumulating head a round chains onto: each member folds its writes in
/// before the next assembles, so member k+1's premise is member k's minted
/// snapshot.
struct Accum {
    view: CatalogSnapshot,
    overlay: Overlay,
    reader: Arc<DbReader>,
    commits: Vec<Commit>,
}

/// Runs one member's closure against the accumulating head and folds its
/// commit in. Holds the accumulator across its own read, so members chain in
/// call order.
async fn assemble_member<F>(f: &F, txid: Uuid, accum: &AsyncMutex<Accum>) -> Product
where
    F: Fn(&mut Transaction) -> Result<()>,
{
    let mut accum = accum.lock().await;
    let Accum {
        view,
        overlay,
        reader,
        commits,
    } = &mut *accum;

    let probe = ProbeHandle::Overlaid {
        store: ReadHandle::Reader(reader.as_ref()),
        overlay,
    };
    match assemble_commit(probe, f, view, None, Some(txid.into_bytes())).await {
        Ok(Prepared::Nothing { .. }) => Product::Nothing,
        Ok(Prepared::Staged(assembled)) => {
            let commit = commit_from(txid, &assembled);
            let writes: Vec<StagedWrite> = assembled.writes.clone();
            if let Err(err) = fold::fold_batch(view, &writes) {
                return Product::Failed(Error::Corruption(format!(
                    "a coalesced commit could not chain onto its batch: {err}"
                )));
            }
            let folded = Envelope {
                commits: vec![commit.clone()],
            };
            overlay.absorb(&folded);
            commits.push(commit);
            Product::Staged {
                minted: assembled.commits,
                ours: assembled.ours,
            }
        }
        Err(err) => Product::Failed(err),
    }
}

/// The one-commit shape a member contributes to the shared envelope.
fn commit_from(txid: Uuid, assembled: &Assembled) -> Commit {
    Commit {
        transaction_id: txid.into_bytes(),
        payload: SlotPayload {
            validated_head: assembled.head_before,
            changes_made: assembled.ours.to_changes_made(),
            writes: assembled
                .writes
                .iter()
                .map(|(key, value)| SlotWrite {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        },
    }
}

/// Asks one follower to assemble against `accum` and waits for its product.
async fn request_assemble(member: &Arc<Member>, accum: Arc<AsyncMutex<Accum>>) -> Product {
    member.direct(Directive::Assemble { accum });
    member.resume.notify_one();
    loop {
        if let Some(product) = member.take_product() {
            return product;
        }
        member.reply.notified().await;
    }
}

/// One queued commit's control block, shared between its own future and the
/// batch driver. The closure never appears here — it stays in the future.
struct Member {
    txid: Uuid,
    cell: Mutex<Cell>,
    resume: Notify,
    reply: Notify,
}

/// The mutable half of a member, always locked without an await held.
struct Cell {
    directive: Directive,
    product: Option<Product>,
    outcome: Option<Result<SnapshotId>>,
}

/// What the driver last told a member to do.
#[derive(Default)]
enum Directive {
    #[default]
    Idle,
    Lead,
    Assemble {
        accum: Arc<AsyncMutex<Accum>>,
    },
    Settle,
}

impl Member {
    fn new() -> Self {
        Self {
            txid: Uuid::new_v4(),
            cell: Mutex::new(Cell {
                directive: Directive::Idle,
                product: None,
                outcome: None,
            }),
            resume: Notify::new(),
            reply: Notify::new(),
        }
    }

    fn cell(&self) -> MutexGuard<'_, Cell> {
        self.cell.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn direct(&self, directive: Directive) {
        let mut cell = self.cell();
        cell.product = None;
        cell.directive = directive;
    }

    fn take_directive(&self) -> Directive {
        std::mem::take(&mut self.cell().directive)
    }

    fn produce(&self, product: Product) {
        self.cell().product = Some(product);
    }

    fn take_product(&self) -> Option<Product> {
        self.cell().product.take()
    }

    fn settle(&self, outcome: Result<SnapshotId>) {
        let mut cell = self.cell();
        cell.outcome = Some(outcome);
        cell.directive = Directive::Settle;
    }

    fn take_outcome(&self) -> Result<SnapshotId> {
        self.cell().outcome.take().unwrap_or_else(|| {
            Err(Error::Corruption(
                "a coalesced commit was settled with no outcome".to_string(),
            ))
        })
    }
}
