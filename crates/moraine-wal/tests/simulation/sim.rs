//! Test support for the deterministic protocol simulation: a seeded
//! fault-injecting object store over a real `InMemory`, the toy commit
//! protocol, and the run record the oracles are stated over.
//!
//! Determinism rests on three disciplines, each of which fails silently if
//! missed. Every random draw comes from a `fastrand::Rng` seeded here or from
//! a `RetryPolicy::seeded` jitter stream — never from `fastrand`'s
//! thread-local global, and never from `RetryPolicy::default`, whose jitter is
//! seeded from system entropy and which struct-update syntax evaluates in
//! full. Nothing iterates a `HashMap`, whose `RandomState` is per-process.
//! And the toy committers mint transaction ids from their own counters, never
//! from a clock or a v4 UUID.
//!
//! If the determinism self-check fails for a cause outside those three, or if
//! `moraine-wal` gains a dependency with internal concurrency, the harness
//! needs a simulation runtime in place of seeded latency. That is the decision
//! rule, not a judgment call.
//!
//! The fault model is limited to what an S3-compatible store can produce. S3
//! is read-after-write and list-consistent, so there are no stale reads and no
//! reordered writes; a design that defends against phantoms is worse than one
//! that does not, because the phantom defences look like real ones.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moraine_wal::{
    Commit, CommitDrive, Committer, CursorStore, Envelope, Error, Race, RetryPolicy, SlotLog,
    SlotPayload, SlotWrite, drive_commit, drive_fold,
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

/// The log root every simulated run uses.
const ROOT: &str = "sim";

/// The classification a commit that no one may lose to carries.
pub const EXCLUSIVE: &str = "exclusive";

/// The classification an ordinary, rebasable commit carries.
pub const SHARED: &str = "shared";

/// Tries a worker spends reading the head before its round starts.
const HEAD_READ_ATTEMPTS: usize = 8;

/// Virtual seconds a whole run may take. Auto-advanced paused time makes this
/// a liveness bound rather than a wall-clock one: a livelocked run trips it
/// instead of hanging.
const RUN_TIMEOUT: Duration = Duration::from_secs(600);

/// How a put fails, relative to the object landing. Every variant is
/// something an S3-compatible store actually produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutFault {
    /// No fault.
    None,
    /// Fails before touching the store: the put did not happen.
    FailBefore,
    /// The conditional create is applied, then a transport error is returned:
    /// the ambiguous put the exactly-once mechanic exists for.
    FailAfter,
    /// The create is applied and the store answers `AlreadyExists`: a retry
    /// below the caller re-issued a put that had already landed, so the code
    /// is no proof this attempt's put failed to land.
    LandedThenAlreadyExists,
    /// `AlreadyExists` with nothing written, which S3 answers while a
    /// competing conditional create is in flight. Drawn only when one is.
    PrematureAlreadyExists,
}

impl PutFault {
    /// The trace label.
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::FailBefore => "fail-before",
            Self::FailAfter => "fail-after",
            Self::LandedThenAlreadyExists => "landed-then-already-exists",
            Self::PrematureAlreadyExists => "premature-already-exists",
        }
    }
}

/// The timing and fault knobs one run draws from, all derived from its seed.
#[derive(Debug, Clone, Copy)]
pub struct Knobs {
    /// Inclusive bounds, in *milliseconds*, on the latency every operation
    /// draws. Randomising per-operation latency is what randomises the
    /// schedule, and milliseconds are the smallest unit that can: tokio's
    /// timer wheel has millisecond granularity, so every sub-millisecond
    /// sleep expires on the same tick and the schedule collapses to task
    /// poll order.
    pub latency_millis: (u64, u64),
    /// Percent of puts that fault.
    pub put_fault_percent: u32,
    /// Percent of gets that fault. A prior defect was reachable only through
    /// an ambiguous put followed by a failed read-back, so this is not
    /// optional.
    pub get_fault_percent: u32,
    /// Percent of list pages that fault.
    pub list_fault_percent: u32,
    /// Objects one list page yields before awaiting, so multi-page
    /// consumption is exercised.
    pub list_page: usize,
}

impl Knobs {
    /// Knobs with every fault rate at zero; timings still vary.
    pub fn without_faults(self) -> Self {
        Self {
            put_fault_percent: 0,
            get_fault_percent: 0,
            list_fault_percent: 0,
            ..self
        }
    }
}

/// The seeded state one [`SimStore`] draws from, plus the trace of every
/// operation it served.
#[derive(Debug)]
pub struct SimState {
    rng: fastrand::Rng,
    knobs: Knobs,
    trace: Vec<String>,
    in_flight: BTreeMap<Path, usize>,
}

impl SimState {
    /// The trace so far, drained.
    pub fn take_trace(&mut self) -> Vec<String> {
        std::mem::take(&mut self.trace)
    }

    /// Appends a line to the trace, so a run's task outcomes sit in the same
    /// transcript as its store operations.
    pub fn record(&mut self, line: String) {
        self.trace.push(line);
    }

    /// Drops every fault and every latency, so the log can be read back
    /// faithfully once a run has finished.
    pub fn quiesce(&mut self) {
        self.knobs = Knobs {
            latency_millis: (0, 0),
            list_page: usize::MAX,
            ..self.knobs.without_faults()
        };
    }

    /// A latency draw from the configured bounds.
    fn latency(&mut self) -> Duration {
        let (low, high) = self.knobs.latency_millis;
        Duration::from_millis(self.rng.u64(low..=high.max(low)))
    }

    /// Whether a `percent`-likely fault fires.
    fn hits(&mut self, percent: u32) -> bool {
        percent > 0 && self.rng.u32(0..100) < percent
    }

    /// The put fault to serve. `contended` reports whether another put to the
    /// same path is in flight, which is the only state in which S3 answers a
    /// conditional create with a 409 it wrote nothing for.
    fn put_fault(&mut self, contended: bool) -> PutFault {
        if !self.hits(self.knobs.put_fault_percent) {
            return PutFault::None;
        }

        let choices: &[PutFault] = if contended {
            &[
                PutFault::FailBefore,
                PutFault::FailAfter,
                PutFault::LandedThenAlreadyExists,
                PutFault::PrematureAlreadyExists,
            ]
        } else {
            &[
                PutFault::FailBefore,
                PutFault::FailAfter,
                PutFault::LandedThenAlreadyExists,
            ]
        };

        choices[self.rng.usize(0..choices.len())]
    }
}

/// `InMemory` plus a seeded fault schedule and per-operation latency: not a
/// mock but a real object store whose failures and timings the seed chooses.
#[derive(Debug)]
pub struct SimStore {
    inner: Arc<InMemory>,
    state: Arc<Mutex<SimState>>,
}

impl SimStore {
    /// A store whose draws are pinned to `seed`.
    pub fn new(seed: u64, knobs: Knobs) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            state: Arc::new(Mutex::new(SimState {
                rng: fastrand::Rng::with_seed(seed),
                knobs,
                trace: Vec::new(),
                in_flight: BTreeMap::new(),
            })),
        }
    }

    /// The shared state, for reading the trace and quiescing the store.
    pub fn state(&self) -> Arc<Mutex<SimState>> {
        Arc::clone(&self.state)
    }

    /// The state guard. Never held across an await, so the single-threaded
    /// runtime acquires it in a deterministic order.
    fn lock(&self) -> MutexGuard<'_, SimState> {
        lock(&self.state)
    }
}

/// Locks shared simulator state.
fn lock(state: &Arc<Mutex<SimState>>) -> MutexGuard<'_, SimState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A response carrying no information about whether the object exists. Its
/// text holds every substring an embedder's retry loop keys on, so the
/// crate's redaction is exercised on the way out.
fn unreadable() -> object_store::Error {
    object_store::Error::Generic {
        store: "sim",
        source: "outcome unknown: conflict, concurrent, unique, primary key".into(),
    }
}

/// The `AlreadyExists` a conditional create is refused with, on S3 and Azure
/// alike.
fn already_exists(location: &Path) -> object_store::Error {
    object_store::Error::AlreadyExists {
        path: location.to_string(),
        source: "409 to a conditional create".into(),
    }
}

/// The trailing path segment, which names the slot.
fn name(location: &Path) -> String {
    location
        .parts()
        .next_back()
        .map_or_else(|| location.to_string(), |part| part.as_ref().to_string())
}

impl fmt::Display for SimStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SimStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SimStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let latency = {
            let mut state = self.lock();
            let latency = state.latency();
            *state.in_flight.entry(location.clone()).or_default() += 1;
            state.record(format!(
                "put {} latency={}ms",
                name(location),
                latency.as_millis()
            ));
            latency
        };
        tokio::time::sleep(latency).await;

        let fault = {
            let mut state = self.lock();
            let contended = state.in_flight.get(location).copied().unwrap_or(0) > 1;
            let fault = state.put_fault(contended);
            state.record(format!("put {} fault={}", name(location), fault.label()));
            fault
        };

        let result = match fault {
            PutFault::None => self.inner.put_opts(location, payload, opts).await,
            PutFault::FailBefore => Err(unreadable()),
            PutFault::FailAfter => {
                let _ = self.inner.put_opts(location, payload, opts).await;
                Err(unreadable())
            }
            PutFault::LandedThenAlreadyExists => {
                let _ = self.inner.put_opts(location, payload, opts).await;
                Err(already_exists(location))
            }
            PutFault::PrematureAlreadyExists => Err(already_exists(location)),
        };

        let mut state = self.lock();
        if let Some(count) = state.in_flight.get_mut(location) {
            *count = count.saturating_sub(1);
        }
        state.record(format!(
            "put {} -> {}",
            name(location),
            match &result {
                Ok(_) => "won",
                Err(object_store::Error::AlreadyExists { .. }) => "already-exists",
                Err(_) => "error",
            }
        ));

        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let latency = {
            let mut state = self.lock();
            let latency = state.latency();
            state.record(format!(
                "get {} latency={}ms",
                name(location),
                latency.as_millis()
            ));
            latency
        };
        tokio::time::sleep(latency).await;

        let faulted = {
            let mut state = self.lock();
            let percent = state.knobs.get_fault_percent;
            let faulted = state.hits(percent);
            state.record(format!("get {} fault={faulted}", name(location)));
            faulted
        };
        if faulted {
            return Err(unreadable());
        }

        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    /// Yields the listing in small pages with an await between them, the way a
    /// paginated LIST behaves: each page is fetched against the store as it
    /// stands, keyed on the last name the previous page yielded.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        let prefix = prefix.cloned();

        futures::stream::unfold(ListPage::First, move |page| {
            let inner = Arc::clone(&inner);
            let state = Arc::clone(&state);
            let prefix = prefix.clone();

            async move { list_page(&inner, &state, prefix.as_ref(), page).await }
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Where a paginated listing stands.
#[derive(Debug, Clone)]
enum ListPage {
    /// No page served yet.
    First,
    /// Continue after this name.
    After(Path),
    /// The listing is finished.
    Done,
}

/// Serves one page of a listing, or `None` once the listing is finished.
async fn list_page(
    inner: &InMemory,
    state: &Arc<Mutex<SimState>>,
    prefix: Option<&Path>,
    page: ListPage,
) -> Option<(Vec<object_store::Result<ObjectMeta>>, ListPage)> {
    let after = match page {
        ListPage::First => None,
        ListPage::After(location) => Some(location),
        ListPage::Done => return None,
    };

    let (latency, faulted, size) = {
        let mut state = lock(state);
        let latency = state.latency();
        let percent = state.knobs.list_fault_percent;
        let faulted = state.hits(percent);
        let size = state.knobs.list_page.max(1);
        state.record(format!(
            "list after={} latency={}ms fault={faulted}",
            after.as_ref().map_or_else(String::new, name),
            latency.as_millis()
        ));
        (latency, faulted, size)
    };
    tokio::time::sleep(latency).await;

    if faulted {
        return Some((vec![Err(unreadable())], ListPage::Done));
    }

    let mut listing: Vec<ObjectMeta> = match inner.list(prefix).try_collect().await {
        Ok(listing) => listing,
        Err(err) => return Some((vec![Err(err)], ListPage::Done)),
    };
    listing.sort_by(|left, right| left.location.cmp(&right.location));

    let served: Vec<ObjectMeta> = listing
        .into_iter()
        .filter(|meta| after.as_ref().is_none_or(|from| meta.location > *from))
        .take(size)
        .collect();

    let next = match served.last() {
        Some(last) if served.len() == size => ListPage::After(last.location.clone()),
        _ => ListPage::Done,
    };
    lock(state).record(format!("list served={}", served.len()));

    Some((served.into_iter().map(Ok).collect(), next))
}

/// A logical unit of work: one worker's one commit round, stable across every
/// attempt that round makes.
///
/// The oracles are stated over these, not only over transaction ids. A
/// committer that mints a fresh id per attempt makes an id-level exactly-once
/// oracle vacuous against a round that commits the same *work* twice under two
/// ids, which is exactly the class of defect this harness must be able to see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Work {
    /// The worker that owns the round.
    pub worker: u8,
    /// The round, counted from one within the worker.
    pub round: u8,
}

impl Work {
    /// The key this work writes; one per worker, so replay is
    /// last-writer-wins per worker.
    fn key(self) -> Vec<u8> {
        vec![b'w', self.worker]
    }

    /// The value that identifies this work in a slot.
    fn value(self) -> Vec<u8> {
        vec![self.worker, self.round]
    }

    /// The work a write carries, if it carries any.
    fn from_write(write: &SlotWrite) -> Option<Self> {
        match write.value.as_deref() {
            Some([worker, round]) => Some(Self {
                worker: *worker,
                round: *round,
            }),
            _ => None,
        }
    }
}

impl fmt::Display for Work {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "w{}r{}", self.worker, self.round)
    }
}

/// Every logical work unit an envelope carries, in commit order.
pub fn works(envelope: &Envelope) -> Vec<Work> {
    envelope
        .commits
        .iter()
        .flat_map(|commit| commit.payload.writes.iter().filter_map(Work::from_write))
        .collect()
}

/// Every transaction id an envelope carries.
pub fn ids(envelope: &Envelope) -> Vec<[u8; 16]> {
    envelope
        .commits
        .iter()
        .map(|commit| commit.transaction_id)
        .collect()
}

/// The toy commit protocol: one write of the round's work unit under a fresh
/// transaction id per attempt, validated against the head last absorbed.
pub struct SimCommitter {
    work: Work,
    exclusive: bool,
    head: u64,
    attempts: u8,
    minted: Vec<[u8; 16]>,
}

impl SimCommitter {
    /// A committer for `work`, starting from `head`.
    pub fn new(work: Work, exclusive: bool, head: u64) -> Self {
        Self {
            work,
            exclusive,
            head,
            attempts: 0,
            minted: Vec::new(),
        }
    }
}

impl Committer for SimCommitter {
    type Error = Error;

    async fn assemble(&mut self) -> Result<Option<Envelope>, Error> {
        self.attempts += 1;
        // A fresh id per attempt: the shape that hides a double-applied round
        // from an id-level oracle, so the harness runs against the hostile
        // case rather than the convenient one.
        let mut transaction_id = [0; 16];
        transaction_id[0] = self.work.worker;
        transaction_id[1] = self.work.round;
        transaction_id[2] = self.attempts;
        self.minted.push(transaction_id);

        let changes_made = if self.exclusive { EXCLUSIVE } else { SHARED };

        Ok(Some(Envelope {
            commits: vec![Commit {
                transaction_id,
                payload: SlotPayload {
                    validated_head: self.head,
                    changes_made: changes_made.to_string(),
                    writes: vec![SlotWrite {
                        key: self.work.key(),
                        value: Some(self.work.value()),
                    }],
                },
            }],
        }))
    }

    fn classify(&self, winner: &Envelope) -> Race {
        let exclusive = winner
            .commits
            .iter()
            .any(|commit| commit.payload.changes_made == EXCLUSIVE);

        if exclusive {
            Race::Conflict
        } else {
            Race::Benign
        }
    }

    fn absorb(&mut self, sequence: u64, _winner: Envelope) -> Result<(), Error> {
        self.head = sequence;

        Ok(())
    }
}

/// Derived state the fold applies into: the work units in slot order, the
/// last-writer-wins view, and the cursor advanced in the same step.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FoldState {
    /// The highest sequence applied.
    pub cursor: u64,
    /// Work units applied, in slot order.
    pub applied: Vec<Work>,
    /// The folded view; `BTreeMap`, never `HashMap`.
    pub view: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl CursorStore for FoldState {
    type Error = Error;

    async fn cursor(&mut self) -> Result<u64, Error> {
        Ok(self.cursor)
    }

    async fn apply(&mut self, sequence: u64, envelope: &Envelope) -> Result<(), Error> {
        for commit in &envelope.commits {
            for write in &commit.payload.writes {
                match &write.value {
                    Some(value) => self.view.insert(write.key.clone(), value.clone()),
                    None => self.view.remove(&write.key),
                };
                if let Some(work) = Work::from_write(write) {
                    self.applied.push(work);
                }
            }
        }
        self.cursor = sequence;

        Ok(())
    }

    async fn finish(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// What one worker is asked to do, including the retry shape it races under.
#[derive(Debug)]
pub struct WorkerPlan {
    /// The worker's label, unique within a run.
    pub worker: u8,
    /// Commit rounds it drives, one after another.
    pub rounds: u8,
    /// Whether its commits are ones no one may rebase onto.
    pub exclusive: bool,
    /// Its retry shape. Always built from `RetryPolicy::seeded`.
    pub retry: RetryPolicy,
}

/// One simulated run, all of it derived from a seed.
#[derive(Debug)]
pub struct Scenario {
    /// The seed every draw in the run comes from.
    pub seed: u64,
    /// The store's timing and fault knobs.
    pub knobs: Knobs,
    /// The committer tasks.
    pub workers: Vec<WorkerPlan>,
    /// The fold tasks.
    pub folders: usize,
    /// Fold rounds each fold task drives.
    pub fold_rounds: usize,
    /// Slots one fold round may apply.
    pub fold_limit: u64,
}

impl Scenario {
    /// The scenario `seed` names. Called twice with the same seed it yields
    /// the same plan, jitter streams included, which is what makes a failure
    /// reproducible from `MORAINE_WAL_SEED` alone.
    pub fn for_seed(seed: u64) -> Self {
        let mut rng = fastrand::Rng::with_seed(seed ^ 0x5EED_0000_0000_0001);

        // A harsh run pairs heavy faults with a tight attempt budget, which
        // is what reaches the outcomes where a put's fate is genuinely
        // unknown; a sweep of gentle runs would never get there.
        let harsh = rng.u32(0..100) < 30;
        let high = rng.u64(4..=30);
        let knobs = Knobs {
            latency_millis: (rng.u64(1..=high.min(4)), high),
            put_fault_percent: if harsh {
                rng.u32(25..=60)
            } else {
                rng.u32(0..=20)
            },
            get_fault_percent: if harsh {
                rng.u32(20..=50)
            } else {
                rng.u32(0..=15)
            },
            list_fault_percent: rng.u32(0..=10),
            list_page: rng.usize(1..=3),
        };

        let workers = (1..=rng.u8(2..=4))
            .map(|worker| WorkerPlan {
                worker,
                rounds: rng.u8(1..=3),
                exclusive: rng.u32(0..100) < 25,
                retry: RetryPolicy {
                    max_attempts: if harsh {
                        rng.usize(2..=5)
                    } else {
                        rng.usize(4..=12)
                    },
                    base_delay: Duration::from_millis(rng.u64(0..=8)),
                    max_delay: Duration::from_millis(rng.u64(2..=50)),
                    ..RetryPolicy::seeded(
                        seed.wrapping_mul(0x9E37_79B9).wrapping_add(worker.into()),
                    )
                },
            })
            .collect();

        Self {
            seed,
            knobs,
            workers,
            folders: rng.usize(0..=2),
            fold_rounds: rng.usize(1..=3),
            fold_limit: rng.u64(1..=4),
        }
    }

    /// The same plan with every store fault suppressed. The exact
    /// slot-count-equals-committed-count identity only holds without faults:
    /// an ambiguous put that lands as the last attempt of a spent budget is a
    /// slot no round reports as committed.
    pub fn without_faults(mut self) -> Self {
        self.knobs = self.knobs.without_faults();

        self
    }
}

/// How one commit round ended, flattened to what the oracles need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoundOutcome {
    /// The round holds this sequence.
    Committed {
        /// The sequence won.
        sequence: u64,
    },
    /// Assembly found nothing to commit.
    Nothing,
    /// A winner judged incompatible holds this sequence.
    Conflict {
        /// The sequence the winner holds.
        sequence: u64,
    },
    /// The budget went to lost races.
    Exhausted,
    /// The budget went to a log that never answered, so the last put's
    /// outcome may be unknown.
    Unavailable,
    /// The driver returned an error.
    Failed(String),
    /// The head could not be read, so the round never raced.
    Unstarted(String),
}

impl RoundOutcome {
    /// Whether this round may have landed a slot it did not report.
    pub fn outcome_may_be_unknown(&self) -> bool {
        matches!(self, Self::Unavailable | Self::Failed(_))
    }

    /// The trace label.
    pub fn label(&self) -> String {
        match self {
            Self::Committed { sequence } => format!("committed@{sequence}"),
            Self::Nothing => "nothing".to_string(),
            Self::Conflict { sequence } => format!("conflict@{sequence}"),
            Self::Exhausted => "exhausted".to_string(),
            Self::Unavailable => "unavailable".to_string(),
            Self::Failed(text) => format!("failed({text})"),
            Self::Unstarted(text) => format!("unstarted({text})"),
        }
    }
}

impl From<Result<CommitDrive, Error>> for RoundOutcome {
    fn from(drive: Result<CommitDrive, Error>) -> Self {
        match drive {
            Ok(CommitDrive::Committed { sequence, .. }) => Self::Committed { sequence },
            Ok(CommitDrive::Nothing) => Self::Nothing,
            Ok(CommitDrive::Conflict { sequence, .. }) => Self::Conflict { sequence },
            Ok(CommitDrive::Exhausted { .. }) => Self::Exhausted,
            Ok(CommitDrive::Unavailable { .. }) => Self::Unavailable,
            Err(err) => Self::Failed(err.to_string()),
        }
    }
}

/// One commit round as the client saw it.
#[derive(Debug, Clone)]
pub struct RoundRecord {
    /// The logical work the round carried, stable across its attempts.
    pub work: Work,
    /// The sequence the round started at.
    pub start: u64,
    /// Every transaction id the round offered the log.
    pub minted: Vec<[u8; 16]>,
    /// How it ended.
    pub outcome: RoundOutcome,
}

/// One fold task's result.
#[derive(Debug, Clone)]
pub struct FoldRecord {
    /// The derived state it folded into.
    pub state: FoldState,
    /// Transport failures it worked through.
    pub transport_failures: usize,
    /// Corruption reports it saw. A healthy log yields none: a hole in the
    /// tail is damage, and this protocol never makes one.
    pub corruptions: Vec<String>,
}

/// One finished run: the transcript, what every task saw, and the log the
/// protocol left behind.
#[derive(Debug)]
pub struct Run {
    /// The seed the run came from.
    pub seed: u64,
    /// Every store operation and task outcome, in order.
    pub trace: Vec<String>,
    /// Every commit round, in worker then round order.
    pub rounds: Vec<RoundRecord>,
    /// Rounds the scenario asked for, which every task must have reached a
    /// terminal outcome for.
    pub planned_rounds: usize,
    /// Every fold task's result.
    pub folds: Vec<FoldRecord>,
    /// The log's final content, read back once the store was quiesced.
    pub slots: Vec<(u64, Envelope)>,
    /// A hole in the final tail. Always `None` on a healthy log.
    pub gap_at: Option<u64>,
    /// The quiesced log, so an oracle can re-read it.
    pub log: SlotLog,
}

impl Run {
    /// The sequence, if any, whose envelope carries `work`.
    pub fn sequences_of(&self, work: Work) -> Vec<u64> {
        self.slots
            .iter()
            .filter(|(_, envelope)| works(envelope).contains(&work))
            .map(|(sequence, _)| *sequence)
            .collect()
    }

    /// The sequence, if any, whose envelope carries `transaction_id`.
    pub fn sequence_of_id(&self, transaction_id: [u8; 16]) -> Option<u64> {
        self.slots
            .iter()
            .find(|(_, envelope)| envelope.contains_transaction(transaction_id))
            .map(|(sequence, _)| *sequence)
    }
}

/// The head sequence, retried past transport faults. A real embedder reads its
/// own materialized head; a read that never answers ends the round before it
/// races anything.
async fn read_head(log: &SlotLog) -> Result<u64, Error> {
    let mut last = None;
    for attempt in 1..=HEAD_READ_ATTEMPTS {
        match log.tail_length(1).await {
            Ok(length) => return Ok(length),
            Err(err) => {
                last = Some(err);
                let backoff = Duration::from_millis(attempt as u64);
                tokio::time::sleep(backoff).await;
            }
        }
    }

    Err(last.expect("the loop records a failure before it ends"))
}

/// Drives one worker's rounds, one after another, each starting from the head
/// as the worker can read it.
async fn run_worker(log: SlotLog, plan: WorkerPlan) -> Vec<RoundRecord> {
    let mut records = Vec::new();
    for round in 1..=plan.rounds {
        let work = Work {
            worker: plan.worker,
            round,
        };

        let head = match read_head(&log).await {
            Ok(head) => head,
            Err(err) => {
                records.push(RoundRecord {
                    work,
                    start: 0,
                    minted: Vec::new(),
                    outcome: RoundOutcome::Unstarted(err.to_string()),
                });
                continue;
            }
        };

        let start = head.saturating_add(1);
        let mut committer = SimCommitter::new(work, plan.exclusive, head);
        let outcome = drive_commit(&log, &mut committer, start, &plan.retry).await;

        records.push(RoundRecord {
            work,
            start,
            minted: committer.minted,
            outcome: outcome.into(),
        });
    }

    records
}

/// Drives one fold task's rounds against its own derived state, working
/// through transport failures the way a host-driven folder does.
async fn run_folder(log: SlotLog, rounds: usize, limit: u64) -> FoldRecord {
    let mut state = FoldState::default();
    let mut transport_failures = 0;
    let mut corruptions = Vec::new();

    for _ in 0..rounds {
        match drive_fold(&log, &mut state, limit).await {
            Ok(_) => {}
            Err(Error::Transport(_)) => transport_failures += 1,
            // Anything that is not a transport failure is the fold refusing,
            // which a healthy log never gives it cause to do.
            Err(refusal) => corruptions.push(refusal.to_string()),
        }
    }

    FoldRecord {
        state,
        transport_failures,
        corruptions,
    }
}

/// Runs one scenario to completion on the current runtime, then reads the log
/// back with the store quiesced.
pub async fn simulate(scenario: Scenario) -> Run {
    let Scenario {
        seed,
        knobs,
        workers,
        folders,
        fold_rounds,
        fold_limit,
    } = scenario;

    let store = Arc::new(SimStore::new(seed, knobs));
    let state = store.state();
    let log = SlotLog::new(store, ROOT);
    lock(&state).record(format!("scenario seed={seed} knobs={knobs:?}"));

    let planned_rounds = workers
        .iter()
        .map(|plan| usize::from(plan.rounds))
        .sum::<usize>();

    let committers: Vec<_> = workers
        .into_iter()
        .map(|plan| tokio::spawn(run_worker(log.clone(), plan)))
        .collect();
    let folder_tasks: Vec<_> = (0..folders)
        .map(|_| tokio::spawn(run_folder(log.clone(), fold_rounds, fold_limit)))
        .collect();

    let joined = tokio::time::timeout(RUN_TIMEOUT, async move {
        let mut rounds = Vec::new();
        for handle in committers {
            rounds.extend(handle.await.expect("a committer task must not panic"));
        }
        let mut folds = Vec::new();
        for handle in folder_tasks {
            folds.push(handle.await.expect("a fold task must not panic"));
        }

        (rounds, folds)
    })
    .await;

    let (rounds, folds) = joined.unwrap_or_else(|_| {
        panic!("seed {seed}: the run did not terminate within {RUN_TIMEOUT:?} of virtual time")
    });

    let mut trace = {
        let mut state = lock(&state);
        state.take_trace()
    };
    for record in &rounds {
        trace.push(format!(
            "round {} start={} ids={} -> {}",
            record.work,
            record.start,
            record.minted.len(),
            record.outcome.label()
        ));
    }
    for (index, fold) in folds.iter().enumerate() {
        trace.push(format!(
            "fold {index} cursor={} applied={:?} transport={} corruption={}",
            fold.state.cursor,
            fold.state.applied,
            fold.transport_failures,
            fold.corruptions.len()
        ));
    }

    lock(&state).quiesce();
    let tail = log.read_tail(1).await.expect("a quiesced log reads back");

    Run {
        seed,
        trace,
        rounds,
        planned_rounds,
        folds,
        slots: tail.slots,
        gap_at: tail.gap_at,
        log,
    }
}

/// Rounds each side of the backoff duel drives.
const DUEL_ROUNDS: u8 = 8;

/// One round of the backoff duel as each side finished it.
#[derive(Debug, Clone, Copy)]
pub struct DuelRound {
    /// The sequence the hot committer's work landed at.
    pub hot: Option<u64>,
    /// The sequence the steady committer's work landed at.
    pub steady: Option<u64>,
    /// Whether either side failed to commit at all.
    pub settled: bool,
}

/// Two committers racing the same sequences over one log, one retrying with a
/// near-zero base delay and one on the standard backoff. Faults are off: the
/// claim under test is distributional, and a lost put would confound it.
pub async fn duel(seed: u64) -> Vec<DuelRound> {
    let knobs = Knobs {
        latency_millis: (1, 8),
        put_fault_percent: 0,
        get_fault_percent: 0,
        list_fault_percent: 0,
        list_page: 2,
    };

    let store = Arc::new(SimStore::new(seed, knobs));
    let log = SlotLog::new(store, ROOT);

    let hot = WorkerPlan {
        worker: 1,
        rounds: DUEL_ROUNDS,
        exclusive: false,
        retry: RetryPolicy {
            max_attempts: 24,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..RetryPolicy::seeded(seed.wrapping_mul(0x1000_0001).wrapping_add(1))
        },
    };
    let steady = WorkerPlan {
        worker: 2,
        rounds: DUEL_ROUNDS,
        exclusive: false,
        retry: RetryPolicy {
            max_attempts: 24,
            ..RetryPolicy::seeded(seed.wrapping_mul(0x1000_0001).wrapping_add(2))
        },
    };

    let hot_task = tokio::spawn(run_worker(log.clone(), hot));
    let steady_task = tokio::spawn(run_worker(log, steady));
    let hot_records = hot_task.await.expect("the hot committer must not panic");
    let steady_records = steady_task
        .await
        .expect("the steady committer must not panic");

    hot_records
        .iter()
        .zip(&steady_records)
        .map(|(hot, steady)| DuelRound {
            hot: committed_at(hot),
            steady: committed_at(steady),
            settled: committed_at(hot).is_some() && committed_at(steady).is_some(),
        })
        .collect()
}

/// The sequence a round committed at, if it committed.
fn committed_at(record: &RoundRecord) -> Option<u64> {
    match record.outcome {
        RoundOutcome::Committed { sequence } => Some(sequence),
        _ => None,
    }
}
