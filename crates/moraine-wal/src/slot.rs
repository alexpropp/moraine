//! The commit-slot log: fixed-width sequence naming, the create-if-absent
//! race that arbitrates each slot, tail enumeration with hole detection,
//! and oldest-first prefix truncation.

use std::sync::Arc;

use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};

use crate::{envelope::Envelope, error::Error};

/// The path segment every slot object lives under.
const COMMITS: &str = "commits";

/// Sequence names are zero-padded to the decimal width of `u64::MAX`, so
/// lexicographic order over names is numeric order over sequences.
const SEQUENCE_WIDTH: usize = 20;

/// A commit-slot log: totally ordered, immutable slots under
/// `<root>/commits/`, each written exactly once with a create-if-absent
/// conditional put.
///
/// The conditional put is the whole arbitration mechanism: exactly one
/// committer can win each sequence, however many race it, and nothing above
/// this layer needs a lock. What a slot's payload *means* is the embedder's
/// business — this type only owns the shape.
#[derive(Debug, Clone)]
pub struct SlotLog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

/// The outcome of racing one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRace {
    /// The put landed: this committer owns the sequence.
    Won,
    /// The sequence was already taken.
    Lost,
}

/// The resolved outcome of racing one slot: a loss carries the winning
/// envelope, because a loser always needs it (rebase is mandatory work).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// This committer owns the sequence.
    Won,
    /// Another committer owns the sequence; here is what it wrote.
    Lost(Envelope),
}

/// A tail read: the contiguous run from the requested sequence, plus
/// whether the log continues past a hole.
#[derive(Debug, Clone)]
pub struct Tail {
    /// The contiguous run from the requested sequence, ascending.
    pub slots: Vec<(u64, Envelope)>,
    /// A sequence absent while higher sequences exist. The log is damaged —
    /// a slot was destroyed out from under the protocol (an external
    /// delete, a lifecycle rule) — and it is never an end of log: replaying
    /// past it is impossible, and a committer that raced it would fork
    /// history behind everyone still replaying. Callers must fail loudly,
    /// never serve the prefix as if it were the head.
    pub gap_at: Option<u64>,
}

impl SlotLog {
    /// A log rooted at `root`, with slots under `<root>/commits/`. An empty
    /// `root` puts them at `commits/`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, root: &str) -> Self {
        let prefix: Path = root
            .split('/')
            .filter(|part| !part.is_empty())
            .chain(std::iter::once(COMMITS))
            .collect();

        Self { store, prefix }
    }

    /// The object path of one sequence.
    pub(crate) fn slot_path(&self, sequence: u64) -> Path {
        self.prefix
            .clone()
            .join(format!("{sequence:0SEQUENCE_WIDTH$}"))
    }

    /// Races `sequence` with a create-if-absent put.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the put neither landed nor reported
    /// the slot already taken; the put's outcome is then unknown, which is
    /// what [`SlotLog::commit_slot`] exists to resolve.
    pub async fn put_slot(&self, sequence: u64, envelope: &Envelope) -> Result<SlotRace, Error> {
        let bytes = envelope.encode();
        let options = PutOptions::from(PutMode::Create);
        match self
            .store
            .put_opts(&self.slot_path(sequence), bytes.into(), options)
            .await
        {
            Ok(_) => Ok(SlotRace::Won),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(SlotRace::Lost),
            Err(err) => Err(Error::Transport(format!("slot {sequence}: {err}"))),
        }
    }

    /// Reads one slot; `None` when the sequence has not been won.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the get failed, or
    /// [`Error::Corruption`] if the object is not a valid envelope.
    pub async fn read_slot(&self, sequence: u64) -> Result<Option<Envelope>, Error> {
        match self.store.get(&self.slot_path(sequence)).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|err| Error::Transport(format!("slot {sequence}: {err}")))?;

                Ok(Some(Envelope::decode(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(Error::Transport(format!("slot {sequence}: {err}"))),
        }
    }

    /// Deletes one slot. Slots are immutable, so deletion is idempotent: an
    /// already-absent slot is not an error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the delete failed.
    pub async fn delete_slot(&self, sequence: u64) -> Result<(), Error> {
        self.delete_if_present(sequence).await?;

        Ok(())
    }

    /// Races `sequence`: `Won` on a landed put; on an already-taken slot,
    /// reads the winner back and returns `Lost`.
    ///
    /// A put whose outcome is unknown (a transport failure that may have
    /// landed) is resolved from the log itself rather than guessed: re-read
    /// the slot — an envelope carrying any of `envelope`'s transaction ids
    /// is this commit (`Won`); a different envelope is `Lost`; an absent
    /// slot means the put did not land, and the transport error surfaces.
    /// This is the exactly-once mechanic.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the put's outcome stayed unknown —
    /// the slot is absent on read-back, so the commit did not land and the
    /// same sequence is still there to race again. Returns
    /// [`Error::Corruption`] if a slot's bytes are not a valid envelope, or
    /// if a slot reported as taken reads absent.
    pub async fn commit_slot(
        &self,
        sequence: u64,
        envelope: &Envelope,
    ) -> Result<CommitOutcome, Error> {
        match self.put_slot(sequence, envelope).await {
            Ok(SlotRace::Won) => Ok(CommitOutcome::Won),
            Ok(SlotRace::Lost) => Ok(CommitOutcome::Lost(self.winner_of(sequence).await?)),
            Err(corruption @ Error::Corruption(_)) => Err(corruption),
            Err(unknown) => match self.read_slot(sequence).await? {
                Some(landed) if is_this_attempt(&landed, envelope) => Ok(CommitOutcome::Won),
                Some(winner) => Ok(CommitOutcome::Lost(winner)),
                None => Err(unknown),
            },
        }
    }

    /// The tail from `from`: one LIST of the prefix (fixed-width names make
    /// lexicographic order numeric), then a GET per listed slot.
    ///
    /// Returns the contiguous run starting at `from`, and reports a hole
    /// when the listing shows slots *above* an absent one. Slots below
    /// `from` are never inspected, so a legitimately truncated prefix reads
    /// as no hole.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing or a get failed, or
    /// [`Error::Corruption`] if a slot's bytes are not a valid envelope.
    pub async fn read_tail(&self, from: u64) -> Result<Tail, Error> {
        let mut slots = Vec::new();
        let mut gap_at = None;

        let mut expected = from;
        for sequence in self.list_sequences(from).await? {
            if sequence != expected {
                gap_at = Some(expected);
                break;
            }

            let envelope = self.read_slot(sequence).await?.ok_or_else(|| {
                Error::Corruption(format!(
                    "slot {sequence} was listed but reads absent; it was destroyed \
                     outside the protocol"
                ))
            })?;
            slots.push((sequence, envelope));
            expected = sequence.saturating_add(1);
        }

        Ok(Tail { slots, gap_at })
    }

    /// Deletes slots `..=through`, oldest first; already-missing slots count
    /// as deleted. Returns how many objects were removed. Choosing a safe
    /// `through` is the caller's policy, not this crate's.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing or a delete failed; the
    /// slots already deleted stay deleted, so a retry resumes.
    pub async fn truncate_through(&self, through: u64) -> Result<u64, Error> {
        let mut removed = 0;
        for sequence in self.list_sequences(0).await? {
            if sequence > through {
                break;
            }
            if self.delete_if_present(sequence).await? {
                removed += 1;
            }
        }

        Ok(removed)
    }

    /// How many contiguous slots are present from `from` — one LIST, no
    /// bodies fetched. The staleness signal.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the listing failed.
    pub async fn tail_length(&self, from: u64) -> Result<u64, Error> {
        let mut length = 0;

        let mut expected = from;
        for sequence in self.list_sequences(from).await? {
            if sequence != expected {
                break;
            }
            length += 1;
            expected = sequence.saturating_add(1);
        }

        Ok(length)
    }

    /// Scans the tail from `from` for a transaction id; the sequence of the
    /// slot that carries it, if any.
    ///
    /// A hole in the tail refuses as [`Error::Corruption`] unless the id was
    /// found below it: past a destroyed slot the scan cannot rule the
    /// transaction out, and reporting "absent" for a transaction that may
    /// have committed is the one wrong answer.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the tail read failed, or
    /// [`Error::Corruption`] on a malformed slot or a tail with a hole the
    /// id was not found below.
    pub async fn find_transaction(
        &self,
        from: u64,
        transaction_id: [u8; 16],
    ) -> Result<Option<u64>, Error> {
        let tail = self.read_tail(from).await?;
        let found = tail
            .slots
            .iter()
            .find(|(_, envelope)| envelope.contains_transaction(transaction_id))
            .map(|(sequence, _)| *sequence);

        match (found, tail.gap_at) {
            (Some(sequence), _) => Ok(Some(sequence)),
            (None, Some(gap)) => Err(Error::Corruption(format!(
                "the tail from {from} has a hole at {gap}; a transaction cannot be \
                 ruled out past a destroyed slot"
            ))),
            (None, None) => Ok(None),
        }
    }

    /// The envelope of a slot known to be taken.
    async fn winner_of(&self, sequence: u64) -> Result<Envelope, Error> {
        self.read_slot(sequence).await?.ok_or_else(|| {
            Error::Corruption(format!(
                "slot {sequence} is taken but reads absent; it was destroyed outside \
                 the protocol"
            ))
        })
    }

    /// Deletes one slot, reporting whether an object was actually removed.
    async fn delete_if_present(&self, sequence: u64) -> Result<bool, Error> {
        match self.store.delete(&self.slot_path(sequence)).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(Error::Transport(format!("slot {sequence}: {err}"))),
        }
    }

    /// Every sequence present at or above `from`, ascending, from one LIST.
    /// Objects under the prefix whose name is not a sequence are not slots
    /// and are skipped; a slot that went missing still shows as a hole.
    async fn list_sequences(&self, from: u64) -> Result<Vec<u64>, Error> {
        // `list_with_offset` is exclusive, so offset by the predecessor's
        // name; below sequence 1 the prefix itself sorts before every slot.
        let offset = match from.checked_sub(1) {
            Some(previous) => self.slot_path(previous),
            None => self.prefix.clone(),
        };

        let listing: Vec<_> = self
            .store
            .list_with_offset(Some(&self.prefix), &offset)
            .try_collect()
            .await
            .map_err(|err| Error::Transport(format!("listing slots from {from}: {err}")))?;

        let mut sequences: Vec<u64> = listing
            .iter()
            .filter_map(|meta| parse_sequence(&meta.location))
            .filter(|sequence| *sequence >= from)
            .collect();
        sequences.sort_unstable();

        Ok(sequences)
    }
}

/// Whether `landed` is the envelope this attempt put: any of the attempt's
/// transaction ids appearing in it settles an ambiguous put. Transaction ids
/// are envelope structure, not payload, which is what lets this layer own
/// the question.
fn is_this_attempt(landed: &Envelope, attempt: &Envelope) -> bool {
    attempt
        .commits
        .iter()
        .any(|commit| landed.contains_transaction(commit.transaction_id))
}

/// The sequence a slot object's path names, or `None` if it names no slot.
fn parse_sequence(location: &Path) -> Option<u64> {
    let name = location.parts().next_back()?;
    let name = name.as_ref();
    if name.len() != SEQUENCE_WIDTH || !name.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    name.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };

    use super::*;
    use crate::envelope::{Commit, Envelope, SlotPayload, SlotWrite};

    fn envelope_with_id(transaction_id: [u8; 16]) -> Envelope {
        Envelope {
            commits: vec![Commit {
                transaction_id,
                payload: SlotPayload {
                    validated_head: 0,
                    changes_made: String::new(),
                    writes: vec![],
                },
            }],
        }
    }

    #[tokio::test]
    async fn racing_puts_admit_exactly_one_winner() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let log = SlotLog::new(store, "cat");
        let envelope = Envelope { commits: vec![] };
        let first = log.put_slot(1, &envelope).await.unwrap();
        let second = log.put_slot(1, &envelope).await.unwrap();
        assert!(matches!(first, SlotRace::Won));
        assert!(matches!(second, SlotRace::Lost));
    }

    #[tokio::test]
    async fn slots_roundtrip_and_missing_reads_none() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        assert!(log.read_slot(1).await.unwrap().is_none());
        let envelope = Envelope {
            commits: vec![Commit {
                transaction_id: [7; 16],
                payload: SlotPayload {
                    validated_head: 41,
                    changes_made: String::new(),
                    writes: vec![SlotWrite {
                        key: vec![1],
                        value: None,
                    }],
                },
            }],
        };
        log.put_slot(1, &envelope).await.unwrap();
        assert_eq!(log.read_slot(1).await.unwrap().unwrap(), envelope);
        assert!(
            log.read_slot(1)
                .await
                .unwrap()
                .unwrap()
                .contains_transaction([7; 16])
        );
    }

    #[test]
    fn sequence_names_are_fixed_width_and_ordered() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        assert_eq!(
            log.slot_path(42).as_ref(),
            "cat/commits/00000000000000000042"
        );
        assert!(log.slot_path(9).as_ref() < log.slot_path(10).as_ref());
    }

    /// An empty root leaves the slots directly under `commits/`, with no
    /// leading delimiter.
    #[test]
    fn an_empty_root_needs_no_leading_delimiter() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "");
        assert_eq!(log.slot_path(1).as_ref(), "commits/00000000000000000001");
    }

    #[tokio::test]
    async fn a_lost_race_returns_the_winning_envelope() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let winner = envelope_with_id([1; 16]);
        let loser = envelope_with_id([2; 16]);
        assert!(matches!(
            log.commit_slot(1, &winner).await.unwrap(),
            CommitOutcome::Won
        ));
        match log.commit_slot(1, &loser).await.unwrap() {
            CommitOutcome::Lost(read_back) => assert_eq!(read_back, winner),
            CommitOutcome::Won => unreachable!("slot 1 was taken"),
        }
    }

    #[tokio::test]
    async fn read_tail_stops_at_the_first_absent_slot_and_reports_a_hole() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope { commits: vec![] };
        for sequence in [1, 2, 4] {
            log.put_slot(sequence, &envelope).await.unwrap();
        }
        // Slot 3 is missing while 4 exists: a destroyed slot, not an end.
        let tail = log.read_tail(1).await.unwrap();
        assert_eq!(
            tail.slots.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(tail.gap_at, Some(3));

        // A clean end of log reports no hole.
        let clean = SlotLog::new(Arc::new(InMemory::new()), "cat");
        clean.put_slot(1, &envelope).await.unwrap();
        let tail = clean.read_tail(1).await.unwrap();
        assert_eq!(tail.slots.len(), 1);
        assert_eq!(tail.gap_at, None);
        assert!(clean.read_tail(5).await.unwrap().slots.is_empty());
    }

    #[tokio::test]
    async fn a_truncated_prefix_is_not_a_hole() {
        // Truncation deletes a prefix; reading from above it sees no gap.
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope { commits: vec![] };
        for sequence in 1..=4 {
            log.put_slot(sequence, &envelope).await.unwrap();
        }
        log.truncate_through(2).await.unwrap();
        let tail = log.read_tail(3).await.unwrap();
        assert_eq!(tail.slots.len(), 2);
        assert_eq!(tail.gap_at, None);
    }

    #[tokio::test]
    async fn truncate_through_deletes_oldest_first_and_tolerates_gaps() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        let envelope = Envelope { commits: vec![] };
        for sequence in [1, 3, 4] {
            log.put_slot(sequence, &envelope).await.unwrap();
        }
        assert_eq!(log.truncate_through(3).await.unwrap(), 2); // 1 and 3; 2 already gone
        assert!(log.read_slot(1).await.unwrap().is_none());
        assert!(log.read_slot(4).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_length_counts_without_bodies_and_find_transaction_scans() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        for (sequence, id) in [(1, [1; 16]), (2, [2; 16])] {
            log.put_slot(sequence, &envelope_with_id(id)).await.unwrap();
        }
        assert_eq!(log.tail_length(1).await.unwrap(), 2);
        assert_eq!(log.tail_length(3).await.unwrap(), 0);
        assert_eq!(log.find_transaction(1, [2; 16]).await.unwrap(), Some(2));
        assert_eq!(log.find_transaction(1, [9; 16]).await.unwrap(), None);
    }

    /// A hole means the scan cannot rule the transaction out, so it refuses
    /// rather than reporting a transaction that may have committed as
    /// absent.
    #[tokio::test]
    async fn find_transaction_refuses_a_tail_with_a_hole() {
        let log = SlotLog::new(Arc::new(InMemory::new()), "cat");
        for (sequence, id) in [(1_u64, [1_u8; 16]), (3, [3; 16])] {
            log.put_slot(sequence, &envelope_with_id(id)).await.unwrap();
        }
        let err = log.find_transaction(1, [3; 16]).await.unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
        // The slots below the hole still answer.
        assert_eq!(log.find_transaction(1, [1; 16]).await.unwrap(), Some(1));
    }

    /// Where a put fails relative to the object landing. The two cases are
    /// indistinguishable to the caller and must resolve differently, which
    /// is the whole reason the ambiguity rule lives in this crate.
    #[derive(Debug, Clone, Copy)]
    enum FaultPoint {
        /// The object lands, then the response is lost: the ambiguous put.
        AfterPut,
        /// The put never reaches the store: the slot stays absent.
        BeforePut,
    }

    /// Wraps a real [`InMemory`] store and fails `put_opts` at
    /// [`FaultPoint`] while the fault is armed; every other operation
    /// forwards untouched.
    #[derive(Debug)]
    struct FaultyPut {
        inner: InMemory,
        fault_point: FaultPoint,
        armed: AtomicBool,
    }

    impl FaultyPut {
        fn armed(fault_point: FaultPoint) -> Self {
            Self {
                inner: InMemory::new(),
                fault_point,
                armed: AtomicBool::new(true),
            }
        }

        fn disarm(&self) {
            self.armed.store(false, Ordering::Relaxed);
        }
    }

    impl std::fmt::Display for FaultyPut {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FaultyPut({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FaultyPut {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if !self.armed.load(Ordering::Relaxed) {
                return self.inner.put_opts(location, payload, opts).await;
            }

            // What a lost response looks like from the caller's side: no
            // information about whether the object exists.
            let unknown = object_store::Error::Generic {
                store: "fault",
                source: "the put's outcome is unknown".into(),
            };
            match self.fault_point {
                FaultPoint::AfterPut => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(unknown)
                }
                FaultPoint::BeforePut => Err(unknown),
            }
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
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
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

    /// The exactly-once mechanic: a put that landed under a lost response
    /// is resolved from the log by transaction id, not guessed.
    #[tokio::test]
    async fn an_ambiguous_put_that_landed_resolves_to_won() {
        let log = SlotLog::new(Arc::new(FaultyPut::armed(FaultPoint::AfterPut)), "cat");
        let envelope = envelope_with_id([3; 16]);
        assert!(matches!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        ));
        assert_eq!(log.read_slot(1).await.unwrap().unwrap(), envelope);
    }

    /// An ambiguous put whose object is absent on read-back genuinely did
    /// not land, so the transport error surfaces — and the same sequence is
    /// still there to win on the retry.
    #[tokio::test]
    async fn an_ambiguous_put_that_never_landed_surfaces_the_transport_error() {
        let store = Arc::new(FaultyPut::armed(FaultPoint::BeforePut));
        let log = SlotLog::new(store.clone(), "cat");
        let envelope = envelope_with_id([4; 16]);

        let err = log.commit_slot(1, &envelope).await.unwrap_err();
        assert!(matches!(err, Error::Transport(_)), "{err}");
        assert!(log.read_slot(1).await.unwrap().is_none());

        store.disarm();
        assert!(matches!(
            log.commit_slot(1, &envelope).await.unwrap(),
            CommitOutcome::Won
        ));
    }
}
