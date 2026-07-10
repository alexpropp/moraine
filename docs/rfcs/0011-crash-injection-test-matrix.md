# RFC 0011: Enumerated crash-injection test matrix

- **Date:** 2026-07-09

## Summary

RFC 0001 mandates that integration tests cover "crash-shaped sequences
(commit, reopen, verify)," and RFCs 0004 and 0007 each restate a slice of
that obligation in their own words. "Crash-shaped" is the right instinct but
the wrong grain for a catalog whose worst failure mode is corruption: a
generic phrase does not tell an implementer *where* a process may die, nor
what must be true after it restarts. This RFC replaces the prose with a
named, enumerated matrix of crash-injection points — one row per reachable
failure seam in the commit, flush, cleanup, takeover, and initialization
paths — each pinned to the post-recovery invariant it verifies. Every
protocol RFC then references a row of this matrix instead of restating its
own crash story, and the integration suite iterates the matrix mechanically
rather than relying on whichever cases an author happened to imagine.

## Goals

- Enumerate every point at which a process can crash during a state-changing
  moraine operation, as a stable, named checklist — exhaustive over the
  seams the protocols actually expose, not a representative sample.
- For each point, state the single invariant that must hold after reopen, in
  terms an assertion can check against real SlateDB on in-memory
  `object_store` (RFC 0001's Integration tests, no store mocks).
- Give the matrix a stable identifier per row (a `CrashPoint`) so tests
  iterate it as data and a new seam cannot be added to the code without a
  corresponding row failing to exist.
- Consolidate: RFC 0004's "partial batch never observable" and RFC 0007's
  "cleanup idempotence" become rows here, cross-referenced, not duplicated.

Non-goals:

- **New protocol.** This RFC invents no mechanism. It tests the atomicity,
  fencing, and ordering invariants already specified in RFCs 0002–0008. If a
  row here cannot be made to pass, the bug is in the referenced RFC's
  implementation, not in this one.
- **Fuzzing / randomized fault injection.** RFC 0001 reserves `cargo-fuzz`
  for `store` codecs and the read path as future work. This matrix is
  enumerated and deterministic; a randomized fault-injection harness that
  crashes at arbitrary WAL offsets is a complementary future layer, noted in
  Open questions, not built here.
- **Real-object-storage crash behavior** (MinIO/localstack, torn multipart
  uploads). RFC 0001 lists real-object-storage tests as a later tier; this
  matrix runs on in-memory `object_store` where the crash seam is the store
  handle, not the network.

## Background: what "crash" means in moraine

Two invariants from earlier RFCs define the entire crash surface, and every
row below is an instance of one of them.

1. **One commit = one `WriteBatch` = one durable WAL flush** (RFC 0002,
   "Atomicity invariant"; RFC 0004, step 4). SlateDB batches are atomic: a
   crash leaves either the whole batch or none of it. So *within* a single
   commit there is no torn state to test — the only two observable outcomes
   are "flush landed" and "flush did not." The interesting crash points are
   therefore **not inside a batch** but at the **seams between a batch and
   the non-batch work around it**: the object-store PUTs that precede a flush
   (RFC 0005), the object-store DELETEs that follow an expiry (RFC 0007), the
   writer-epoch fence at takeover (RFC 0004), and the fencing-guarded
   genesis that births a fresh store (RFC 0002, D-rows below).

2. **Data before metadata** (RFC 0005 flush; RFC 0007 cleanup). Bytes are
   written before the catalog record that references them and deleted after
   the catalog record that referenced them is gone. Every multi-step path in
   moraine is ordered so that a crash at any seam leaves either an
   *orphaned-but-harmless* object (bytes with no live reference) or a
   *resumable* record (reference whose bytes still exist) — never a *live
   dangling reference* (a catalogued path whose bytes are gone).

The durability boundary is the WAL flush. "Inject a crash" means: stop the
process at a chosen seam without a graceful close, then reopen the store from
object storage and assert. Concretely, the harness realizes a crash two ways:

- **Seam crash** (multi-step paths). The test drives the operation up to a
  named seam — e.g. issues the Parquet PUTs of a flush but not the commit
  batch — then drops the writer handle and reopens. This requires the `txn`,
  flush, and GC code to be decomposable at those seams (a testing surface,
  not a production API; see Open questions).

- **Unflushed-batch crash** (single-batch atomicity). To observe the "flush
  did not land" outcome, the harness reopens a store whose last `WriteBatch`
  was staged but whose WAL was never durably written to object storage.
  SlateDB exposes exactly the knobs for this (see "The unflushed-WAL knob"
  below): open the store with `Settings { flush_interval: None, .. }`
  (background WAL flushing disabled), issue the commit with
  `WriteOptions { await_durable: false, .. }`, and then **abandon** the
  handle — never calling `Db::flush()` or `Db::close()`, both of which force
  the WAL durable. The in-memory `object_store` is the sole arbiter of
  durability: a batch that never reached it is invisible to a freshly opened
  handle. No corruption of SlateDB internals and no `unsafe` is involved.

A crash is thus identified by a `CrashPoint` — a stable enum whose variants
are exactly the rows below. A `#[cfg(any(test, feature = "fault-injection"))]`
hook consults an injected `Option<CrashPoint>` at each seam and unwinds the
operation there; production builds compile the hook to nothing.

### The unflushed-WAL knob

The "process died before its commit was durable" outcome (rows A1, C1, C3,
D2) is testable with SlateDB's public API alone — no fork, no internal
corruption, no `unsafe`. Three facts about SlateDB combine:

- **`WriteOptions { await_durable: bool }`**, default `true`. A write with
  `await_durable: false` returns as soon as it is in the in-memory WAL
  buffer, *before* that buffer is written to object storage. This is
  precisely a commit whose batch is staged but not yet durable. (moraine's
  production commit path uses `await_durable: true` — that is exactly RFC
  0002/0004's "one durable WAL flush" latency floor; the test overrides it to
  `false` only to freeze the pre-flush instant.)
- **`Settings { flush_interval: Option<Duration> }`**. Its doc: *"If this
  value is `None`, automatic flushing will be disabled."* Setting it to
  `None` guarantees no background task flushes the WAL out from under the
  test between the write and the reopen — the crash instant is deterministic,
  not a race against a 100 ms timer.
- **`Db::flush()` and `Db::close()` both force durability** (the `flush_interval`
  doc explicitly notes the app can flush "by closing the database"). So the
  harness simulates a crash by doing *neither*: it drops/leaks the writer
  handle and constructs a fresh `Db::open` over the same in-memory
  `object_store`. Because the WAL never reached the object store, the new
  handle's manifest and WAL replay simply do not contain the abandoned batch.

So A1 is: open with `flush_interval: None`, drive to head `N` durably (an
explicit `flush()`), stage the next commit with `await_durable: false`, then
open a second handle and assert `sys/head == N`. Reopening the second handle
is not incidental — a new SlateDB writer bumps the manifest `writer_epoch`
via CAS on open, which is the same mechanism that powers the takeover and
zombie-writer rows next.

### The fencing knob (takeover / zombie writer)

Rows C1 and C2 need no simulation either — SlateDB's writer fencing *is* the
behavior under test. Per SlateDB's manifest design, `writer_epoch` is a
monotonic `u64` a writer increments transactionally (manifest CAS) on
`Db::open`; a writer whose epoch is below the manifest's current
`writer_epoch` is a **zombie** and is fenced on its next SST/WAL write. The
harness drives the real protocol: open writer A, stage an
`await_durable: false` commit (C1's crash seam), open writer B over the same
`object_store` (B bumps the epoch — the takeover), assert B sees head `N`;
then have the still-live A attempt its flush/commit and assert it fails
**fenced**, writing nothing (C2). No injected fault — just two real handles
and SlateDB's own split-brain guard.

## Design: the matrix

Rows are grouped by path. Each has a stable name, the seam where the process
dies, the invariant asserted after reopen, and the RFC that owns the
mechanism. "Reopen" always means: construct a fresh moraine handle over the
same object store, with no in-memory carryover from the crashed process.

### A. Single-commit atomicity — RFC 0002 / RFC 0004

| Point | Crash seam | Post-reopen invariant |
|---|---|---|
| **A1** `CommitBatchUnflushed` | Batch staged (step 3), WAL flush withheld (step 4 not durable) | `sys/head` still `N`; snapshot `N+1` absent; no `cur`/`hist`/`inline`/`tstat` record from the commit is visible. All-or-none: none. |
| **A2** `CommitBatchLandedNoAck` | Flush landed, process dies before returning to caller | Commit durable: `sys/head` = `N+1`, snapshot `N+1` resolves fully. A caller re-drive of the same logical operation runs against the advanced head and never corrupts: ids never collide (counters advanced with the landed commit) and logically-guarded operations surface their guard (`AlreadyExists` for a re-driven create, `CommitConflict`/`NotFound` where the premise moved). The invariant's scope is deliberate: a re-driven *data-only* operation — `register_data_file` of the same file, an identical inline insert — is a fresh commit and lands a second time; nothing in the protocol dedups it, absent the idempotence machinery the `seqnum` open question defers. A2 asserts consistency of the landed commit and guard-surfacing on re-drive, not universal exactly-once. |
| **A3** `DropAtomicity` | A `DROP TABLE`/`DROP SCHEMA` that ends *many* records (the table row, every column, every `file`/`delfile` → `hist`; every live `inline` chunk deleted — or archived, RFC 0016), crashed at every reachable WAL boundary | Reopen shows the table **fully present** or **fully dropped** — never a torn table missing some columns or files. There is **no** reachable seam "after the first tombstone, before the last," because the entire multi-tombstone `DROP` is one `WriteBatch` (RFC 0002). This row's job is to *prove that absence*: it fails if any code path splits a `DROP` across batches. |

A3 gives the user-requested "after first tombstone of a multi-tombstone
`DROP` but before the last" scenario a home — by asserting it is
**unreachable**. In a relational catalog a multi-row `DELETE` can tear; in
moraine it cannot, and the test is a regression guard on that guarantee. If someone later implements a `DROP` of a huge table as chunked
batches "for memory reasons," A3 goes red and forces the row-count limit to
be handled some other way (or this invariant to be explicitly renegotiated in
an RFC).

### B. Multi-step commits — data before metadata (RFC 0005 flush, RFC 0007 cleanup)

| Point | Crash seam | Post-reopen invariant |
|---|---|---|
| **B1** `FlushAfterPutBeforeBatch` | Inline flush: all Parquet PUT(s) done, commit batch not staged | Catalog unchanged: inline chunks still live, no `file`/`delfile` record exists. The written Parquet is an **orphan** (no catalog reference) reclaimed by RFC 0007's orphaned-file sweep. Re-running flush is idempotent — it re-writes from the still-live inline chunks and commits cleanly. No live dangling reference. |
| **B2** `FlushMidMultiPut` | Inline flush writing several data/delete files: some PUTs done, some not | Identical outcome to B1 — *every* written file is an orphan because the referencing batch never landed. Partial PUT progress never produces a partially-referenced file set. |
| **B3** `CleanupAfterDeleteBeforeBatch` | GC cleanup (RFC 0007): object `DELETE` issued, `gcfile`-removal batch not landed | Resumable state: the `gcfile` record survives, its bytes are gone. Re-run re-issues the `DELETE` (no-op on a missing key) and removes the record; converges with no error, no data loss. (This is RFC 0007's "cleanup idempotence" obligation, relocated here.) |
| **B4** `CleanupMidMultiDelete` | GC cleanup selecting N `gcfile` records: crash after the k-th object `DELETE`, before the batch | The batch that would forget the k deleted records never landed, so **all N `gcfile` records survive**; k of their files are already gone (safe — bytes-before-record), N−k remain. Re-run deletes the rest and forgets all N. The multi-tombstone analogue of A3, but on the reclamation path, where — unlike `DROP` — the steps are *deliberately* not one batch (the DELETEs are object-store ops outside SlateDB) and idempotence, not atomicity, is what carries safety. |

The B rows encode the deep asymmetry the user's checklist gestures at: a
multi-tombstone **`DROP`** (A3) is safe by *atomicity* (one batch, torn state
unreachable), whereas a multi-object **cleanup** (B4) is safe by
*idempotence* (steps not atomic, but ordered bytes-before-record so any
prefix is resumable). Testing them the same way would miss that one of them
has no seam to inject at all.

### C. Writer lifecycle and fencing — RFC 0004 topology / RFC 0002 fenced writer

| Point | Crash seam | Post-reopen invariant |
|---|---|---|
| **C1** `TakeoverMidCommit` | Writer A dies mid-commit; writer B opens the store and takes over (SlateDB epoch fence) | B reads a consistent head. If A's flush had not landed, B sees `sys/head` = `N` and A's batch is invisible (⇒ A1 under a second writer). If A's flush *had* landed, B sees `N+1` and continues from there. No split-brain, no partial-A visibility. |
| **C2** `ZombieWriterAfterTakeover` | A resumes and attempts its CAS/flush *after* B has taken over and bumped the writer epoch | A is fenced: it loses the `sys/head` CAS or is epoch-fenced by SlateDB, returns a typed error, and writes nothing. The store is byte-identical to the no-zombie timeline. Confirms RFC 0004's "an accidental second writer never corrupts." |
| **C3** `GroupCommitCrash` | Crash mid group-commit — several catalog commits staged into one batch (RFC 0004, "Group commit") | All-or-none over the *whole group*: reopen shows every member committed or none. No partial group (a batch is a batch regardless of how many logical commits it carries). Pairs with A1/A2 at group grain. |

C1 is the user's "during writer takeover" point. It decomposes into the
two A-outcomes under a *new* process rather than being a third outcome — the
takeover itself adds no torn state; it only changes *who* observes the
all-or-none result. C2 is its necessary twin: takeover is only safe if the
displaced writer, should it wake up, cannot still land its stale batch.

### D. Fresh-catalog initialization — RFC 0002 (`sys/format`, `sys/head`)

| Point | Crash seam | Post-reopen invariant |
|---|---|---|
| **D1** `ConcurrentFreshInit` | Two processes initialize the *same empty* store concurrently, each trying to write `sys/format` + genesis `sys/head`/snapshot `0` | Exactly one wins. There is no create-if-absent primitive to lean on; the guards are the real ones: SlateDB's `writer_epoch` (the second `Db::open` fences the first — the fenced initializer's genesis batch fails, writing nothing) and, within one writer, transactional write-write conflict detection on `sys/format`. The loser observes the store already initialized and adopts it (or returns a typed error), never writing a second `sys/format`, a divergent genesis snapshot, or a conflicting head. Reopen shows a single, coherent genesis. |
| **D2** `InitMidWrite` | A single initializer dies after writing `sys/format` but before `sys/head`/snapshot `0` | Reopen shows the store **empty** (genesis re-attempted from scratch) or **fully initialized** — never half-initialized (a `sys/format` with no head). This forces initialization to be **one `WriteBatch`** like any commit, i.e. the atomicity invariant extends to genesis. |

D1 is the user's "two processes initializing a fresh catalog" point. It is
subtly different from every other row: there is no `sys/head` to conflict
on yet, so the guard is writer fencing across processes plus transactional
conflict detection on `sys/format` within one, and the loser's correct
behavior is to **adopt**, not to conflict — a fresh store is not a true
conflict but a benign "someone beat me to genesis" race.
D2 forces genesis itself under the one-batch rule so that D1's winner never
leaves a torn genesis for the loser (or a reader) to observe.

### Coverage argument

The matrix is exhaustive over the two invariants in Background:

- **Every single-batch operation** (ordinary commit, multi-tombstone `DROP`,
  group commit, genesis) reduces to A1/A2 atomicity — rows A1, A2, A3, C3,
  D2 instantiate it across the operations that could plausibly be tempted to
  span batches.
- **Every multi-step (non-batch) operation** in moraine is bytes-before /
  bytes-after a batch: inline flush (B1, B2), GC cleanup (B3, B4). There are
  no other multi-step state-changing paths — compaction (RFC 0008) reduces to
  a flush-shaped "PUT the rewritten Parquet, then one commit batch that ends
  the inputs," so it is covered by the B-rows' shape; a compaction-specific
  row is added only if RFC 0008 introduces a seam the flush rows do not
  already cover (Open questions).
- **Every writer-lifecycle transition** is C1/C2, and **birth** is D1/D2.

A seam that is not an instance of one of these is, by RFC 0002's atomicity
invariant, not reachable. The `CrashPoint` enum is the executable form of
that claim: adding a fault-injection hook in the code without a matching enum
variant fails to compile the test that iterates all variants.

## Test obligations

Per RFC 0001, these run in `crates/moraine/tests/` against real SlateDB on
in-memory `object_store`, no store mocks. This RFC's obligation is a single
data-driven test that iterates `CrashPoint`:

- For each variant, build the pre-crash state, inject the crash at that seam,
  reopen, and assert the row's invariant. A variant with no assertion is a
  test failure (prevents a row from being silently stubbed out).
- Idempotence rows (A2, B1, B3, B4) additionally re-drive the operation after
  reopen and assert convergence — no id collision, no torn state, and no
  error other than the typed ones the protocol permits. For A2 the assertion
  matches the row's stated scope: guarded operations surface their guard; a
  data-only re-drive is asserted to land as a *clean second commit* (the
  duplicate is the caller's to prevent until the `seqnum` question is
  settled), never a corrupted one.
- The absence rows (A3, D2) assert torn intermediate states are
  *unobservable* across **all** WAL boundaries of the operation, not just one
  — the test enumerates the boundaries and checks each.

This test **replaces** the generic "crash-shaped sequences (commit, reopen,
verify)" line in RFC 0001's testing table and **subsumes** the crash bullets
in RFC 0004 ("partial batch never observable") and RFC 0007 ("cleanup
idempotence"); those RFCs cite the corresponding rows (A1/A2, B3) rather than
carrying their own crash prose. A `docs/plans/` entry tracks that mechanical
edit to those three RFCs.

## Open questions

- **SlateDB crash-simulation surface — resolved (source-verified).**
  Rows A1/C1/C3/D2 (reopen a store whose last batch was not durably flushed)
  and C2 (fence a stale writer) are all reachable through SlateDB's *public*
  API, so the "unflushed" arm does **not** fall back to the fuzzing tier. The
  mechanism is the `flush_interval: None` + `await_durable: false` +
  abandon-the-handle recipe in "The unflushed-WAL knob," and the real
  `writer_epoch` CAS-on-open for takeover/zombie fencing. Both knobs are
  confirmed present in the pinned 0.14.x source
  (`Settings { flush_interval: Option<Duration> }`,
  `WriteOptions { await_durable: bool }`). The residual confirmations are
  also source-settled: (a) a *plain drop* of the abandoned handle performs
  no flush — there is **no `Drop` impl on `Db` at all** (only
  `DbTransaction` and `DbSnapshot` implement `Drop`, for rollback and
  deregistration bookkeeping), so no `mem::forget` gymnastics are needed;
  the WAL's only durability triggers are the background timer
  (`flush_interval`, disabled by `None`), the `max_wal_size` threshold
  (unreachable for small test batches), and explicit `flush()`/`close()`;
  (b) writer fencing is `FenceableManifest::init_writer` bumping
  `writer_epoch` via manifest CAS on every `Db::open` — store-agnostic by
  construction, so it holds on in-memory `object_store`; the probe test
  regression-pins it.
- **`seqnum` and idempotent re-drive (rows A2/B1/B3/B4).** `WriteOptions` also
  carries a user `seqnum`. Whether moraine pins sequence numbers itself or
  lets SlateDB generate them affects how an A2 re-drive is detected as a
  duplicate versus a fresh commit. Settle with RFC 0004's implementation; the
  invariant (no duplicate snapshot/id) holds either way, but the assertion's
  shape depends on it.
- **Where the seam hooks live.** The `CrashPoint` injection points are a
  test-only surface on `txn`/flush/GC. Confirm they can be `#[cfg(...)]`-gated
  with zero production footprint and no `unsafe` (RFC 0001 forbids `unsafe`
  in `moraine`), rather than leaking a fault-injection parameter into public
  signatures.
- **Compaction seams (RFC 0008).** Does compaction introduce any state-change
  seam not already shaped like an inline flush (PUT-then-batch)? If it writes
  multiple output files across multiple commits, it needs its own B-style
  rows; if it is one flush-shaped commit, the existing rows cover it. Settle
  when RFC 0008 lands as Implemented.
- **Randomized fault injection as a successor.** Once the enumerated matrix
  is green and the codecs stabilize, a `cargo-fuzz` target that crashes at
  arbitrary WAL offsets and reopens (asserting the same two invariants) would
  catch seams this enumeration missed. Complementary future work under RFC
  0001's fuzzing tier, not a replacement for the named matrix (named rows
  give regressions a stable identity; fuzzing gives coverage breadth).

## Alternatives considered

- **Keep the generic "crash-shaped sequences" prose (RFC 0001 status quo).**
  Rejected: it under-specifies the seams, so coverage depends on author
  imagination, and it duplicates partial crash stories across RFCs 0004 and
  0007 that can drift out of sync. An enumerated matrix is the checklist the
  user asked for and the single source those RFCs cite.
- **Randomized/fuzz crash injection only.** Rejected as the *primary*
  mechanism: a fuzzer that crashes at random offsets gives breadth but no
  stable per-scenario identity, so a regression cannot be named, pinned, or
  guaranteed to re-run. The named matrix is the floor; fuzzing is a future
  ceiling (Open questions).
- **One crash test per RFC, owned by that RFC.** Rejected: the crash
  invariants are cross-cutting (atomicity and data-before-metadata recur in
  commit, flush, cleanup, takeover, init), so per-RFC ownership re-derives
  the same two invariants five times and makes the coverage argument
  impossible to state in one place. Centralizing the matrix and citing rows
  from each RFC keeps the invariants stated once.
- **Assert torn `DROP` states are handled rather than unreachable.**
  Rejected: it would encode the *wrong* guarantee. moraine's whole atomicity
  claim is that a multi-tombstone `DROP` cannot tear; a test that tolerates a
  torn intermediate would silently permit chunked-`DROP` implementations that
  violate RFC 0002. A3 asserts unreachability precisely to forbid that.
