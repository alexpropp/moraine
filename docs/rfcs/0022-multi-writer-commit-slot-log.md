# RFC 0022: Multi-writer commits over a commit-slot log

- **Date:** 2026-07-27

## Summary

Takes up the multi-writer coordination design RFC 0004 declared out of
scope. RFC 0004's topology is single-writer, many-readers: exactly one
process holds the read-write `Db`, so a fleet of independent DuckDB
clients cannot commit concurrently — the common lakehouse shape. This RFC
adds an opt-in **multi-writer topology** that supports it with no change
to SlateDB and no new stateful service.

The mechanism moves the commit point out of SlateDB and into the bucket.
A **commit-slot log** — immutable objects written with create-if-absent
conditional puts — becomes the source of truth for commits; exactly one
committer can win each slot, so the object store's conditional put
serializes writers the way SlateDB's transactional head-CAS does today.
SlateDB is demoted to a **derived index** of the log, maintained by a
single fenced **folder** process using the existing single-writer
machinery unchanged. Commit liveness depends on one process and the
bucket; a commit is durable the moment its slot PUT is acknowledged.

The design ships in two stages. **Stage 1 is leaderless and complete**:
the slot log and the folder — no coordinator, no RPC surface, no
discovery. **Stage 2 adds the batcher** — an advisory, self-appointed
group-commit funnel discovered through the log itself — triggered by
measured contention and never a liveness or safety dependency. Safety
never depends on clocks, leases, leadership, or any node's disk.

## Goals

- **Fleet multi-writer.** N independent DuckDB processes commit
  concurrently against one catalog with nothing beyond the bucket.
- **Liveness = one process + the bucket.** No quorum, reachable leader,
  or lease renewal is ever a precondition for commit progress.
- **Safety from conditional puts alone.** Leadership (the batcher) is
  advisory: wrong, stale, or duplicated leadership degrades throughput,
  never correctness.
- **Durability at the slot.** The commit record and the durable artifact
  are the same object; no acknowledged commit is ever the sole property
  of any node's disk.
- **Exactly-once client outcome.** A committer that crashes mid-commit
  can always determine whether its commit landed; a retry never
  double-applies.
- **Existing semantics preserved.** RFC 0004's conflict model and
  typed-conflict surface are unchanged — what moves is *where* the race
  is arbitrated. RFC 0002's atomicity holds in the index: one slot folds
  as exactly one `WriteBatch`, fold cursor included.
- **Additive staging.** Stage 1 satisfies every guarantee above on its
  own; stage 2 is a pure optimization over unchanged committed state,
  adopted only on measured evidence of contention.

Non-goals:

- **Sub-object-store commit latency.** The floor is one conditional PUT;
  millisecond-class durability requires a consensus-replicated log (see
  Alternatives).
- **Changing SlateDB.** Runs on stock SlateDB; cooperative multi-writer
  inside SlateDB is the successor design (see Alternatives), and
  migration to it retires the slot log without discarding the batcher.
- **Replacing the single-writer topology.** RFC 0004 remains the
  default, byte-identical on disk; multi-writer is an explicit opt-in
  with a format-version consequence (below).
- **Read-path changes.** RFC 0009 governs reader consistency; this RFC
  only adds tail replay past the last fold.
- **Cross-slot transactions.** One commit is one atomic unit.

## Design

### Staging

Stage 1 is a complete protocol, not a degraded preview of stage 2.

**Stage 1 — leaderless.** The slot log and envelope codecs, the commit
protocol, the folder and fold cursor, truncation, transaction-id dedup,
and the format-version bump. Zero RPC surface; the cost is
storage-shaped (codecs, a fold loop, a format bump), landing in the
layers and test regimes the codebase already has. Its one obligation to
stage 2 is reserving the optional advert field in the envelope, encoded
absent, so stage 2 arrives as a code change rather than a format change.
Per-commit latency (one conditional PUT) sits in the same band as the
single-writer topology's durable flush — stage 1 regresses nothing.

**Stage 2 — the batcher.** Built when metrics ask for it: sustained
slot-race retry rates or commit-latency inflation under fleet load. The
contention analysis below says a catalog workload may never get there.
Stage 2 touches no committed state and no on-disk format; a deployment
that never sees the trigger never builds it.

Beyond stage 2 — retiring the slot log into a cooperative multi-writer
SlateDB while the batcher survives as the contention valve — see
Alternatives.

### Layering: the log is truth, the store is an index

1. **The commit-slot log** — objects `commits/<seq>` (fixed-width,
   lexicographically ordered), each written exactly once with
   `PutMode::Create`. The authoritative, totally ordered record of
   commits: any process may *attempt* slot N+1; the object store
   guarantees exactly one winner.
2. **The SlateDB store** — unchanged on-disk format, maintained by a
   single **folder** holding the one read-write `Db` under the existing
   fencing rules (RFC 0004, Topology). The folder tails the log and
   applies each slot as one atomic `WriteBatch` that also advances the
   fold cursor (`sys/fold`), so a successor resumes exactly — no gap, no
   double-apply.

Every other process is a `DbReader` (RFC 0017's read-only attach) plus a
log tail; the head is store state as of `sys/fold` plus replay of slots
past it. This mirrors SlateDB's own WAL-to-L0 relationship: log
authoritative, index derived by one process to make reads cheap.

At fleet scale, RFC 0004's `DbReader` caveat compounds: default
latest-mode readers each write a manifest checkpoint and pin SSTs
against SlateDB's GC. Large fleets should open against existing
checkpoint ids per RFC 0004's zero-write-reader note — traffic hygiene,
not correctness.

### The slot

An immutable object containing one envelope:

- **commits** — one or more committed change sets, each carrying a
  committer-generated transaction id (UUID), the change set (the same
  logical content RFC 0004 stages into a commit batch), and the head
  sequence it validated against.
- **batcher advert** (optional; reserved at stage 1, encoded absent
  until stage 2) — see The batcher below.

Snapshot ids and every other allocated id derive deterministically from
slot order (sequence, position within slot), so every process computes
identical allocations; no id is minted outside the log. Envelope codecs
live in `store` with the mandatory proptest roundtrip, versioned
independently of slot position (RFC 0015).

### Commit protocol

1. **Materialize the head**: `DbReader` state plus tail replay past
   `sys/fold`. Note the freshest batcher advert as a byproduct.
2. **Validate**: RFC 0004's conflict detection against that head; a
   genuine conflict surfaces as the same typed error as today.
3. **Payload first**: objects the change set references beyond the
   envelope are written unconditionally to collision-free keys before
   the slot. A slot never references bytes not already durable. The
   standard DuckLake write path satisfies this for free: table rows live
   in data files DuckDB writes to the lake during the statement, before
   `COMMIT` triggers the catalog commit — the slot carries only the
   metadata records pointing at them. Inlined writes (RFC 0005) ride the
   change set inside the envelope itself, making the rule vacuous and
   the rows durable atomically with the slot.
4. **Race the slot**: conditional-put the envelope at the next sequence.
   A win is committed and durable at the PUT ack. On `AlreadyExists`:
   read the winning slot (mandatory work — the winner must be folded
   into the local head anyway), re-validate, and retry at the next
   sequence with jittered backoff up to a bounded attempt count;
   re-validation failure surfaces as a typed conflict.

Crash analysis: before the PUT, nothing committed (orphaned payloads are
garbage, collected below). After the PUT but before the caller learns
the outcome, the transaction id resolves it: the recovering committer
scans the tail and folded state for its id and either reports success or
safely retries. Exactly-once is a property of the log being inspectable.

The slot PUT is the commit point and the durability point at once:
standard object storage acknowledges a PUT only after redundant
multi-zone storage, so no window exists in which a commit is not
durable. Single-zone classes (e.g. S3 Express One Zone) weaken this and
are not supported as the log's home.

### Worked example: two writers

A catalog at snapshot 41, fold cursor `sys/fold = 89`, one unfolded slot
`commits/0090`. Writer A (`INSERT INTO events …`) and writer B
(`INSERT INTO metrics …`) commit concurrently from different hosts; no
batcher, no folder currently awake.

1. **Both materialize the head**: `DbReader` state at fold 89 plus
   replay of slot 90 → snapshot 41, next slot 91. Each mints a
   transaction id.
2. **Both validate** their change sets against head 41; both pass.
   DuckDB already wrote A's rows as a lake data file during the
   statement, so payload-first held before the race began.
3. **Both race `commits/0091`.** B's PUT lands: B is committed and
   durable at that ack, as snapshot 42 — before any SlateDB write
   exists. A gets `AlreadyExists`.
4. **A rebases**: it reads slot 91 (work it needed anyway to advance its
   head), re-validates against snapshot 42 — disjoint tables, so it
   passes — re-derives its allocations to snapshot 43, and wins
   `commits/0092`. Durable at the ack.

Variants:

- **Semantic conflict.** Had B instead altered the `events` schema, A's
  re-validation fails and the typed conflict surfaces to DuckLake's
  retry loop exactly as under RFC 0004. Mechanical contention (the lost
  race) retries silently; semantic contention surfaces.
- **Crash after the PUT.** A dies after winning slot 92 but before its
  caller learns the outcome. Recovery scans the tail for A's
  transaction id, finds it, and reports committed; nothing re-applies.
- **Crash before the slot.** A dies at step 2: its data file sits in the
  lake referenced by nothing — an orphaned payload, swept by GC (below).
- **The fold arrives later.** Some session's tail threshold trips; it
  folds slots 90–92 as three atomic batches and closes. Until then every
  reader was fully consistent — store-at-89 plus tail replay — just
  replaying three slots more.

SlateDB appears nowhere in the commit critical path: both commits
reached durability through reads and one conditional PUT each.

### The folder

RFC 0004's single writer with a new job description: tail the log, fold
each slot as one atomic batch, advance `sys/fold`. Folding is
deterministic and idempotent — a successor re-reads `sys/fold` and
resumes at the next unfolded slot.

The folder is **availability-optional**. Down, commits continue
unimpeded; the only symptom is a growing tail that lengthens head
materialization. A dead folder can never lose a commit, because folding
happens strictly after durability.

Folding never enters the commit path, in either direction. Committers
do not fold: folding requires the fenced writer role, and acquiring it
per commit would have every committer fencing the last (RFC 0004's
ping-pong). Commits do not wait for folds: a fold adds no durability and
no visibility — tail replay already shows every slot — only shorter
future replays, so awaiting one would spend a SlateDB durable flush and
a liveness dependency to buy nothing. Folding is derived-state
maintenance, like compaction: lazy is the floor the design guarantees, a
designated folder folding each slot as it lands is the normal ceiling,
and the commit path cannot tell the difference.

**Appointment is the act of opening.** `Db::open` read-write bumps the
writer epoch and fences the incumbent — SlateDB's existing ceremony is
the whole mechanism; the only design question is *when* to open. The
failure detector is the tail itself: every session computes the unfolded
tail during head materialization, and a growing tail is the slot-ordinal,
clock-free signal that no live folder exists — the folder counterpart of
batcher-advert decay. The self-appointment rule:

1. Observe unfolded tail > threshold.
2. After a jittered delay, re-check **fold progress**: if `sys/fold`
   advanced, someone else won — stand down.
3. Otherwise open read-write and fold; either stay (long-lived host) or
   fold to drained and close (an ephemeral session's bounded **fold
   sprint**).
4. If fenced: stand down; re-enter only when step 1 triggers again.

The progress check turns a stampede into a single opener; standing down
matters because the fence is the opposite of a lock — newest writer
wins, so contenders who re-open alternately fence each other (RFC 0004,
Topology). Wrongful appointment is harmless: fencing plus idempotent
folds means duelling folders waste, never corrupt, which is why the
protocol may be this sloppy and this simple. Throughput hygiene, never
safety machinery.

Deployment postures, in order of preference: **designated** — any
long-lived process (RFC 0004's funnel host, an RFC 0021 maintenance
sidecar) is configured as folder and the rule above becomes failover
backstop; **opportunistic** — an all-ephemeral fleet folds via fold
sprints, and since commits never wait on folding, "whoever last
bothered" is a legitimate steady state; at stage 2, the **batcher**
takes the role — long-lived, warm head, already tailing every slot.
Bootstrap is the degenerate case: creating a multi-writer store is a
deliberate attach, and the creator is trivially the first folder,
mirroring RFC 0004's bootstrap-as-commit.

**The folder runs with the store's WAL disabled.** The slot log is the
WAL; journaling fold batches a second time through SlateDB's WAL would
duplicate every logged byte as WAL SSTs — doubled write amplification
protecting nothing, since no commit ever depends on the folder's WAL:
commits are durable at the slot PUT, fold state is re-derivable, and the
one-durable-copy invariant holds unchanged with slots retained until the
fold is L0-durable. So the folder sets `wal_enabled = false` (the
`wal_disable` cargo feature, present in 0.14.1): fold batches go
straight to memtable, durability arrives at L0 flush, and recovery is
what it always was — a successor replays unfolded slots from the durable
cursor, idempotently. Two consequences, neither safety-relevant: the
durably-folded horizon is the L0 flush, so truncation lags further and
the retained tail grows; and a fenced zombie folder discovers its
demotion at its next L0 flush or manifest operation rather than its next
WAL write — later, but detection latency was never load-bearing, because
folds are idempotent. Leaving the WAL on is a tuning choice that
shortens the retained tail at the price of double-journaling; it is
never required.

### Truncation and GC

Slot deletion is the one operation that can destroy the authoritative
copy of a commit, so:

> A slot may be deleted only when its sequence is ≤ the fold cursor **as
> durably flushed by SlateDB** — the folder's `await_durable` horizon,
> not its memtable — and no reader checkpoint or snapshot retained under
> RFC 0007 still requires tail replay across it.

Truncate to the last *durably folded* slot, never the last *applied*
one; there is then always at least one durable copy of every commit.
Slot GC and orphaned-payload GC ride RFC 0007's expiry machinery, where
an orphaned payload — a data file written for a commit that never won a
slot — is the multi-writer face of DuckLake's aborted-transaction
cleanup: sweep any payload referenced by no slot and no retained
snapshot. Bucket guard rails (versioning on `commits/`, lifecycle
exclusions) are deployment documentation, not protocol.

### The batcher (stage 2)

The batcher is the contention valve: a process that accepts forwarded
change sets, validates them against its warm head, folds many into one
slot, and wins slots on the fleet's behalf — restoring group commit when
leaderless racing (N committers, ~N rounds to drain N change sets)
starts to hurt.

Discovery obeys one governing principle: **advertisement is data carried
by real commits, discovered through reads every participant performs
anyway — no heartbeats, no side-channel objects, no clocks anyone must
trust.** A signal that needs its own object, write cadence, or trusted
clock is a second coordination channel and does not belong. The one
exception is the takeover slot — an advert carried by an empty commit,
needed because a batcher that only serves forwards may never originate a
commit to carry its first advert. It stays in the main channel so
adverts share the log's total order, which is what lets duels resolve by
"freshest slot wins" with no tiebreak.

- **Advisory only.** Forwarding is an optimization over step 4, never a
  requirement; a client that cannot reach the batcher commits directly.
  The degraded path *is* the base protocol, exercised by the batcher on
  every slot it wins — no rarely-run fallback to rot.
- **Discovered through the log.** The advert field (endpoint, instance
  UUID, protocol version) in the highest-numbered slot that has one; the
  folder folds the freshest advert into a `sys` key so the hint survives
  truncation — a cache of log content, not a second channel. Freshness
  is measured in slots, not wall-clock: direct-commit slots landing
  *after* an advert are themselves the failure signal that decays it.
- **Self-appointed.** On observing no fresh advert (or the decay
  signal), a batch-capable process binds its endpoint and announces with
  a one-time **takeover slot**, won through the normal CAS. This must
  not generalize into heartbeats: an idle store needs none — the worst
  cost of a stale advert is one bounded probe.
- **Duels are safe.** Two simultaneous batchers are transient slot
  contention. On observing a fresher advert with a different instance
  id, a batcher drains, stops advertising, and redirects.
- **Client behavior.** Forward with a short timeout; on timeout or
  refusal, direct-commit with no advert (aging the hint) and do not
  retry the endpoint this session; on ambiguity, scan the tail for the
  transaction id before retrying.

A batcher normally also holds the folder role (see The folder), the two
staying distinct: folder fenced and safety-relevant, batcher advisory
and safety-irrelevant. Forwarding grants no authority a bucket
credential does not already grant.

### Contention behavior

Slot races are lock-free with a guaranteed winner per round; the system
cannot livelock. Under sustained contention the failure shape is
quadratic request amplification and per-commit latency linear in the
number of *processes* (single digits for a catalog fleet) — damped by
self-batching (a loser's next attempt carries every commit that arrived
while it backed off), capped by backoff, eliminated by the batcher. The
slot ceiling is roughly one win per conditional-PUT latency. Distinguish
this **mechanical contention** (curable by funneling) from **semantic
contention** (genuinely overlapping change sets), which conflicts under
any serialization scheme and surfaces through RFC 0004's conflict model
unchanged.

### Format and compatibility

Multi-writer is an explicit opt-in stamping a new format version (RFC
0002's mechanism): the store has a `commits/` prefix, a `sys/fold`
cursor, and log-derived id allocation, and an older binary — which would
commit through the RFC 0004 path and bypass the log — must refuse it.
Single-writer stores are byte-identical; migration between modes is RFC
0015 work.

The crash matrix (RFC 0011) grows the multi-writer cases: crash between
payload and slot, crash after slot before ack, folder crash mid-batch,
fold-then-truncate races, folder-takeover fencing races, duelling
batchers, and takeover-slot races. Each must resolve to "commit durable
and discoverable" or "commit never happened" — nothing in between.

## Alternatives considered

- **Replicated WAL via consensus (openraft).** Quorum fsync gives
  millisecond durability; snapshots collapse to manifest pointers since
  bulk data stays in shared object storage. Rejected: a 3-node stateful
  service, recent commits' sole copy on node disks, and latency this
  workload doesn't need. It remains the only path below object-store
  latency.
- **Consensus for leader election only.** Group commit and fast
  failover, but commit liveness comes to depend on a quorum — paid for a
  safety property (unique leadership) the design doesn't need, since
  serialization already comes from CAS. Under partition, strictly less
  available than the single writer it replaces.
- **Mandatory batcher (no leaderless base).** Commit liveness hangs on a
  designated process in a system of ephemeral DuckDB sessions; a client
  partitioned from a healthy funnel can only seize the writer role,
  inviting epoch ping-pong. Advisory-over-leaderless dissolves both.
- **Cooperative multi-writer inside SlateDB.** SlateDB's WAL SSTs are
  already written with `PutMode::Create`; reinterpreting a lost race as
  contention rather than fencing — plus deterministic sequence
  assignment and cross-process transaction validation — would make the
  WAL itself the commit log. The preferred endgame, and it belongs
  upstream, not in a fork; this RFC is shaped so the slot log retires,
  the batcher survives, and the envelope codecs are the only deleted
  module.
- **External object-storage queue (e.g. OpenData Buffer).** Its
  consumer-side contracts (deterministic commit identity, ack strictly
  after durable sink commit, explicit committed-check) validate the
  folder design and are borrowed here. The queue itself is blind-append
  — no conditional commit, which is the entire multi-writer mechanism —
  and would make an external wire format the catalog's source of truth
  while relocating, not removing, the CAS contention point.
- **Sequence-then-validate (Calvin-style).** Blind-append intents,
  validate deterministically at apply order. Coherent, but the
  durability ack no longer means the transaction *succeeded* — every
  commit reads the prefix to learn its own verdict — and admission still
  serializes on a CAS somewhere.
- **Lease objects or election services for batcher discovery** (bucket
  lease via CAS, etcd, Kubernetes Lease, gossip). Advisory leadership
  needs no consensus-grade guarantee, so every election dependency pays
  for exclusion the design doesn't use. A bucket lease is the fallback
  if discovery is ever needed before the first slot exists; in-log
  advertisement rides reads every participant already performs, and its
  failure signal cannot be stale relative to the commits themselves.
