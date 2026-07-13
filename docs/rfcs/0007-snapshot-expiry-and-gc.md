# RFC 0007: Snapshot expiry and garbage collection

- **Date:** 2026-07-09

## Summary

DuckLake catalog history accumulates without bound: ended entity versions
pile up in `history`, snapshot records in `snapshot`, and superseded data/delete
files linger in object storage after no
snapshot references them. This
RFC defines how moraine reclaims both — **catalog-record GC** (pruning
`history` / `snapshot`) and **file cleanup** (scheduling and physically deleting
object-store files) — mirroring DuckLake's `ducklake_expire_snapshots`,
`ducklake_cleanup_old_files`, and orphaned-file deletion. Every reclamation
is an ordinary commit under RFC 0004; safety under the single-writer /
many-reader topology comes from a **retention horizon** no reclamation
crosses and a **grace window** between logical expiry and physical deletion.
This is the RFC that RFC 0004's snapshot-expiry/GC non-goal delegates to.

## Goals

- Loading the current catalog stays proportional to *live* state (RFC
  0002's load-bearing guarantee). Expiry keeps `history` / `snapshot` from growing
  with total history rather than live catalog size.
- Reclamation is **safe under the RFC 0004 topology** (single writer, many
  uncoordinated readers): no snapshot a reader may still resolve is removed,
  and no file a surviving snapshot references is deleted.
- **Physical deletion is decoupled from logical expiry** by a grace window
  (`ducklake_files_scheduled_for_deletion`), so a reader holding a slightly
  stale view never dereferences bytes that were just reclaimed.
- **Faithful to DuckLake.** moraine implements the semantics of
  `ducklake_expire_snapshots` / `ducklake_cleanup_old_files` /
  orphaned-file deletion; it invents no stricter or looser policy
  (consistent with RFC 0004: implement DuckLake's model, don't impose one).
- Expiry and cleanup are **ordinary commits** — one SlateDB `WriteBatch`
  each, head CAS, no read-modify-write across batches (RFC 0002 atomicity
  invariant, RFC 0004 commit sequence, RFC 0003 API). No separate mutation
  path.

Non-goals:

- **Auto-expiry policy / scheduling** — *when* to expire is operational, like
  RFC 0005 flush cadence. This RFC defines the mechanism and the safety
  contract a policy must respect, not the policy.
- **Compaction / data-file rewriting** — RFC 0008. Expiry removes *dead*
  state; compaction rewrites *live* state. The two meet only at cleanup:
  the files compaction supersedes are reclaimed by this RFC.
- **Multi-writer coordination** — the RFC 0004 topology stands; expiry and
  cleanup run inside the single committer.
- **Changing the key layout** — RFC 0002. This RFC only defines which
  records are deleted and when.

## Background

RFC 0002 established the relevant shape:

- `history` (tag `0x03`) is append-only; its keys end with the version's
  `end_snapshot`. RFC 0002 delegates its garbage collection here.
- `snapshot` (tag `0x01`) is one immutable record per snapshot; `sys/head`
  holds the latest committed `snapshot_id`.
- `current/gcfile`, keyed by `deletion_id`, maps `ducklake_files_scheduled_for_deletion`
  — a live bookkeeping list, not a time-versioned entity (no begin/end).
- `file` / `delfile` records reference object-store paths in their values.

RFC 0005's flush hard-deletes inlined chunks outright (DuckLake's
delete-at-flush semantics), so the `inline` subspace leaves nothing for
this RFC to reclaim.

RFC 0004 fixes the topology: readers attach read-only (`DbReader`) with no
coordination, and it scopes snapshot expiry / GC out to this RFC while
establishing the commit sequence (load head → allocate → stage one atomic
batch → commit under head conflict detection) that this RFC's operations
obey.

DuckLake's maintenance surface:

- `ducklake_expire_snapshots(older_than / versions, ...)` marks snapshots
  removed and schedules now-unreferenced data/delete files into
  `ducklake_files_scheduled_for_deletion`.
- `ducklake_cleanup_old_files(older_than / cleanup_all, ...)` physically
  deletes scheduled files past a period.
- Orphaned-file deletion removes storage files no catalog snapshot ever
  referenced (the residue of writes whose commit never landed).

## Design

Reclamation is **two phases plus one sweep**, deliberately separated:

1. **Expiry (logical)** — one commit: remove snapshot records and history
   below a retention horizon, and *schedule* the files they alone
   referenced. Deletes no bytes.
2. **Cleanup (physical)** — one commit + the object-store DELETEs: delete
   scheduled files whose grace window has elapsed, then forget their
   `current/gcfile` records.
3. **Orphaned-file deletion (sweep)** — a storage LIST diffed against the
   referenced set; deletes never-referenced residue. Touches no catalog
   record.

The split *is* the reader-safety mechanism: logical expiry never removes
bytes, and physical cleanup respects a grace window longer than any reader's
view can be stale.

### The retention horizon

Let **`H`** be the oldest `snapshot_id` that must remain resolvable. Under
tail retention (keep a contiguous suffix of snapshots — DuckLake's
`older_than` / "keep last N"), everything strictly below `H` is eligible for
reclamation. `H` is derived from a **retention policy** — keep snapshots
newer than time `T`, and/or keep the last `N` — never from live reader
state, because RFC 0004 readers are uncoordinated and invisible to the
committer.

The safety contract follows directly and must be documented for operators
(as Iceberg/DuckLake document theirs): **the retention window must exceed the
maximum expected read/attach duration.** A reader that has held a view
longer than the window may find its snapshot expired and must re-resolve
from `sys/head`. The grace window on physical deletion (below) is what makes
this a *retry*, not a *corruption*: even after logical expiry, the bytes a
stale reader is mid-scan on survive until cleanup.

### Phase 1 — the expiry commit

Inputs: the policy yields the set `E` of expirable snapshot ids (all `< H`)
and the horizon `H`. The commit is one RFC 0004 batch:

- **Remove snapshot records.** Delete `snap/{s}` for each `s ∈ E`. This is
  the one sanctioned exception to `snapshot` immutability (RFC 0002) — expiry
  exists precisely to reclaim this space.
- **Prune history.** A `history` version is visible only for
  `begin_snapshot ≤ S < end_snapshot`. Under tail retention it is **dead iff
  `end_snapshot ≤ H`** — its entire visibility interval lies below the
  horizon, so only removed snapshots ever saw it. The scan cost, stated
  precisely: `end_snapshot` is the *final* key component, so dead records
  are contiguous only *within each entity's range* — enumerating them is a
  seek-skip walk over `history` (per entity: scan the `end_snapshot ≤ H`
  prefix, then seek past the entity's remaining versions). The cost is
  therefore **O(distinct entities with history)**, each contributing a
  bounded dead-prefix read — not O(dead records), and not proportional to
  dead history alone. What the layout buys over a naive full scan is
  skipping the *live-version bytes* per entity, not skipping entities. If
  profiling ever shows the per-entity seek cost dominating (very many
  entities, little dead history per run), an `end_snapshot`-major secondary
  index would make expiry truly proportional to the dead set — deferred as
  complexity without current evidence.
- **Schedule files.** When pruning removes a `history` `file` / `delfile`
  record, its object-store path references bytes no surviving snapshot
  needs (the record was ended at `end_snapshot ≤ H`; it is not in `current`, so
  no live snapshot references it). Insert a `current/gcfile` record keyed by a
  fresh `deletion_id`, carrying the path and `schedule_start` = commit
  timestamp (µs UTC, RFC 0002).
- **Advance head.** Write `snap/{N+1}` recording the expiry in
  `snapshot_changes`, and CAS `sys/head` `N → N+1`. Expiry is a commit like
  any other; DuckLake surfaces maintenance in `ducklake_snapshot`.

**`deletion_id`** comes from a global counter alongside `next_file_id` in
the `snapshot` record (RFC 0004 id-allocation), so a shared read of it is a
benign race, never a conflict.

#### Why expiry needs no special conflict grain

Everything expiry deletes lies **strictly below the immutable horizon
`H < sys/head`**. A concurrent commit can only *end* a version at the new
snapshot id `> H`, so it can never produce a record with `end_snapshot ≤ H`
that expiry would have touched, and it never references a file already ended
below `H` (such files are not in `current`). The only state expiry shares with a
concurrent commit is `sys/head` and the global counters — exactly RFC 0004's
**benign-race** territory. So an expiry that loses the head CAS simply
re-derives against the new head and retries; it never needs the
schema-list / global conflict grain, and it never aborts a concurrent
data commit. This falls out of the horizon invariant, and it makes expiry
cheap to run concurrently with the write workload.

### Phase 2 — the cleanup commit (physical deletion)

Mirrors `ducklake_cleanup_old_files`. Scan `current/gcfile`; select records whose
`schedule_start` is older than the **grace window `G`** (or all, under
`cleanup_all`). For each:

1. Issue the object-store `DELETE` for the path.
2. In one RFC 0004 batch, delete the selected `current/gcfile` records and
   advance head (recording the cleanup in `snapshot_changes`).

**Order and crash-safety.** Delete the bytes *first*, then forget the record
— the mirror of RFC 0005's "data before metadata." A crash after the
`DELETE` but before the batch leaves a `gcfile` record whose file is already
gone; the next cleanup re-issues the `DELETE` (a no-op on a missing key) and
removes the record. A `gcfile` record whose bytes still exist is the safe,
resumable state, so partial progress never loses data or strands a
dangling reference in a live snapshot.

**`G` is the reader-safety knob.** A file's bytes survive at least `G` beyond
the moment it was scheduled — which is itself no earlier than the expiry
that made it dead. `G` must exceed the maximum reader/scan duration; tie its
default to DuckLake's cleanup period.

### The orphaned-file sweep

Mirrors DuckLake's orphaned-file deletion. Aborted or crashed writes can
leave Parquet in the bucket that **no catalog record ever referenced** (the
commit that would have referenced it never landed). These are invisible to
`history` pruning because they were never in the catalog. The sweep:

- LISTs the data prefix in object storage.
- Diffs against the referenced set — every path in `current` + `history` `file` /
  `delfile` records and every `current/gcfile` path.
- Deletes only paths **older than a threshold** that exceeds the maximum
  write-to-commit latency, so a Parquet file written moments before its
  (still in-flight) commit is never reaped.

The threshold is a heuristic, not a guarantee, and the failure it fails
toward must be closed from the other side. A committer stalled past the
threshold — a long GC pause, a partition, an object-store retry storm —
could have its already-PUT Parquet reaped by the sweep and *then* land its
commit, creating a live catalog reference to deleted bytes: the one state
this document set promises never to construct. Two rules close it:

- **Committer write-deadline.** A commit whose earliest data PUT is older
  than the orphan threshold **must abort** (typed error; the caller
  re-drives, re-PUTting fresh data) rather than land. The committer knows
  its own PUT timestamps; enforcing the deadline locally is one comparison
  before step 4 of RFC 0004. With the deadline enforced, "older than the
  threshold and uncatalogued" really does imply orphaned.
- **Re-verify before delete.** The sweep re-checks a candidate path against
  the referenced set immediately before issuing its `DELETE` (cheap — the
  candidate list is small), narrowing the race window from
  list-diff-duration to a single check-then-delete.

The sweep mutates no catalog record (there is nothing to remove — these
paths were never catalogued); it is a storage read plus storage deletes.
Mechanism sketched here; the scan cadence is operational, like expiry policy.

### One reclamation path for flush and compaction

Inline flush (RFC 0005) and compaction (RFC 0008) both *end* file and entity
records, moving them to `history`. Nothing about their outputs bypasses this
RFC: their superseded inputs become dead `history` records and are reclaimed by
the `end_snapshot ≤ H` rule and the gcfile → cleanup path like any other
ended entity. This RFC is the **single** reclamation path in moraine.

### Test obligations

Per RFC 0001, integration tests run against real SlateDB on in-memory
`object_store` — no mocks of the store:

- **Current view unchanged.** Loading the catalog at head is byte-identical
  before and after an expiry (only dead history removed).
- **Horizon respected.** A snapshot `< H` no longer resolves; a snapshot
  `≥ H` resolves identically to pre-expiry (time travel intact for survivors).
- **History pruned.** `history` / `snapshot` records with `end_snapshot ≤ H`
  are gone; those above remain.
- **Two-phase file safety.** A file ended `≤ H` appears in `current/gcfile`
  after expiry with its bytes still present; the bytes are absent only after
  a cleanup past the grace window.
- **Cleanup idempotence.** A crash injected between the byte `DELETE` and the
  `gcfile`-record removal leaves a resumable state; re-running cleanup
  converges with no error and no data loss.
- **Expiry is benign under concurrency.** A concurrent disjoint commit during
  expiry causes expiry to re-derive and re-CAS; it never removes a record the
  concurrent commit relied on, and the concurrent commit never aborts because
  of expiry.
- **Orphan threshold.** An orphaned file older than the threshold is deleted;
  a just-written pre-commit file within the threshold is preserved.
- **Committer write-deadline.** A commit forced to stall until its data PUTs
  age past the orphan threshold aborts with a typed error and lands nothing —
  a stalled writer and a concurrent sweep can never combine into a live
  reference to deleted bytes.

## Open questions

- **Do DuckLake maintenance ops mint snapshots?** This RFC assumes expiry and
  cleanup advance head (every head-visible mutation is a snapshot, RFC 0004);
  the e2e suite confirms whether DuckLake records
  `expire_snapshots` / `cleanup_old_files` in `ducklake_snapshot` and, if
  not, cleanup can drop the head advance and mutate `current/gcfile` under CAS
  alone.
- **Retention / grace defaults.** The window sizes are tuning parameters;
  pick defaults once e2e and perf show realistic reader durations and
  history-growth rates.
- **Interior (non-tail) expiry.** DuckLake's `versions => [...]` may expire
  arbitrary interior snapshots. Then the clean `end_snapshot ≤ H` rule
  generalizes to "the version's `[begin, end)` interval contains no surviving
  snapshot" — correct but costlier to evaluate. Resolve the exact predicate
  against DuckLake's `versions` semantics via e2e; tail retention is the
  common and cheap case and ships first.
- **Reader-pin visibility.** Whether the extension layer (RFC 0006) can
  expose live reader snapshots so the committer raises `H` only as far as
  safe (tighter reclamation than a time/count policy). Deferred; policy-only
  for now, which is always safe if the window is sized correctly.

## Alternatives considered

- **Immediate physical deletion at expiry (no grace window).** Rejected: it
  breaks a reader mid-scan under RFC 0004's no-coordination model. The grace
  window *is* reader safety, which is exactly why DuckLake interposes
  `ducklake_files_scheduled_for_deletion` rather than deleting inline.
- **Reference-counting files instead of scan-based scheduling.** Rejected: a
  per-file count is mutable shared state requiring read-modify-write on every
  commit that adds or ends a reference — contradicting RFC 0002's append-only
  writes and inflating RFC 0004's conflict sets. The `end_snapshot ≤ H` range
  rule identifies unreferenced files without maintaining any count.
- **Tombstoning `snapshot` records instead of deleting them.** Rejected: the goal
  is to reclaim space; a tombstone still occupies the `snapshot` range a
  time-travel loader scans. `snapshot` immutability is relaxed *only* for expiry,
  and deletion is the whole point.
- **Expiry / cleanup as background mutations outside RFC 0004** (direct puts
  and deletes, no batch, no CAS). Rejected: violates the single-`WriteBatch`
  atomicity invariant (RFC 0002) and the commit discipline (RFC 0004). A
  half-applied expiry could strand `gcfile` records or delete a `snapshot` record
  without its head update. Reclamation is a commit like any other.
- **Whole-subspace scan per GC run.** Rejected as the default, with the
  claim stated precisely: the `end_snapshot ≤ H` bound does *not* make
  reclamation proportional to the dead set (the true cost is the
  seek-skip walk above, O(entities with history)); what it avoids is
  reading every live version's bytes on every run. A full flat scan reads
  all of `history`; the skip-scan reads dead prefixes plus one seek per
  entity. The gap between those grows with retained history per entity,
  which is exactly the regime GC runs in. An `end_snapshot`-major index is
  the true O(dead) design, deferred (see Prune history).
- **Fusing logical expiry and physical deletion into one commit.** Rejected:
  it couples object-store `DELETE` latency and failure modes to the catalog
  commit and destroys the grace window that protects readers — the same
  reason DuckLake separates `expire_snapshots` from `cleanup_old_files`.
