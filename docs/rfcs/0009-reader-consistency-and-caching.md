# RFC 0009: Reader consistency and snapshot caching

- **Date:** 2026-07-09

## Summary

RFC 0003 defines `CatalogSnapshot` as "an immutable, materialized read view
built by scanning `current` (or `current` + `history`)" that "touches the store never"
after construction. It does not say how that view is built *consistently*
while a committer writes concurrently, how a long-lived reader learns of new
commits, or what happens to a held view when RFC 0007 reclaims the files it
references. This RFC fills those gaps: a `CatalogSnapshot` is pinned to a
single **SlateDB read-snapshot** so its scans are mutually consistent; it is
**refreshed incrementally** from the `snapshot_changes` changelog rather than
rematerialized; it observes **snapshot isolation** (a fixed catalog snapshot
`S`, never a torn mix); and it carries a **validity window** tied to RFC
0007's retention horizon, past which a reader must re-resolve from head. This
is the read-side companion to RFC 0004's write-side commit protocol.

## Goals

- **Consistent materialization.** The `current` / `history` / `snapshot` scans that build
  one `CatalogSnapshot` observe a single, consistent store state — never a mix
  of pre- and post-commit records torn by a concurrent commit.
- **Snapshot isolation for reads.** Every accessor on a `CatalogSnapshot`
  reflects exactly the catalog at one snapshot id `S`. Reads never block
  writes and a concurrent commit never mutates a held view (RFC 0004
  append-only + immutable `snapshot`).
- **Cheap refresh.** Advancing a long-lived reader from `S` to head costs work
  proportional to *churn* (the entities changed between `S` and head), not to
  live catalog size — using the `snapshot_changes` already in each `snapshot`
  record (RFC 0002).
- **Committer read-your-writes.** The single committer (RFC 0004) advances its
  own view by folding in the batch it just committed, without a re-read.
- **A defined validity window.** A held `CatalogSnapshot` is valid only within
  RFC 0007's retention window; a reader that outlives it gets a typed error and
  re-resolves, never a silent dereference of reclaimed files.
- **No new coordination.** All of this holds under RFC 0004's single-writer /
  many-uncoordinated-readers topology. Readers learn of commits by reading
  `sys/head`, never by being notified.

Non-goals:

- **The read *API*** — `CatalogSnapshot`, its accessors, `snapshot()` /
  `snapshot_at()` — RFC 0003. This RFC is the machinery behind them.
- **The physical `current` / `history` layout and the current-vs-time-travel split**
  — RFC 0002. This RFC consumes that layout; it does not change it.
- **Reclamation policy** — RFC 0007. This RFC defines only how a reader
  *reacts* to a view falling outside the retention window.
- **The sync↔async / runtime model** under the DuckDB extension — RFC 0010.

## Background

RFC 0002 makes the current-catalog load a scan of `current` and time travel a scan
of `current` + the relevant `history` ranges; it merges `ducklake_snapshot` and
`ducklake_snapshot_changes` into one immutable `snapshot` record per snapshot, and
notes that `snapshot` is append-only and `sys/head` holds the latest committed
`snapshot_id`. It also chose *not* to maintain a persistent name index —
name→id resolution runs "against the in-memory catalog snapshot that a client
builds by scanning `current` at attach."

RFC 0004 fixes the topology: readers attach through read-only handles
(SlateDB `DbReader`) that follow the manifest with no coordination and no
bound on their number, and a commit is one atomic batch that advances
`sys/head` under transactional write-write conflict detection.

RFC 0003 makes `CatalogSnapshot` an immutable value — "Reads issue no store
I/O after the snapshot is built; a `CatalogSnapshot` is a value, not a cursor"
— and has `commit` read a *fresh* `CatalogSnapshot` on every attempt.

RFC 0007 introduces the retention horizon `H` (snapshots `< H` are reclaimed)
and the grace window `G` (bytes survive `G` past scheduling), and states the
operator contract that the retention window must exceed the maximum reader
duration. This RFC honors that contract on the reader side.

## Design

### Materialization rests on one SlateDB read-snapshot

The correctness foundation: building a `CatalogSnapshot` issues *many* store
reads (a `current` range scan, `history` ranges for time travel, the `snapshot` record,
`sys/head`). These must observe one consistent store state, or a commit
landing mid-build could yield a view that has a table's new `column` record
but not its new `file` record — a torn read.

So materialization **pins a single SlateDB read-snapshot** and issues every
get and scan against *that* handle. This is not a hoped-for primitive: the
pinned SlateDB version (0.14.x) exposes exactly it — `Db::snapshot()`
returns a `DbSnapshot` fixed at a sequence number, with the same
`get`/`scan`/`scan_prefix` surface as the live handle (and `DbReader`
provides the equivalent for read-only processes that never hold the
writer). The moraine snapshot id `S` is the `sys/head` value read *under
the same handle*. The result: the entire `CatalogSnapshot` is a consistent
cut at `S`, immune to concurrent commits, with no lock or reader/writer
coordination — the consistency is inherited from SlateDB's read isolation,
not re-implemented. The handle is released once the in-memory view is
built; per RFC 0003 the finished `CatalogSnapshot` touches the store never
again.

Materialization also reads the **`sys/migration` marker** (RFC 0002 /
RFC 0015) under the same handle. If the marker is present, a structural
migration is rewriting the keyspace and any scan of it may be missing
records mid-move; materialization fails loudly with the typed `Migration`
error (RFC 0003) rather than returning a silently partial catalog. This
check is part of every materialization and refresh from format version 1
onward — it must predate the first migration ever run, or old readers
would have no way to know to refuse (RFC 0015).

`snapshot_at(S')` (time travel) pins the same way, reads `sys/head` only to
validate `S'` is resolvable (≥ RFC 0007's horizon `H`), and materializes from
`current` + `history` filtered by begin/end per RFC 0002.

**Commit attempts materialize through the transaction, not a
read-snapshot.** SlateDB's write-write conflict detection sees only commits
that land after the transaction's start sequence. If a commit attempt
materialized its planning `CatalogSnapshot` from a `DbSnapshot` pinned at
sequence `X` and then opened its `DbTransaction` at sequence `Y > X`, a
concurrent commit landing in the `X → Y` gap would invalidate the attempt's
premises *without* tripping conflict detection — the attempt's `sys/head`
write only conflicts with writes after `Y`. The commit would land on stale
premises: a silent lost update. So the rule is: a `CatalogSnapshot` built
for a commit attempt (RFC 0003 `commit`, each retry included) is
materialized **through the `DbTransaction` itself** — SlateDB transactions
expose the same `get`/`scan`/`scan_prefix` surface, reading the MVCC view
at the transaction's own start sequence — so the premise view and the
conflict-detection window are anchored to the same sequence number by
construction, and no gap exists. Concretely, materialization is generic
over its read source: `DbSnapshot` (or `DbReader`) for plain readers,
`DbTransaction` for commit attempts. The finished `CatalogSnapshot` is
identical in both cases; only the handle it was built through differs.

### Snapshot isolation is free

Because `snapshot` is immutable and every entity version is append-only with an
`end_snapshot` that is only ever *set once, at commit* (RFC 0002/0004), the set
of records visible at `S` never changes after `S` exists. A concurrent commit
`S+1` adds records and ends others *for `S+1`*, but nothing it does alters what
`begin ≤ S < end` selects. A held `CatalogSnapshot` is therefore a stable
snapshot-isolated view for its whole lifetime with no defensive copying beyond
the initial materialization — this is why RFC 0003 can promise "no store I/O
after build."

### Incremental refresh from the changelog

A long-lived reader (or the committer's planning view) advances from `S` to a
newer head `S+k` without rescanning `current`:

1. Pin a fresh read-snapshot; read `sys/head` and the `sys/migration`
   marker under it (refusing with `Migration` if the marker is present, as
   in materialization). If head is unchanged, done — the cached view is
   current.
2. Otherwise scan `snap/{S+1 .. head}` **under the same pinned handle**.
   Each record carries its `snapshot_changes` (RFC 0002) — the precise set
   of entities the commit touched.
3. For each changed entity, re-read just that entity's `current` record (or, if
   it was ended, drop it from the view), still under the same handle — a
   refresh, like a materialization, is one consistent cut, never a mix of
   per-step reads torn by a concurrent commit. Apply to the in-memory
   catalog.

Cost is proportional to churn across the gap, not to catalog size. This is the
payoff of merging `snapshot_changes` into the `snapshot` record: the changelog a
reader needs to refresh is exactly the changelog a commit already writes.

**Fallback to full rematerialization** when incremental is impossible or not
worth it:

- **`S` fell below the horizon** (`S < H`, RFC 0007): the `snapshot` records for
  the gap may have been reclaimed, so there is no changelog to replay. The
  reader rematerializes at head. (If the reader specifically wanted the *old*
  `S`, that snapshot is gone — see validity window.)
- **The gap is large** relative to catalog size (churn ≥ live entities): a
  full `current` rescan is cheaper than replaying a huge changelog. A threshold
  picks the cheaper path; the two produce identical views, so the choice is
  purely a cost optimization.

### Committer read-your-writes

The single committer (RFC 0004) holds a planning `CatalogSnapshot`. After it
commits `N → N+1`, it must see its own write to plan the next commit. It does
**not** re-read: it already assembled the `WriteBatch`, so it folds the exact
staged mutations into its in-memory view and stamps it `N+1`. This is strictly
cheaper than incremental refresh (no `snapshot` scan, no re-read) and is always
correct because the committer *is* the source of the delta. Under RFC 0004
group commit, the committer folds in the whole committed group at once. A
committer that loses the CAS and retries simply rebuilds from the new head like
any reader.

### Validity window and expiry reaction

A held `CatalogSnapshot` at `S` names object-store files (`data_files_of`,
`delete_files_of`). RFC 0007 may schedule those files for deletion once `S`
passes below the horizon and delete their bytes after the grace window `G`. So
a view is safe to *use for data access* only while `S` remains within the
retention window.

This RFC states the reader-side contract that RFC 0007's operator obligation
implies:

- A `CatalogSnapshot` whose `S` is still `≥ H` is fully valid.
- A reader that tries to materialize or refresh at an `S` that has fallen below
  `H` — detected because its `snapshot`/`history` range is no longer resolvable —
  receives a typed **`SnapshotExpired`** error (RFC 0003 error taxonomy, one
  variant per failure domain) and must re-resolve from head.
- The safety margin is the retention window minus the reader's lifetime; RFC
  0007 sizes the window to exceed the maximum reader duration, so a
  well-behaved reader that refreshes faster than `G` never observes expiry.

The reader never silently dereferences a reclaimed file: either its `S` is
still retained (files present) or materialization/refresh fails loudly with
`SnapshotExpired`.

### Caching is per-handle, in-memory, logical

The materialized in-memory catalog *is* the cache. A `Catalog` handle (RFC
0003, an `Arc`-backed clone-cheap handle over `slatedb::Db`) may hold the
latest `CatalogSnapshot` and hand out clones; a refresh replaces it. There is:

- **No cross-process cache.** RFC 0004's "many readers" are independent
  processes/handles; each materializes its own view. Coordinating a shared
  logical cache would reintroduce exactly the coordination the topology avoids.
- **No separate physical cache.** SlateDB's own block cache serves repeated
  physical reads underneath; this RFC's cache is the *logical* materialized
  catalog, cheap to rebuild from `current` because RFC 0002 keeps the live catalog
  small. A cold reader pays one `current` scan; a warm reader pays incremental
  refresh.

### Test obligations

Per RFC 0001, integration tests run against real SlateDB on in-memory
`object_store` — no mocks of the store:

- **Consistent cut.** A commit forced to land mid-materialization yields a view
  that is entirely pre- or entirely post-commit, never torn (pin-read-snapshot
  correctness).
- **Isolation.** A `CatalogSnapshot` built at `S`, then `k` commits applied,
  still returns exactly the `S` view from every accessor.
- **Incremental == full.** Refreshing `S → head` incrementally yields a view
  byte-identical to a full rematerialization at head.
- **Committer read-your-writes.** After `commit`, the committer's folded view
  reflects the just-committed entities without a store re-read.
- **Fallback.** A reader whose `S` fell below the horizon rematerializes at
  head; a reader asking for an expired `S` gets `SnapshotExpired`.
- **No dangling file.** A view within the retention window resolves every file
  it names; a view driven past the window fails loudly rather than naming a
  reclaimed file.
- **Migration marker refusal.** A materialization or refresh attempted while
  `sys/migration` is present returns the typed `Migration` error and never
  yields a partial view (RFC 0015's mid-migration reader contract).
- **No conflict-window gap.** A commit forced to land between a commit
  attempt's materialization and its batch write is always detected: the
  attempt either observes it (materialized through the transaction, so the
  premise view includes it) or conflicts on `sys/head`. There is no
  interleaving in which the attempt commits against premises that omit a
  landed commit.

## Open questions

- **Read-snapshot hold cost — resolved in shape (source-verified, SlateDB
  0.14.x), residual is quantitative.** A `DbSnapshot` is a
  `(uuid, started_seq)` pair registered with an in-process snapshot
  manager — open and drop are O(1), no store I/O, no manifest writes. The
  one holding cost is version retention: the snapshot manager's
  `min_active_seq` feeds SlateDB's flush/compaction paths, so a held
  snapshot pins pre-snapshot versions from being garbage-collected
  in-process while it lives. For moraine's usage — held only for the
  duration of one materialization, released before the `CatalogSnapshot`
  is returned (Design) — that window is milliseconds and the retention
  effect is negligible; only a pathologically long-held handle would
  matter, and moraine's design never holds one. What remains is measuring
  materialization duration itself on large catalogs during bring-up.
- **Incremental-vs-full threshold.** The churn ratio at which full rescan wins;
  a tuning parameter, defaulted after perf on realistic churn shapes.
- **Snapshot lifetime under DuckLake.** Whether DuckLake holds one catalog
  snapshot per `BEGIN…COMMIT` transaction or re-resolves per statement
  determines how long a `CatalogSnapshot` is held and thus how tight the
  retention window must be. Resolved via RFC 0006 / e2e.
- **Memory bound.** For an unusually large live catalog the materialized view's
  footprint may matter; whether a partial/lazy materialization is ever needed
  is deferred until profiling shows the full in-memory view is a problem (RFC
  0002 bets it is not).

## Alternatives considered

- **Per-get consistency (no pinned read-snapshot).** Rejected: independent gets
  across a concurrent commit can observe a torn view (new `column`, missing
  `file`). A single pinned read-snapshot is the only cheap way to get a
  consistent cut without locking writers.
- **Re-scanning `current` on every read (no cache / no incremental refresh).**
  Rejected: defeats RFC 0003's "no store I/O after build" and pays the full
  scan per refresh. `snapshot_changes` gives a precise cheap changelog; use it.
- **Push-based invalidation (committer notifies readers of new commits).**
  Rejected: contradicts RFC 0004's no-coordination, cross-process topology and
  reintroduces a notification channel the design deliberately omits. Readers
  poll `sys/head`; that is the whole protocol.
- **A persistent name→id index to skip materialization.** Already rejected by
  RFC 0002 (complexity without payoff at live-catalog scale); nothing here
  changes that calculus. Name resolution stays against the in-memory view.
- **Rematerializing fully on every refresh (no incremental path).** Rejected as
  the default: correct but pays catalog-size cost per refresh for a
  churn-sized delta. Kept only as the fallback when the gap is large or the base
  snapshot was reclaimed.
- **Holding files alive for any live reader (reference-tracking readers against
  expiry).** Rejected: it is exactly the cross-process reader coordination RFC
  0004 forbids and RFC 0007's reference-counting alternative already rejected.
  The retention *window* plus a typed `SnapshotExpired` is the coordination-free
  contract.
