# RFC 0008: Maintenance operations — compaction and delete-file consolidation

- **Date:** 2026-07-09

## Summary

Frequent commits and inline flushes (RFC 0005) leave a table with many small
Parquet data files; merge-on-read deletes leave data files shadowed by
delete files. Both slow scans. This RFC defines how moraine performs
**compaction** — merging small/adjacent data files into fewer larger ones
(`ducklake_merge_adjacent_files`) and **materializing deletes** by rewriting
a data file free of its delete files (`ducklake_rewrite_data_files`) — as
ordinary RFC 0004 commits that **preserve DuckLake row-id lineage**.
Compaction rewrites *live* state, the mirror of RFC 0007, which removes
*dead* state; the files compaction supersedes become RFC 0007's reclamation
input. The load-bearing invariant is that compaction **never allocates row
ids** — it carries the input rows' ids through unchanged, so `next_row_id`
(RFC 0004) is untouched and time travel over compacted rows stays correct.

## Goals

- **Compaction is a commit** (RFC 0004): Parquet PUTs of the rewritten files
  first, then one `WriteBatch` that ends the superseded `file` / `delfile` /
  `fstat` records (→ `hist`), inserts the replacements, updates table stats,
  and advances head. No read-modify-write across batches — the RFC 0005 flush
  skeleton, reused.
- **Row-id lineage preserved exactly.** Rewritten rows keep their row ids
  (DuckLake preserves row ids across compaction for UPDATE and time-travel
  row identity), so the per-table `next_row_id` in `tstat` is read-only
  during compaction.
- **Merge-on-read → copy-on-write on demand.** After a data file is rewritten
  with its deletes applied, the output has no live rows for deleted positions
  and carries no delete file; scans stop paying the delete-overlay cost.
- **Conflict semantics faithful to RFC 0004.** Compaction touches specific
  tables and conflicts at table grain with concurrent commits mutating the
  same table. A compaction that loses the head CAS to a same-table commit
  aborts with `CommitConflict` — it never silently rewrites over a concurrent
  change.
- **Time-travel safe.** A snapshot older than the compaction still resolves
  the pre-compaction files (they survive in `hist` until RFC 0007 expiry), so
  compaction is invisible to readers of past snapshots.
- **Faithful to DuckLake.** moraine implements
  `merge_adjacent_files` / `rewrite_data_files` semantics; the *trigger* and
  *target sizing* are operational, the mechanism/policy split of RFC 0005 and
  RFC 0007.

Non-goals:

- **Physical deletion of superseded files** — RFC 0007. Compaction only
  *ends* the old records (moving them to `hist`, whence RFC 0007 reclaims
  them); it deletes no bytes.
- **Auto-compaction policy** (which files, when, target size) — operational,
  like RFC 0005 flush cadence and RFC 0007 expiry policy.
- **Inline flush** — RFC 0005, itself a compaction-shaped op (small inlined
  rows → Parquet). This RFC compacts *existing Parquet data files*.
- **Multi-writer coordination** — RFC 0004 topology stands.
- **Key layout** — RFC 0002.

## Background

RFC 0002 keys `file` (`ducklake_data_file`), `delfile`
(`ducklake_delete_file`), and `fstat` (`ducklake_file_column_stats`)
`table_id`-first, so "everything about table T" is one contiguous range;
`tstat` (`ducklake_table_stats`) holds the per-table `next_row_id`. Ending a
version deletes its `cur` key and writes a `hist` key with `end_snapshot`
appended, in the same batch.

RFC 0005 set the precedent this RFC follows: flush writes Parquet PUTs, then
one batch that creates the new `file` / `delfile` records and moves consumed
records to `hist` — data before metadata, one `WriteBatch`.

RFC 0004 states the invariant this RFC leans on hardest: `next_row_id` is
per-table and **"preserved across UPDATE and compaction for row lineage."**
It also fixes the table-level conflict rule and the commit sequence (load
head → allocate → stage → CAS).

DuckLake's maintenance functions: `merge_adjacent_files` (combine small
adjacent files) and `rewrite_data_files` (rewrite a file with its deletions
applied). The files these replace are scheduled for deletion through the
expiry/cleanup path — RFC 0007.

## Design

### Two shapes, one commit skeleton

1. **Merge** (scan/space efficiency): combine `N` small files of a table
   into `M` larger files. No rows added or removed; row ids carried through
   verbatim.
2. **Rewrite-with-deletes** (materialize deletes): read a data file, drop the
   rows its delete files cover, write a new data file without them; the
   output has no delete file. Rows *shrink* (the deleted ones), surviving
   rows keep their ids.

Both are the same commit; they differ only in whether delete files are
consumed and whether the row count falls.

### The compaction commit

Given input files `F_in` (with their delete files `D_in`) for table `T` and
planned outputs `F_out`:

1. **Write `F_out`** to object storage (Parquet PUTs). Each output row
   carries the **original row id** of its input row — merge: verbatim;
   rewrite-with-deletes: the surviving subset. Compute per-file column stats
   and the row-id range/mapping for each new file.
2. **One `WriteBatch`** (RFC 0004 step 3):
   - **End `F_in`.** For each input file delete its `cur/file` key and write
     the `hist/file` key with `end_snapshot = N+1`; same for consumed `D_in`
     `delfile` records and the inputs' `fstat` records.
   - **Insert `F_out`.** New `cur/file` records (file ids from `next_file_id`
     — new files get new ids) and their `fstat` records.
   - **`tstat`.** `next_row_id` is **unchanged** — no row ids allocated. This
     is the invariant that distinguishes compaction from an insert. In the
     rewrite case, the table's `record_count` (and `tstat` / `tcstat`) is
     recomputed downward by the number of materialized deletes.
   - **Snapshot + head.** Write `snap/{N+1}` noting the compaction in
     `snapshot_changes`; CAS `sys/head` `N → N+1`.
3. Superseded `F_in` / `D_in` **bytes are not deleted here.** Their `hist`
   records (`end_snapshot = N+1`) make them reclaimable by RFC 0007 once no
   retained snapshot is `< N+1`.

### Row-id lineage — the load-bearing invariant

DuckLake assigns stable row ids at insert (`next_row_id += record_count`) and
**preserves them across compaction** so UPDATE-as-delete+insert and
time-travel row identity hold. Therefore **compaction must not allocate row
ids.** `next_row_id` in `tstat` is read-only for the whole operation; the
output Parquet carries the input rows' ids unchanged.

This is the sharp difference from RFC 0005 inline flush, which *does* allocate
(it materializes new inserts). Reusing flush's id-allocation path here would
be a correctness bug — it would renumber rows and bump `next_row_id`, turning
a maintenance no-op into row-id churn and breaking lineage. Called out
explicitly so the flush and compaction paths never share id allocation.

For **rewrite-with-deletes**, the output's row-id set is the input's minus
the deleted ids, so a file's row-id range may become non-contiguous (holes
where deletes were). The `file` record stores whatever DuckLake's
`ducklake_data_file` stores for this (a row-id start plus counts, or an
explicit mapping) — moraine records what DuckLake records; the exact
representation for holed ranges is an open question below.

### Conflict semantics

Compaction touches `T` (its `file` / `delfile` / `fstat` / `tstat`
records), and classifies against concurrent commits by what they did to
`T`, mirroring DuckLake's own conflict matrix (source-verified in
`DuckLakeTransactionState::CheckForConflicts`, where `merge_adjacent` /
`rewrite_delete` conflict with drops, delete-froms, and other compactions —
and with nothing else):

- **Concurrent `DELETE` on `T`, `DROP` of `T`, or another compaction of
  `T`** → **true conflict.** The premise is invalid or contested: new
  deletes landed on an input file, the table vanished, or a competing
  rewrite consumed the same inputs. Abort with `CommitConflict`; a
  maintenance driver re-plans against the new head.
- **Concurrent append to `T`** (data-file registration, inline insert) →
  **benign.** The append's new files are not among compaction's inputs —
  the file sets are disjoint — so the plan stays valid; retry re-derives
  output file ids and stats against the new head and re-commits. Likewise
  benign: concurrent inline flushes and schema alters of `T` (a flush
  produces new files compaction never read; files reference columns by
  field id, RFC 0012, so an alter does not invalidate a rewrite), and all
  commits disjoint from `T`. Exactly RFC 0004's benign-race retry.

**The correctness crux:** a concurrent `DELETE` that adds a delete file to an
input file *after* compaction read it but *before* the CAS touches `T` — a
true conflict, so compaction aborts and re-plans, picking up the new
deletes. The table-grain CAS guarantees compaction either committed against
the state it planned on, or aborted. It never silently drops a concurrent
delete by rewriting over it. This falls straight out of RFC 0004 and is the
reason table-grain detection is sufficient here.

The delete-vs-compaction conflict is the one that carries the correctness
weight above; the append-vs-compaction compatibility is what keeps
maintenance schedulable at all on a live table — the dominant concurrent
traffic (appends) never aborts a compaction.

**Liveness, not just latency.** Abort-and-replan still has a starvation
mode, now confined to **delete-heavy** tables: a compaction reads and
rewrites Parquet for seconds to minutes, and any `DELETE` on the table in
that window aborts it. On a table under sustained deletes, compaction may
lose every race indefinitely — "maintenance is throughput-insensitive"
answers the latency question but not this one. The resolution needs no
protocol change, because compaction runs inside the same single-writer
process as every commit (RFC 0004 topology): the **maintenance driver may
briefly defer admission of same-table deletes** while a compaction is
staging its final batch — in-process scheduling above the protocol,
invisible to the store, bounded to the commit-staging tail (not the long
Parquet rewrite, which tolerates concurrent deletes up to the re-plan).
Bounded re-plan retries plus admission deferral give compaction guaranteed
progress; if e2e shows even that insufficient under realistic churn, the
fallback is the finer file-set grain already listed in Open questions.

### Delete files cannot outlive their data file

A `delfile` record references a specific `data_file_id`. A merge that
destroys that `data_file_id` cannot leave the delete file dangling.
Therefore **merging any input file that carries delete files is promoted to
rewrite-with-deletes**: the deletes are materialized into the merged output
rather than re-pointed. Re-associating a delete file to a new file id would
mean rewriting the delete file anyway *and* leaving the scan-time overlay
cost that compaction exists to remove — strictly more work for a worse
result. So the rule is simple: a plain merge applies only to files with no
live deletes; any input with deletes forces materialization.

### Interaction with inline (RFC 0005) and reclamation (RFC 0007)

Inline flush is the compaction of *inlined rows* to Parquet and **allocates**
row ids (new inserts); data-file compaction here touches no `inline` records
and **never allocates**. They are complementary maintenance ops. A full
maintenance pass is three independent commits in sequence: flush inline
(0005) → compact data files (0008) → expire and clean up (0007). Compaction
produces the superseded `hist` records that 0007 later reclaims; it never
deletes bytes itself.

### Test obligations

Per RFC 0001, integration tests run against real SlateDB on in-memory
`object_store` — no mocks:

- **Merge preserves rows and ids.** `N` small files → `M` files; a scan at
  head returns identical rows with identical row ids before and after;
  `next_row_id` unchanged.
- **Rewrite materializes deletes.** file + delete file → one file, no delete
  file; deleted rows absent; survivors keep their row ids; `record_count` /
  `tstat` decreased by exactly the deleted count.
- **Time travel unaffected.** A snapshot before the compaction resolves the
  pre-compaction files and returns identical results (superseded files still
  in `hist`).
- **True conflict aborts.** A concurrent `DELETE` on a table under compaction
  makes the compaction return `CommitConflict`; a replan + recommit then
  succeeds and reflects the concurrent delete.
- **Benign race retries.** A concurrent commit on a *disjoint* table lets the
  compaction benign-retry and commit.
- **Appends never abort compaction.** A concurrent insert into the *same*
  table during a compaction lets the compaction benign-retry and commit; the
  post-commit table contains both the inserted files and the compacted
  outputs, with stats consistent (mirrors DuckLake's insert-⟂-compaction
  compatibility).
- **Superseded files become reclaimable.** After compaction the old `file`
  records are in `hist` with `end_snapshot = N+1`; an RFC 0007 expiry past
  that horizon schedules and deletes their bytes.
- **Lineage survives merge + UPDATE.** An UPDATE issued after a merge still
  resolves row lineage correctly (ids were preserved through the merge).

## Open questions

- **Row-id representation for holed survivors.** After rewrite-with-deletes
  the surviving ids are non-contiguous. Does `ducklake_data_file` store an
  explicit per-file row-id mapping, a start+count plus a residual deletion
  vector, or per-row ids in the Parquet itself? Resolve against the spec and
  e2e; it fixes the `file` record's fields (RFC 0002 update if needed).
- **Does DuckLake ever renumber row ids on compaction** (e.g. to re-densify
  a holed range)? The spec says preserved; e2e must confirm no renumbering,
  or the "`next_row_id` untouched" invariant needs a defined re-mapping step.
- **Merge eligibility predicate.** Almost certainly inputs must share
  partition and schema version to merge; pin the exact predicate against
  `merge_adjacent_files` before implementing.
- **Compaction of disjoint file sets of the same table.** RFC 0004 makes two
  compactions of the *same* `table_id` a true conflict even on disjoint
  files. Finer (file-set) grain would let them run concurrently, but
  maintenance is not latency-critical; deferred, table grain matches RFC
  0004.

## Alternatives considered

- **Allocating fresh row ids for compacted rows** (reuse the RFC 0005 flush
  id path). Rejected: breaks DuckLake row lineage — UPDATE and time-travel
  row identity depend on stable ids — and bumps `next_row_id`, turning a
  no-op into row-id churn. Compaction must preserve ids; flush and compaction
  do not share id allocation.
- **Deleting superseded bytes inside the compaction commit.** Rejected: a
  past snapshot still references them, so this breaks time travel, and it
  couples object-store `DELETE` to the catalog commit. Superseded files take
  the same `hist` → `gcfile` → cleanup path as every other ended file (RFC
  0007).
- **Compaction as a background mutation outside RFC 0004** (direct puts).
  Rejected for the same reason as RFC 0007 expiry: it violates the
  single-`WriteBatch` / CAS / atomicity invariant (RFC 0002). A partial
  compaction could strand output files or lose the head update.
- **Internally rebasing a true-conflict compaction** (replay the rewrite
  against the new head). Rejected on the same grounds RFC 0004 rejects
  internal rebase of true conflicts: replay requires re-reading data files
  and re-planning, which is the maintenance driver's job, not the commit
  path's. Abort + typed error is honest; the driver re-plans.
- **Finer-than-table (per-file) conflict grain** so two compactions of one
  table run concurrently. Rejected: contradicts RFC 0004's table-level model
  for machinery maintenance does not need (it is throughput-insensitive).
  Revisit only if profiling shows table-grain maintenance starves under
  contention.
- **Carrying delete files forward onto merged outputs** instead of
  materializing them. Rejected: a delete file references a `data_file_id` the
  merge destroys, so it must be rewritten to the new ids anyway — strictly
  more work than materializing — and it leaves the scan-time overlay cost
  that compaction exists to eliminate.
