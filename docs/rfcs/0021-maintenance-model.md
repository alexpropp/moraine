# RFC 0021: Maintenance orchestration

- **Date:** 2026-07-24

## Summary

Three systems reclaim space beneath a moraine lake: DuckLake (expiry,
cleanup, compaction), SlateDB (LSM compaction and its own file collection),
and moraine itself (orphaned index entries, RFC 0016). An operator drives
the first two by hand in an order nothing documents, and the third does not
exist — RFC 0016 defers its sweep to "RFC 0007's maintenance posture" and
RFC 0007 owns no verb to hang it on. This RFC makes maintenance **a
scheduled pass inside the writer**: a thread the shim starts at `ATTACH`
issues DuckLake's own maintenance SQL through its own connection and then
reclaims moraine's orphaned index ranges, in a fixed order, retaining one
report per pass. It lives in the shim because that is the only layer
reaching both DuckLake's SQL and moraine's core, a deliberate exception to
the thin-shim rule. It adds no SlateDB step — the substrate already
collects itself. moraine computes no retention policy: **every interval and
every step is configured at attach**, and an attach that configures nothing
schedules nothing.

## Goals

- **Maintenance runs without an operator driving it**, in an order that is
  enforced rather than documented.
- **moraine chooses no policy.** No default interval, no default steps.
  RFC 0007's and RFC 0008's non-goals survive.
- **The orphaned-index-entry sweep gets a home and a design.** RFC 0016
  designed it and left it to "a bounded background sweep". Per-index
  reclamation exists (`Catalog::reclaim_index_entries`), but nothing
  discovers *which* indexes are dead or drives the reclamation, so the
  ranges leak in practice.
- **Safe by construction.** The sweep rests on an invariant (catalog ids
  are never reused), not on running at a quiet moment. Every step is
  idempotent, so an interrupted pass is safe to re-run.
- **One mechanism.** The scheduled pass and the on-demand trigger run the
  same code, in the same order, producing the same report.

Non-goals:

- **Reimplementing any DuckLake maintenance function.** They are called,
  not wrapped. RFC 0007 and RFC 0008 remain the specifications of what they
  do to moraine's keyspace.
- **A retention policy**, compaction threshold, or grace period.
- **Forcing SlateDB compaction** — possible, and declined (Design).
- **Checkpoint lifecycle** — an RFC 0017 / RFC 0006 concern; if it lands it
  becomes a consumer of this surface.

## Background

**DuckLake** drives the data-and-history story: expiry issues the row
cascade moraine translates, cleanup deletes Parquet and drains the
schedule, compaction rewrites live files. RFC 0007 and RFC 0008 establish
that moraine's job is translation and projection, and that a moraine-side
engine was rejected. This RFC sequences DuckLake's functions; it does not
replace them.

**SlateDB** already collects its own superseded objects, in every moraine
writer, with no help from this design. `StoreBuilder::settings`
(`open.rs:104`) overrides only the flush cadence and cache options, so
`garbage_collector_options` stays at `Settings::default()`'s
`Some(GarbageCollectorOptions::default())` (`config.rs:993`), and
`Db::builder(…).build()` constructs and starts a collector from it
(`db/builder.rs:699-725`): all six tasks — manifest, WAL, WAL fence,
compacted, compactions, detach — every 60s, deleting only what is
superseded, unreferenced by any active checkpoint, and older than a
5-minute `min_age`. A size-tiered compactor polls alongside it every 5s.

The steady state therefore needs nothing: expiry's deletes become
tombstones ordinary compaction removes, the collector reclaims the SSTs
that compaction supersedes, and `TagSegmentExtractor` (`store/segment.rs`)
gives each subspace its own segment so `history` churn compacts without
disturbing `current`. **This RFC adds no substrate step**, because there is
no gap to fill (Alternatives).

**moraine** owns one reclamation duty and does not discharge it. RFC 0016's
`index` subspace holds one entry per indexed row. `drop_index` ends the
definition into `history` (and `drop_table` ends its indexes with it), so
entries become invisible immediately — lookups resolve only against live
definitions — but are never deleted. The range leaks permanently, on every
dropped index and every dropped table that had one.

## Design

### Why the writer schedules itself

Maintenance cannot be cronned against a live lake. Opening a SlateDB `Db`
fences — `WriterFencer::fence` bumps the writer epoch and any parallel
older writer fails with `SlateDBError::Fenced` — so a second process
invoking the CLI would kill the application's writer. Since every step but
orphan detection mutates the catalog through the single writer handle, both
the work *and* its schedule must live in the process holding it. That is
RFC 0004's topology, not a preference.

Host-driven scheduling was the obvious alternative and does not survive the
same constraint: the writer is often a DuckDB CLI or an application with
nowhere to put a timer, and an external timer cannot help.

**The core cannot orchestrate.** moraine's core is a Rust library *beneath*
DuckDB, holding a SlateDB handle and knowing nothing of SQL, so
`Catalog::maintain` can never call `ducklake_expire_snapshots`. Nor can the
per-attach tokio runtime (`runtime.rs:32`), which is Rust-side and cannot
reach SQL. The scheduler is therefore a C++ thread holding the
`DatabaseInstance`, opening a fresh `Connection` per pass — the pattern
`wal_replay.cpp:399` already uses.

**The thin-shim exception, stated.** The repository rule is that logic
accumulating in `moraine-duckdb` belongs in the core. This RFC carves out
one exception: *sequencing DuckDB SQL, and scheduling that sequence, is
shim work, because the core can do neither.* It is bounded to sequencing —
the shim composes calls and collects outcomes, parses no results into
catalog state, and holds no maintenance logic of its own. The one
substantive mechanism, the sweep, lives in the core behind
`Catalog::maintain`. The core keeps its no-threads charter (RFC 0003); the
thread is shim-side.

### The sequence

Each pass runs these steps in order, skipping any the attach did not
configure:

| # | Step | Configured by | Issues |
|---|---|---|---|
| 1 | Expire | `EXPIRE_SNAPSHOTS[_OLDER_THAN\|_VERSIONS]` | `CALL ducklake_expire_snapshots('lake', …)` |
| 2 | Flush | `FLUSH_INLINED_DATA` | `CALL ducklake_flush_inlined_data('lake')` |
| 3 | Merge | `MERGE_ADJACENT_FILES` | `CALL ducklake_merge_adjacent_files('lake')` |
| 4 | Rewrite | `REWRITE_DATA_FILES[_DELETE_THRESHOLD]` | `CALL ducklake_rewrite_data_files('lake', …)` |
| 5 | Cleanup | `CLEANUP_OLD_FILES[_OLDER_THAN\|_CLEANUP_ALL]` | `CALL ducklake_cleanup_old_files('lake', …)` |
| 6 | Orphans | `DELETE_ORPHANED_FILES[_OLDER_THAN\|_CLEANUP_ALL]` | `CALL ducklake_delete_orphaned_files('lake', …)` |
| 7 | Sweep | `SWEEP_INDEXES` (default **true**) | `Catalog::maintain` — core |

The call syntax is what the e2e suite already exercises against real
DuckLake (`tests/ducklake_load/maintenance.rs:47,91,150,230,333`).

**Why this order.** Expiry first, because it is the only step that
*shrinks* the catalog rather than adding to it, and DuckLake re-reads the
snapshot projection at the start of every transaction (RFC 0009) — every
later step is served a smaller one. Flush before merge, so its small
Parquet files are merge input. Merge and rewrite before cleanup, because
merge schedules its superseded bytes directly (RFC 0008) and cleanup drains
that schedule in the same pass. Cleanup before orphan detection, so the
schedule is drained first. The sweep last, because its input is everything
the earlier steps left behind. The order is a cost preference —
every step is independently safe and idempotent in any order.

**Compaction is deliberately not placed before expiry.** The tempting
rationale — that expiry would then reclaim what compaction superseded —
fails for both verbs, for opposite reasons (RFC 0008). *Merge* leaves
nothing to reclaim: its output backdates `begin_snapshot` to cover every
snapshot the sources covered and carries a per-row `snapshot_id`, so time
travel into it filters rows rather than selecting a different file. Having
subsumed the sources' whole visibility history, it hard-deletes their
catalog rows — current and history alike — and schedules their bytes at
once. *Rewrite* leaves rows ended but not dead: it materializes deletes, so
its output holds fewer rows than the source and a reader below it must
still see the deleted ones. The source is ended into history with nothing
scheduled — but at a snapshot minted moments earlier, so every retained
snapshot sits below it and RFC 0007's dead-row rule (no surviving snapshot
in `[begin_snapshot, end_snapshot)`) cannot fire. Those rows wait under any
ordering, reclaimable only once the rewrite's own snapshot ages out.

### Configuration

Everything is an attach option, alongside the existing family
(`catalog.cpp:553`). `META_MAINTENANCE_INTERVAL` sets the cadence; without
it no thread starts. Steps are named `META_MAINTENANCE_<step>[_<param>]`,
where `<step>` and `<param>` derive from DuckLake's own names by one rule:

> **`<function name minus its `ducklake_` prefix>` enables the step, and
> `<that>_<DuckLake's own parameter name>` supplies a parameter.**

So `META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN` and
`META_MAINTENANCE_CLEANUP_OLD_FILES_CLEANUP_ALL`. The stutter in the second
is deliberate — a derivable rule that occasionally reads awkwardly beats a
vocabulary an operator learns twice. The step prefix is required because
three steps take a parameter named `older_than` and two take `cleanup_all`.

A bare `META_MAINTENANCE_<step> true` runs it with DuckLake's own defaults;
supplying a parameter implies enabling the step. Values pass through
unvalidated — a nonsensical `delete_threshold` is DuckLake's to reject.

Two options are moraine's own rather than derived, because the sweep is:
`META_MAINTENANCE_SWEEP_INDEXES` (default true) turns it off, and
`META_MAINTENANCE_BATCH_SIZE` bounds the deletes per commit. Both are
validated at bind — an unknown option, an unknown parameter for a known
step, a non-positive interval or batch size, and a step disabled while one
of its own parameters is supplied are all `BinderException`s, so a
misconfigured attach fails rather than starting a scheduler that quietly
does the wrong thing.

**Defaults are the safe floor.** Steps 1–6 mutate the lake — writing
Parquet, minting snapshots, or deleting bytes — so none has a default. An
interval alone schedules only the sweep, which touches nothing
a query can observe. Destructive steps run unattended only because an
operator wrote down that they should.

### The scheduler

One thread per read-write attach, started at `ATTACH` when an interval is
configured, stopped and joined at detach *before* `moraine_detach`
(`abi.rs:775`) releases the handle. Three properties it must hold:

- **Single-flight.** A pass still running when the next tick fires skips
  that tick rather than overlapping. Concurrent passes are safe — the sweep
  is idempotent — but their DuckLake steps collide under RFC 0008's
  conflict matrix, and a scheduler that manufactures its own conflicts is
  indefensible.
- **Stops before the database does.** Detach sets the stop flag and joins;
  the thread holds no reference that would keep the `DatabaseInstance`
  alive past shutdown.
- **Failures are visible.** An unattended pass has no one to return an
  error to, so `moraine_maintenance_status` serves the **last 16 passes**,
  newest first, each carrying `started_at` and whether it was `scheduled`
  or `manual`, then one row per step: `step`, `status` (`ran` / `skipped`
  / `failed`), and `detail`. Retaining a window rather than only the
  newest pass is load-bearing — with one slot a failure is erased by the
  next success, and a short interval would hide strictly more than a long
  one. The window is in-memory per attach and bounded so a fast interval
  cannot grow it without limit.

**Read-only attaches never schedule.** A `DbReader` never opens a writer.

A failed step aborts that pass, naming the step; earlier effects stand, and
the next tick re-runs from the top.

Single-tier by design: one interval, one step set. Steps have genuinely
different natural cadences — the sweep is two seeks when nothing was
dropped, while `delete_orphaned_files` LISTs the entire data prefix — but
encoding tiers into flat attach options is unwieldy, and a second attach
with a different configuration covers the case. Deferred until asked for.

### The on-demand trigger

`CALL moraine_maintenance('lake')` runs one pass immediately and returns
its report. It takes **no parameters**: configuration lives at attach, and
a second configuration surface would be a second thing to keep faithful.

It issues no SQL on the caller's context. It runs the pass inline on the
calling thread but through a **connection of its own**, under the same
single-flight lock the timer takes — so a trigger and a tick can never
overlap, and the trigger waits out a pass already in flight. The separate
connection is what avoids the re-entrancy a SQL-issuing table function
would hit: `ClientContext::Query` takes `context_lock`, a plain
non-recursive `mutex` (`client_context.hpp:318`), so a query issued from
inside a running operator on the *same* context deadlocks. A fresh
connection has its own context, so the problem does not arise.

Running inline rather than handing work to the thread also means the
trigger needs no thread at all: an attach that configured no interval
still answers it.

The trigger exists for two reasons, not for ergonomics: `cargo xtask e2e`
needs a deterministic way to run a pass without waiting on wall-clock, and
an operator needs a way to run one before a backup or after a bulk load
without re-attaching.

**Explicit transactions are refused.** The caller blocks while its own
second connection writes the catalog, so running inside a user's `BEGIN`
invites a self-deadlock. Refused unless
`context.transaction.IsAutoCommit()` (`transaction_context.hpp:49`).

### Orphaned index-entry reclamation

**The invariant that makes this safe.** `index_id` is allocated from the
global `next_catalog_id` counter (`verbs.rs:683`), so ids are monotonic and
never reused. Entries under an `index_id` not live at snapshot `S` can
never become live again — a dead range is dead forever. The sweep needs no
lock, no quiet period, and no coordination with the writer: it cannot
delete an entry a live index will want, because a live index's id was live
at `S` and is skipped.

**Discovery is a skip-scan.** Entry keys place `index_id` immediately after
the kind discriminant — `idx_index_prefix(kind, index_id)` covers exactly
`INDEX_KIND_PREFIX_LEN + size_of::<u64>()` bytes (`store/key.rs:551`) — so
the subspace is ordered by index id within each kind. For each kind: seek
to the start, read one key, decode its `index_id`; if live in the
`CatalogSnapshot` at `S`, seek past the whole index at
`idx_index_prefix(kind, index_id + 1)`; otherwise delete the range in
batches and continue. Cost is one seek per *distinct index id present*, not
one read per entry. A store whose indexes are all live pays two seeks per
index and deletes nothing.

**History is not consulted.** The dead set derives from what the `index`
subspace contains, checked against the live catalog — never from ended
definitions in `history`. Expiry may prune an index definition's history
record long before anyone sweeps, after which a history-derived dead set
loses the id and leaks the range forever. Deriving from the data being
reclaimed has no such failure mode, mirroring RFC 0007's preference for a
scan-based dead-row rule over maintained reference counts.

**Each batch is a head-preserving maintenance commit** — RFC 0007's shape:
one `WriteBatch`, no `ducklake_snapshot` insert, no `sys/head` advance.
Between batches the sweep yields, so a large reclamation never holds the
writer, and it cannot conflict with one: the only keys it writes are
deletes under dead index ids, which no live commit touches.

### The core verb

```rust
pub struct MaintenanceRequest {
    pub sweep_orphaned_index_entries: bool,  // default true
    pub batch_size: usize,                   // default 1024
}

pub struct MaintenanceReport {
    pub indexes_swept: u64,
    pub index_entries_reclaimed: u64,
}

impl Catalog {
    pub async fn maintain(&self, request: MaintenanceRequest)
        -> Result<MaintenanceReport>;
}
```

Both structs are `#[non_exhaustive]`, so a future leg is additive. No
`slatedb::` type appears — RFC 0003's substrate rule holds. RFC 0003's
operation table gains this under a **Maintenance** group.

### The `DATA_PATH` overlap guard

`ducklake_delete_orphaned_files` LISTs the data prefix and deletes
everything the catalog does not reference (RFC 0007), and cannot know some
of those objects *are* the catalog. Nothing today prevents attaching
`'ducklake:moraine:s3://bucket/lake/catalog'` with `DATA_PATH
's3://bucket/lake/'`, which places every SlateDB SST, manifest, and WAL
object under the swept prefix. Step 6 running unattended on a timer makes
that a standing hazard rather than a one-off mistake, so the guard ships
with it: attach refuses, with `Constraint`, when the store path and
`DATA_PATH` are on the same object store and either is a prefix of the
other. Containment is compared by path component, so sibling prefixes that
merely share leading text (`…/lake` and `…/lakehouse`) attach normally, as
do different buckets and different store kinds.

Two limits are load-bearing. The check runs **before the catalog is
opened**, because bootstrapping a fresh store records `data_path` — a
check that waited until after the open would persist the dangerous value
and *then* refuse, leaving it for the next attach to inherit. And the
guard sees only the path moraine is told about: `META_DATA_PATH`, or a
value already recorded for the lake. DuckLake keeps its own unprefixed
`DATA_PATH` for the data layer and does not forward it to this metadata
attach, so an attach naming only that leaves nothing to compare.

### Test obligations

Per RFC 0001, core tests run against real SlateDB on in-memory
`object_store`; the live path is pinned by `cargo xtask e2e`.

Core:

- **Sweep reclaims a dropped index**, and the drop-table cascade reclaims
  both indexes of a two-index table; a later scan of `index` finds nothing.
- **Sweep spares live indexes** interleaved by id; lookups unchanged. A
  second `maintain` then reports zero and writes nothing.
- **Batching is bounded**, each batch head-preserving — `sys/head`
  unchanged across the whole sweep.
- **No writer conflict.** A sweep interleaved with commits landing entries
  for a *live* index completes, and both ranges end up correct.
- **Discovery seeks by id.** From any starting id, the scan returns the
  lowest index at or after it and `None` past the last — the property that
  makes skipping a live index one seek rather than a walk of its entries.
- **A zero batch size is refused**, and a read-only catalog refuses the
  whole pass.
- **Key ordering.** Index prefixes sort ascending within a kind and the two
  kinds occupy disjoint ranges, so the skip-scan's seek target is sound.

Live (e2e):

- **Unconfigured attach schedules nothing** — every DuckLake step reports
  `skipped`, the sweep runs, and no data moves.
- **The sweep reclaims a dropped index** and only that: a live index is
  spared, the drop orphans its range, the next pass reports exactly that
  range, and a third finds nothing.
- **Configured pass.** Steps report in sequence order with the configured
  ones `ran` and the rest `skipped`, and the lake's contents are unchanged.
- **The trigger refuses inside an explicit transaction** rather than
  hanging.
- **Misconfiguration fails at bind**, naming the unknown option or the
  step disabled alongside its own parameter.
- **Status retains earlier passes.** A pass that reclaimed stays visible
  after a later pass that did not, each carrying its trigger.
- **Read-only never schedules.** A read-only attach starts no thread, so
  its status window stays empty.
- **Path overlap refused**; sibling locations attach normally.

Shim unit tests cover the ABI edge the pass rides: `moraine_maintain`
writes its counts through the out-parameters and accepts null slots for
either, and the overlap guard is exercised across store kinds, buckets,
and sibling-versus-nested prefixes without needing a lake.

Verified against a live lake during bring-up but not yet pinned by a test,
because each needs a multi-second wait that would slow the gate: the timer
thread running a pass unattended, detach stopping it cleanly, and
single-flight under a pass longer than the interval. These are the first
candidates if the scheduler grows.

## Open questions

- **Scheduler thread lifetime against DuckDB shutdown.** `OnDetach` stops
  and joins, and the destructor repeats it for paths that never reach that
  hook — both verified against a live scheduler between passes. Two cases
  remain unresolved. A detach arriving *mid-pass* blocks until the pass
  finishes, which is the property that keeps a pass from ever running
  against a detached database, but it means detach waits on SQL running on
  another connection against the same instance — whether that can deadlock
  against whatever detach itself holds is untested. And a process torn
  down without detaching at all has no defined outcome, since DuckDB's
  extension teardown ordering is not something this design can assert.
- **Trigger-under-transaction.** Resolved for the autocommit case: the
  trigger runs the pass through a second connection and works against a
  real lake, including a full seven-step pass. The explicit-transaction
  refusal remains a guard rather than a proof — whether a blocked
  autocommit caller can still hold something the pass's connection needs
  under heavier concurrency is untested.
- **Range delete.** SlateDB 0.14.1 exposes none, so the sweep is a batched
  scan-and-delete — this RFC's answer to RFC 0016's open question, for the
  pinned version. If one appears, the sweep collapses to a call per dead
  index and this design is the fallback.
- **Batch size default.** 1024 is a strawman, to be set from measurement.
- **Status durability.** The retained window is in memory, so it starts
  empty at every attach and a schedule that failed overnight in a process
  since restarted leaves no trace. Persisting it — and whether that
  belongs in the catalog or outside it — is open. The window size (16) is
  a strawman.
- **DuckLake version coupling.** The shim names DuckLake's function
  signatures and, through the derivation rule, its parameter names. A
  signature change breaks the pass where it previously broke only the
  operator's script, and a new DuckLake parameter stays invisible until
  moraine exposes it. The e2e suite pins the tracked version; whether to
  detect and degrade rather than fail is open.

## Alternatives considered

- **A documented ordering, with no orchestration and no schedule** (this
  RFC's first draft). Rejected: it left the operator running six statements
  in an order nothing enforced, and called that unification.

- **A fully parameterized table function** as the primary surface, with
  scheduling layered on top. Rejected once the writer had to schedule
  itself: it would be a second configuration surface saying the same things
  as the attach options, and a table function that issues SQL on the
  caller's context runs into `context_lock` re-entrancy. Collapsing
  configuration into attach options and reducing the verb to a
  parameterless trigger removes both problems.

- **No trigger at all** — timer only. Tempting, and rejected for the test
  gate: every maintenance e2e would have to configure a tiny interval and
  wait on wall-clock, which is precisely the flaky slow test `cargo xtask
  e2e` should not contain. The trigger also serves the run-it-now case
  without a re-attach.

- **Host-driven scheduling only** (no thread; the verb composes into
  whatever scheduler the host already has). Rejected once the single-writer
  constraint was followed through — see "Why the writer schedules itself".
  The residual cost of the thread is real and accepted: the shim now owns a
  lifecycle it did not have.

- **A substrate-collection step** running `Admin::run_gc_once` in the pass,
  to reclaim without waiting for the background cadence. Carried through
  several drafts and cut on checking the defaults: moraine already runs
  that exact collector, all six tasks, every 60 seconds
  (Background). A pass scheduled at minutes-to-hours cadence gains nothing
  by saving up to a minute, and `Admin::run_gc_once` would build a *second*
  collector racing the built-in one — including a second
  `remove_expired_checkpoints`, which is a read-modify-write on the
  manifest. It also could not report anything: `run_gc_task` logs each
  error and continues (`garbage_collector.rs:384-398`) and `run_gc_once`
  returns `()`, so a wholly failed pass is indistinguishable from a clean
  one. Redundant, mildly racy, and unobservable.

- **Forcing SlateDB compaction**, so a bulk expiry's tombstones reach the
  last sorted run promptly rather than on the size-tiered scheduler's own
  timing. The motivation is real and the APIs are all public at the pinned
  version (`read_compactor_state_view`'s `VersionedManifest` exposes `l0()`
  and `compacted()`; `SsTableView::id` and `SortedRun::id` are public
  fields, `db_state.rs:67,465`; `SourceId`, `CompactionSpec::new` /
  `for_segment` / `drain_segment`, and `submit_compaction` likewise) — so
  this is a choice, not a limitation. Rejected because choosing which
  sources merge into which destination *is* the compaction policy
  SlateDB's scheduler exists to make; because `TagSegmentExtractor` gives
  each subspace its own tree, making a correct spec per-segment and a
  substrate detail RFC 0003 keeps out; and because `submit_compaction` only
  queues work for a worker, so it would not even deliver the synchronous
  reclaim that motivates it. The residual want is a "compact now and wait"
  primitive — an upstream request.

- **A compaction filter that prunes dead history.** SlateDB accepts a
  `CompactionFilterSupplier`, so moraine could drop `history` entries below
  the oldest live snapshot during LSM compaction — history pruning for
  free. Rejected: it is exactly the moraine-side reclamation policy
  RFC 0007 and RFC 0008 each rejected, it would diverge moraine's state
  from the catalog DuckLake believes it wrote, and SlateDB documents that
  filters break snapshot consistency (`compaction_filter.rs`).

- **Unifying the retention windows.** DuckLake's `older_than`, cleanup's
  grace, SlateDB's GC `min_age`, and `checkpoint_lifetime` look like one
  knob stated four times. They are not: RFC 0009 releases the SlateDB read
  handle as soon as a `CatalogSnapshot` is materialized, so no moraine
  reader outlives a 5-minute `min_age`, and the DuckLake windows govern
  Parquet and catalog history, which SlateDB's collector never touches.
  Deriving one from another would invent a coupling that does not exist.

- **Two rejected parameter alignments.** *One shared `older_than`* across
  expiry, cleanup, and orphan detection is the most tempting, and actively
  harmful: they are three different windows (history to retain, how long
  scheduled bytes survive, how old an unreferenced file must be),
  independent in RFC 0007 for good reason. *Short moraine-chosen names*
  would be a second vocabulary to learn, drifting from DuckLake's as they
  evolve; the occasional stutter is the price of never looking anything up.

- **Defaulting the DuckLake steps** so a bare interval does everything.
  Rejected: it would make moraine the author of a retention policy against
  RFC 0007's non-goal, and turn configuring a cadence into silent data
  deletion.

- **A durable pending-sweep list** written on drop and drained by the
  sweep, the shape `ducklake_files_scheduled_for_deletion` uses. Rejected:
  it adds a mutable bookkeeping record to every drop commit, and the
  skip-scan derives the same set from the data at negligible cost.
  RFC 0007 rejected reference counts for the same reason.

- **Deriving the dead set from ended definitions in `history`.** The
  obvious source, and wrong: expiry may prune a definition's history record
  while its entries remain, leaving the id unrecoverable and the range
  leaked forever.
