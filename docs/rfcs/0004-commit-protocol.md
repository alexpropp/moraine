# RFC 0004: Commit and transaction protocol

- **Date:** 2026-07-09

## Summary

Defines how a DuckLake catalog mutation becomes a durable, atomic commit in
moraine: how a committer reads the head snapshot, allocates ids, detects
conflicts against concurrent commits, and lands everything in a single
atomic SlateDB write. Builds directly on RFC 0002's atomicity invariant
(one commit = one batch) and treats RFC 0005 inlined writes as first-class
commit participants. The design is optimistic and single-writer-oriented:
commits race on the head pointer under SlateDB's transactional write-write
conflict detection, disjoint concurrent commits retry internally, and
genuine conflicts surface as a typed error for the caller — DuckLake or the
application — to re-drive. The protocol has **two front doors** over one
commit core: the verb/closure API of RFC 0003 (retryable internally,
because moraine authors the mutations) and the staged-row path of RFC 0006
(never retried internally, because DuckLake authors the rows — a lost race
surfaces to DuckLake). The two paths have different retry rights, and
keeping them distinct is load-bearing; this RFC specifies both.

## Goals

- Every commit is exactly one SlateDB `WriteBatch` (RFC 0002). Snapshot
  allocation, id allocation, conflict detection, and the write itself all
  fit inside that envelope — no read-modify-write spanning batches.
- Inlined inserts/deletes (RFC 0005) ride the same batch as their catalog
  metadata. Inlining adds no round trips to the commit path.
- On the verb path (RFC 0003), concurrent commits touching disjoint tables
  — and concurrent pure appends to the *same* table — both succeed without
  caller involvement; incompatible commits resolve to a typed conflict. On
  the staged-row path (RFC 0006), every lost race surfaces to DuckLake.
- Commit latency floor is one durable WAL flush; the protocol enables group
  commit so throughput under a funnel of concurrent transactions is not
  one-flush-per-commit.
- Faithful to DuckLake's own concurrency semantics — moraine implements
  DuckLake's conflict model, it does not invent a stricter or looser one.

Non-goals:

- **Multi-writer coordination.** The supported topology is single-writer,
  many-readers (see Topology). Concurrent *commit coordination* across
  processes is out of scope; the escape hatch is funneling commits through
  one process.
- **Key layout and value codec** — RFC 0002.
- **Inlined data record formats** — RFC 0005. This RFC covers only how
  inlined writes participate in a commit.
- **Snapshot expiry / GC** of `hist` and flushed inline data — RFC 0007.

## Background

RFC 0002 established the keyspace: `sys/head` holds the latest committed
`snapshot_id`; the `snap` subspace is append-only snapshot records; `cur`
and `hist` split live from ended entity versions; commits are atomic
`WriteBatch` writes under a single fenced SlateDB writer.

SlateDB (pinned at 0.14.x) provides the concrete primitives this protocol
uses, verified against its source rather than assumed from prose:
`Db::begin(IsolationLevel::Snapshot)` opens a transaction whose commit
performs **write-write conflict detection** on the keys it writes and fails
with a typed `TransactionConflict`; `Db::snapshot()` gives a pinned
consistent read handle (RFC 0009); `DbReader` opens a store **read-only**
without becoming a writer; and writer fencing is a monotonic
`writer_epoch` bumped by manifest CAS on `Db::open`. There is no bespoke
key-level compare-and-swap API, and none is needed — the "head CAS" in
this RFC is realized as conflict detection on `sys/head` (see the commit
sequence).

DuckLake allocates
ids from counters: `next_catalog_id` and `next_file_id` live in the
`ducklake_snapshot` record; `next_row_id` is **per-table** in
`ducklake_table_stats` (`next_row_id += record_count` on insert), preserved
across UPDATE and compaction for row lineage. The `ducklake_snapshot` record
also carries `schema_version` — a counter DuckLake advances only when a
commit changes the catalog *schema*, and that DuckDB uses as its
schema-cache key. This RFC defines the protocol that maintains all of these
under optimistic concurrency.

## Design

### Topology

**Single writer, many readers.** Readers open the store through SlateDB's
`DbReader` — a handle that follows the manifest, never becomes a writer,
and never participates in fencing (only `Db::open` and the compactor bump
manifest epochs); there is no bound on their number. Writers are expected
to be a single logical committer holding the one read-write `Db`.

One source-verified caveat keeps "read-only" from being oversimplified:
`DbReader::open` in latest mode **writes a checkpoint into the manifest**
(a manifest CAS update) and maintains it from a background poller for the
reader's lifetime. So a reader does not fence and does not write data,
but it *does* need write access to the manifest object and generates
manifest traffic, and its checkpoint pins SSTs against SlateDB's own GC
for the checkpoint lifetime. Deployments that want truly zero-write
readers configure `DbReader` against an existing checkpoint id instead;
the default "follow latest" mode trades a small manifest write for
consistent WAL-inclusive reads.

Be precise about what fencing does, because its direction is the opposite
of a lock: SlateDB's `writer_epoch` means **the newest writer wins**. A
second process opening the store read-write bumps the epoch and **fences
the incumbent** — the first writer's next durable write fails, not the
second's. Safety is absolute (a fenced writer writes nothing, never
corrupts — RFC 0011 C2), but availability is not: two processes attaching
read-write do not degrade to "second one fails," they take turns killing
each other's committer. The operational rule follows: **exactly one
process attaches read-write; every other process attaches read-only**
(`DbReader`), and RFC 0006 exposes a read-only attach option so a DuckDB
process can opt into the reader role explicitly.

This is a real product limitation relative to the multi-client SQL
catalogs DuckLake otherwise uses — many DuckDB processes writing through
one PostgreSQL catalog is the common lakehouse shape, and moraine does not
support it: there is no funnel component in the DuckLake deployment model,
so "run one long-lived committer process and route commits through it" is
an escape hatch for embedding hosts, not something a fleet of independent
DuckDB clients gets for free. This constraint is stated here once,
surfaced in README-level documentation, and revisited only if a
multi-writer coordination design (out of scope, per Non-goals) is ever
taken up.

### The commit sequence

A commit takes a set of staged catalog mutations (entity inserts, entity
ends, inlined writes) and lands them atomically:

1. **Load head.** Open the SlateDB transaction, then read `sys/head` →
   snapshot id `N`, the snapshot `N` record for the global counters, and
   the `tstat` records for any tables this commit inserts rows into — all
   **through the transaction handle**, so the premise view and the
   conflict-detection window share one start sequence and no commit can
   land unseen between them (RFC 0009, "Commit attempts materialize
   through the transaction").
2. **Allocate ids** locally by bumping in-memory copies of the counters:
   `next_catalog_id`/`next_file_id` from the snapshot record, `next_row_id`
   from each touched table's `tstat`. No store writes yet.
3. **Stage the batch.** Assemble one atomic write set:
   - new entity records into `cur` (and, for ended versions, delete the
     `cur` key + write the `hist` key — RFC 0002);
   - inlined `inline/ins` chunks and `inline/idel`/`inline/fdel` records
     (RFC 0005);
   - updated `tstat` records carrying advanced `next_row_id`;
   - the new `snap` record `N+1` with advanced global counters,
     `schema_version` (bumped or carried forward per the rule below), and
     merged `snapshot_changes`;
   - the `sys/head` update `N → N+1`.
4. **Commit under head conflict detection.** The staged writes commit as
   one SlateDB transaction (`begin(IsolationLevel::Snapshot)`) opened at
   step 1. Every commit writes `sys/head`, so SlateDB's write-write
   conflict detection on that one key *is* the head CAS: if a concurrent
   commit advanced the head since this transaction began, `commit()`
   returns `TransactionConflict` and nothing was written — go to conflict
   handling. If it lands, the commit is durable once the WAL flush lands —
   done. (This is deliberately not a bespoke compare-and-swap: SlateDB
   exposes no key-level CAS API, and the transaction primitive it does
   expose is exactly the required shape. A transaction's write set is
   applied as one atomic batch, so RFC 0002's one-commit-one-batch
   invariant is preserved verbatim. Two source-verified mechanics worth
   knowing: the conflict check runs inside SlateDB's single batch-writer
   event loop immediately before the write is sequenced, so check and
   apply cannot interleave; and the detection state is **in-process** —
   which is sufficient precisely because the topology admits one writer
   process, with cross-process safety carried by fencing, not by conflict
   detection. Plain non-transactional writes are tracked too, modeled as
   single-op transactions, so a stray direct write cannot slip past a
   concurrent commit's conflict check.)

The read in step 1 is "load head," not a lock; the write in step 4 is the
whole commit. Nothing spans two batches. Within one process, concurrent
commit attempts (multiple connections) may run steps 1–3 optimistically in
parallel — the transaction conflict at step 4 arbitrates — or the
committer may serialize them (see Group commit); across processes, the
topology above guarantees there is no second writer to race.

### Conflict detection — table-level

When the head conflict fires, the committer compares the set of
`table_id`s it touched against those touched by the intervening commits
`N+1 … N+k` (recoverable from their `snapshot_changes`):

- **Disjoint table sets → benign race.** The concurrent commits do not
  invalidate this commit's premise. Retry from step 1 against the new head
  (what a retry re-runs is defined in Conflict resolution below — on the
  verb path it includes re-running the caller's closure, not just
  re-stamping ids). This is bounded internal retry inside `txn`; no caller
  involvement.
- **Overlapping table set → true conflict** (subject to the append-append
  refinement below). A concurrent commit mutated a table this commit also
  mutated. The premise may be invalid (the table was dropped, a column
  altered, files this commit references were compacted away). Abort with a
  typed `CommitConflict`.

**Counter advancement is never, by itself, a conflict.** Every commit reads
and bumps `next_catalog_id`/`next_file_id`; that shared read is precisely
what benign-race retry re-derives. Only overlapping `table_id`s are a
conflict. This is the rule that keeps table-level detection from collapsing
into serialize-everything.

**Append-append refinement.** A same-table overlap is *not* automatically a
true conflict when both sides touched the shared table only as **pure
appends**: `register_data_file` / `inline_insert` records plus the `tstat`
counter/stat advance they imply. Data-file registrations are commutative —
neither invalidates the other's premise — and the `next_row_id`/stats
updates are mechanically re-derivable on retry exactly like the global
counters. So append-append overlap classifies as **benign** on the verb
path: retry re-reads the advanced `next_row_id`, re-allocates row ids,
re-merges stats, and re-commits.

This mirrors DuckLake's own conflict matrix, verified in its source
(`DuckLakeTransactionState::CheckForConflicts`,
`src/storage/ducklake_transaction_state.cpp`): an insert conflicts with a
concurrent **drop**, **alter**, or **delete** on the same table — and with
nothing else. Two inserts never conflict, and an insert is also compatible
with a concurrent inline **flush** or **compaction** of the same table
(their file sets are disjoint: the insert's new files are not the
maintenance operation's inputs). moraine's verb path adopts the same
classification: an append is benign against other appends, flushes, and
compactions of its table; drop/alter/delete on that table are true
conflicts, in both directions. DuckLake is finer-grained in exactly one
place — two transactions *deleting* from the same table conflict only if
they touched the same **data files** (its `CheckForConflicts` fetches the
files deleted after the transaction's snapshot and conflict-checks at
`data_file_id` grain). moraine keeps delete-delete at table grain on the
verb path: stricter is safe there, because the verb path is moraine's own
embedding API — while on the extension path DuckLake applies its own
matrix itself (see Staged-row commits), so fidelity to DuckLake is
structural, not something moraine's classifier must reproduce. File-grain
delete-delete is a possible later refinement if an embedding workload
wants it.

**Name collisions are invisible to id-set comparison.** Two concurrent
commits each creating a table named `orders` in the same schema allocate
fresh, *disjoint* table ids — the set comparison classifies the race
benign. That is correct as classification (neither commit's staged state is
invalidated), but uniqueness must still be enforced: on the verb path it is
the **closure re-run** on retry that catches it — the re-run resolves names
against the new head, sees the winner's table, and returns `AlreadyExists`;
on the staged-row path every lost head surfaces to DuckLake (below), which
re-drives with full knowledge of the new catalog state. Set comparison
alone never establishes name uniqueness; re-validation against the new head
does. This is stated explicitly because it is the tempting shortcut: a
purely mechanical re-stage-and-retry, with no re-validation, would commit
duplicate names.

One operation does not fit table grain and is handled at **schema-list
granularity**: `CREATE`/`DROP SCHEMA` (mutates the set of schemas).
Concurrent commits that both touch the schema list conflict with each
other — matching DuckLake's matrix, where created/dropped schemas
conflict-check against each other and against creations inside a dropped
schema. Rare enough that coarse handling costs nothing. Option changes
need no grain at all: DuckLake neither versions nor conflict-detects them
(`set_option` writes `ducklake_metadata` outside the snapshot protocol,
last-write-wins — see Schema-version tracking); the verb path mirrors
that, treating `set_option`/`unset_option` as non-conflicting overwrites.

The benign/true classification is mechanical — set comparison over
`table_id`s and mutation kinds — but classification only decides *whether*
to retry; the retry itself re-validates (next section). `txn` still never
needs to understand what SQL produced the mutations.

### Conflict resolution — abort, with internal benign-race retry

**What a benign-race retry re-runs (verb path).** A retry is a full re-run
of the commit cycle, exactly as RFC 0003 specifies for `commit`: re-read a
fresh `CatalogSnapshot` at the new head, **re-run the caller's closure**
against it, re-allocate ids from the advanced counters, re-stage, re-commit.
The closure re-run is load-bearing, not an implementation nicety: it is
what re-resolves names and re-validates logical premises against the state
that actually won (the name-collision case above), and it is why RFC 0003
requires the closure to be pure and re-runnable. A retry that mechanically
re-stamped ids onto the previously staged records — without re-running the
closure — would be cheaper and wrong (see Alternatives).

The core resolves benign races itself (bounded retry, above) and **aborts
true conflicts with a typed `CommitConflict`**. It does not attempt to
rebase a true conflict. Rebasing a true conflict means re-running the
*DuckLake operation* — which `moraine` cannot do, because by RFC 0001 the
core is DuckLake-agnostic and never held the originating SQL. Re-driving a
true conflict belongs to whoever authored the operation:

- **DuckLake**, whose commit loop retries retryable conflict signals
  internally and re-checks its own conflict matrix (source-verified; see
  Staged-row commits and Open questions), or
- **the application**, re-running its statement when DuckLake classifies
  the conflict as true and throws — as a PostgreSQL `SERIALIZABLE`
  transaction retries on a `40001` serialization failure.

The core's responsibility ends at a truthful typed error.

The internal benign-race retry is bounded (a small fixed attempt count);
exceeding it returns `CommitConflict` rather than looping unbounded, so a
pathological write storm degrades to caller-visible conflicts instead of a
hang.

### Id allocation

- **`next_catalog_id`, `next_file_id`** — global, in the `snap` record,
  matching DuckLake. Allocated by bumping the in-memory copy from head and
  writing the advanced value in the new snapshot record. Shared reads of
  these counters are benign (re-derived on retry), never conflicts.
- **`next_row_id`** — **per-table, in the `tstat` record**, matching
  DuckLake's `ducklake_table_stats.next_row_id`. Because the counter lives
  in table-scoped state, bumping it *is* a mutation to that table and is
  inside the table's conflict set: two commits inserting into different
  tables share no row-id counter, so they never contend; two inserting into
  the same table overlap on that `table_id` — and resolve under the
  **append-append refinement** above: the shared counter read is
  re-derivable, so on the verb path both commits land via benign-race retry
  (the loser re-allocates its row ids from the winner's advanced
  `next_row_id`). Row ids stay dense per table for RFC 0005's inlined
  chunks. Verified against the DuckLake spec; the e2e suite validates the
  increment mechanics (`next_row_id += record_count`) and DuckLake's own
  behavior under concurrent same-table inserts.

### Schema-version tracking

`schema_version` is a counter separate from the id counters: DuckLake
advances it **only when a commit changes the catalog schema**, and carries
it forward unchanged for data-only commits. DuckDB keys its schema-metadata
cache on it, so getting this wrong is not a storage bug but a
client-visible one — in both directions. *Always* incrementing defeats the
cache: every `INSERT` looks like a schema change and forces DuckDB to
re-fetch every table's columns, multiplying catalog read traffic on exactly
the small-append workload moraine targets. *Never* incrementing serves a
stale cache: after `add_column`, DuckDB keeps using its cached column list
and returns wrong results or errors on the schema mismatch.

The committer tracks a `schema_changed` flag over the staged mutations. On
commit, the new snapshot record's `schema_version` is `prev + 1` if the
flag is set, else copied from snapshot `N`. The flag is set by the
structural mutators — those that alter the catalog's shape rather than its
data (RFC 0003 verb names):

- **Schema-changing** (`schema_version` bumps): create/drop schema;
  create/rename/drop table and move-to-schema; create/alter/rename/drop
  view; add/remove/rename column, column type change, default and
  null-constraint changes; set partition key; set sort key; **table and
  column comments** (they are alters in DuckLake and ride the same path);
  create/drop macros.
- **Data-only** (`schema_version` carried forward): `register_data_file` /
  `expire_data_file`, `register_delete_file` / `expire_delete_file`,
  statistics updates, column/name-mapping registration, and the RFC 0005
  inlined `inline_insert` / `inline_delete` / `flush_inlined_data`.
- **Outside the snapshot protocol entirely**: option changes.
  `set_option` in DuckLake is an immediate check-then-write on
  `ducklake_metadata` within the transaction's metadata connection — it
  mints no snapshot, bumps no `schema_version`, and is not recorded in
  `snapshot_changes` (so it is invisible to conflict detection; options
  are last-write-wins).

This classification is source-verified against DuckLake
(`DuckLakeTransactionState::SchemaChangesMade`, which tests exactly the
new/dropped table, schema, view, and macro sets — every `AlterEntry`
variant lands its new entry in `new_tables` and therefore bumps).

The flag is an explicit property of the mutation set, not something
inferred from the batch's key set after the fact — an inference would
misclassify (e.g. a stats-only touch of a table's state) and silently
re-introduce one of the two failure modes. The exact schema-mutating set is
pinned against DuckLake's own behaviour in the e2e suite (Open questions).

The flag machinery above belongs to the **verb path only**. On the
staged-row path (next section) DuckLake computes `schema_version` itself
and writes it in the `ducklake_snapshot` row it authors; moraine stores
that value row-faithfully and neither tracks nor second-guesses a flag.
The two paths must agree — the e2e schema-version matrix pins both.

### Staged-row commits — the extension path

RFC 0006's extension surface does not call RFC 0003's verbs. DuckLake
drives moraine with row-level DML against the `ducklake_*` tables: a
DuckLake transaction stages row inserts/updates/deletes, and — critically —
DuckLake itself authors the catalog mechanics. It reads `next_catalog_id` /
`next_file_id` from the snapshot row, computes the new ids, computes
`schema_version`, and writes the new `ducklake_snapshot` row, with the new
snapshot id and `begin_snapshot` values **embedded in every row it
stages**. moraine's job on this path is narrower and different from the
verb path:

- **Translate, don't author.** moraine maps each staged row onto the
  RFC 0002 keyed layout — an INSERT becomes a `cur` record; an UPDATE that
  sets `end_snapshot` becomes the end-version bookkeeping (delete `cur`
  key, write `hist` key); the snapshot row becomes the `snap` record and
  the `sys/head` advance. This begin/end-lifecycle translation is the one
  semantic convention moraine interprets (RFC 0006 states and bounds it);
  ids, counters, and `schema_version` are DuckLake's values, stored
  verbatim.
- **Same commit core.** The translated records land through steps 3–4
  above: one transaction, one atomic batch, head conflict detection.
  Nothing about atomicity or durability differs between the paths.
- **No internal benign-race retry.** A retry on the verb path re-runs the
  closure and re-stamps ids moraine allocated. On this path there is
  nothing moraine can safely re-run: the snapshot id, `begin_snapshot`,
  counter values, and `schema_version` are DuckLake-authored and embedded
  in row values moraine must carry faithfully — re-stamping them would be
  semantic surgery on another system's data, the exact re-modeling RFC
  0006 forbids. Therefore **every lost head on the staged-row path
  surfaces as a typed `CommitConflict`** to DuckLake — benign-shaped or
  not — and this is exactly the composition DuckLake expects of its
  catalog, verified in its source: DuckLake's `RunCommitLoop` wraps every
  metadata-catalog commit in its own bounded retry (default
  `ducklake_max_retry_count = 10`, jittered exponential backoff from
  `ducklake_retry_wait_ms = 100` × `ducklake_retry_backoff = 1.5`),
  re-checking its own conflict matrix against `ducklake_snapshot_changes`
  before each re-attempt and re-staging with freshly derived ids. Benign
  races are absorbed by *DuckLake's* loop; true conflicts throw its
  `TransactionException` to the application. The "disjoint concurrent
  commits both succeed without caller involvement" goal is thus a
  **verb-path** property; on the extension path the equivalent behavior
  is provided by DuckLake, one layer up, where it belongs.

The two front doors share the commit core (staging, one-batch atomicity,
head conflict detection, durability) and differ only in who authors the
mutations and therefore who may retry. Keeping that split explicit is what
lets the verb path be aggressive (closure re-run, append-append benign)
without ever risking a silent rewrite of DuckLake-authored state.

### Inlined writes as commit participants

Per RFC 0005, an inlined insert/delete is not a separate path: its
`inline/*` records are added to the same `WriteBatch` in step 3, allocating
row ids from the same per-table `next_row_id` bump as a Parquet write
would. An inline flush is itself a commit — Parquet PUTs first, then one
batch that creates the `file`/`delfile` records and moves consumed inline
records to `hist`. Nothing about inlining changes the commit sequence or
the conflict rules; it only adds record kinds to the batch.

### Group commit

Because the topology funnels concurrent transactions through one committer,
the committer may batch several pending catalog commits into one WAL flush:
allocate ids for each, stage their records into one `WriteBatch`, one CAS,
one durable flush. Per-commit latency stays one-PUT-bound (RFC 0002 /
README); throughput under load stops being one-flush-per-commit and PUT
costs amortize. Group commit is an optimization the protocol permits, not a
correctness requirement — a batch of one is the normal path. Ordering
within a group is the committer's choice; conflicts *within* a group are
impossible because a single committer serializes them before staging.

### Reader visibility after commit

Step 4 makes a commit **durable**; a *separate* reader observing it is a
second, distinct property. Readers are uncoordinated (Topology) and resolve
snapshots by following the manifest from object storage. A reader that
opened before the commit — or a fresh reader whose manifest view is a poll
interval stale — therefore does not see snapshot `N+1` the instant `commit`
returns.
For moraine's own committer this is a non-issue: it holds the advanced
`sys/head` in memory and reads its own writes. It matters at the boundary
where another process must observe a just-returned commit —
read-your-writes across processes, and writer takeover, where a new writer
must publish the last committed state before it serves.

The contract: `commit` does not return success until the new snapshot is
both durable and reflected in the manifest that readers follow, so a reader
opened *after* `commit` returns resolves `N+1`. (A reader opened *before*
may still lag by its manifest poll interval — that is ordinary snapshot
isolation, not a violation.) The exact SlateDB call that provides this — a
WAL/manifest flush versus relying on the commit's own durability step, and
its per-backend latency cost — is not assumed from prose; it is pinned by a
validation test in the store harness (open `object_store`, commit, open a
*fresh* reader, assert `N+1` is visible) before the commit path is built on
it.

### Test obligations

Per RFC 0001, integration tests exercise the protocol against real
SlateDB on in-memory `object_store` — no mocks of the store:

- Disjoint concurrent commits both succeed (benign-race retry).
- Concurrent pure appends to the same table both succeed (append-append
  refinement); the loser's rows carry row ids re-allocated above the
  winner's advanced `next_row_id`, with no collision or gap.
- Concurrent same-name creates (two commits each creating table `orders`
  in one schema): exactly one succeeds; the other's retry re-runs the
  closure and returns `AlreadyExists` — never two live tables with one
  name.
- Incompatible overlapping concurrent commits: one wins, the other returns
  `CommitConflict`.
- Staged-row commits never retry internally: a lost head on the extension
  path returns `CommitConflict` with the store unchanged by the loser.
- Crash-shaped sequences: partial batch never observable (atomicity);
  commit, reopen, verify head and snapshot resolve consistently.
- Counter monotonicity: `next_catalog_id`/`next_file_id`/per-table
  `next_row_id` never regress or collide across concurrent commits.
- Bounded retry terminates: a forced write storm returns `CommitConflict`
  rather than looping.
- Schema-version matrix: every schema-changing operation increments
  `schema_version`; every data-only operation — including inline
  insert/delete, flush, and mapping registration — carries it forward;
  comment/tag changes increment; `set_option` mints no snapshot and
  changes no version.
- Fresh-reader visibility: after `commit` returns, a newly opened reader
  resolves the new snapshot (the store-harness validation above, run
  against real SlateDB on in-memory `object_store`).

E2E testing validates the protocol against real DuckLake SQL — especially
the open questions below.

## Open questions

- **Does DuckLake retry conflicts internally, or propagate to the user? —
  resolved (source-verified, DuckLake main 2026-07).** Both, split exactly
  as this RFC's benign/true distinction: DuckLake's client-side
  `RunCommitLoop` retries a failed metadata commit internally (bounded,
  defaults above), but before each retry runs
  `DuckLakeTransactionState::CheckForConflicts` over the intervening
  `ducklake_snapshot_changes` — a true conflict per its matrix throws
  `TransactionException` to the application; only benign races are
  re-driven. Two load-bearing details for moraine: (a) retryability is
  decided by `DuckLakeTransaction::RetryOnError`, a **substring match on
  the error message** — it retries iff the lowercased message contains
  `"primary key"`, `"unique"`, `"conflict"`, or `"concurrent"` — so the
  error text moraine surfaces for a lost commit is a wire contract, not
  cosmetics (RFC 0006); (b) between attempts DuckLake queries the catalog
  for snapshots and `ducklake_snapshot_changes` after its transaction
  snapshot, which moraine must serve mid-retry. What remains for e2e is
  regression-pinning this behavior against the DuckLake version the e2e
  suite tracks, not discovering it.
- **Row-id increment mechanics.** Spec says `next_row_id += record_count`;
  e2e confirms moraine's allocation matches DuckLake's expectation exactly,
  including under UPDATE/compaction row-id preservation.
- **DuckLake's concurrent same-table-append semantics — resolved
  (source-verified, DuckLake main 2026-07).** Concurrent inserts into one
  table do not conflict: in `CheckForConflicts`, `tables_inserted_into`
  and `tables_inserted_inlined` are checked against drops, alters, and
  deletes only — never against other transactions' inserts — and inserts
  are likewise compatible with concurrent flushes and compactions. The
  append-append refinement is therefore required for verb-path fidelity,
  not optional; the strict table-grain fallback would have been strictly
  stricter than DuckLake. E2e regression-pins the matrix against the
  tracked DuckLake version.
- **Benign-race retry bound.** The fixed attempt count is a tuning
  parameter; DuckLake's own loop defaults (10 retries, 100 ms base wait,
  1.5× backoff with jitter) are the natural starting point for the verb
  path — confirm once e2e shows realistic contention shapes.
- **Exact schema-mutating operation set — resolved (source-verified,
  DuckLake main 2026-07).** The boundary cases land as follows:
  comments/tags on tables and columns **bump** (they are `AlterEntry`
  variants and enter `new_tables`); column/name-mapping registration does
  **not** bump (it rides data-file registration, outside
  `SchemaChangesMade`); option changes neither bump nor mint a snapshot at
  all (`set_option` writes `ducklake_metadata` directly). The full
  classification is in Schema-version tracking. E2e regression-pins it
  against the tracked DuckLake version.
- **Fresh-reader visibility call — resolved (source-verified, SlateDB
  0.14.x).** No extra flush call is needed: a commit with
  `await_durable: true` blocks on a durability watcher that fires only
  after the WAL object is PUT to object storage
  (`WalBufferManager::do_flush_one_wal` → `notify_durable`), and both
  fresh-open paths — `Db::open` and `DbReader::open` in latest mode —
  replay WALs from object storage past the manifest state. So once
  `commit` returns, a freshly opened handle resolves the new snapshot by
  construction. The store-harness validation test remains as the
  regression pin (and measures the per-backend latency), not as discovery.

## Alternatives considered

- **Mechanical re-stage on benign-race retry** (re-derive ids, re-stamp the
  previously staged records, re-commit — without re-running the closure).
  Rejected: cheaper per retry, but it re-validates nothing. Logical premises
  the closure established against the old head — name uniqueness above all —
  are never re-checked against the commits that won the race, so two
  concurrent same-name creates would both land. The closure re-run costs one
  in-memory pass and is the only retry form that is correct by construction.
- **Internal rebase of true conflicts (replay staged mutations against the
  new head).** Rejected: replaying a true conflict requires re-running the
  DuckLake operation, which lives above the DuckLake-agnostic core. It
  would put DuckLake transaction semantics in the wrong layer and can
  silently produce wrong results if the replay logic is incomplete. Abort
  + typed error is honest; benign races — safely re-drivable because the
  verb path re-runs the caller's closure against the new head — are the
  only thing retried internally.
- **Snapshot-level conflict detection (any concurrent commit conflicts).**
  Rejected: serializes all commits and defeats the benign-race retry —
  disjoint-table writers would falsely conflict. The single-`WriteBatch`
  CAS already serializes the physical write; there is no reason to also
  serialize logically-disjoint commits.
- **Entity-level conflict detection (per-file/column).** Rejected for the
  verb path, with the precise shape of DuckLake's model in view: DuckLake's
  matrix is operation-grained over tables everywhere except one case —
  delete-vs-delete on the same table is checked at `data_file_id` grain.
  Reproducing that one file-grain check buys concurrency only for
  concurrent deleters of one table (rare in the workloads moraine targets)
  at real machinery cost, and matters not at all on the extension path,
  where DuckLake applies its own matrix. Table grain plus the
  append-append refinement matches DuckLake everywhere else; the
  file-grain delete-delete check is the noted candidate if an embedding
  workload ever needs it.
- **Global `next_row_id` in the snapshot record.** Rejected: contradicts
  DuckLake (`next_row_id` is per-table in `ducklake_table_stats`) and would
  make every insert into any table contend on one counter, turning benign
  cross-table concurrency into counter churn. Per-table placement aligns
  allocation with conflict granularity.
- **Pessimistic locking (a lease/lock record in `sys` a writer must hold).**
  Rejected: reintroduces coordination state and a liveness problem (crashed
  lock-holder) that the optimistic head-CAS avoids. SlateDB fencing already
  provides the safety a lock would; optimism provides the concurrency.
