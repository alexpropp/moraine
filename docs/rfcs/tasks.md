# Open work

The consolidated open questions and deferred work for every RFC in this
directory. RFCs state the binding design; this file states what about that
design is undecided or unbuilt. An RFC that has no entry here is fully
settled and fully implemented.

Each item is tagged:

- **DECISION** — a design question with no answer yet.
- **DEFERRED** — agreed work, postponed on purpose.
- **IMPL** — specified in an RFC, not built.
- **VALIDATE** — a test the design depends on, closed by writing it.
- **MEASURE** — a number the design wants from a benchmark or profile. No
  assertion closes one of these; running something and recording the result
  does.
- **DOC** — an operator- or user-facing gap.

A VALIDATE whose subject does not exist yet is blocked on the IMPL item above
it, not independently actionable: writing that test *is* building the feature.

Resolving an item means updating the owning RFC and deleting the entry.

RFC 0022 (the commit log and the leader role) is wholly unimplemented and is
deliberately not itemized here.

## 0004 — Commit and transaction protocol

- **DEFERRED** — Let the staged-row path join a batch. Both front doors now
  land through one function, but the staged path still commits its own
  transaction: its snapshot id is DuckLake's, authored against the head it
  read, so it can only ever lead a batch, never fold onto a member. Worth
  building when DuckLake's serialized metadata connection admits concurrent
  committers — until then there is nothing to batch it with.
- **MEASURE** — Commit latency against a *real* S3, not a loopback one. The
  composition is settled and measured both ways (`BENCHMARK.md` → Core
  measurements: `max(flush cadence, write RTT) + ~2 ms`, confirmed by an
  injected-RTT sweep and validated against MinIO), so what remains is only
  the absolute S3 PUT term the harness needs a real bucket to see:
  point `object_storage.rs` at one and record the row.

## 0005 — Data inlining on SlateDB

- **DEFERRED** — Auto-flush policy: when to trigger an inline flush. This RFC
  specifies only the mechanism; the policy is an operational concern.

## 0006 — Extension surface (DuckDB)

- **DEFERRED** — The upstream DuckLake catalog-cache multi-threaded race: a
  fresh attach's listing comes back empty right after a write, so every e2e
  session pins `SET threads=1`. Not moraine's to fix, and re-verified live at
  the tracked version — 23 of 40 runs against a plain duckdb-file-backed
  catalog with no moraine in the chain, and not `RENAME`-specific (a plain
  `CREATE TABLE` is enough). A test now asserts the race's *presence* against
  the reference chain, so its failure is the signal that the workaround can
  go.

## 0007 — Snapshot expiry and garbage collection

- **DEFERRED** — Expose live reader snapshots at the extension layer so
  operators can size retention windows from observed reader durations.
  Policy-only for now.

## 0008 — Compaction and delete-file consolidation

- **DEFERRED** — Finer file-set-grain conflict detection, so two compactions of
  disjoint file sets in one table can run concurrently. Table grain today.

## 0009 — Reader consistency and snapshot caching

- **DEFERRED** — Partial or lazy materialization to bound memory for an
  unusually large live catalog. Deferred until profiling shows the full
  in-memory view is a problem. This is the same decision as server-side filter
  pushdown (0002, 0006, 0013): lazy materialization needs predicates to know
  what to fetch, and pushdown buys nothing while the whole view is resident.
  Whichever is taken first pulls the other with it. A replay's base-view
  copy is the one part of a refresh that scales with catalog size
  (`BENCHMARK.md`), so this would bound that too.

- **DECISION** — Whether to carry the migration state as a field on the
  `sys/head` record, so a read-only handle's probe rides along with a read
  it already issues instead of being a guaranteed miss that must consult
  every level's filter. Sized rather than guessed: a read-only warm read
  costs 6.35 µs against a read-write one's 0.09 µs, ~70×, because it opens
  a session and issues both point reads before it can serve a cache hit
  (`BENCHMARK.md`). So the saving is real and lands entirely on the reader.
  What blocks it is the compatibility gate, not the encoding. Field
  presence cannot be the signal on its own: an old binary starts a
  migration by writing the marker and never touching `sys/head`, so a new
  reader would find a head record that says "no migration" beside a marker
  that says otherwise, and sail through a keyspace being rewritten. Closing
  that needs `MAX_FORMAT_VERSION` raised, which makes the store unopenable
  by older binaries — and rolling a fleet across a structural bump with
  mixed versions online is itself deferred under 0015. Settle that first,
  or accept the reader cost.

## 0013 — Partitioning, sorting, and pruning

- **DEFERRED** — Server-side partition-pruning pushdown. One deferral with
  0002's stats pushdown, 0006's pushdown surface, and 0009's partial
  materialization — not four. Nothing pushes a predicate into moraine today, and
  0009 records why pushdown cannot pay off while the whole catalog is resident,
  so this revives only alongside that decision. If built it must be
  transform-aware and type-aware, never a naive compare.

## 0014 — Catalog and data encryption

- **DECISION** — Whether to support untrusted-bucket deployments, via SlateDB's
  `BlockTransformer` or a native scheme, if demand appears. Nothing is
  designed.
- **DEFERRED** — If store objects ever need encryption independent of the
  bucket, implement it at SlateDB's `BlockTransformer` seam. Manifests and SST
  footers stay plaintext today.

## 0015 — On-disk format migration

- **IMPL** — The first real `v_n → v_{n+1}` unit. The registry ships empty:
  the driver, the unit shape, and the composition are built and tested
  against synthetic units, but every format to date is additive, so no
  rewrite exists to register. The first format that moves an existing key
  adds the first entry — and must raise `MIN_FORMAT_VERSION` with it, since
  its `to_format` is where the keys then live. A test pins the two together
  so that cannot be forgotten, along with the chain shape `chain_from`
  depends on.
- **VALIDATE** — Drive a real rewrite end to end through SQL. Blocked on a
  shipped unit, and on nothing else that is buildable: the core tier drives
  the whole protocol against a synthetic unit the `fault-injection` feature
  installs, but the e2e tier loads a *released* extension binary, and
  building that one with fault injection would ship test scaffolding to
  every user. So this closes when the first real key-moving format lands,
  not before. No candidate is in sight: the mapping kinds that were the
  near-term one are now pinned in 0002's map as built, which moved no key,
  and every format to date remains additive.
- **DEFERRED** — Allowing a trivial, bounded `system`-only migration to
  auto-run on open. The shipping behavior is explicit-verb-only for every
  migration regardless of size; auto-run is a later refinement, not the first
  cut.
- **DEFERRED** — Rolling a fleet across a structural bump with mixed binary
  versions online.

## 0016 — Equality indexes

- **DECISION** — The oversized-indexed-value refusal threshold. The strawman is
  1 KiB per composite key, with hash-overflow as the recorded escape if a
  workload needs larger values.
- **DECISION** — Whether staged build steps can loosen from `altered_table` to
  the benign `inserted_into_table` classification. This requires re-examining
  the delete race that surfaced conflicts currently protect against.
- **DECISION** — Whether to carry an upstream DuckLake binder patch accepting
  `CREATE INDEX` and `PRIMARY KEY` and routing equality pushdown to the moraine
  index.
- **DECISION** — Whether to offer a deferred, post-commit index-maintenance
  mode for non-unique indexes on SQL writes, trading an under-coverage window
  for the scoped read's commit-time latency.
- **DECISION** — Whether to add a store-level reverse iterator, so one index
  serves both directions and a composite its exact-opposite order, versus
  keeping "declare the direction or build a second index". Reverse currently
  materializes the row-id vector and reverses it.
- **DEFERRED** — Make the per-commit index-entry cap a `CatalogOptions` field
  threaded through both commit paths and the FFI, instead of a hardcoded
  constant, once a caller has a legitimate reason to raise it.
- **DEFERRED** — Bound the staged-build derivation's driver memory. The whole
  live backfill is materialized into one entry vector before stepping; only
  per-commit staging is capped.
- **DEFERRED** — Ordered emission of stored NULL rows at the declared
  `NULLS FIRST` or `NULLS LAST` end of an ordered scan. Range scans clamp to
  the non-null region, so this becomes observable only once `ORDER BY` routes
  to the index.
- **DEFERRED** — Replace the batched scan-and-delete orphan sweep with a single
  SlateDB range-delete once that primitive exists at the pinned version. Shared
  with 0021.
- **DEFERRED** — Route comparison and `ORDER BY` pushdown into DuckLake's
  optimizer. The encoding blocker is gone; this waits on a binder change.
- **DEFERRED** — Map a native `ducklake_*` index onto the same `index` range via
  the reserved `ducklake_index_id` field, if DuckLake grows index metadata,
  with maintenance arriving as writer-supplied entries.

## 0021 — Maintenance orchestration

- **DECISION** — The outcome when a process is torn down without detaching at
  all. DuckDB extension teardown ordering leaves scheduler-thread lifetime
  undefined in that case.
- **DECISION** — Whether to persist the maintenance status window, and whether
  inside the catalog or outside it, so an overnight failure in a
  since-restarted process leaves a trace. Today it is a per-attach in-memory
  deque. Settle the strawman size of 16 at the same time.
- **DECISION** — Whether the shim should detect and degrade rather than fail
  when DuckLake's maintenance function signatures or parameter names change,
  and how new DuckLake parameters get exposed. Today a signature change is a
  binder exception at attach, or an aborting failed step at pass time.
- **DEFERRED** — Multi-tier scheduling: per-step cadences and step sets, so a
  cheap sweep interval can differ from an expensive
  `delete_orphaned_files` interval. One interval drives the whole fixed
  sequence today.
- **DEFERRED** — Collapse the batched sweep into one range-delete per dead
  index if SlateDB exposes a range delete. Shared with 0016.
- **DEFERRED** — Wire checkpoint lifecycle in as a consumer of the maintenance
  pass surface, if and when it lands.
- **MEASURE** — The absolute number against a real endpoint. Both halves of
  the model are measured and recorded (`BENCHMARK.md`): attach cost tracks
  the physical bytes a read touches, and under injected per-GET latency it
  tracks the GET count, which a merge cuts. Both are linear, so the
  production regime extrapolates — what is missing is only the endpoint's
  own latency term, which `object_storage.rs` needs a real bucket to see,
  exactly as the 0004 commit-latency row does.
- **DECISION** — Whether the read-ahead and fetch-concurrency figures
  (4 MiB, 8) deserve to be attach options. They are moraine's choice today,
  picked to make a scan latency-insensitive rather than measured against a
  ladder, and the right values plainly differ between a local store and S3.
