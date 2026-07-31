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

## Where the weight is

Three things gate disproportionately much of the list:

- **Reader refresh** (RFC 0009). Reads now serve from the cache, so a warm
  read costs one point read. What is missing is *incremental* refresh: a
  cache that has fallen behind head rematerializes wholesale rather than
  replaying the gap. Its main beneficiary would be read-only catalogs, which
  cache nothing at all pending the read-snapshot pin below.
- **Format migration** (RFC 0015). The `sys/migration` marker is reserved and
  refused-on-open, and nothing else. The reader gate must ship in format
  version 1 — before any migration exists — or fielded readers are unsafe
  against the first real one.
- **The crash harness** (RFC 0011). No `CrashPoint`, no `fault-injection`
  feature, no matrix test. RFC 0015's seam coverage depends on it.

## 0001 — Repository structure and conventions

- **DEFERRED** — A `fuzz/` directory with `cargo-fuzz` targets for the `store`
  codecs and the commit-protocol read path (decode arbitrary bytes without
  panicking), on a nightly cadence, once the codecs stabilize.
- **DEFERRED** — A dedicated MSRV-check CI job. `rust-version` is declared in
  workspace metadata with nothing checking it.
- **DEFERRED** — The first crates.io release, which activates the dormant
  `release-plz` workflow and its `cargo-semver-checks` gate.
- **DEFERRED** — Decouple the shared workspace version so `moraine` and
  `moraine-duckdb` release independently. Post-1.0.

## 0002 — SlateDB key encoding for catalog state

- **VALIDATE** — Exercise the segmented-store configuration (one-byte segment
  extractor) through the crash and recovery matrix. The segmented path is
  less-exercised in SlateDB, and the choice is only free to reverse before the
  first release.
- **DEFERRED** — If server-side stats pruning is ever added, it needs
  type-aware min/max comparison rather than lexicographic. A wrong compare
  silently drops rows. Part of the single pushdown deferral tracked under 0009.
- **DEFERRED** — Map future DuckLake spec catalog tables into the keyspace
  using the established conventions (own kind, embedded child, or merged 1:1
  side table) by updating the RFC rather than diverging.

## 0003 — Public API shape of the core

- **IMPL** — `Error::Interrupted` and `Error::Unsupported` exist and are
  bridged, but nothing in the core raises them yet: the interrupt path is
  0010's shielding work, and the one live unsupported-feature rejection
  (VARIANT inlining) is thrown shim-side. Wire the core to raise them as those
  paths land. `SnapshotExpired` and `Migration` are raised.
- **IMPL** — `set_partitioning` / `clear_partitioning` verbs and the
  `PartitionSpec` domain type. Specs reach the store only through the generic
  staged-row path today.
- **IMPL** — The `partitioning_of(table)` snapshot accessor, or a decision to
  drop it from the RFC. The code currently asserts the opposite intent.
- **IMPL** — Public `set_tag` / `remove_tag` verbs. The read side (`tags_of`)
  exists with no mutator.
- **IMPL** — Public `inline_insert`, `inline_delete`, and `flush_inlined_data`
  on `Transaction`. Only the raw ABI staging shim exists.
- **IMPL** — `recent_rows(table)` / `recent_row(table, row_id)` accessors
  serving the `inline` subspace natively.
- **DEFERRED** — A verb for registering a row-id-preserving (rewrite) data
  file. Inexpressible on the verb surface today; owned by a future RFC
  covering the compaction surface (0016).
- **DEFERRED** — A `create_mapping` verb, if an embedding consumer
  materializes (0018).
- **DEFERRED** — A snapshot-expiry verb (0007).
- **DEFERRED** — Verbs for the DuckLake v1.0 spec's remaining tables, as the
  e2e suite reaches them.
- **DECISION** — The extension-contract question in 0005 still gates knowing
  the full set of operations the core must expose.

## 0004 — Commit and transaction protocol

- **IMPL** — Group commit: batch several pending catalog commits into one
  `WriteBatch` and one WAL flush. The protocol permits it; a batch of one is
  the only path today, so throughput is one flush per commit.
- **IMPL** — A zero-write reader mode that opens `DbReader` against an
  explicit existing checkpoint id instead of following latest, which CASes the
  manifest and pins SSTs against SlateDB GC. Shared with 0006.
- **DECISION** — The benign-race retry attempt count and backoff. DuckLake's
  10 retries / 100 ms base / 1.5× jittered backoff is the strawman; confirm
  once e2e shows realistic contention shapes.
- **DEFERRED** — File-grain (`data_file_id`) delete-delete conflict detection,
  matching DuckLake's finer granularity. Table grain today.
- **VALIDATE** — Regression-pin DuckLake's `RunCommitLoop` /
  `CheckForConflicts` retry behavior against the tracked version, including
  the error-text substring contract and the mid-retry
  `ducklake_snapshot_changes` queries moraine must serve.
- **VALIDATE** — Confirm row-id allocation matches DuckLake's
  `next_row_id += record_count` exactly, including preservation under UPDATE
  and compaction.
- **VALIDATE** — Regression-pin DuckLake's conflict matrix for concurrent
  same-table appends (inserts against drops, alters, deletes, and
  compatibility with flushes and compactions).
- **VALIDATE** — Regression-pin the schema-mutating classification boundary
  cases: comments and tags bump, column and name-mapping registration does
  not, `set_option` neither bumps nor mints a snapshot.
- **MEASURE** — Commit latency against a *real* object store. In-memory is
  settled (`BENCHMARK.md` → Core measurements: `flush_interval + ~2 ms`); what
  remains is the S3 write-RTT term that localhost MinIO understates, giving
  `max(flush cadence, write RTT) + ~2 ms`.
- **DOC** — The single-read-write-process, many-readers limitation belongs in
  the root README. It currently appears only in `ARCHITECTURE.md`. Shared with
  0006.

## 0005 — Data inlining on SlateDB

- **IMPL** — A column-oriented flush decode path handing the imported
  `DataChunk` straight to the writer, eliminating the row-by-row
  `duckdb::Value` materialization and making flush closer to transcode-free.
- **DEFERRED** — Auto-flush policy: when to trigger an inline flush. This RFC
  specifies only the mechanism; the policy is an operational concern.

## 0006 — Extension surface (DuckDB)

- **IMPL** — A per-DuckDB-version build-and-signing distribution pipeline. The
  loadable's signature region is left zero, the release workflow publishes
  unsigned, and every e2e harness passes `-unsigned`.
- **IMPL** — A truly-zero-write attach option opening against a pre-created
  SlateDB checkpoint id, for deployments with strictly read-only credentials.
  Shared with 0004.
- **DECISION** — Should scan results cross the C ABI as Arrow (the Arrow C Data
  Interface) rather than the current owned `#[repr(C)]` row-struct arrays?
- **DECISION** — The DuckDB and DuckLake pin bump policy: does moraine track
  each DuckDB minor as DuckLake cuts its matching branch, and how many past
  series receive builds?
- **DECISION** — How DuckLake's `ducklake:` prefix names or nests the moraine
  attach.
- **DEFERRED** — Make `moraine_attach` and `moraine_detach` cancellable. They
  take no interrupt probe.
- **DEFERRED** — Concurrent multi-read cancellation on a single handle. One
  `Notify` per handle today.
- **DEFERRED** — Model variant-column stats. `ducklake_file_variant_stats` is
  the last always-empty stand-in.
- **DEFERRED** — Resolve the upstream DuckLake catalog-cache multi-threaded
  race (an empty listing right after a write) so the `SET threads=1` workaround
  in the e2e harness can go.
- **DEFERRED** — Semantic projection of the catalog: store a re-modeled form
  and project it into `ducklake_*` on read. Revisit only with e2e evidence
  that a specific access pattern demands it.
- **VALIDATE** — Verify the DuckDB v1.5.4 pin has no patch-level ABI friction
  against DuckLake's `v1.5-variegata`, which CI-builds on v1.5.3, and fall
  back if it does.
- **VALIDATE** — Determine which reads and writes DuckLake issues against
  `ducklake_*`, to know which scans must be optimized. The filter half is
  settled: DuckDB pushes no row filter into these tables, so there is no
  predicate to optimize for and the pruning deferrals downstream of it are
  inert (0009 records the condition that would revive them).
- **VALIDATE** — Keep pinning the exact nested `ATTACH 'moraine:<uri>'` string
  DuckLake generates, and re-verify on every pin bump.
- **VALIDATE** — Keep pinning the two conflict-propagation wire obligations:
  the `conflict` substring in the lost-commit message, and serving
  `ducklake_snapshot` / `ducklake_snapshot_changes` reads mid-retry.
- **VALIDATE** — Verify the id-collision backstop (a typed `Constraint` error
  on inserting an id already live) covers all five primary-keyed kinds, and
  that no name-uniqueness is enforced anywhere.
- **DOC** — The single-writer and `READ_ONLY` fencing limitation belongs at the
  user surface: the ATTACH docs and the root README. Shared with 0004.

## 0007 — Snapshot expiry and garbage collection

- **IMPL** — Read-your-writes for the `dump_*` projections beyond snapshots.
  Only the snapshot projection overlays the active staged transaction; every
  other dump opens a fresh read-only transaction, so a cascade SELECT cannot
  observe its own uncommitted deletes.
- **DEFERRED** — Expose live reader snapshots at the extension layer so
  operators can size retention windows from observed reader durations.
  Policy-only for now.
- **DEFERRED** — A moraine-native maintenance and expiry surface, if a
  non-DuckLake consumer appears. v0.1 targets DuckLake parity.
- **VALIDATE** — Pin interior (non-tail) snapshot expiry via `versions => […]`,
  confirming nothing in the translation is tail-specific.
- **VALIDATE** — A verb-path retry whose base predates a concurrent expiry must
  treat a missing intervening snapshot record as conflict-and-refresh, not
  corruption.
- **DOC** — The operator safety contract: the retention window must exceed
  maximum read and attach duration, and the cleanup grace period must exceed
  maximum reader and scan duration.

## 0008 — Compaction and delete-file consolidation

- **DEFERRED** — Finer file-set-grain conflict detection, so two compactions of
  disjoint file sets in one table can run concurrently. Table grain today.
- **VALIDATE** — Pin that a merge never crosses a partition boundary: files
  spread over two partition values merge to one file per value, never one
  combined file. The rule is DuckLake's and recorded in the RFC; the pin
  guards moraine against a future DuckLake that batches differently.

## 0009 — Reader consistency and snapshot caching

- **IMPL** — Incremental refresh: pin a fresh read-snapshot, check `sys/head`
  and the migration marker, scan `snap/{S+1..head}` under the same handle, and
  re-read or drop just the entities each record's `snapshot_changes` names.
  Nothing exists; the only forward-folding applies the committer's own batch.
- **IMPL** — Fall back to full rematerialization when `S` has fallen below the
  horizon `H` and the gap's `snapshot` records may have been reclaimed.
- **IMPL** — A churn-versus-catalog-size threshold that falls back to a full
  `current` rescan when replaying the changelog would cost more.
- **DECISION** — The churn ratio for that threshold. Full-materialization cost
  is measured (`BENCHMARK.md` → Core measurements: ~5–7 µs per live entity,
  linear); the crossover it trades against needs incremental refresh built to
  measure the other side.
- **DECISION** — How a read-only handle gets a cut that is both live and
  consistent. `DbReader` exposes no `snapshot()`, and SlateDB offers only two
  modes: an explicit `checkpoint_id`, which is consistent but stops polling
  entirely (it never sees new commits), or no checkpoint, which follows the
  manifest but gives no consistent cut. Neither is what a reader needs.
  `DbReader::manifest()` returning a public `VersionedManifest::id()` suggests
  an optimistic validate — capture the id, read, re-check, retry on change —
  but that is unverified against the reestablish path.
- **IMPL** — Pin an explicit read-snapshot on the read-only path, per the
  decision above. Until it lands, a read-only materialization is not
  snapshot-isolated the way the read-write path is, which is also why
  read-only catalogs must not cache: a torn view would persist and compound
  instead of being discarded with the read that built it.
- **IMPL** — Return `SnapshotExpired` for a view driven past the retention
  window, so a reader re-resolves from head instead of dereferencing reclaimed
  files. Depends on the 0003 error variants.
- **IMPL** — Stop cloning the whole entity set to serve one kind.
  `ffi_support::dump_entities` takes its extractor by value, so it clones every
  record in the cached `Arc<Vec<EntityRecord>>` and then discards the kinds it
  was not asked for. Fourteen `dump_*` functions route through it and populating
  DuckLake's metadata tables issues roughly two dozen calls, so one population
  clones the catalog over and over — each clone heap-allocating, since the
  records hold strings and vectors. Taking the extractor by reference and
  cloning only the matched record confines the cost to what is returned. The
  uncached branch already moves rather than clones, so the waste falls
  exclusively on the path the cache exists to make cheap.
- **DEFERRED** — Extend caching and changelog-based incremental refresh to
  read-only catalogs, which today rematerialize on every read. Blocked on the
  read-snapshot pin above, not on cost: a reader cannot fold its own commits
  forward (it has none), so replay is the only way to advance its cache, and
  caching a non-isolated view would be worse than not caching.
- **DEFERRED** — Partial or lazy materialization to bound memory for an
  unusually large live catalog. Deferred until profiling shows the full
  in-memory view is a problem. This is the same decision as server-side filter
  pushdown (0002, 0006, 0013): lazy materialization needs predicates to know
  what to fetch, and pushdown buys nothing while the whole view is resident.
  Whichever is taken first pulls the other with it.
- **DECISION** — Does DuckLake hold one catalog snapshot per `BEGIN…COMMIT`, or
  re-resolve per statement? This sets how tight the retention window must be.
- **VALIDATE** — The refresh test suite: a commit landing mid-materialization
  yields an entirely pre- or entirely post-commit view, never torn; a view
  built at `S` still returns the `S` view after `k` commits; an incremental
  refresh to head is byte-identical to a full rematerialization; a reader below
  the horizon rematerializes while an expired request errors; a
  materialization under `sys/migration` returns the typed error and never a
  partial view; a commit landing between a commit attempt's materialization
  and its batch write is always detected.

## 0010 — Async↔sync bridge

- **IMPL** — Split the commit at the point of no return: pre-write phases
  inside the `select!`, the durable batch write via `Handle::spawn`, the FFI
  thread awaiting the join handle while still racing the token. No `spawn`
  exists in either crate, so an interrupt arriving mid-commit drops the
  durable-write future.
- **IMPL** — Document the ambiguous interrupted-but-landed outcome on the FFI
  commit entry point.
- **DECISION** — Pin exactly how DuckDB hands an interrupt to a C-ABI
  extension — pollable flag, callback, or cancellation handle — which
  determines whether the token is polled or signal-driven.
- **DECISION** — The runtime's default worker-thread count, likely derived from
  DuckDB's own thread setting to avoid core oversubscription.
- **DEFERRED** — Share one runtime across many attached catalogs in a single
  process, if many-catalog processes prove common.
- **DEFERRED** — Track DuckDB's extension API for a future async catalog or
  operator contract, without pre-building for it.
- **DEFERRED** — A commit-funnel dispatcher serializing a many-connection
  process through a single committer, if a many-committer process appears.
- **MEASURE** — Whether CPU-bound SST decode monopolizes a worker under a
  decode-heavy scan — the one axis where a `spawn_blocking` discipline might
  help. IO latency is settled: it does not starve the pool (`BENCHMARK.md` →
  Core measurements), so this is the only remaining part of the question.
- **VALIDATE** — Interrupt coverage: before the commit write, head unchanged
  with no partial records; during the shielded write, prompt return while the
  write completes untorn and head is exactly `N` or `N+1`; after the durable
  write, the committed snapshot still reported; an interrupted materialization
  releases its read-snapshot with no durable effect.

## 0011 — Crash-injection test matrix

- **IMPL** — The `CrashPoint` enum, whose variants are exactly the matrix rows,
  plus a `#[cfg(any(test, feature = "fault-injection"))]` seam hook that
  consults an injected `Option<CrashPoint>` and unwinds there. Neither the enum
  nor the feature exists.
- **IMPL** — Decompose the `transaction`, flush, and GC code so tests can drive
  an operation up to a named seam and drop the writer handle mid-path.
- **DECISION** — Does moraine pin `WriteOptions` `seqnum` itself, or let
  SlateDB generate them? This determines how an A2 re-drive is detected as a
  duplicate rather than a fresh commit.
- **DECISION** — Does compaction introduce a state-change seam not already
  shaped like PUT-then-batch, requiring its own B-style rows?
- **VALIDATE** — The single data-driven test iterating every `CrashPoint`
  variant: build pre-crash state, inject, reopen, assert that row's invariant.
  A variant with no assertion must fail.
- **VALIDATE** — For idempotence rows A2, B1, B3, B4: re-drive after reopen and
  assert convergence — no id collision, no torn state, only protocol-permitted
  typed errors.
- **VALIDATE** — For absence rows A3 and D2: enumerate all WAL boundaries and
  assert the torn intermediate is unobservable at each.
- **VALIDATE** — Confirm the seam hooks gate to zero production footprint, no
  `unsafe`, and no fault-injection parameter in public signatures.
- **DEFERRED** — A `cargo-fuzz` target that crashes at arbitrary WAL offsets
  and reopens, asserting the same two invariants, once the matrix is green.
- **DOC** — 0001 and 0004 still carry generic crash-shaped-sequence bullets;
  replace them with citations to rows A1, A2, and B3.

## 0012 — Schema evolution and versioning

- **DEFERRED** — Define the exact `column_mapping` and `name_mapping` key
  components in 0002's keyspace map once implementation reaches
  external-Parquet interop. The kinds themselves are built.
- **VALIDATE** — A property test that for an arbitrary sequence of column
  operations and an arbitrary snapshot `S`, the reconstructed column set,
  order, and types equal what DuckLake reports. No such proptest exists.
- **VALIDATE** — After a widening type promotion, reconstruction at a
  **pre-promotion** snapshot yields the old type. Current coverage asserts
  promotion at head only.
- **VALIDATE** — Pin that verb-path `add_column` allocates **nested** field ids
  as DuckLake does, in pre-order. The flat case is pinned on both paths now —
  `column_order_numbers_from_one_and_keeps_gaps` for the verb path and
  `ducklake_column_ids_and_positions_match_stock_ducklake` differentially for
  the staged one — but nothing covers a nested `STRUCT`'s field ids.
- **DOC** — Give `ducklake_schema_versions` a named home in 0002's keyspace
  map. It is implemented as a fold into the snapshot record, but the map's
  `snapshot` row never mentions it.

## 0013 — Partitioning, sorting, and pruning

- **DEFERRED** — Server-side partition-pruning pushdown. One deferral with
  0002's stats pushdown, 0006's pushdown surface, and 0009's partial
  materialization — not four. Nothing pushes a predicate into moraine today, and
  0009 records why pushdown cannot pay off while the whole catalog is resident,
  so this revives only alongside that decision. If built it must be
  transform-aware and type-aware, never a naive compare.
- **VALIDATE** — Capture DuckLake's `SET SORTED BY`, sorted-`INSERT`, and
  `RESET SORTED BY` round trips in the e2e suite to validate the mapping.

## 0014 — Catalog and data encryption

- **DECISION** — Whether to support untrusted-bucket deployments, via SlateDB's
  `BlockTransformer` or a native scheme, if demand appears. Nothing is
  designed.
- **DEFERRED** — If store objects ever need encryption independent of the
  bucket, implement it at SlateDB's `BlockTransformer` seam. Manifests and SST
  footers stay plaintext today.
- **DOC** — Operator documentation for bucket KMS key policy, grants, and
  rotation posture. It exists only inside this RFC.

## 0015 — On-disk format migration

- **IMPL** — Dispatch an older-than-binary store from the on-attach
  `sys/format` check into the migrate path. The typed `Migration` error, the
  three-way equal/older/newer split, and the newer-than-binary refusal are
  done; the older arm currently refuses toward migrate but no migrate path
  yet consumes it.
- **IMPL** — The start phase: one atomic batch writing the `sys/migration`
  marker with `{from_format, to_format, cursor}`.
- **IMPL** — The idempotent step loop: write new-format keys before deleting
  superseded old-format keys, advancing the durable cursor in the same batch.
- **IMPL** — The finish phase: one `WriteBatch` atomically flipping
  `sys/format` and clearing the marker.
- **IMPL** — Reopen-state detection and resume-from-cursor across the three
  (marker, format) states. Nothing reads the marker's cursor field.
- **IMPL** — An explicit operator-triggered `migrate` verb or flag, distinct
  from ordinary attach.
- **IMPL** — Named, individually tested `v_n → v_{n+1}` units that compose for
  multi-version jumps, each with its own start, step, finish, and cursor.
- **DEFERRED** — Allowing a trivial, bounded `system`-only migration to
  auto-run on open. The shipping behavior is explicit-verb-only for every
  migration regardless of size; auto-run is a later refinement, not the first
  cut.
- **DEFERRED** — Rolling a fleet across a structural bump with mixed binary
  versions online.
- **VALIDATE** — Crash injection at every migration seam — the start batch,
  each step's new-key write and old-key delete, each cursor advance, the finish
  flip — asserting reopen always yields a coherent store and never
  new-format-with-marker. Depends on 0011.
- **VALIDATE** — With the marker present, materialization and refresh on either
  binary version return the typed error and never a partial view.
- **VALIDATE** — Running the migrate verb against an already-migrated store is
  a no-op, not a re-rewrite.
- **VALIDATE** — Migrate, then time-travel: resolving at a pre-migration
  snapshot after migration still returns correct historical state.

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
- **MEASURE** — Confirm on a *real* object store that bounded-concurrency point
  reads keep bulk uniqueness resolution linear as the index grows. Resolution
  now uses one path — bloom-filtered point reads with a bounded fan-out — at
  every batch size; the earlier sorted-scan mode was removed after it measured
  as a pessimization (in-memory, store-proportional and CPU-bound; under a
  5 ms-per-GET model, a serial near-whole-index block sweep tens to ~190x
  slower than the concurrent probes, since a bulk load's values are not
  store-ordered). `measure_index_maintenance_by_store_size` in
  `tests/it/measure.rs` shows the point-read path stays flat in-memory —
  scattered no longer tracks the store; a real-store run (a `ThrottledStore`
  GET latency, as the 0010 harness models, or S3) would close it. No
  real-store number recorded yet.
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

## 0017 — Read-write and read-only attach paths

- **VALIDATE** — Whether DuckLake forwards outer `READ_ONLY` into the nested
  moraine metadata attach. This needs a two-process no-fence probe, not a
  single-CLI e2e; current coverage exercises the chain but asserts only that
  rows read back.
- **DEFERRED** — If DuckLake does not forward it, document `READ_ONLY` on the
  `moraine:` attach itself as an escape hatch. The option is parsed; the
  documentation is not written.
- **DEFERRED** — Fully type the read-only `Catalog` handle so `commit` is
  unavailable at compile time rather than returning `Error::Constraint` at
  runtime (0003).

## 0018 — Column and name mapping for external Parquet

- **DEFERRED** — Physical deletion of mapping rows during snapshot expiry:
  delete `ducklake_column_mapping` by `table_id`, then sweep orphan
  `ducklake_name_mapping` rows. Served tables stay insert-only until then.
  0007 work.


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
- **DEFERRED** — File or pursue an upstream SlateDB compact-now-and-wait
  primitive, the residual want after rejecting in-pass forced compaction.
- **DEFERRED** — Wire checkpoint lifecycle in as a consumer of the maintenance
  pass surface, if and when it lands.
- **VALIDATE** — Whether a blocked autocommit caller can still hold something
  the trigger's second connection needs under heavier concurrency. The
  explicit-transaction refusal is currently a guard, not a proof.

## RFC prose to reconcile

Implementation has diverged from these RFCs. Each is either a code gap or an
RFC edit, and the RFC is binding until edited.

- **0001** marks the real-object-storage test tier as future and not built. It
  exists, as an integration test plus an xtask target.
- **0003** specifies a `partitioning_of(table)` accessor; the snapshot code
  states that partition and sort specs carry no read accessors by design.
  Tracked as an item under 0003.
- **0006** prescribes no `build.rs` and a plain `staticlib` crate type. The
  crate has a `build.rs` and declares `staticlib` plus `rlib`.
- **0018** says reject UPDATE and DELETE against the mapping tables. UPDATE is
  rejected; DELETE is deliberately accepted to serve expiry cleanup.
