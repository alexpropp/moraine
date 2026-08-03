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
  extractor) through the crash-recovery cases. The segmented path is
  less-exercised in SlateDB, and the choice is only free to reverse before the
  first release.
- **DEFERRED** — If server-side stats pruning is ever added, it needs
  type-aware min/max comparison rather than lexicographic. A wrong compare
  silently drops rows. Part of the single pushdown deferral tracked under 0009.
- **DEFERRED** — Map future DuckLake spec catalog tables into the keyspace
  using the established conventions (own kind, embedded child, or merged 1:1
  side table) by updating the RFC rather than diverging.

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

- **IMPL** — A column-oriented flush decode path handing the imported
  `DataChunk` straight to the writer, eliminating the row-by-row
  `duckdb::Value` materialization and making flush closer to transcode-free.
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

- **DEFERRED** — Extend the three fold-forward *served projections*
  (snapshots, table stats, table column stats) to read-only catalogs, which
  rescan per serve. A reader has no local batch to fold, so it would advance
  them by the same changelog replay the view already uses. Deferred until
  reader-side serve cost is shown to matter; the view cache, which is the
  expensive one, is no longer waiting on anything.
- **DEFERRED** — Partial or lazy materialization to bound memory for an
  unusually large live catalog. Deferred until profiling shows the full
  in-memory view is a problem. This is the same decision as server-side filter
  pushdown (0002, 0006, 0013): lazy materialization needs predicates to know
  what to fetch, and pushdown buys nothing while the whole view is resident.
  Whichever is taken first pulls the other with it. A replay's base-view
  copy is the one part of a refresh that scales with catalog size
  (`BENCHMARK.md`), so this would bound that too.

## 0011 — Crash recovery

- **IMPL** — Move `CrashCase` from the integration suite into the library,
  gated on `#[cfg(any(test, feature = "fault-injection"))]`, once a *driven*
  case needs an in-code seam hook. It and its coverage table live in
  `tests/it/crash_recovery.rs` today because no driven case needs one:
  crashes come from outside the operation. `MigrationInterrupted` is the
  case that will trip this, since the migration driver's boundaries are
  internal to one call and its `CrashPoint` seams are the only way in — but
  it is blocked below, so the move is not yet due.
- **VALIDATE** — Drive `MigrationInterrupted` at the integration tier.
  Blocked on the first shipped migration unit: `MIGRATIONS` is empty because
  every format so far is additive, so `Catalog::migrate` is a no-op against
  every store and no public path can put one mid-migration. All four seams
  are covered today in the driver's own unit tests, which drive `Db`
  directly against a caller-supplied registry — the right assertions at the
  wrong tier, since RFC 0001 puts crash coverage against the public API.
- **DEFERRED** — A `cargo-fuzz` target that crashes at arbitrary WAL offsets
  and reopens, asserting the same two guarantees. Every driven case is green,
  so this is now waiting only on the fuzzing tier itself (0001).

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

- **DOC** — Name `moraine_migrate` in the below-the-floor refusal, now that
  an operator has something to run. The typed `Migration` error and the
  four-way in-range/below-floor/newer/absent split are done, and the arm
  stays dormant until a rewriting format raises the floor above the base
  version, so nothing reaches it in the field today.
- **IMPL** — The first real `v_n → v_{n+1}` unit. The registry ships empty:
  the driver, the unit shape, and the composition are built and tested
  against synthetic units, but every format to date is additive, so no
  rewrite exists to register. The first format that moves an existing key
  adds the first entry — and must raise `MIN_FORMAT_VERSION` with it, since
  its `to_format` is where the keys then live. A test pins the two together
  so that cannot be forgotten, along with the chain shape `chain_from`
  depends on.
- **VALIDATE** — Drive a real rewrite end to end through SQL once such a unit
  exists. `moraine_migrate` is only reachable against stores that need
  nothing today, so the dormant path is covered and the one that moves keys
  is not yet reachable.
- **DEFERRED** — Allowing a trivial, bounded `system`-only migration to
  auto-run on open. The shipping behavior is explicit-verb-only for every
  migration regardless of size; auto-run is a later refinement, not the first
  cut.
- **DEFERRED** — Rolling a fleet across a structural bump with mixed binary
  versions online.
- **VALIDATE** — With the marker present, a read-only attach that was already
  open when the migration started returns the typed error. The gate itself is
  shared (every read opens its session through one place, which refuses), and
  the read-write side is covered; what is untested is a reader that meets a
  marker planted by another process after it attached, which needs a second
  writer and a manifest poll to stage.

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
- **0018** says reject UPDATE and DELETE against the mapping tables. UPDATE is
  rejected; DELETE is deliberately accepted to serve expiry cleanup.
