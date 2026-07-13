# RFC 0006: Extension surface

- **Date:** 2026-07-09

## Summary

Defines how the `moraine-duckdb` extension exposes the moraine core to DuckDB.
moraine is a **DuckLake metadata-catalog backend**: the extension registers a
DuckDB `StorageExtension` so DuckLake `ATTACH`es moraine as its catalog and
drives it with ordinary SQL, exactly as it drives a PostgreSQL or SQLite
catalog. moraine serves the `ducklake_*` metadata tables **row-faithfully**
from SlateDB — the tables *are* the catalog state (RFC 0002 encodes their
rows), not a re-modeled projection. Because DuckDB's stable C extension API
cannot register a catalog, the extension is a thin **C++ shim** linking
DuckDB's internals over a **C ABI** to the Rust core; all catalog logic stays
in `moraine`.

## Goals

- **DuckLake drives moraine unmodified.** moraine implements DuckLake's
  catalog contract and invents nothing (consistent with RFC 0004: moraine
  implements DuckLake's conflict model, it does not impose its own). Whatever
  SQL DuckLake issues against `ducklake_*`, moraine serves.
- **Thin extension, language-agnostic.** No DuckLake domain logic lives in
  `moraine-duckdb` — only `StorageExtension` registration, C-ABI marshalling,
  and the sync↔async bridge. Everything else is in the Rust core, testable
  without DuckDB (RFC 0001 Unit/Integration tests).
- **Faithful catalog state.** The `ducklake_*` rows are the source of truth;
  moraine stores and returns those rows (B1). No semantic re-modeling that
  could drift from DuckLake's own reading of its tables.

### Non-goals

- **Finalized `ducklake:` chaining.** A standalone moraine attach —
  `ATTACH '<path>' AS m (TYPE moraine)` — is now the DECIDED first surface
  (the extension-loads slice registers it and serves attach, read-only
  metadata, and table scans), reversing this RFC's earlier deferral: it is
  the smallest real end-to-end proof of the shim/ABI/core stack, and every
  layer it exercises is on the DuckLake path anyway. What stays out of
  scope for that slice is the chaining question — how DuckLake's
  `ducklake:` prefix names or nests the moraine attach (see Open
  questions). The `moraine` core remains a standalone Rust library
  regardless.
- **Semantic projection of the catalog** (storing a re-modeled form and
  projecting it into `ducklake_*` on read). Deferred as a possible
  optimization; see Alternatives.
- **The data-file read/write path.** DuckLake and DuckDB own object-store
  reads/writes of Parquet data files. moraine serves catalog *metadata* and
  the inlined-data tables (RFC 0005) only.

## Design

### Positioning: moraine is DuckLake's catalog

```
DuckDB engine
  └─ ducklake extension        planner, transactions, query execution
       └─ moraine catalog      DuckDB StorageExtension  (the extension surface)
            └─ moraine core     DuckLake catalog semantics on SlateDB  (Rust)
                 └─ SlateDB → object store
```

DuckLake stays the query/planner/transaction layer. moraine occupies exactly
the slot a PostgreSQL/SQLite/DuckDB catalog database occupies today: an
`ATTACH`-able catalog whose tables are the `ducklake_*` metadata tables. The
DuckLake specification requires the catalog to be a transactional SQL store
with primary-key constraints; moraine satisfies that contract over SlateDB.

### How DuckLake reaches moraine

moraine registers a `StorageExtension` under a catalog attach type so DuckLake
can point its metadata connection at it. The intended user surface:

```sql
ATTACH 'ducklake:moraine:<slatedb-uri>' AS lake (DATA_PATH '<object-store-uri>');
```

DuckLake connects to its metadata catalog by executing a literal nested
DuckDB `ATTACH` of everything after its `ducklake:` prefix
(source-verified: `DuckLakeInitializer::Initialize` builds and runs the
statement text; backend dialects are a six-entry map keyed by the path's
extension prefix). So `ducklake:moraine:<slatedb-uri>` nests
`ATTACH 'moraine:<slatedb-uri>'`, resolved by DuckDB's ordinary attach
dispatch to moraine's registered storage extension — the same mechanism
`postgres:`/`sqlite:` ride, no DuckLake-side hook. moraine therefore
accepts the `moraine:` path-prefix form alongside `TYPE moraine`. Absent
from DuckLake's dialect map, moraine is spoken to in the **default
dialect**: plain DuckDB SQL, native types, no wrapper calls.

**The standalone attach is a metadata-only surface.** Table *data* is
served through DuckLake, which owns delete-file merging, row lineage, and
pushdown; a standalone data scan re-implementing that read path would
silently return deleted rows once merge-on-read exists. Standalone
`TYPE moraine` therefore serves listings, `DESCRIBE`, and the `ducklake_*`
projections (below), while user-table scans bind normally (so `DESCRIBE`
and `EXPLAIN` work) and raise a redirect error naming the
`ducklake:moraine:` attach at execution time. No opt-out option exists.

**Metadata projections.** Every `ducklake_*` table the keyed store models
is queryable through the attached catalog, served row-faithfully — `current`
and `history` rows both, since DuckLake filters lifecycles in SQL; unversioned
kinds serve current values. DuckDB's executor plans joins over per-table
scans. This row-faithfulness is what makes **time travel** work with no
time-travel logic in moraine: `AT (VERSION => N)` is DuckLake filtering the
served rows by begin/end snapshot — reconstructing past *schema* from the
`ducklake_column` versions as readily as past data — and it is verified live
across inline inserts, schema evolution, and flush (`ducklake_load.rs`). `ducklake_metadata` is synthesized from store facts (format
version, options) so DuckLake's exists-probe and version reads succeed on
any initialized moraine store: a moraine store is a valid DuckLake catalog
from birth, and DuckLake's bootstrap DDL batch never runs against one.

**Attach modes and the single writer.** The RFC 0004 topology is enforced at
the attach surface. An attach is either **read-write** — opening the one
SlateDB `Db` writer — or **read-only** (`READ_ONLY`, mapped to SlateDB's
`DbReader`), which never becomes a writer and never participates in
fencing. The plumbing that carries the `READ_ONLY` flag from `ATTACH`
through the shim to the store open is specified and implemented in RFC 0017. This distinction is not cosmetic: SlateDB fencing means *the
newest writer wins* — a second read-write attach from another process
fences the incumbent's committer rather than failing itself, so two
processes attaching read-write take turns breaking each other. A
deployment therefore designates exactly one read-write process; every
other DuckDB process attaches `READ_ONLY`. This is a real limitation
relative to the multi-client SQL catalogs DuckLake otherwise targets, and
it is documented at the user surface (ATTACH docs, README), not only here.

`READ_ONLY` is read-only at the *catalog* level, not at the IAM level:
SlateDB's `DbReader` in its default follow-latest mode writes a checkpoint
into the manifest on open and refreshes it for the attach's lifetime
(RFC 0004, Topology), so reader credentials need manifest write access.
The truly-zero-write alternative — attaching against a pre-created
checkpoint id — is exposed as an attach option for deployments with
strictly read-only credentials, at the cost of reading a fixed checkpoint
rather than following head.

### Interception level: catalog-entry, row-faithful (B1)

moraine intercepts at DuckDB's **Catalog / table-scan / DML layer** — the
`postgres_scanner` pattern — never by parsing raw SQL. Parsing DuckLake's SQL
would mean reimplementing a query engine; instead DuckDB's own executor plans
DuckLake's statements and calls moraine per table.

moraine's catalog exposes the fixed set of `ducklake_*` tables (and the
per-schema-version inlined-data tables) as catalog entries with the DuckLake
schema, and implements:

- **Scan** — given a table, a projection, and pushed-down filters, produce
  rows from SlateDB. moraine serves filter/projection pushdown where it maps
  cleanly onto the RFC 0002 key layout (e.g. snapshot-range and id-prefix
  scans); DuckDB's executor handles anything not pushed down over the
  returned rows.
- **Insert / Update / Delete** — apply row mutations to the store.
- **Transactions** — `begin`/`commit`/`rollback` mapped onto RFC 0004's
  **staged-row commit path**: a transaction stages row mutations; commit
  drives them through the single fenced atomic batch under head conflict
  detection. On this path moraine performs **no internal retry** — DuckLake
  authored the ids, counters, and snapshot values embedded in the staged
  rows, so any lost race (benign-shaped or not) aborts with the typed
  `CommitConflict`, surfaced to DuckLake as a transaction failure. DuckLake
  then re-drives it: its `RunCommitLoop` (source-verified) retries
  metadata-catalog commit failures with bounded jittered backoff,
  re-checking its own conflict matrix first. Two wire-contract consequences
  for the shim, both load-bearing: **(a) the error message matters** —
  DuckLake's `RetryOnError` decides retryability by substring match on the
  lowercased message (`"primary key"`, `"unique"`, `"conflict"`,
  `"concurrent"`), so the text moraine surfaces for a lost commit must
  contain `"conflict"` or DuckLake will abort instead of retrying; **(b)
  moraine must serve conflict-resolution reads mid-retry** — between
  attempts DuckLake queries `ducklake_snapshot` /
  `ducklake_snapshot_changes` for everything after its transaction
  snapshot, through the ordinary scan hook.
- **Constraints** — the primary-key/uniqueness constraints DuckLake's spec
  relies on, enforced on the tables that require them.

Because the `ducklake_*` rows are the catalog state (B1), RFC 0002 keys are an
efficient encoding of those rows and RFC 0005's inlined chunks are the storage
of specific `ducklake_*` tables — not a separate model that must be
reconciled. This keeps moraine robust to DuckLake evolving its SQL: the same scan/DML
hooks serve new access patterns over the same tables.

"No semantic re-modeling" comes with **exactly one interpreted
convention**, stated so its scope is bounded. The RFC 0002 `current`/`history`
split physically encodes the begin/end-snapshot lifecycle columns, so
moraine must *recognize* the lifecycle in DuckLake's DML — an `UPDATE` that
sets a row's `end_snapshot` translates to end-version bookkeeping (delete
the `current` key, write the `history` key), not a blind value overwrite. That is
a semantic mapping, and it is where the residual drift risk concentrates:
if DuckLake ever mutates those columns in a shape moraine does not
recognize (un-ending a row, say), the translation would misfile it. The
convention is deliberately minimal — lifecycle columns only, everything
else opaque — and the e2e suite pins it against every lifecycle transition
real DuckLake SQL produces. The contract is not zero interpretation; it is
exactly one, tested.

### Composition: C++ shim over the Rust core (forced)

DuckDB's stable C extension API (`duckdb_ext_api_v1`) exposes scalar
functions, table functions, and a handful of other hooks — **not**
storage/catalog registration. A writable DuckLake catalog requires a
`StorageExtension` (DuckLake issues `CREATE`/`INSERT`/`UPDATE` against it;
read-only table functions cannot serve that), and registering one means
**linking DuckDB's C++ internals**. This is the same path the built-in
postgres/sqlite/mysql catalog attachers take. The pure-Rust extension crates
(`duckdb-rs`, `extension-template-rs`) ride the C API and therefore cannot
register a catalog.

Therefore `moraine-duckdb` is:

- a **thin C++ shim** that links DuckDB's internal C++ API and registers the
  `StorageExtension`, `Catalog`, and `TransactionManager`, plus
- the Rust **`moraine` core** compiled as a staticlib exposing a **minimal C
  ABI**: open/attach, `begin`/`commit`/`rollback`, `scan(table, projection,
  filters) -> Arrow`, and `apply(mutations)`.

The shim contains no domain logic — it translates DuckDB catalog callbacks
into C-ABI calls. This preserves RFC 0001's "thin by policy" intent, restated
**language-agnostically**: no catalog semantics in the extension layer,
regardless of the language it is written in.

- **Boundary format: typed C structs, plus the Arrow C Data Interface for
  inline chunks.** Metadata and inline *scan* results cross the ABI as
  owned arrays of `#[repr(C)]` row structs (`crates/moraine-duckdb/src/
  dumps.rs`/`inline.rs`), one `_free` per array — not Arrow arrays as
  originally intended here. Inline chunk *bodies* are the exception and do
  use the Arrow C Data Interface: the shim converts a `DataChunk` to
  `ArrowArray`/`ArrowSchema` with DuckDB's `ArrowConverter` and the Rust
  bridge (`src/arrow_ipc.rs`) serializes to Arrow IPC, with the structs
  crossing the ABI by pointer (`moraine_arrow_encode_*`/`_decode_stream`,
  consuming on encode and producing on decode; ownership rules in
  `arrow_ipc.rs`). Moving scan results generally to Arrow crossing the ABI
  remains open.
- **Sync↔async bridge lives in the Rust C-ABI layer.** The core is async
  (SlateDB requires tokio, RFC 0001). The C-ABI layer owns the tokio runtime
  and `block_on`s core futures, so the C++ shim only ever calls synchronous C
  functions. This is the "FFI boundary" of RFC 0001's async rule.

### C ABI error mapping (v0)

Pinned by the extension-loads slice (`moraine-duckdb/src/error.rs`, `mod
codes`). Every fallible C-ABI entry point returns an `i32` code and, on
failure, fills a caller-provided `(code, message)` pair (`MoraineError`);
messages are UTF-8, allocated by Rust, and freed only via the exported
`moraine_error_free`. Every entry point wraps its whole body — `block_on`
included — in `catch_unwind`, so a core panic surfaces as a code, never as
an unwind into C++. The shim translates codes to DuckDB exceptions:

| Code | ABI constant | Source | Shim raises |
|---|---|---|---|
| 0 | `OK` | success | — |
| 1 | `NOT_FOUND` | `Error::NotFound` | `CatalogException` |
| 2 | `ALREADY_EXISTS` | `Error::AlreadyExists` | `CatalogException` |
| 3 | `CONSTRAINT` | `Error::Constraint` | `CatalogException` |
| 4 | `COMMIT_CONFLICT` | `Error::CommitConflict` | `TransactionException` |
| 5 | `CORRUPTION` | `Error::Corruption`, plus catalog strings that cannot cross the boundary (embedded NUL) | `IOException` |
| 6 | `STORE` | `Error::Store` (and, conservatively, any future core variant) | `IOException` |
| 7 | `INVALID_ARGUMENT` | ABI-layer validation: null pointer, invalid UTF-8, unsupported store scheme | `InvalidInputException` |
| 8 | `INTERNAL` | a panic caught at the FFI boundary | `InternalException` |
| 9 | `INTERRUPTED` | `moraine_interrupt` cancelled the read in flight (or about to start) on the handle | `InterruptException` |

Wire contract: the `COMMIT_CONFLICT` message always contains the literal
substring `conflict` — DuckLake's `RetryOnError` keys its retry decision on
that substring (see the Transactions bullet above) — so the message text
is part of the ABI contract, not incidental diagnostics.

### Read cancellation seam

Pinned by the extension-loads slice (`moraine-duckdb/src/{abi,runtime}.rs`).
Each attached handle owns one `tokio::sync::Notify` (`runtime.rs`) alongside
its runtime and catalog. `moraine_interrupt(handle)` calls `notify_one`;
cancellable read entry points (`moraine_snapshot`) `block_on` a `select!`
between the core future and `notify.notified()`, `biased` toward the
interrupt so a pending signal always wins a tie. `Notify`'s `notify_one`
semantics make the signal single-use without extra bookkeeping: it either
wakes a read already waiting, or stores exactly one permit consumed by the
next `notified()` call — either way it is consumed by the read that
observes it, so an interrupt never carries over to a later, unrelated read.
This assumes at most one cancellable read in flight per handle at a time;
concurrent multi-read cancellation and commit-path shielding (no commit
path exists in the ABI yet) are both out of scope for this slice. The seam
is scaffolding only: the C++ shim does not yet call `moraine_interrupt`
anywhere, so Ctrl-C during a blocked snapshot read is not wired up yet.

### Version pinning and distribution

Linking DuckDB's C++ internals ties the extension to a specific DuckDB build;
the ABI is not stable across releases. moraine-duckdb is therefore **pinned to
a single supported DuckDB release**, recorded in the workspace/CI and bumped
deliberately, with the extension rebuilt (and signed) per DuckDB version. This
is the "FFI/build/version-pinning tax" RFC 0001 explicitly chose to defer out
of the core and pay only at the extension boundary; RFC 0006 makes it
concrete. The per-version build/signing is the substance of the roadmap's
"extension distribution story."

**The pin (as of 2026-07-09), tracking latest stable:**

| What | Pinned at | Notes |
|---|---|---|
| DuckDB | **v1.5.4** | latest release; DuckLake's own v1.5 CI builds against v1.5.3 (`.github/duckdb-version`), so v1.5.3 is the fallback if patch-level ABI friction appears |
| DuckLake | branch **`v1.5-variegata`** @ `c23aca43` (2026-06-17) | DuckLake publishes no release tags — it versions by DuckDB-series branches (`v1.3-ossivalis`, `v1.4-andium`, `v1.5-variegata`); `main` is development |
| DuckLake catalog format | **`1.0`** (`DuckLakeVersion::V1_0`) | the highest version the stable branch writes (its migration chain ends at `'1.0'`); `V1_1_DEV_1` exists on `main` only and is not targeted |

The source-verified behaviors this RFC suite cites (conflict matrix, commit
retry loop, `SchemaChangesMade` classification, per-table column-id
allocation, the five primary keys) were verified on `main` @ `34db89b`
(2026-07-09) and re-checked **identical on the pinned branch** — the diffs
between the two are cosmetic (accessor renames, formatting). The e2e suite
regression-pins against the table above; moving any row of it is a
deliberate, reviewed bump.

### Build and pin mechanics (as implemented)

**Full source-tree headers, not the amalgamation.** The C++ shim compiles
against DuckDB's own `src/include/` tree — every internal header at its
real path (`duckdb/parser/tableref/table_function_ref.hpp`,
`duckdb/storage/storage_extension.hpp`, ...) — fetched by `build.rs` via a
blobless partial clone plus a sparse checkout of `src/include/` from the
`duckdb/duckdb` tag `v1.5.4`, cached at `target/duckdb-src/v1.5.4/` (never
committed, never vendored: at 1,555 files it is a large jump over the ~10
hand-picked headers an amalgamated build needed, and it is deterministically
reproducible from a public tag in under 3 seconds, so a `target/`-cached
copy costs nothing a git-committed one would buy). The build's first
generation compiled against the amalgamated single-header release asset
plus a handful of hand-fetched supplementary headers; that approach hit a
hard wall once the write path needed DuckDB's parser/physical-operator
types (`TableFunctionRef`, `LogicalInsert`/`LogicalUpdate`/`LogicalDelete`
subclassing), which the amalgamation never exposes at all — the full tree
was the fix, and it superseded the amalgamation outright rather than
supplementing it. No DuckDB library is linked and `duckdb.cpp` itself is
never compiled: a loadable extension is `dlopen`'d into a process that has
already resolved every DuckDB symbol, so the shim only needs to compile
against declarations, not link definitions (`-undefined dynamic_lookup` on
macOS; tolerated by default on Linux `-shared` links) — this keeps the
build to a few seconds regardless of which header source feeds it.

**Entry point and packaging.** DuckDB's loader `dlsym`s a fixed entry
symbol derived from the artifact's filename with every `.`-suffix
stripped, so the packaged file must carry exactly one dot
(`moraine_duckdb.duckdb_extension` → `moraine_duckdb_duckdb_cpp_init`). The
loader also requires a trailing 512-byte metadata footer (ABI type,
extension version, the exact DuckDB version string, target platform, magic
value, and a signature region left zero for unsigned local loading)
appended after the compiled cdylib's bytes. Both the artifact rename and
the footer construction live in `xtask`, not a throwaway script, so
`cargo xtask e2e` produces a real loadable `.duckdb_extension` from a
plain `cargo build` output on every run.

**e2e harness.** `cargo xtask e2e` downloads and caches the pinned DuckDB
CLI under `target/` (skipping the fetch once cached), redirects
`extension_directory` under `target/duckdb-extensions/` before `INSTALL
ducklake`/`LOAD ducklake` (also cached, never the CLI's home-directory
default), then drives the CLI with `-unsigned` — required because the
extension is never actually signed — through `LOAD`, `ATTACH`, listing,
and metadata-projection queries against the standalone attach, and through
`ducklake:moraine:` for the real DuckLake read/write round trip, against
stores seeded through the `moraine` API. Full mechanics, including every
build-time discovery, are recorded in `crates/moraine-duckdb/README.md`.

### The staged-transaction ABI surface

DuckLake's own `INSERT`/`UPDATE`/`DELETE` against `ducklake_*` tables
reach moraine through four C-ABI entry points (`moraine_abi.h`):
`moraine_txn_begin(catalog) -> txn`, `moraine_txn_stage(txn, table_kind,
op_kind, cells)` (accumulates one typed row operation — `insert`, `delete`,
or `update_set_end` — without touching the store), `moraine_txn_commit(txn)
-> snapshot_id` (translates every staged operation into one atomic SlateDB
batch and returns the new head), and `moraine_txn_rollback(txn)` (discards
the accumulated operations, no store access). The C++ shim's `PlanInsert`/
`PlanUpdate`/`PlanDelete` (`cpp/catalog.cpp`, `cpp/staged_write.cpp`)
recognize exactly one target: a `ducklake_*` metadata table entry whose
spec names a writable `table_kind`; every other target — a real user table,
or a `ducklake_*` kind this crate does not model as a store entity — still
throws `NotImplementedException`, matching the "translate staged rows,
author nothing else" scope above. Per the staged-row rules: DuckLake
authors every id/counter/`schema_version`/`begin_snapshot` value, carried
across the ABI verbatim; the one interpreted convention is an `UPDATE`
setting `end_snapshot`, translated to a `current`-key delete plus `history`-key
write; a lost race at commit returns an error whose message contains the
literal substring `conflict` (never retried internally, per the C ABI
error mapping table above).

### `ducklake_metadata` synthesis: `data_inlining_row_limit = 10` and dynamic inline-table interception

Beyond the keys the exists-probe path reads (version, encrypted,
created_by — see the metadata-catalog section below), the synthesized
`ducklake_metadata` also serves `data_inlining_row_limit = "10"` —
DuckLake's own compiled default, declaring catalog-wide that inlining is
**on**. (An earlier revision of this shim served `"0"` to keep inlining
off while RFC 0005 was unimplemented; that stopgap is gone now that it
is.) This is load-bearing, not informational: DuckLake's
`WriteNewInlinedTables` (source-verified) skips registering a table's
per-schema-version inlined-data table when `DataInliningRowLimit(...)`
resolves to 0 for that table, and that limit's only inputs are catalog
configuration options — absent a served value the default is 10 anyway,
so serving `10` explicitly just makes the choice legible.

With inlining on, DuckLake **dynamically creates and drives per-table
physical tables** in the metadata catalog rather than writing fixed
`ducklake_*` rows (RFC 0005's "Extension surface (as implemented)" has
the full operation → keyspace mapping). This shim recognizes two dynamic
name families by pattern, not by a fixed catalog-entry list — this is
the one place moraine's catalog lookup does more than serve the fixed
`ducklake_*` set B1 describes above:

- `ducklake_inlined_data_<table_id>_<schema_version>` — recognized once
  `moraine_inline_schemas` has a matching `(table_id, schema_version)`
  record (so a `CREATE TABLE IF NOT EXISTS` existence probe correctly
  reports "does not exist" before the first `CREATE`, and the same
  connection's own `LookupInlineTableEntry` accepts the `CREATE` that
  follows).
- `ducklake_inlined_delete_<table_id>` — recognized once at least one
  `inline/file_delete` has been staged for `table_id` (DuckLake probes this
  table's existence with `SELECT NULL FROM ... LIMIT 1` and treats a
  bind error as "does not exist"; unlike the data family this one must
  report missing for a real table_id until its first inlined
  file-delete, or DuckLake's own existence discipline breaks).

Both route through the same staged-row commit path (`cpp/inline_tables.
cpp`) as the fixed `ducklake_*` tables, translating into `inline/*`
records — see RFC 0005 for the exact wire shape and the encoding
deviation from that RFC's Arrow-IPC design.

`ducklake_flush_inlined_data` and DuckLake's compaction/rewrite cleanup
paths also touch several fixed `ducklake_*` tables this shim did not
previously need to serve, even though moraine models none of the
features they back (partitioning, variant-column stats, name/column
mapping, scheduled file deletion): `ducklake_file_partition_value`,
`ducklake_file_variant_stats`, `ducklake_files_scheduled_for_deletion`,
`ducklake_column_mapping`, `ducklake_name_mapping`. These are served as
always-empty stand-ins (`metadata_tables.cpp`, same pattern as
`ducklake_partition_info`/`ducklake_sort_info`/the macro tables) purely
so DuckLake's generic cleanup `DELETE`/`INSERT` batch — issued
unconditionally as part of a commit that removes or supersedes data
files, not gated on any of these features actually being in use — binds
against an existing table instead of failing the whole commit with a
"table could not be found" error.

### Standalone data-scan retirement

User-table *data* is served only through DuckLake now, matching the
Non-goals decision above: the standalone attach's own scan
(`MoraineTableEntry::GetScanFunction`, `cpp/scan.cpp`) still binds
unconditionally, so `DESCRIBE`/`EXPLAIN` keep working, but its
`init_global` — called once per query execution, not once per bind —
unconditionally throws `InvalidInputException`, naming the table and the
`ducklake:moraine:<store>` attach to use instead (`DATA_PATH` stays a
placeholder; this shim has no store-level source of truth for a lake-wide
data path). The message is deliberately built to exclude DuckLake's own
retry substrings (`conflict`/`unique`/`primary key`/`concurrent`, per the C
ABI error mapping table): this is a permanent redirect, not a race to
retry. The scan machinery this replaced — a nested `read_parquet` query
over resolved file paths, path-resolution rules, streaming, and a
column-count guard — is deleted outright; see
`crates/moraine-duckdb/README.md`'s "User-table data is served only
through DuckLake" section for the full account and the exact error text.

### A known upstream race: pin `threads=1` for DuckLake re-reads after a write

DuckLake's own catalog cache has a multi-threaded race, independent of
moraine: a fresh attach's catalog listing can come back empty immediately
after a write (observed after `RENAME`) under DuckDB's default multi-
threaded query execution. Confirmed upstream, not a moraine defect — the
identical sequence reproduces against a plain duckdb-file-backed DuckLake
attach with zero moraine code in the chain, at a similar failure rate;
moraine's own row-faithful projections independently verify that every
write this crate translates lands correctly regardless of whether
DuckLake's cache race fires on the read side. `SET threads=1;` before the
attach closes the race deterministically (see
`crates/moraine-duckdb/tests/ducklake_load.rs`'s `run_ducklake_sql`); the
tests that drive DuckLake's own write path pin it for exactly that reason,
not as a production recommendation.

## Open questions

- **The exact SQL/access pattern DuckLake issues.** Which reads, writes, and
  filter pushdowns DuckLake relies on against `ducklake_*` determines which
  scan pushdowns moraine must implement for acceptable performance. This is
  the standing E2E validation (RFC 0001/0004), not a blocking
  prerequisite — the design serves any pattern; the question is which to
  optimize.
- **ATTACH ergonomics — resolved (source-verified, DuckLake main
  2026-07).** The metadata connection is a literal nested `ATTACH` of the
  path after `ducklake:`; `ducklake:moraine:<uri>` reaches moraine through
  DuckDB's own prefix dispatch (see Front door). The e2e suite
  regression-pins the exact string against the tracked DuckLake version.
- **Conflict propagation — resolved (source-verified, DuckLake main
  2026-07).** DuckLake re-drives internally: benign races are retried by
  its `RunCommitLoop` (bounded, backoff), true conflicts per its own matrix
  throw `TransactionException` to the application (RFC 0004, "Staged-row
  commits"). The shim's obligations are the two wire-contract points in
  the Transactions bullet above; e2e regression-pins them against the
  tracked DuckLake version.
- **Constraint responsibility — resolved (source-verified, DuckLake main
  2026-07).** The constraint surface is smaller than the spec's
  "transactional SQL store with primary-key constraints" phrasing
  suggests. Exactly **five** metadata tables carry a `PRIMARY KEY` —
  `ducklake_snapshot(snapshot_id)`,
  `ducklake_snapshot_changes(snapshot_id)`, `ducklake_schema(schema_id)`,
  `ducklake_data_file(data_file_id)`,
  `ducklake_delete_file(delete_file_id)` — and there are **no**
  name-uniqueness constraints anywhere (duplicate names are prevented by
  DuckLake's own conflict matrix, not by the catalog; `ducklake_metadata`
  is entirely unconstrained). All five PKs are id-collision guards, and
  their one load-bearing role is the commit-race signal: racing commits
  collide on the snapshot-row `INSERT` (and, downstream of the same shared
  counters, on schema/file ids). In moraine that role is subsumed by RFC
  0004's head conflict detection — a racing staged-row commit fails
  wholesale before any per-row collision could matter. What moraine
  enforces is the equivalent backstop: an insert whose id already exists
  as a live record of the same kind (the five keyed kinds above) fails
  with a typed `Constraint` error rather than silently overwriting the
  `current` key — one existence check per translated insert, no general
  constraint machinery, and no name-uniqueness enforcement (DuckLake owns
  that).
- **DuckDB version cadence.** How often the pin must move, and the support
  window for older DuckDB releases. The initial pin is recorded above
  (DuckDB v1.5.4 / DuckLake `v1.5-variegata` / catalog format 1.0); what
  remains open is the bump policy — whether moraine tracks each DuckDB
  minor as DuckLake cuts its matching branch, and how many past series
  (v1.4-andium, …) get builds.

## Alternatives considered

- **A2 — a standalone moraine `ATTACH` DuckDB catalog** in addition to
  the DuckLake surface. Originally deferred; since **adopted** as the first
  shipping surface (`ATTACH ... (TYPE moraine)`, see Non-goals) — not for a
  direct-query consumer, but because it proves the shim/ABI/core stack
  end-to-end with the least machinery while the DuckLake chaining
  ergonomics are still open.
- **B2 — semantic projection.** Store a re-modeled catalog form and project it
  into the `ducklake_*` views on read, translating writes back. Rejected for
  v1: it couples moraine to DuckLake's exact SQL shapes and re-encodes logic
  DuckLake already owns, so a DuckLake change can silently misread. B1 keeps
  moraine faithful and evolution-robust. Revisit as an optimization only with
  e2e evidence that a specific access pattern demands it.
- **Raw-SQL interception.** moraine parses and answers the SQL DuckLake emits.
  Rejected: reimplements a query engine for no benefit; DuckDB's executor
  already does this over moraine's table scans.
- **Pure-Rust cdylib over the stable C extension API.** Rejected on
  feasibility: the C API exposes scalar/table functions only, not catalog
  registration. A read-only table-function surface cannot be DuckLake's
  writable catalog. A `StorageExtension` requires DuckDB's C++ internals.
- **Wire-impersonating an existing backend** (present as PostgreSQL/DuckDB over
  the wire so DuckLake attaches to moraine as one of its known types).
  Rejected: reimplements a wire protocol and still must satisfy the same
  metadata semantics — cost with no offsetting benefit, and brittle against
  protocol changes.
