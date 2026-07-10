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

- **A standalone `ATTACH 'moraine:...'` DuckDB catalog** (queryable without
  the `ducklake` extension). Deferred — no near-term consumer, and it adds
  DuckDB-catalog surface area orthogonal to every hard problem, which is the
  DuckLake path. The `moraine` core remains a standalone Rust library
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

DuckLake's `METADATA_PATH`/`METADATA_CATALOG` machinery resolves to the
attached moraine catalog; `<slatedb-uri>` and moraine-specific options are
passed through DuckLake's metadata-parameter channel to the Rust core, which
opens the SlateDB store. The e2e suite validates the exact chaining ergonomics — whether DuckLake's
`ducklake:` prefix nests a sub-attach, or moraine is named as a standalone
attach type that `METADATA_PATH` references — against the real `ducklake`
extension; see Open questions.

**Attach modes and the single writer.** The RFC 0004 topology is enforced at
the attach surface. An attach is either **read-write** — opening the one
SlateDB `Db` writer — or **read-only** (`READ_ONLY`, mapped to SlateDB's
`DbReader`), which never becomes a writer and never participates in
fencing. This distinction is not cosmetic: SlateDB fencing means *the
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
convention**, stated so its scope is bounded. The RFC 0002 `cur`/`hist`
split physically encodes the begin/end-snapshot lifecycle columns, so
moraine must *recognize* the lifecycle in DuckLake's DML — an `UPDATE` that
sets a row's `end_snapshot` translates to end-version bookkeeping (delete
the `cur` key, write the `hist` key), not a blind value overwrite. That is
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

- **Boundary format: the Arrow C Data Interface.** Scan results and RFC 0005's
  Arrow-typed inlined data cross the ABI as Arrow arrays rather than marshalled
  DuckDB `DataChunk`s — a stable, language-neutral, near-zero-copy boundary
  that both DuckDB and the Rust core already speak.
- **Sync↔async bridge lives in the Rust C-ABI layer.** The core is async
  (SlateDB requires tokio, RFC 0001). The C-ABI layer owns the tokio runtime
  and `block_on`s core futures, so the C++ shim only ever calls synchronous C
  functions. This is the "FFI boundary" of RFC 0001's async rule.

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

## Open questions

- **The exact SQL/access pattern DuckLake issues.** Which reads, writes, and
  filter pushdowns DuckLake relies on against `ducklake_*` determines which
  scan pushdowns moraine must implement for acceptable performance. This is
  the standing E2E validation (RFC 0001/0004), not a blocking
  prerequisite — the design serves any pattern; the question is which to
  optimize.
- **ATTACH ergonomics.** The precise `ATTACH` string and how DuckLake's
  `METADATA_PATH`/`METADATA_CATALOG` names the moraine catalog, confirmed
  against the real `ducklake` extension.
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
  `cur` key — one existence check per translated insert, no general
  constraint machinery, and no name-uniqueness enforcement (DuckLake owns
  that).
- **DuckDB version cadence.** How often the pin must move, and the support
  window for older DuckDB releases. The initial pin is recorded above
  (DuckDB v1.5.4 / DuckLake `v1.5-variegata` / catalog format 1.0); what
  remains open is the bump policy — whether moraine tracks each DuckDB
  minor as DuckLake cuts its matching branch, and how many past series
  (v1.4-andium, …) get builds.

## Alternatives considered

- **A2 — a standalone `ATTACH 'moraine:...'` DuckDB catalog** in addition to
  the DuckLake surface. Deferred, not rejected forever: it adds a second
  DuckDB-catalog surface with no near-term consumer while every hard problem
  lives on the DuckLake path. Revisit if a direct-query use case appears.
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
