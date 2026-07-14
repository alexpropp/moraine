# RFC 0003: Public API shape of the moraine core

- **Date:** 2026-07-09

## Summary

Defines the public API of the `moraine` core crate: how a host opens a
catalog, reads it, and commits changes to it. This is the third
expensive-to-reverse decision the project requires an RFC for (key layout —
RFC 0002/0005; commit protocol — RFC 0004; public API shape — here). The
surface is three types — `Catalog` (the handle), `CatalogSnapshot` (an
immutable materialized read view), and `Transaction` (the mutation handle passed to a
commit closure) — over an error taxonomy with one variant per failure domain.
Writes go through a closure-with-retry model so the RFC 0002 single-`WriteBatch`
atomicity invariant and conflict-retry loop live in the core, not duplicated in
every host. SlateDB never appears in a public signature: the substrate is an
implementation detail behind `object_store` plus moraine's own options type.
The operation set is enumerated in full, grounded in DuckLake v1.0 catalog
semantics.

## Goals

- One read API, usable both standalone (`catalog.snapshot()`) and inside a
  commit (the `Transaction` handed to the closure exposes the same accessors). A host
  learns the accessors once.
- The atomicity invariant (RFC 0002: one catalog commit is exactly one SlateDB
  `WriteBatch`) and the conflict-retry loop are the core's responsibility.
- SlateDB is an implementation detail. No `slatedb::` type crosses the public
  boundary, so the substrate's version churn stays out of moraine's semver
  surface
- The `prost`-generated value types (RFC 0002) stay private to `store`.
- Every DuckLake v1.0 catalog mutation the entities in RFC 0002 imply has a
  named, DuckLake-shaped operation on `Transaction`. The version and `current`↔`history`
  bookkeeping is internal.
- Errors are matchable per failure domain, so the DuckDB bridge maps each to a
  DuckDB error code without parsing strings.

Non-goals:

- The commit protocol itself — conflict detection, snapshot allocation, group
  commit, the CAS/fencing discipline on `sys/head`. That is RFC 0004.
- The DuckDB extension entry points (RFC 0006) and the sync↔async bridge
  (RFC 0010). This RFC defines the async core surface that bridge wraps.
- Snapshot expiry / `history` garbage collection — RFC 0007. No public verb is
  reserved for it here.

## Background

Moraine's core is a plain Rust library any tokio host can embed (README).
Its first and defining consumer is `crates/moraine-duckdb`, a cdylib that is
thin by policy: if logic accumulates there it belongs in the core. That
charter is only honorable if the core surface is drawn so the bridge has
nothing left to do but translate sync↔async and map errors. In particular,
the bridge must not host a conflict-retry loop or assemble writes — those are
core concerns.

SlateDB is async (tokio). The core surface is therefore `async`; the core
spawns no runtime and no threads of its own — the caller drives it, and the
DuckDB bridge owns the sync↔async translation.

RFC 0002 establishes the read model this API exposes: a client builds an
in-memory catalog by scanning the `current` subspace at attach; the live catalog
is small by design, name→id resolution happens against that in-memory
snapshot, and the hot path never scans history. This RFC turns that model into
types.

## Design

### Types and layering

Three public types map onto the existing private modules (`catalog`, `store`,
`transaction`). `store` stays entirely private; `lib.rs` re-exports the public types
alongside `Error`/`Result`.

- **`Catalog`** — the handle. Owns the `slatedb::Db` (private field).
  Constructed once via `open`, cheap to clone (an `Arc` internally), drives
  reads and commits. Lives in `catalog`.
- **`CatalogSnapshot`** — an immutable, materialized read view built by
  scanning `current` (or `current` + the relevant `history` ranges, for time travel) per
  RFC 0002. All accessors are in-memory; after construction it never touches
  the store. Lives in `catalog`.
- **`Transaction`** — the mutation handle passed to the commit closure. It `Deref`s to
  `CatalogSnapshot`, so every read accessor is available inside a commit for
  name→id resolution and validation, and adds the mutators. The commit
  machinery (retry, `WriteBatch` assembly, `current`↔`history` bookkeeping) lives in
  `transaction`; `Transaction` is its public face.

### Front door

```rust
use std::sync::Arc;
use object_store::ObjectStore;

let object_store: Arc<dyn ObjectStore> = /* bucket + credentials */;
let catalog = Catalog::open(object_store, CatalogOptions::default()).await?;
```

`object_store` is the only substrate primitive in the signature — it is the
deployment unit the README already sells ("a deployment is a bucket and
credentials"). `CatalogOptions` surfaces deliberate SlateDB/WAL tuning (e.g.
WAL bucket, flush cadence) through moraine's own type, so no `slatedb::` name
appears publicly and options can be documented and evolved on moraine's terms.
It also carries the store's path within the bucket, defaulting to the bucket
root — the default deployment stays "a bucket and credentials", and a prefix
is opt-in for hosts sharing a bucket.

### Reads

```rust
let snaphot = catalog.snapshot().await?;                 // current catalog
let past = catalog.snapshot_at(snapshot_id).await?;   // time travel
```

Both return `CatalogSnapshot`. `snapshot()` scans `current`; `snapshot_at(S)`
additionally scans the relevant `history` ranges and filters by begin/end per RFC
0002. Accessors (all in-memory, name→id resolved internally):

| Accessor | Returns |
|---|---|
| `schemas()` | all live schemas |
| `schema_by_name(name)` / `schema_by_id(id)` | one schema |
| `tables_in(schema)` | tables of a schema |
| `table_by_name(schema, name)` / `table_by_id(id)` | one table |
| `views_in(schema)` / `view_by_name` / `view_by_id` | views |
| `columns_of(table)` | ordered columns (tags embedded) |
| `partitioning_of(table)` | partition spec, if any |
| `data_files_of(table)` | live data files (incl. inlined chunks, RFC 0005) |
| `delete_files_of(table)` | live delete files (incl. inlined deletes) |
| `table_stats(table)` / `column_stats(table, column)` | statistics |
| `option(scope, key)` | resolved option value |
| `current_snapshot()` | snapshot id + metadata of this view |
| `recent_rows(table)` / `recent_row(table, row_id)` | recently inlined rows served natively from the store's `inline` subspace |

Reads issue no store I/O after the snapshot is built — a `CatalogSnapshot` is a
value, not a cursor.

### Writes: closure-with-retry

```rust
let new_snapshot: SnapshotId = catalog.commit(|tx| {
    let s = tx.create_schema("sales")?;
    let t = tx.create_table(s, "orders", columns)?;
    tx.register_data_file(t, data_file)?;
    Ok(())
}).await?;
```

`commit` reads a fresh `CatalogSnapshot`, constructs a `Tx` over it, runs the
closure to accumulate mutations, assembles exactly one `WriteBatch` (RFC 0002
atomicity invariant), and commits it under the protocol of RFC 0004. On a
transient write-write race it retries the whole cycle — including re-reading
the snapshot and re-running the closure — up to a bounded budget. On a logical
error it aborts immediately.

Because the closure may run more than once, it must be pure and idempotent:
its only effects are the `Transaction` mutators it calls and the value it returns.
This is documented on `commit` and is the single most important contract of
the API. The re-run is not merely permitted, it is load-bearing: RFC 0004's
benign-race retry re-runs the closure against the fresh snapshot precisely
so that logical premises (name uniqueness, entity existence) are
re-validated against the state that won the race — a mechanical re-stage
without the closure would commit duplicate names. Two Rust-shape
consequences follow and are part of the contract, not incidental: the
closure is **`Fn`, not `FnOnce`** (it cannot consume captured values — move
clones in, or stage owned data on the `Transaction`), and it is **synchronous** (no
I/O of its own; name→id resolution and staging run against the in-memory
snapshot, and anything slow — writing Parquet, say — happens *before*
`commit`, per the RFC 0005 data-before-metadata order). Snapshot allocation
is implicit — a successful `commit` produces one new snapshot; there is no
explicit `begin_snapshot` verb.

This closure/verb surface is the **embedding API** — one of RFC 0004's two
commit front doors. The DuckDB extension (RFC 0006) does not call it: that
path commits DuckLake-authored row mutations through RFC 0004's staged-row
protocol, which never retries internally. The retry contract described here
is a verb-path property.

### Error taxonomy

`Error` is `#[non_exhaustive]`, one variant per failure domain:

| Variant | Meaning | Retried? |
|---|---|---|
| `CommitConflict` | write-write race, retry budget exhausted | (internal retries preceded it) |
| `NotFound` | operation references a missing entity | no — abort |
| `AlreadyExists` / `Constraint` | logical conflict (duplicate name, constraint violation) | no — abort |
| `Store(#[source])` | underlying object-store / SlateDB I/O failure | no |
| `Corruption` | value decode failure or unknown `sys/format` version (RFC 0002) | no |
| `Unsupported` | a DuckLake feature moraine does not yet implement (e.g. VARIANT inlining, RFC 0005) | no |
| `SnapshotExpired` | a held/requested snapshot fell below the RFC 0007 retention horizon (RFC 0009) — re-resolve from head | no |
| `Interrupted` | operation cancelled by a host interrupt before its point of no return (RFC 0010) | no |
| `Migration` | store requires, is undergoing, or is newer than a structural format the binary supports (RFC 0015) | no |

The conflict split follows from closure-with-retry: transient races
(another writer advanced `sys/head`) are retried internally and only surface
as `CommitConflict` when the budget is exhausted; logical conflicts
(`AlreadyExists`, `NotFound`) surface immediately because re-running the
closure against fresher state cannot resolve them. Source errors are preserved
via `thiserror` `#[source]`, so the DuckDB bridge maps each variant to a
DuckDB error code without inspecting message text.

### Domain types

The public surface is hand-written domain types, decoupled from the
`prost`-generated messages that `store` uses on disk:

- **Newtype ids** (wrapping the DuckLake-allocated `u64`s of RFC 0002):
  `SchemaId`, `TableId`, `ViewId`, `MacroId`, `MappingId`, `ColumnId`,
  `DataFileId`, `DeleteFileId`, `SnapshotId`.
- **Value structs:** `TableInfo`, `ViewInfo`, `MacroInfo`, `MappingInfo`,
  `ColumnDef`, `DataFile`, `DeleteFile`, `PartitionSpec`, `ColumnStats`,
  `TableStats`, `OptionScope`.

Keeping these separate from the wire types is what lets RFC 0002's protobuf
field evolution stay an internal change instead of a public breaking one.

### Operation enumeration

Every mutator is a method on `Transaction`. Operations read as DuckLake catalog verbs,
not as low-level "put entity version" primitives; the `current`↔`history` version
bookkeeping of RFC 0002 is internal to each. Grounded in DuckLake v1.0
semantics and the entities RFC 0002 maps:

| Group | Operations |
|---|---|
| **Schemas** | `create_schema`, `drop_schema` (no rename verb — DuckLake models no schema rename: `ducklake_schema` is one row per id under a primary key, and its alter path handles tables and views only) |
| **Tables** | `create_table`, `rename_table`, `set_table_schema` (move to another schema), `drop_table` |
| **Views** | `create_view`, `alter_view`, `drop_view` |
| **Macros** (RFC 0019) | `create_macro`, `drop_macro` (no alter verb — DuckLake models macro replacement as drop + create under a fresh `macro_id`; macro names collide with live macros in the schema only, not with tables/views) |
| **Columns** | `add_column`, `rename_column`, `alter_column` (type / default / nullability), `drop_column` |
| **Partitioning** | `set_partitioning`, `clear_partitioning` |
| **Data files** | `register_data_file` (carries its file column stats), `expire_data_file` |
| **Delete files** | `register_delete_file`, `expire_delete_file` |
| **Statistics** | `update_table_stats`, `update_column_stats` |
| **Tags** | `set_tag`, `remove_tag` |
| **Options** | `set_option`, `unset_option` (global / schema / table scopes) |
| **Inlined data** (RFC 0005) | `inline_insert`, `inline_delete`, `flush_inlined_data` |

Notes:

- `ducklake_files_scheduled_for_deletion` (RFC 0002 `gcfile`) has no public
  verb, and the `expire_*` operations do **not** write it: expiring a file
  only ends its live version into history — historical snapshots still
  reference the bytes, so scheduling physical deletion belongs to the
  RFC 0007 expiry commit, which writes `gcfile` records when it prunes the
  `history` records below the retention horizon.
- `alter_column` is one verb taking an optional change per attribute, rather
  than three verbs, because DuckLake models a column alteration as a single
  new column version regardless of which attributes changed.
- Column/name mappings (RFC 0018) have **no verb**: DuckLake creates them
  only as a side effect of `ducklake_add_data_files`, and the embedding API
  has no consumer registering foreign Parquet today. `register_data_file`
  leaves `mapping_id` unset on the verb path; the staged path carries it
  verbatim. A verb is added if an embedding use case appears (additive).
- Snapshot expiry / `history` GC has no verb (deferred, per non-goals).
- This table covers the entities the core models today. As the DuckLake v1.0
  spec's remaining tables and the extension contract (RFC 0005 open question)
  are reached in e2e, operations are added here — this RFC is updated, not
  diverged from.

### Testing obligations

Per RFC 0001:

- Integration tests exercise the public API only, against real SlateDB on an
  in-memory `object_store` (the existing `tests/smoke.rs` pattern) — no mocks
  of the store layer.
- `Catalog::open` and `commit` carry doctests that teach by worked example.
- The `crates/moraine/examples/` program on the ROADMAP ("first runnable
  example once the API exists") becomes the crate-root worked example the RFC
  0001 docs rule asks for.

## Alternatives considered

- **Explicit `begin`/`commit` transactions** (`let mut tx =
  catalog.begin().await?; …; tx.commit().await?`): gives the host full
  control of the retry loop, but pushes retry boilerplate to every call site —
  including inside the DuckDB bridge, exactly where "thin by policy" forbids
  it. Rejected: the conflict-retry loop and the atomicity invariant belong in
  the core, and a closure is how the core keeps ownership of them.

- **Build-a-changeset, then `apply`** (host constructs an inspectable data
  value describing all mutations, then `catalog.apply(changeset).await?`):
  maximally decoupled and testable, but pushes read and name→id resolution
  onto the host and reads least like DuckLake DDL/DML. Rejected: the `Transaction`
  closure already gives an inspectable, testable unit while keeping resolution
  in the core where the in-memory snapshot lives.

- **Lazy read handle** (accessors issue store scans on demand, nothing
  materialized): contradicts RFC 0002's "scan `current` at attach" model and
  reintroduces the hot-path history-filtering cost the `current`/`history` split
  exists to avoid. Rejected: `CatalogSnapshot` is materialized because the
  live catalog is small by design.

- **Caller provides a `slatedb::Db`**: exposes the substrate in the public
  API, ties moraine's semver to SlateDB's, and contradicts keeping `store`
  internal. Rejected: `object_store` is the right primitive to expose;
  the KV engine is not.

- **Exposing the `prost` value types directly**: would save the hand-written
  domain layer, but turns every RFC 0002 protobuf field change into a public
  breaking change and leaks the codec into the API. Rejected: the domain
  layer is what preserves RFC 0002's evolution promise.

- **Coarse two-variant error type** (`Conflict` + `Backend(source)` with
  stringly-typed detail): simpler enum, but forces the DuckDB bridge (and any
  host) to parse message text to react. Rejected: one variant per failure
  domain is a small, non-exhaustive enum that maps cleanly to DuckDB error
  codes.

- **Structural-shape-only scope** (define the types and models, leave the
  operation set to fill in later): considered and rejected in favor of the
  full enumeration above, grounding the operation set in DuckLake v1.0
  semantics rather than the still-unresolved extension contract, so the
  surface is legible now and updated (not redesigned) as e2e reveals detail.
