# RFC 0002: SlateDB key encoding for DuckLake catalog state

- **Date:** 2026-07-08

## Summary

Defines how DuckLake catalog state (spec v1.0) is laid out in SlateDB:
keyspace structure, key layout, and value codec. The layout serves the two
read patterns that matter: loading the **current** catalog (the hot path,
every attach) and reconstructing the catalog **at a past snapshot** (time
travel, rare). Inlined data (RFC 0005) gets a reserved subspace but is
otherwise out of scope.

## Goals

- Loading the current catalog costs proportional to the live catalog, never
  to its history.
- Time travel may scan an entity range's history, never unrelated tables'
  state.
- Every DuckLake v1.0 catalog table has a defined home in the keyspace.
- Values can evolve (add/deprecate fields) without rewriting the store.

Non-goals: the commit protocol (RFC 0004); inlined record formats
(RFC 0005); multi-writer coordination.

## Background

DuckLake versions catalog entities temporally: rows carry `begin_snapshot` /
`end_snapshot`, and the catalog at snapshot `S` is the rows with
`begin_snapshot <= S < end_snapshot` (live rows have no end). A relational
catalog answers "current" and "as of S" with the same filtered scan; a KV
layout must choose a physical split.

SlateDB (pinned 0.14.x) provides ordered keys, prefix/range scans, point
gets, atomic `WriteBatch` writes, pinned read-snapshots (`Db::snapshot()`),
and transactions with write-write conflict detection (`Db::begin`), under a
single fenced writer; readers attach via `DbReader`. RFC 0004 builds the
commit protocol on the transactions; RFC 0009 builds reader consistency on
the read-snapshots.

## Design

### Subspaces

The keyspace is partitioned by its leading byte — the subspace
discriminant — and that byte is registered with SlateDB: every store is
created with a **fixed-length one-byte segment extractor** (SlateDB
RFC-0024), making each subspace a SlateDB **segment** with its own LSM
state. `inline` churn — bulky row
data, the launch workload of RFC 0005 — therefore compacts independently
of the small metadata subspaces. Multi-subspace commit batches remain
atomic (the segment check precedes the single WAL append), and SlateDB
persists the extractor identity, refusing a mismatched open. Everything
below the leading byte is moraine convention, opaque to SlateDB.

| Subspace | Contents | Mutability |
|---|---|---|
| `sys` | Format version, head pointer, catalog-level options | Overwritten in place |
| `snap` | One record per snapshot | Append-only, immutable |
| `cur` | Live catalog entities (no `end_snapshot`) | Insert + delete |
| `hist` | Ended entity versions | Append-only |
| `inline` | Inlined data — reserved for RFC 0005 | Per RFC 0005 |

(Subspace declaration order — and therefore each subspace's discriminant
byte — is fixed by the `Key` type and pinned by golden vectors; see Keys.)

The `cur`/`hist` split is the load-bearing decision. Loading the current
catalog is a scan of `cur` only. Ending an entity version (drop, alter,
file compacted away) atomically deletes its `cur` key and writes a `hist`
key in the same commit batch. History accumulates in `hist`, where the hot
path never looks; snapshot expiry (RFC 0007) garbage-collects it.
Reconstructing the catalog at snapshot `S` scans both and filters: from
`cur`, keep `begin_snapshot <= S`; from `hist`, keep
`begin_snapshot <= S < end_snapshot`.

### Keys

A key is a **typed tree** — subspace, kind, components — defined as
nested Rust enums, and its on-disk bytes are the derived order-preserving
encoding from the **`storekey`** crate (pinned 0.11.x; SlateDB's own
data-modeling guidance recommends it): one discriminant byte per enum
level, assigned by declaration order, then fixed-width big-endian `u64`
components in field order. The structure is the format — variant order is
permanent once written — and golden-vector tests pin the exact bytes per
kind, so any drift (a reordered variant, a changed derive, a `storekey`
bump that alters encoding) fails CI before it can reach a store.

- The **first byte is the subspace discriminant** — the invariant the
  segment extractor keys on.
- Scan bounds are derived, never hand-assembled: encode a sampled key of
  the target shape and truncate (a shorter path through the tree is a
  byte prefix of every key beneath it). Table-scoped bounds go through a
  typed constructor that only accepts the kinds whose first component is
  a table id.
- **No strings in keys.** Entities are keyed by their DuckLake-allocated
  ids — schemas/tables/views/macros from the global `next_catalog_id`,
  files from `next_file_id`, columns from a per-table counter (RFC 0012).
  Names live in values; name→id resolution runs against the in-memory
  snapshot built by scanning `cur` at attach (a persistent name index is
  complexity without payoff at catalog scale).
- `hist` keys append the version's `end_snapshot` as the final component,
  making ended versions of one entity distinct and time-ordered.
- SlateDB iterates **forward only**; the layout depends on no descending
  scan — `sys/head` is an explicit pointer (never a find-max-key scan),
  and every range read (`cur` load, time travel, `hist` dead-prefix
  expiry, `snap` refresh) is ascending by construction. New kinds must
  preserve this.

### Keyspace map

Kinds within `cur` (and mirrored in `hist` with `end_snapshot` appended):

| Kind | Key components | DuckLake table(s) |
|---|---|---|
| `schema` | `schema_id` | `ducklake_schema` |
| `table` | `table_id` | `ducklake_table` |
| `view` | `view_id` | `ducklake_view` |
| `column` | `table_id, column_id` | `ducklake_column` (+ `ducklake_column_tag` embedded). One record per row, **nested fields included** — struct members / list elements / map key-value are their own rows with per-table field ids; `parent_column` lives in the value (RFC 0012). |
| `partition` | `table_id, partition_id` | `ducklake_partition_info` (+ `ducklake_partition_column` embedded) |
| `file` | `table_id, data_file_id` | `ducklake_data_file` |
| `delfile` | `table_id, delete_file_id` | `ducklake_delete_file` |
| `fstat` | `table_id, data_file_id, column_id` | `ducklake_file_column_stats` (+ variant stats) |
| `tstat` | `table_id` | `ducklake_table_stats` |
| `tcstat` | `table_id, column_id` | `ducklake_table_column_stats` |
| `tag` | `object_id` | `ducklake_tag` (object ids are unique across entity types via the shared counter — no type discriminator needed) |
| `option` | `scope_kind, scope_id` | `ducklake_metadata` / `set_option` scopes. `scope_kind` ∈ {global = 0, schema = 1, table = 2}; global uses `scope_id` 0. One record per scope holding its options as a map (option *names* are strings and stay out of keys); set/unset rewrites the record. Options are **unversioned** — DuckLake's `set_option` writes outside the snapshot protocol, last-write-wins (RFC 0004) — so they never transition to `hist`, and an options-only mutation doesn't advance head. |

Other subspaces:

| Subspace/kind | Key components | Contents |
|---|---|---|
| `sys/format` | — | Layout format version (this RFC = 1), moraine version that wrote it |
| `sys/head` | — | Latest committed `snapshot_id` |
| `sys/migration` | — | Structural-migration marker (RFC 0015): `{from_format, to_format, cursor}`, present only mid-migration. **Reserved from format v1**: every materialization checks it and refuses a mid-migration store (RFC 0009) — the check must predate the first migration ever run. |
| `snap` | `snapshot_id` | `ducklake_snapshot` + `ducklake_snapshot_changes` merged into one record (1:1, always written together) |
| `cur/gcfile` | `deletion_id` | `ducklake_files_scheduled_for_deletion` |
| `inline/*` | `table_id, schema_version, …` | Reserved — RFC 0005 |

Two mapping conventions apply throughout: **1:1 side tables merge** into
their parent record, and **pure child tables with no independent lifecycle
embed** in the parent's value (partition columns, column tags). A DuckLake
table earns its own kind only when its rows have an independent begin/end
lifecycle. The v1.0 spec has ~28 tables; the remainder (e.g. column/name
mapping) follow the same key structure and are added here as implementation
reaches them — this RFC is updated, not diverged from.

Per-table collections (`column`, `file`, `fstat`, …) are keyed
`table_id`-first so "everything about table T" — the unit DuckLake reads
and invalidates — is one contiguous range per subspace.

### Value codec

Values are protobuf messages (via `prost`; schemas compiled at build time
with `protox` feeding `prost-build`, so there is no system `protoc`
dependency), one message type per kind, behind a fixed 5-byte framing
header.

- **Framing header:** a 4-byte magic (`b"MRNE"`) and a 1-byte encoding
  version precede the payload. Corrupt, truncated, or wrong-kind values
  fail loudly as `Corruption` (RFC 0003) instead of decoding
  plausibly-wrong, and a reader that meets a newer encoding version errors
  rather than misreads.
- Explicit field tags give forward/backward compatibility (old readers
  skip unknown fields, new readers default missing ones), and the format
  stays language-neutral for external tooling.
- `sys/format` gates **structural** changes (subspace or key-structure
  changes bump it and require migration, RFC 0015); protobuf field
  evolution and the per-value encoding version do not.

Entity values carry `begin_snapshot` (in `hist`, `end_snapshot` appears in
both key and value — values are self-contained). Timestamps are
microseconds since epoch, UTC.

**Statistics are stored verbatim, never interpreted.** DuckLake encodes
min/max stats as strings regardless of column type; moraine round-trips
them exactly (RFC 0006 row-faithfulness) — re-serializing through a typed
value is lossy and would corrupt pruning. DuckLake owns the comparison. If
moraine ever pruned server-side (no such verb today, RFC 0003), the
comparison would have to be **type-aware**, never lexicographic — for a
numeric column `'9'` is not `> '10'`, and a naive compare silently drops
correct rows.

### Atomicity invariant

One DuckLake catalog commit — snapshot record, head-pointer update, every
entity insert/end it implies — is **exactly one SlateDB `WriteBatch`**. No
commit spans batches or depends on read-modify-write across them. Batches
are atomic, so a crash leaves the whole commit or none; one commit ≈ one
durable WAL flush. RFC 0004 builds on this invariant; the layout
guarantees it is *possible* — every mutation a commit needs is puts and
deletes at statically computable keys.

### Property-test obligations

Per RFC 0001:

- `decode(encode(k)) == k` for every key kind, with golden vectors pinning
  the exact on-disk bytes per kind (so an encoding change in a `storekey`
  bump fails CI instead of silently forking the format).
- Order preservation: lexicographic byte order equals component-tuple
  order for keys of one kind.
- Value roundtrip (framing included) for every message type; framing
  rejection — corrupt magic, truncated header, unknown encoding version —
  fails as `Corruption`, never a partial decode.

## Open questions

- **`fstat` key ordering.** File-major (`table_id, data_file_id,
  column_id`) makes "all stats for one file" contiguous — the write unit
  and the per-file predicate shape. DuckLake's own stats query filters by
  `(table_id, column_id)` across files, which file-major serves via a
  table-range scan filtered in memory; column-major would invert the
  trade. The wrong choice costs a factor of the column count on wide
  tables, so the ordering is settled against captured DuckLake stats
  queries in e2e before the table grows migration-sized. File-major stands
  until then.

## Alternatives considered

- **Single subspace, begin/end in values:** every current read filters
  full history — the hot path pays for time travel whether used or not,
  degrading without bound between expiries. Rejected.
- **Version-in-key (`id, begin_snapshot`), no `cur`/`hist` split:**
  append-only and elegant, but live-version reads become reverse scans and
  the live-catalog load still filters ended versions. Rejected; the split
  buys O(live) reads for one extra delete+put per ended version.
- **Names in keys:** renames become key churn across every child range.
  Rejected; attach-time maps suffice.
- **Postcard/bincode values:** positional — adding a field is a format
  break. Rejected for durable state.
- **Arrow IPC for entity values:** a columnar-batch model for
  record-at-a-time metadata; wrong shape.
- **SQL-engine pages over KV (SQLite/DuckDB file in SlateDB):** abandons
  control of the layout, the split, and single-batch atomicity. Worst of
  both worlds.
- **Hand-rolled key codec** (or hand-written `storekey` trait impls with
  explicit tag/kind byte constants): viable, and byte values would be
  explicit rather than declaration-derived — but it maintains a parallel
  encode/decode surface (~300 lines) whose only job is restating the type
  structure, and the golden vectors pin the bytes either way. Rejected in
  favor of deriving `Encode`/`Decode` on the key tree: the structure is
  the format, and the tests are the byte contract.
- **Unsegmented stores (SlateDB's default):** the longer-exercised
  crash/recovery path, but the extractor is fixed per store at creation,
  making format-v1 genesis the only free adoption moment — deferring would
  price the `inline`-isolation payoff at an RFC 0015 rebuild per store.
  The risk is testable instead: the RFC 0011 matrix runs against segmented
  stores, and the decision can still flip for free before first release.
  Prefix bloom filters (the other half of SlateDB's prefix machinery) stay
  unused — a catalog this small doesn't earn them; they can be enabled per
  open at any time.
