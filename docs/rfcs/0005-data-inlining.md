# RFC 0005: Data inlining on SlateDB

- **Date:** 2026-07-08

## Summary

DuckLake data inlining stores small inserts as rows in the catalog database
instead of writing a Parquet file per tiny commit, flushing them to Parquet
later. This RFC defines how moraine implements inlining in the `inline`
subspace reserved by RFC 0002: chunked Arrow-encoded inserts, append-only
tombstones for deletes, and a flush operation that mirrors
`ducklake_flush_inlined_data`. Inlining is strategically important for
moraine — small frequent writes are the workload an LSM is built for, and
this is where a KV catalog can beat a SQL catalog rather than merely match
it — and it is a launch feature: moraine ships with it from the first
release.

## Goals

- An inlined insert is part of the same single-`WriteBatch` commit as its
  catalog metadata (RFC 0002 atomicity invariant) — inlining must not add
  round trips to the commit path.
- Reading a table's live inlined rows is one contiguous range scan.
- Time travel over inlined data works exactly as DuckLake specifies:
  live inlined rows are visible for `begin_snapshot <= S < end_snapshot`;
  after flush, pre-flush snapshots are served from the **flushed Parquet**
  (backdated file record + per-row snapshot columns — Background), never
  from retained catalog rows.
- Storage is append-only on the write path — no read-modify-write of
  existing records inside a commit.

Non-goals:

- Auto-flush policy (when to flush is an operational/maintenance concern;
  this RFC defines only the mechanism).
- Inlining `VARIANT` columns (DuckLake excludes them for third-party
  catalogs; moraine matches until the e2e suite proves more is possible).

## Background

DuckLake v1.0 models inlined data as catalog tables:

- **Inlined insert tables**, one per `(table, schema_version)`: columns
  `row_id`, `begin_snapshot`, `end_snapshot`, plus the user table's
  columns. A new inlined table per schema version keeps the row layout
  matched to the schema.
- **Inlined deletion tables**, one per table: `(file_id, row_id,
  begin_snapshot)` — deletes against rows in *existing Parquet files*,
  inlined to avoid writing a tiny deletion file.
- Deletes that target inlined insert rows set that row's `end_snapshot`.
- `ducklake_flush_inlined_data()` materializes inlined inserts to Parquet
  and then **hard-deletes** the inlined rows from the catalog
  (source-verified: `DELETE FROM <inlined table> WHERE begin_snapshot <=
  flush_snapshot`; empty superseded inlined tables are then dropped).
  Time travel survives because the flushed file carries hidden per-row
  `_ducklake_internal_row_id` / `_ducklake_internal_snapshot_id` columns
  and its `ducklake_data_file` record is **backdated** to the minimum
  per-row snapshot — a pre-flush time-travel scan reads the Parquet with
  a per-row snapshot filter. Accumulated deletions consolidate into a
  partial deletion file. The catalog retains nothing; retaining and
  serving flushed rows on any catalog path would **double-count** them
  against the backdated file.
- The row threshold comes from `data_inlining_row_limit`, settable
  globally, per attach, or persistently per table/schema (RFC 0002
  `option` records).

SQL catalogs store nested types as `VARCHAR` because they are limited to
their host's type system. Moraine controls its value format and does not
inherit that limitation.

## Design

### Keyspace (fills in the RFC 0002 `inline` reservation)

| Kind | Key components | Value |
|---|---|---|
| `inline/schema` | `table_id, schema_version` | Arrow IPC schema message (written once per schema version) |
| `inline/ins` | `table_id, schema_version, begin_snapshot, chunk_seq` | Arrow IPC record-batch **body** + `row_id_start`, `row_count` |
| `inline/idel` | `table_id, row_id` | `end_snapshot` (tombstone for an inlined insert row) |
| `inline/fdel` | `table_id, data_file_id, row_id` | `begin_snapshot` (inlined delete against a Parquet file) |

All three are append-only on the commit path.

### Write path

An insert below the row limit becomes one `inline/ins` **chunk record**:
the commit's rows for that table, Arrow-IPC-encoded, with row ids
allocated from the table's row-id counter exactly as a Parquet write would
allocate them. Chunk-per-commit (not row-per-key) because the read unit is
"all live inlined rows of table T", and because one key per commit rides
the `WriteBatch` with negligible overhead. `chunk_seq` disambiguates
multiple chunks in one commit (how rows are batched within a commit is an
implementation detail).

Arrow IPC is the value format because inlined data is *row data*, not
metadata: it carries the table's actual types — including nested
STRUCT/MAP/LIST natively, where SQL catalogs degrade to `VARCHAR` — and
the flush path can feed record batches to a Parquet writer without a
transcoding step. The chunk's schema is pinned by `schema_version` in the
key, mirroring DuckLake's inlined-table-per-schema-version design: schema
evolution never rewrites existing chunks.

Deletes:

- Against an inlined insert row → `inline/idel` tombstone carrying
  `end_snapshot`. DuckLake's SQL form updates the row in place; a
  tombstone is the append-only equivalent and keeps chunks immutable.
- Against a Parquet-file row, when small enough to inline →
  `inline/fdel`, matching the spec's inlined deletion table shape.

### Encoding overhead

Arrow IPC is chosen for the flush path and type fidelity, not for
compactness at a few rows per chunk — and inlining is nothing but the
small-chunk regime. Two costs are inherent to the format there: buffer
alignment (each column buffer is padded to an 8/64-byte boundary, which
at three rows can exceed the row bytes themselves) and the per-message
flatbuffer header. Both are fixed per chunk, so they are worst when
chunks are smallest — exactly the workload this feature targets. This is
a deliberate trade for a transcode-free flush, and it is bounded: chunk
sizes are capped by the row limit and drained by flush cadence.

The schema is *not* one of these costs, and is not paid per chunk.
`schema_version` is already a key component (`inline/ins`), so the reader
schema is recoverable without re-embedding it in every value. It is
stored once per `(table_id, schema_version)` as an `inline/schema`
record — the Arrow IPC schema message verbatim — written in the same
`WriteBatch` as the first inlined insert for that schema version. Each
`inline/ins` value then carries the record-batch message body only, not
a self-contained IPC stream, so the WAL append for a tiny commit never
re-serializes the schema. Zero-copy flush and native nested types are
unaffected; only the redundant schema bytes leave the hot path.

Storing the schema rather than deriving it from the catalog's per-
version column metadata is deliberate: the stored message is self-
describing, so a chunk decodes identically no matter how moraine's
DuckLake-type → Arrow-type mapping evolves after it was written. That
keeps decode correct under the append-only immutability invariant, for
the price of one small record per schema version — amortized across every
chunk of that version, and schema versions do not churn per commit.

Buffer compression stays off. Arrow's LZ4/ZSTD codecs are framed per
buffer and lose to their own overhead at these sizes; whatever cross-
chunk redundancy remains is reclaimed by SlateDB's SST block compression
at rest, where it costs nothing on the write path. (That reclamation
assumes plaintext values — RFC 0014 records how envelope encryption of
value payloads would forfeit it.)

### Read path

Live inlined rows of table T at snapshot S: range-scan
`inline/ins/{table_id}` (all schema versions), keep chunks with
`begin_snapshot <= S`, subtract row ids from `inline/idel` tombstones with
`end_snapshot <= S`. Inlined file-deletes overlay Parquet scans the same
way delete files do. The tombstone set for a table is scanned once and
held in memory — inlined data is bounded by the row limit and flush
cadence, so these sets are small by construction.

### Flush

Flush reproduces `ducklake_flush_inlined_data` semantics as one catalog
commit (still one `WriteBatch`, plus the Parquet PUTs that precede it, in
that order — data before metadata, like any DuckLake write):

1. Write live inlined rows to Parquet data file(s); write a partial
   deletion file consolidating tombstones if any, preserving per-row
   snapshot metadata.
2. In the commit batch: create the `file` (and `delfile`) records — the
   file record backdated to the minimum per-row snapshot, row-faithfully,
   as DuckLake writes it — and **delete** the flushed `inline/ins` chunks
   and consumed `inline/idel`/`inline/fdel` records, matching DuckLake's
   delete-at-flush semantics. Pre-flush time travel is served by the
   flushed Parquet (per-row snapshot columns), not by retained chunks —
   retained chunks visible to any catalog scan would double-count rows.

With the recent-row archive (RFC 0016) enabled, the deletion is
**deferred**: chunks are re-keyed to an archive form in the `inline`
segment, invisible to every catalog read at every snapshot, served only
by the native API, and reclaimed when the archive window passes.

### Why this is a fit for the substrate

Every inlined commit is a small append into SlateDB's WAL — the access
pattern LSMs exist for. Sustained small-insert throughput is then governed
by WAL group commit (many catalog commits per PUT), not by
one-Parquet-file-per-commit overhead. And because the `inline` subspace is
its own SlateDB segment (RFC 0002: format v1 stores are created with a
tag-byte segment extractor), inline churn compacts independently — the
heaviest write traffic in the store never drags the small metadata
subspaces into shared rewrites. Latency per commit remains
PUT-bound as documented in the README; what inlining changes is that tiny
commits stop costing a Parquet file, a data-file record, and eventual
compaction debt each. Flush is the explicit analogue of LSM compaction,
converting accumulated small writes into scan-optimized storage.

### Extension surface (as implemented)

DuckLake does not write inlined rows as fixed `ducklake_*` rows. When
`data_inlining_row_limit != 0` (its compiled default is 10; moraine
synthesizes a nonzero value in `ducklake_metadata` to enable inlining, in
place of the `0` it advertised while inlining was unsupported), DuckLake
**dynamically creates and drives per-table physical tables** in the
metadata catalog and issues ordinary SQL against them. moraine's
`StorageExtension` recognizes these table-name patterns and routes every
operation into the `inline/*` keyspace instead of materializing real
tables — the same staged-commit substrate the fixed `ducklake_*` tables
ride, extended to two dynamic name families:

- `ducklake_inlined_data_<table_id>_<schema_version>` — inlined inserts.
  Columns `(row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT,
  <the table's user columns at that schema version>)`.
- `ducklake_inlined_delete_<table_id>` — inlined deletes against Parquet
  rows. Columns `(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)`.

The operation → keyspace mapping (source-verified against DuckLake
`ducklake_metadata_manager.cpp` / `ducklake_inline_data.cpp` /
`ducklake_flush_inlined_data.cpp`):

| DuckLake SQL | moraine record |
|---|---|
| `CREATE TABLE ducklake_inlined_data_<t>_<v>(...)` (batched with the `INSERT INTO ducklake_inlined_data_tables` registration) | `inline/schema` at `(t, v)` holding the Arrow IPC schema of the user columns; the table appears in the now-live `ducklake_inlined_data_tables` projection |
| `INSERT INTO ducklake_inlined_data_<t>_<v> VALUES (row_id, {snap}, NULL, <cols>), …` (one multi-row `VALUES` per commit) | one `inline/ins` chunk at `(t, v, begin_snapshot={snap}, chunk_seq)`: the user-column cells encoded as one Arrow IPC record-batch body, plus `row_id_start` (first row's `row_id`) and `row_count`. The `row_id`/`begin_snapshot`/`end_snapshot` columns are moraine-derived on read (`row_id = row_id_start + offset`, `begin_snapshot` from the key, `end_snapshot` from `inline/idel`), never stored in the body |
| `UPDATE ducklake_inlined_data_<t>_<v> SET end_snapshot={snap} WHERE row_id=r …` | `inline/idel` at `(t, r)` holding `end_snapshot={snap}` |
| `SELECT <cols> FROM ducklake_inlined_data_<t>_<v> WHERE {snap} >= begin_snapshot AND ({snap} < end_snapshot OR end_snapshot IS NULL) ORDER BY row_id` (and the `SCAN_INSERTIONS`/`SCAN_DELETIONS`/`SCAN_FOR_FLUSH` filter variants) | range-scan `inline/ins` for `t` at `v`, decode Arrow, reconstruct the three virtual columns, apply the snapshot predicate, subtract `inline/idel` tombstones, project and order by `row_id` |
| `INSERT INTO ducklake_inlined_delete_<t> VALUES (file_id, row_id, {snap}), …` | `inline/fdel` at `(t, file_id, row_id)` holding `begin_snapshot={snap}` |
| `DELETE FROM ducklake_inlined_data_<t>_<v> WHERE begin_snapshot <= {flush_snap}` then `DROP TABLE …` + `DELETE FROM ducklake_inlined_data_tables …` (flush / superseded-table cleanup) | remove the flushed `inline/ins` chunks and consumed `inline/idel`; drop the `inline/schema` and deregister. The flushed data lives on as the backdated `ducklake_data_file` DuckLake registers through the ordinary file path |
| `DROP TABLE lake.<schema>.<t>` cascade | drop every `inline/*` record for `t` |

This is served through the same staged-row commit path (RFC 0004): the
inline operations arrive as staged INSERT/UPDATE/DELETE inside DuckLake's
metadata-commit transaction and translate at commit into one atomic batch
of `inline/*` records — same one-batch atomicity, same no-internal-retry,
same `conflict` wire contract. Values DuckLake authors (`row_id`,
`begin_snapshot`, `end_snapshot`, user cells) are stored verbatim per the
keyspace; nothing is re-derived on write.

Two reconciliations with the surrounding RFCs, recorded here because they
governed the implementation:

- **Flush removes inline records; it does not move them to `hist`.**
  RFC 0004's commit-participants note phrases flush as "moves consumed
  inline records to `hist`"; the accurate behavior — matching DuckLake's
  hard `DELETE` and RFC 0005's flush section — is that `inline/*` records
  are *deleted* at flush (the data survives as the backdated Parquet
  file). The `inline` subspace is append-then-delete, not begin/end
  versioned like the entity subspaces. RFC 0016's recent-row archive is
  the only thing that defers the deletion (by re-keying to `inline/arch`);
  it is out of scope here, so this slice hard-deletes.
- **Schema stored, not derived.** `inline/schema` holds the Arrow schema
  written at `CREATE` time (transcoded from DuckLake's column list), so an
  `inline/ins` chunk decodes self-describingly without coupling to the
  mutable `ducklake_column` type mapping — as the Alternatives section
  requires.

## Open questions

- **VARIANT/GEOMETRY inlining.** DuckLake's `CanInlineColumns` excludes
  only `GEOMETRY`; VARIANT is inlinable there. moraine's Arrow transcode
  bounds what it can store; unsupported column types make the whole table
  fall back to the non-inlined (Parquet) path, which is always correct.
  The exact inlinable-type set is pinned in the e2e suite.
- **Row-id counter placement — settled by RFC 0004.** The per-table row-id
  high-water mark lives in `tstat`, matching DuckLake, which also aligns
  row-id allocation with conflict granularity (and, via RFC 0004's
  append-append refinement, lets concurrent inlined inserts to one table
  both land on the verb path).

## Alternatives considered

- **Row-per-key inserts (`inline/ins/<table_id>/<row_id>`):** enables
  point deletes by key but makes every read a per-row decode and bloats
  key overhead for the dominant scan workload. Rejected; chunks match the
  read unit.
- **In-place `end_snapshot` updates on delete (mirror the SQL design):**
  requires read-modify-write of a chunk inside the commit path, breaking
  append-only writes and inflating write amplification for one deleted
  row. Rejected in favor of tombstones.
- **Encoding rows as protobuf like metadata records:** loses native
  columnar types, adds a transcode on flush, and reimplements what Arrow
  already defines. Rejected; the metadata codec argument (RFC 0002) does
  not transfer to row data.
- **Self-contained IPC stream per chunk (schema embedded in every
  value):** the obvious encoding, and the one to avoid. It re-serializes
  the schema into the WAL on every tiny commit — the exact hot path
  inlining exists to optimize — for bytes the key already determines via
  `schema_version`. Rejected in favor of storing the record-batch body
  alone and the schema once per `(table_id, schema_version)` in an
  `inline/schema` record.
- **Deriving the reader schema from catalog column metadata** (instead of
  the `inline/schema` record): saves that record, but couples chunk
  decode to a frozen DuckLake-type → Arrow-type mapping — a later mapping
  change would silently misread existing chunks. Rejected; a self-
  describing stored schema is worth one small record per schema version
  under the append-only immutability invariant.
- **Storing inlined rows outside SlateDB (small Parquet in the bucket):**
  that is just… not inlining; it recreates the tiny-file problem inlining
  exists to solve.
