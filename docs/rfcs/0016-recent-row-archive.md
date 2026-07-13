# RFC 0016: Recent-row archive — native fast reads for inlined data

- **Date:** 2026-07-10

## Summary

moraine can serve recently written rows at KV latency because it owns its
store — a capability no SQL-catalog deployment of DuckLake has. Live
inlined rows (RFC 0005) are already served from SlateDB; this RFC extends
that window past flush. DuckLake's contract **deletes** inlined rows at
flush (RFC 0005: hard `DELETE`, time travel served thereafter by the
backdated flushed Parquet) — the archive is moraine **deferring that
deletion**: flushed chunks are re-keyed to an archive form, invisible to
every catalog read at every snapshot, readable only through a native API
without touching Parquet, DuckDB, or the DuckLake read path. Correctness
under the Parquet-side mutation channel (deletes, updates, compaction —
bytes moraine never reads) comes from **catalog-metadata invalidation**:
a chunk is served only while the catalog proves its flushed target file
untouched. The archive adds no write amplification (retention instead of
deletion, never a copy), lives in the `inline` segment, and does not
exist as far as the DuckLake contract is concerned.

## Goals

- Tail and point reads of recently inlined rows at SlateDB latency — no
  Parquet GET, no DuckDB — from any moraine host, including read-only
  `DbReader` attaches.
- Zero additional write amplification: retention in place of deletion,
  never a copy.
- Never serve a stale row: any Parquet-side mutation that *could* have
  touched a row invalidates its chunk, provable from catalog metadata
  alone.
- Decouple the fast-read window from live-catalog size: delaying flush
  keeps rows fast but bloats the materialized `CatalogSnapshot`
  (RFC 0009); the archive keeps rows fast *after* flush shrinks the live
  catalog.
- DuckLake-visible semantics unchanged: flush, time travel, and the scan
  hooks behave exactly per RFC 0005 / RFC 0006.

Non-goals:

- **Mandatory inlining.** Bulk inserts keep writing Parquet directly;
  routing them through the inline path would double-write bulk data
  through the WAL for a fast-read window bulk data doesn't need. The row
  limit stands.
- **Serving flushed data to DuckLake.** DuckLake plans scans over the
  files the catalog reports; moraine cannot route those reads to the
  archive without double-reporting rows (RFC 0006). The archive is a
  native surface only.
- Predicates, projections, SQL — this is tail/point retrieval, not a
  query engine.
- Update-aware serving (returning the *latest version* of a mutated row)
  — that requires reading delete-file bytes; invalidation is conservative
  instead.

## Background

DuckLake deletes inlined rows at flush and serves pre-flush time travel
from the flushed Parquet itself — the file record is backdated to the
minimum per-row snapshot and each row carries a hidden snapshot column
(RFC 0005, source-verified). Two consequences shape this RFC. First,
retained chunks must be invisible to **all** catalog reads, time travel
included: the same rows are reachable through the backdated file, so
serving them on any catalog path would double-count. Second, retention
owes nothing to the snapshot horizon — archived chunks participate in no
snapshot — so the archive window is the *only* retention clock.

Two facts make invalidation tractable without reading data files. Inlined
deletes against Parquet rows are **catalog records** — `inline/fdel` keys
carry `(table_id, data_file_id, row_id)` — so those deletes are visible at
row grain. Non-inlined delete files are Parquet bytes moraine never reads,
but their *records* (`delfile`, and compaction's file endings, RFC 0008)
are catalog-visible, so their targets are known at file grain.

RFC 0003 already exposes live inlined chunks through `CatalogSnapshot`,
and RFC 0009 notes the memory cost of holding them there — which is why
"just flush later" is not the answer this RFC gives.

## Design

### Placement — re-keyed within the `inline` segment

This RFC amends RFC 0005's flush step: where the base semantics delete
flushed `inline/insert` chunks (and consumed `idel`/`fdel` records), the
archive re-keys them to a distinct **archive form within the `inline`
subspace** — same commit batch, same atomicity, and a key shape the
catalog read paths (live scan *and* time travel) never touch. Bulk row
data stays out of the `hist` segment, and the archive is one contiguous
per-table range in one segment.

At flush, each archived chunk record gains the **`data_file_id` it
flushed into** — the hook for file-grain invalidation.

### Retention — the archive window

An archived chunk is reclaimable when its flush is older than the
**archive window `A`** — nothing else. Archived chunks are invisible to
snapshots, so the RFC 0007 horizon has no claim on them; `A = 0`
degenerates to DuckLake's delete-at-flush exactly. The window is policy
(operational, like flush cadence and expiry); reclamation rides the
ordinary RFC 0007 expiry commit as extra deletes.

### Reads

RFC 0003 gains one accessor family, materialized under a pinned read
handle (RFC 0009) so results are a consistent cut:

- `recent_rows(table)` — Arrow batches of the table's live inlined rows
  plus still-valid archived rows, ascending. SlateDB iterates forward
  only (RFC 0002); newest-first is a caller-side reversal of a
  window-bounded result.
- `recent_row(table, row_id)` — point lookup by row-id filter over the
  same scan (chunks carry `row_id_start`/`row_count`); no secondary index
  unless profiling demands one.

Rows carry their row ids and `begin_snapshot`, so consumers can correlate
results with DuckLake queries.

### Invalidation — two grains, all catalog-resident

The archive must never serve a row the Parquet path may have mutated:

- **Row grain (free).** `inline/fdel` records subtract exactly the rows
  they name; live `idel` tombstones subtract from live chunks as today
  (RFC 0005).
- **File grain (conservative).** A registered delete file against — or
  the ending of (compaction, rewrite, RFC 0008) — a chunk's target
  `data_file_id` invalidates every archived chunk that flushed into that
  file. The rows may in fact be intact; the catalog cannot prove it
  without reading Parquet, so they are not served.

Append-only tables — the workload inlining targets — never trip
file-grain invalidation.

### What this is and isn't

A read-through window over data already in the store, for hosts that need
"the row I just wrote" or "the last N minutes" without a Parquet round
trip: ingest verification, CDC-shaped tails, operational point reads. It
is not a cache DuckLake knows about, not a second storage tier, and not a
query engine.

### Test obligations

Per RFC 0001, against real SlateDB on in-memory `object_store`:

- **Flush transparency.** Rows readable via `recent_rows` before flush
  remain readable after, byte-identical, until window expiry.
- **Catalog invisibility.** Archived chunks are served by no catalog
  read: a live scan never returns them, and a time-travel scan at a
  pre-flush snapshot resolves the flushed *file* (never the archived
  chunks) — the double-count is unrepresentable.
- **Row-grain invalidation.** An inlined delete excludes exactly that
  row; siblings in the same chunk still serve.
- **File-grain invalidation.** Registering a delete file (or compacting)
  against a flushed target excludes all chunks for that file; chunks
  flushed into other files are unaffected.
- **Retention.** A chunk survives within the archive window and is
  reclaimed after it, independent of snapshot expiry in both directions
  (aggressive snapshot expiry never shortens the archive; a long archive
  never delays snapshot expiry).
- **Segment isolation.** Archived records' keys remain in the `inline`
  segment; `hist` contains no chunk bodies.
- **Consistency.** `recent_rows` under concurrent commits returns a
  consistent cut (pinned handle), never a torn mix.
- **e2e equivalence.** On an append-only table, `recent_rows` equals
  DuckDB's scan of the same table over the window.

## Open questions

- **Archive-window default.** Sizing signals (ingest rate × read-lag
  tolerance); pick after real workloads exist.
- **Point-lookup index.** A `row_id → chunk` index if range-filter
  latency matters at large windows; deferred until profiling.
- **A non-Rust surface.** Arrow Flight, or a DuckDB *table function* —
  the latter is a non-DuckLake surface (RFC 0006's deferred A2
  territory), useful for SQL over the window without breaking the
  catalog contract.
- **Availability mode.** Whether file-grain invalidation can downgrade to
  serving-with-staleness-flag for consumers that prefer availability over
  strict freshness.

## Alternatives considered

- **Retain chunks as `hist`-style temporal versions** (end them with
  `end_snapshot` and let time travel resolve them). Rejected as a
  correctness bug, not a style choice: DuckLake serves pre-flush time
  travel from the backdated flushed Parquet, so temporally-visible
  retained chunks would double-count every flushed row at pre-flush
  snapshots. The archive must be invisible to the catalog, which is why
  it is a distinct key form and not a version.
- **Copy chunks to a dedicated archive subspace at flush.** Write-
  amplifies bytes that a re-key retains for free. Rejected.
- **Mandatory inlining of all writes.** Double-writes bulk data through
  the WAL to feed a window bulk data doesn't need. Rejected (non-goal).
- **Routing DuckLake reads to the archive.** Requires double-reporting
  rows or misreporting the flush to the planner — catalog corruption by
  construction. Rejected (RFC 0006).
- **Row-per-key archive.** Point-lookup friendly, but the dominant read
  is the tail scan and per-row keys bloat the store — the same trade
  RFC 0005 already decided chunk-ward. Rejected.
- **No archive — just delay flush.** The zero-cost version, and
  operators can still use it; rejected as the *whole* answer because it
  couples the fast-read window to live-catalog size (every unflushed row
  sits in the materialized `CatalogSnapshot`, RFC 0009) and to DuckLake's
  inlined-scan costs. The archive provides the window after flush has
  already shrunk the live catalog.
