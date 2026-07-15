# Roadmap

One release target: **v0.1 — DuckLake consistency**. moraine ships v0.1
when it is a consistent DuckLake catalog: parity with the complete DuckLake
spec v1.0 catalog feature set, every feature a SQL-backed DuckLake catalog
offers served from SlateDB instead. Each of the 28 `ducklake_*` catalog
tables gets a home in the keyspace (RFC 0002) and is validated against real
DuckLake SQL in the e2e suite before it is checked off. Everything below is
on that path.

## Foundations

### Catalog core on SlateDB
- [x] SlateDB key encoding for DuckLake catalog state (RFC 0002)
- [x] Commit/transaction protocol (RFC 0004)
- [x] Key layout and codecs with proptest roundtrips
- [x] Snapshots, schemas, tables, and data-file metadata
- [x] Atomic commit with conflict detection
- [x] First runnable example

### DuckDB extension
- [x] Extension surface: moraine as a DuckLake catalog via a DuckDB `StorageExtension` (RFC 0006)
- [x] C++ shim registering the catalog and transaction manager over a C ABI to the Rust core
- [x] Extension entry points and loading into a real DuckDB
- [x] Read-write and read-only attach; read-only never fences the live writer (RFC 0017)
- [x] Full DuckLake SQL against moraine as the catalog: `ATTACH`, `CREATE`/`INSERT`/`UPDATE`/`DELETE`/rename/`DROP`, `SELECT`/`COUNT`/time travel
- [x] Query interruption: Ctrl-C aborts a blocked read; `COMMIT` runs to completion (RFC 0006)

## Catalog & schema
- [x] Schemas, tables, and views
- [x] Schema evolution: `ADD`/`RENAME`/`DROP COLUMN` and `ALTER COLUMN … TYPE` promotion, over inlined and flushed data (RFC 0012)
- [x] All DuckLake scalar and nested `LIST`/`STRUCT`/`MAP` types, inline and post-flush; `VARIANT` pending (RFC 0005)
- [x] Column and name mapping for externally written Parquet, including hive-partitioned foreign files (RFC 0018)
- [x] Scalar and table macros with parameters, overloads, defaults, and `CREATE OR REPLACE`/`DROP` (RFC 0019)

## Data, deletes & layout
- [x] Parquet data files on object storage (`data_file`)
- [x] Row-level deletes via delete files / merge-on-read (`delete_file`)
- [x] Data inlining: inlined inserts/deletes plus flush, on by default (RFC 0005)
- [x] Partitioning: definitions, per-file values, and partition-pruned reads (RFC 0013)
- [x] Sort orders, set/changed/reset, with history (RFC 0013)
- [x] Statistics for pruning: table, column, and per-file (`table_stats`, `table_column_stats`, `file_column_stats`); variant stats pending

## Transactions & time travel
- [x] Multi-statement, cross-table ACID transactions with conflict detection; lost races surface as conflicts for DuckLake's retry (RFC 0004)
- [x] Snapshots and time travel to any snapshot, reading past data and schema across inline, evolution, and flush (`snapshot`)
- [x] Change data feed between snapshots, verified differentially against a stock DuckLake catalog (`snapshot_changes`, RFC 0020)

## Maintenance & operations
- [x] Compaction and data-file rewriting: `ducklake_merge_adjacent_files` and `ducklake_rewrite_data_files`, preserving row ids and pre-op time travel (RFC 0008)
- [x] Snapshot expiry and orphaned-file cleanup / deletion scheduling; expired snapshots resolve cleanly (RFC 0007, `files_scheduled_for_deletion`)
- [x] Data-file encryption: `ENCRYPTED` attaches, encrypted Parquet at rest, per-file keys round-tripped; moraine holds no crypto (RFC 0014)
- [x] Table/column tags and catalog options via `COMMENT ON` and options (`tag`, `column_tag`, `metadata`)

## Performance
- [x] Commit-served projections: `snapshot`, `table_stats`, and `table_column_stats` are folded forward from each commit and served from an in-memory cache when current, removing per-commit latency growth with snapshot history. Attach-tunable WAL flush cadence bounds the per-commit durable wait.
- [ ] Per-commit latency is now dominated by DuckDB/DuckLake executing moraine's table-function-backed metadata (~72% of a single-row commit is outside moraine), not by moraine's own code. Closing that gap would mean presenting metadata as native tables rather than table functions — a large, off-axis storage rewrite — so the remaining difference is architectural.

## Hardening & release
- [x] Real object storage tests (MinIO)
- [x] Arbitrary-bytes decode proptests for store codecs (never panic on garbage)
- [x] Release pipeline armed for v0.1 (crates.io publish on tag); not yet published
- [x] Extension distribution: per-DuckDB-version, per-platform builds attached to GitHub releases (unsigned)
- [ ] Signed distribution via a duckdb/community-extensions submission
