# Roadmap

## Milestones
### Catalog core on SlateDB
- [x] RFC 0002: SlateDB key encoding for DuckLake catalog state
- [x] RFC 0004: commit/transaction protocol
- [x] `store`: key layout + codecs (proptest roundtrips)
- [x] `catalog`: snapshots, schemas, tables, data-file metadata
- [x] `transaction`: atomic commit with conflict detection
- [x] First runnable example in `crates/moraine/examples/` once the API exists

### DuckDB extension loads
- [ ] RFC 0006: extension surface (moraine as a DuckLake catalog via a DuckDB `StorageExtension`)
- [x] C++ shim registering the `StorageExtension`/`Catalog`/`TransactionManager`, over a C ABI to the Rust core
- [x] Extension entry points in `moraine-duckdb`
- [x] `cargo xtask e2e` loads the extension into a real DuckDB

### DuckLake end-to-end
- [x] DuckLake SQL operations against moraine as the catalog — `ATTACH
  'ducklake:moraine:<store>' AS lake (DATA_PATH ...)`; `CREATE`/`INSERT`/
  `UPDATE`/`DELETE`/rename/`DROP` translate through the staged-row commit
  path, `SELECT`/`COUNT`/time travel read through DuckLake's own reader
  over moraine's row-faithful `ducklake_*` projections (RFC 0006)
- [x] Data inlining (RFC 0005): inlined inserts/deletes + flush — launch
  feature; on by default (`ducklake_metadata` serves
  `data_inlining_row_limit = 10`, DuckLake's own default). DuckLake's
  dynamic `ducklake_inlined_data_<t>_<v>`/`ducklake_inlined_delete_<t>`
  tables route into the `inline/*` keyspace over the staged-row commit
  path; `INSERT`/`SELECT`/`DELETE`/`ducklake_flush_inlined_data` all
  verified live (`ducklake_load.rs`'s
  `ducklake_inline_data_round_trip_through_flush`). Chunk bodies are Arrow
  IPC: the shim converts a `DataChunk` to the Arrow C Data Interface with
  DuckDB's `ArrowConverter` and the Rust bridge (`src/arrow_ipc.rs`)
  serializes to IPC; decode feeds the structs back to DuckDB's own arrow
  importer. Flush is still a transcode (not zero-copy); see RFC 0005's
  reconciliations
- [ ] Real object storage tests (MinIO/localstack) — pending
- [ ] `cargo-fuzz` targets for store codecs — pending

### Publish
- [ ] First crates.io release (switch `release.yml` trigger to `push`)
- [ ] Extension distribution story: per-DuckDB-version build + signing (RFC 0006 pins one supported DuckDB release)

## 1.0 — Full DuckLake catalog parity

The milestones above get moraine to a working catalog. 1.0 is the bar for
calling it *done*: parity with the complete DuckLake spec v1.0 catalog
feature set, every feature a SQL-backed DuckLake catalog offers served from
SlateDB instead. Each of the 28 `ducklake_*` catalog tables gets a home in
the keyspace (RFC 0002) and is validated against real DuckLake SQL in the
e2e suite before it is checked off.

### Catalog & schema
- [x] Hierarchy: schemas, tables, views (`schema`, `table`, `view`,
  `column`)
- [x] Schema evolution (RFC 0012): every column op DuckLake's `ALTER TABLE`
  can express — `ADD`/`RENAME`/`DROP COLUMN` and `ALTER COLUMN … TYPE`
  (type promotion, verified over data inlined under the old type) — round
  trips live end to end (`ducklake_load.rs`'s
  `ducklake_column_schema_evolution_through_staged_writes` and
  `ducklake_column_type_promotion_over_inlined_data`), carried by the
  generic staged-commit version transitions with no dedicated path. Column
  reorder is not reachable through DuckLake SQL (no reorder `ALTER`); the
  version-transition machinery supports position changes, but nothing issues
  them, so it stays a latent core capability, not a shipped surface.
- [ ] All DuckLake types: scalars and nested `LIST`/`STRUCT`/`MAP` create,
  inline, and round-trip live (RFC 0005); `VARIANT` awaits the extension
  surface (RFC 0005 non-goal until proven)
- [ ] Column and name mapping for externally written Parquet
  (`column_mapping`, `name_mapping`)
- [ ] Macros: scalar/table macros with parameters (`macro`, `macro_impl`,
  `macro_parameters`)

### Data, deletes & layout
- [x] Parquet data files on object storage (`data_file`)
- [x] Row-level deletes via delete files / merge-on-read (`delete_file`)
- [x] Data inlining: inlined inserts/deletes + flush (RFC 0005;
  `inlined_data_tables`)
- [ ] Partitioning: partition definitions, values, and pruning
  (`partition_info`, `partition_column`, `file_partition_value`) (RFC 0013)
- [ ] Sort orders (`sort_info`, `sort_expression`)
- [x] Statistics for pruning: table, column, per-file, and variant stats
  (`table_stats`, `table_column_stats`, `file_column_stats`,
  `file_variant_stats`) (variant stats pending)

### Transactions & time travel
- [ ] Multi-statement, cross-table ACID transactions with conflict
  detection (RFC 0004)
- [ ] Snapshots and time travel to any snapshot (`snapshot`)
- [ ] Change data feed: changes between snapshots (`snapshot_changes`)

### Maintenance & operations
- [ ] Compaction / data-file rewriting (RFC 0008)
- [ ] Snapshot expiry and orphaned-file cleanup / deletion scheduling
  (RFC 0007; `files_scheduled_for_deletion`)
- [ ] Data-file encryption (RFC 0014)
- [ ] Table/column tags and catalog options (`tag`, `column_tag`,
  `metadata`) (options done; tags pending a keyspace decision)
