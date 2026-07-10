# Roadmap

## Milestones
### Catalog core on SlateDB
- [ ] RFC 0002: SlateDB key encoding for DuckLake catalog state
- [ ] RFC 0004: commit/transaction protocol
- [ ] `store`: key layout + codecs (proptest roundtrips)
- [ ] `catalog`: snapshots, schemas, tables, data-file metadata
- [ ] `txn`: atomic commit with conflict detection
- [ ] First runnable example in `crates/moraine/examples/` once the API exists

### DuckDB extension loads
- [ ] RFC 0006: extension surface (moraine as a DuckLake catalog via a DuckDB `StorageExtension`)
- [ ] C++ shim registering the `StorageExtension`/`Catalog`/`TransactionManager`, over a C ABI to the Rust core
- [ ] Extension entry points in `moraine-duckdb`
- [ ] `cargo xtask e2e` loads the extension into a real DuckDB

### DuckLake end-to-end
- [ ] DuckLake SQL operations against moraine as the catalog
- [ ] Data inlining (RFC 0005): inlined inserts/deletes + flush — launch feature
- [ ] Real object storage tests (MinIO/localstack)
- [ ] `cargo-fuzz` targets for store codecs

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
- [ ] Hierarchy: schemas, tables, views (`schema`, `table`, `view`,
  `column`)
- [ ] Full schema evolution: add / drop / rename / reorder columns, type
  promotion, schema versioning (`schema_versions`) (RFC 0012)
- [ ] All DuckLake types including nested `STRUCT`/`LIST`/`MAP`; `VARIANT`
  where the extension surface allows (RFC 0005 non-goal until proven)
- [ ] Column and name mapping for externally written Parquet
  (`column_mapping`, `name_mapping`)
- [ ] Macros: scalar/table macros with parameters (`macro`, `macro_impl`,
  `macro_parameters`)

### Data, deletes & layout
- [ ] Parquet data files on object storage (`data_file`)
- [ ] Row-level deletes via delete files / merge-on-read (`delete_file`)
- [ ] Data inlining: inlined inserts/deletes + flush (RFC 0005;
  `inlined_data_tables`)
- [ ] Partitioning: partition definitions, values, and pruning
  (`partition_info`, `partition_column`, `file_partition_value`) (RFC 0013)
- [ ] Sort orders (`sort_info`, `sort_expression`)
- [ ] Statistics for pruning: table, column, per-file, and variant stats
  (`table_stats`, `table_column_stats`, `file_column_stats`,
  `file_variant_stats`)

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
  `metadata`)
