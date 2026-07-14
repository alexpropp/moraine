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
- [x] RFC 0002: SlateDB key encoding for DuckLake catalog state
- [x] RFC 0004: commit/transaction protocol
- [x] `store`: key layout + codecs (proptest roundtrips)
- [x] `catalog`: snapshots, schemas, tables, data-file metadata
- [x] `transaction`: atomic commit with conflict detection
- [x] First runnable example in `crates/moraine/examples/`

### DuckDB extension
- [x] RFC 0006: extension surface (moraine as a DuckLake catalog via a
  DuckDB `StorageExtension`)
- [x] C++ shim registering the `StorageExtension`/`Catalog`/
  `TransactionManager`, over a C ABI to the Rust core
- [x] Extension entry points in `moraine-duckdb`
- [x] `cargo xtask e2e` loads the extension into a real DuckDB
- [x] Read-write and read-only attach (RFC 0017): the `READ_ONLY` attach
  flag threads through the shim's `AttachOptions::access_mode` to
  `Catalog::open_read_only`, opening a SlateDB `DbReader` (never the writer
  `Db`, so it never fences the live writer) behind a `ReadHandle` read
  abstraction. Reads verified live standalone and through a read-only
  DuckLake chain; reads/write-rejection/no-fence pinned by the core suite
- [x] DuckLake SQL operations against moraine as the catalog — `ATTACH
  'ducklake:moraine:<store>' AS lake (DATA_PATH ...)`; `CREATE`/`INSERT`/
  `UPDATE`/`DELETE`/rename/`DROP` translate through the staged-row commit
  path, `SELECT`/`COUNT`/time travel read through DuckLake's own reader
  over moraine's row-faithful `ducklake_*` projections (RFC 0006)
- [x] Query interruption: cancellable read entry points take an interrupt
  probe polled while blocked on store I/O; the shim's probe reads the
  interrupted flag DuckDB's executor polls, so Ctrl-C aborts a blocked
  metadata/inline read with `InterruptException`. The commit path is
  deliberately shielded (an interrupt during `COMMIT` lets it finish);
  attach/detach stay non-cancellable (RFC 0006's read cancellation seam)

## Catalog & schema
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
- [x] All DuckLake types: scalars and nested `LIST`/`STRUCT`/`MAP` create,
  inline, and round-trip live (RFC 0005). The full scalar matrix — every
  signed/unsigned integer width, `FLOAT`/`DOUBLE`, `DECIMAL(w,s)`
  (width/scale preserved through the type round trip), `VARCHAR`/`BLOB`/
  `BOOLEAN`, `DATE`/`TIME`/`TIMESTAMP`/`TIMESTAMPTZ`/`INTERVAL`, and
  `UUID` — is pinned live with values, `NULL`s, and stored DuckLake type
  names, both pre-flush (inline Arrow IPC) and post-flush (Parquet)
  (`ducklake_load.rs`'s `ducklake_scalar_type_matrix_round_trip_through_flush`);
  nested types by `ducklake_inline_nested_types_round_trip_through_flush`.
  `VARIANT` awaits the extension surface (RFC 0005 non-goal until proven)
- [ ] Column and name mapping for externally written Parquet
  (`column_mapping`, `name_mapping`) (RFC 0018)
- [x] Macros: scalar/table macros with parameters (`macro`, `macro_impl`,
  `macro_parameters`) (RFC 0019). One versioned `macro` record embeds its
  impl and parameter rows (folded from the staged child-table inserts,
  served back in ordinal order — DuckLake's `LIST()` reconstruction has no
  `ORDER BY`); `DROP` is the generic end-snapshot update, `CREATE OR
  REPLACE` upstream's own drop + fresh id. Verified live end to end
  (`ducklake_load.rs`'s `ducklake_macros_round_trip_through_staged_writes`
  — overloads, a defaulted parameter, a table macro, replace, drop, and a
  `SNAPSHOT_VERSION` attach that still calls the dropped definition), with
  the verb surface (`create_macro`/`drop_macro`) and `changes_made` tokens
  pinned by the core suite

## Data, deletes & layout
- [x] Parquet data files on object storage (`data_file`)
- [x] Row-level deletes via delete files / merge-on-read (`delete_file`)
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
- [x] Partitioning: partition definitions, values, and pruning
  (`partition_info`, `partition_column`, `file_partition_value`) (RFC 0013).
  The spec lives at the `partition` kind with its columns embedded;
  per-file values embed in the `file` record and DuckLake's separate
  `ducklake_file_partition_value` inserts fold into the file's record at
  commit. `SET PARTITIONED BY` / repartition / `RESET PARTITIONED BY`,
  partition-pruned reads (`Total Files Read: 1`), and time travel verified
  live (`ducklake_load.rs`'s
  `ducklake_partitioning_specs_values_and_pruning`); moraine serves
  specs/values verbatim and DuckLake's planner prunes
- [x] Sort orders (`sort_info`, `sort_expression`) (RFC 0013). The spec
  lives at the new `sort` kind with its expressions embedded
  (expression/dialect/direction/null order stored verbatim; moraine never
  sorts — DuckLake's writer does, on `INSERT`). `SET SORTED BY` / change /
  `RESET SORTED BY` and drop-with-history verified live
  (`ducklake_load.rs`'s `ducklake_sort_specs_round_trip_and_reset`); a
  sort change does not bump `schema_version` (RFC 0004's
  altered-but-not-schema-versioned class)
- [x] Statistics for pruning: table, column, per-file, and variant stats
  (`table_stats`, `table_column_stats`, `file_column_stats`,
  `file_variant_stats`) (variant stats pending)

## Transactions & time travel
- [x] Multi-statement, cross-table ACID transactions with conflict
  detection (RFC 0004): a DuckLake `BEGIN … COMMIT` spanning tables stages
  every statement's writes into one moraine staged tx (opened lazily on
  the first write, reused across the transaction) and lands them as one
  atomic snapshot; `ROLLBACK` discards them and mints none. Verified live
  end to end (`ducklake_load.rs`'s
  `ducklake_multi_statement_transaction_commits_atomically` — two writes
  across two tables advance the head by exactly one snapshot). A lost
  write-write race aborts without internal retry, the loser's error
  carrying the `conflict` substring DuckLake's retry loop scans for
  (`staged.rs`'s `lost_race_is_not_retried_and_carries_conflict_text`); a
  genuine concurrent race is not reachable through DuckLake's single
  serialized metadata connection (a second read-write attach fences rather
  than races — RFC 0004's single-writer topology), so it is pinned at the
  core
- [x] Snapshots and time travel to any snapshot (`snapshot`): DuckLake's
  `AT (VERSION => N)` reads past data *and* schema, verified live across
  inline inserts, schema evolution, and flush (`ducklake_load.rs`'s
  `ducklake_time_travel_reads_past_data_and_schema` and
  `ducklake_time_travel_survives_flush`). moraine adds no time-travel logic:
  it serves every `ducklake_*` row current-and-history with begin/end
  snapshots and backdates flushed files, and DuckLake filters by version in
  its own SQL
- [x] Change data feed: changes between snapshots (`snapshot_changes`)
  (RFC 0020): DuckLake's `ducklake_table_changes`/`_insertions`/`_deletions`
  are DuckLake's own scans over the served projections — moraine adds no
  feed logic. Verified live *and differentially* against a stock DuckLake
  catalog fed identical statements (`ducklake_load.rs`'s
  `ducklake_change_feed_attributes_inline_changes` and
  `ducklake_change_feed_survives_flush_and_file_deletes`): per-snapshot
  insert attribution with stable rowids, deletes carrying preimage values,
  `UPDATE` pre/postimage pairing, ranges crossing a flush (backdating +
  `partial_max`), timestamp bounds, and insertions/deletions agreeing with
  the feed. The differential surfaced two serving fixes: an inline
  `UPDATE`'s tombstone no longer ends the same commit's re-inserted row
  (`materialize_inline_rows` now mirrors DuckLake's
  `begin_snapshot != {SNAPSHOT_ID}` writer guard), and
  `ducklake_inlined_delete_<t>` reads back through the new
  `moraine_inline_file_deletes` ABI — it was write-only, so the first
  post-flush `DELETE`/`UPDATE` used to wedge every later attach

## Maintenance & operations
- [x] Compaction / data-file rewriting (RFC 0008): DuckLake rewrites the
  Parquet and authors the rows; moraine translates the two shapes its
  ordinary write path lacked — merge's hard deletes of superseded file
  rows (current and history alike, no history mirror; time travel
  survives via the backdated, row-filtered merged file) plus its direct
  deletion-schedule inserts, and rewrite's `SET begin_snapshot` rebase of
  the replacement file. `row_id_start` is nullable end to end (compaction
  and UPDATE outputs carry explicit per-row ids), `next_row_id` is never
  touched, and reading `ducklake_inlined_delete_<t>` back (the overlay an
  UPDATE writes) is served for real. `ducklake_merge_adjacent_files`
  (rows/row ids identical, pre-merge time travel intact, sources
  scheduled then cleaned, UPDATE-after-merge lineage) and
  `ducklake_rewrite_data_files` (delete file consumed, survivors keep
  ids, pre-rewrite time travel intact) verified live (`ducklake_load.rs`'s
  `ducklake_merge_adjacent_files_preserves_rows_and_time_travel` and
  `ducklake_rewrite_data_files_materializes_deletes`)
- [x] Snapshot expiry and orphaned-file cleanup / deletion scheduling
  (RFC 0007; `files_scheduled_for_deletion`): DuckLake computes the dead
  set and deletes the bytes; moraine translates the cascade — snapshot
  records and dead entity versions hard-pruned (`current` or `history`,
  named by `end_snapshot`), files scheduled into `current/gcfile` (keyed
  by `data_file_id`, DuckLake's own row identity), dead tables' inline
  registrations dropped — as head-preserving commits (maintenance mints
  no snapshot), with the snapshot projection serving read-your-writes
  inside the transaction (the cascade's `NOT EXISTS` re-reads its own
  deletes). Expired snapshots resolve as `NotFound`, never corruption,
  and a racing verb commit treats a vanished intervening snapshot as a
  conflict. `ducklake_expire_snapshots` → `ducklake_cleanup_old_files`
  (schedule drains, bytes deleted, current view unchanged, expired time
  travel refused) and `ducklake_delete_orphaned_files` (stray reaped,
  catalogued files survive) verified live (`ducklake_load.rs`'s
  `ducklake_expire_and_cleanup_reclaims_files` and
  `ducklake_delete_orphaned_files_ignores_catalogued_paths`)
- [x] Data-file encryption (RFC 0014): moraine is a faithful conduit —
  `ENCRYPTED` reaches a fresh store through DuckLake's `META_` passthrough,
  is recorded once at bootstrap as the stored global `encrypted` option and
  served back through `ducklake_metadata` (later attaches adopt it), and the
  per-file keys DuckLake writes round-trip verbatim on
  `ducklake_data_file`/`ducklake_delete_file`. Verified live end to end
  (`ducklake_load.rs`'s
  `ducklake_encrypted_writes_encrypted_files_and_reads_back`: encrypted
  attach → non-plaintext Parquet at rest → plain re-attach decrypts →
  catalog rows carry the keys). Catalog-at-rest stays delegated to bucket
  SSE-KMS per the RFC; moraine holds no crypto
- [x] Table/column tags and catalog options (`tag`, `column_tag`,
  `metadata`). Options were done earlier (`option` kind, unversioned
  last-write-wins). Tags land per RFC 0002's `tag` kind: one container
  record per tagged object with individually begin/end-versioned embedded
  entries, column tags embedded in the column record (carried forward
  across column version transitions, which a tags-only change never
  mints — the record overwrites in place). `COMMENT ON TABLE/COLUMN`,
  re-comment end+insert pairs, row-faithful `ducklake_tag`/
  `ducklake_column_tag` projections, and comments surviving a column
  rename are verified live (`ducklake_load.rs`'s
  `ducklake_table_and_column_comments_round_trip`)

## Hardening & release
- [ ] Real object storage tests (MinIO/localstack)
- [ ] Arbitrary-bytes decode proptests for store codecs (never panic on
  garbage)
- [ ] v0.1 crates.io release (switch `release.yml` trigger to `push`)
- [ ] Extension distribution story: per-DuckDB-version build + signing
  (RFC 0006 pins one supported DuckDB release)
