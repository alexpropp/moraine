//! Drives a real, pinned DuckDB CLI + the `ducklake` extension against a
//! store pre-seeded through the `moraine` API, proving the whole nested
//! attach chain: `ATTACH 'ducklake:moraine:<dir>' AS lake (DATA_PATH
//! '<dir2>')` resolves DuckLake's metadata connection through this shim's
//! `moraine:` prefix dispatch and synthesized `ducklake_*` tables (see
//! `crates/moraine-duckdb/cpp/metadata_tables.cpp`), and DuckLake's own
//! reader — not this crate's scan — serves the data back.
//!
//! Ignored by default: needs the downloaded DuckDB CLI, the packaged
//! `.duckdb_extension`, and network access to `INSTALL ducklake` (cached
//! under `target/duckdb-extensions/`, gitignored). Run manually after
//! `cargo xtask e2e` has produced the CLI/extension artifacts once:
//!
//! ```text
//! MORAINE_DUCKDB_CLI=target/duckdb-cli/cli/duckdb \
//! MORAINE_DUCKDB_EXT=target/duckdb-cli/artifact/moraine_duckdb.duckdb_extension \
//! cargo test -p moraine-duckdb --release --test ducklake_load -- --ignored
//! ```

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile};
    use object_store::local::LocalFileSystem;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "moraine-ducklake-load-{tag}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("test setup: create temp dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn cli_path() -> PathBuf {
        PathBuf::from(
            env::var("MORAINE_DUCKDB_CLI")
                .expect("MORAINE_DUCKDB_CLI must be set (see this module's doc comment)"),
        )
    }

    fn ext_path() -> PathBuf {
        PathBuf::from(
            env::var("MORAINE_DUCKDB_EXT")
                .expect("MORAINE_DUCKDB_EXT must be set (see this module's doc comment)"),
        )
    }

    /// Cache root for `INSTALL ducklake`'s downloaded artifact, matching
    /// the repo's `target/`-cached-not-committed convention (see
    /// `crates/moraine-duckdb/README.md`).
    fn extension_directory() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/duckdb-extensions")
    }

    const ROW_COUNT: u64 = 5;

    /// Seeds a store via the `moraine` API: `main` from bootstrap (no
    /// explicit `create_schema` call), one table `t` with a relative-path
    /// data file, then a rename to give the table row hist depth (two
    /// `ducklake_table` versions). `file_size_bytes`/`footer_size` must be
    /// the real Parquet file's stats — DuckLake's own reader (unlike this
    /// crate's `read_parquet`-delegating standalone scan) uses the
    /// registered `footer_size` to seek straight to the file's metadata
    /// footer; a placeholder `0` throws `Invalid Input Error: Invalid
    /// footer length` the moment DuckLake reads the file (discovered
    /// live).
    fn seed(dir: &Path, file_size_bytes: u64, footer_size: u64) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");

        rt.block_on(async {
            let store = Arc::new(
                LocalFileSystem::new_with_prefix(dir).expect("test setup: open local store"),
            );
            let catalog = Catalog::open(store, CatalogOptions::default())
                .await
                .expect("test setup: open catalog");

            catalog
                .commit(|tx| {
                    let main = tx.schema_by_name("main").expect("bootstrap mints main").id;

                    let t = tx.create_table(
                        main,
                        "t_old",
                        &[
                            ColumnDef {
                                name: "id".into(),
                                column_type: "BIGINT".into(),
                                nulls_allowed: false,
                                default_value: None,
                            },
                            ColumnDef {
                                name: "amount".into(),
                                column_type: "DOUBLE".into(),
                                nulls_allowed: true,
                                default_value: None,
                            },
                        ],
                    )?;
                    tx.register_data_file(
                        t,
                        DataFile {
                            path: "data.parquet".into(),
                            path_is_relative: true,
                            file_format: "parquet".into(),
                            record_count: ROW_COUNT,
                            file_size_bytes,
                            footer_size,
                            column_stats: vec![],
                        },
                    )?;
                    // Hist depth: `t_old`'s cur row ends, a hist row is
                    // written, and a new cur row for `t` begins.
                    tx.rename_table(t, "t")?;

                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// Writes `<data_path>/main/t_old/data.parquet`. Two path facts,
    /// discovered live against real DuckLake, drive this:
    ///
    /// - DuckLake resolves a relative data-file path against `<DATA_PATH
    ///   from ATTACH>/<schema.path>/<table.path>/`, never against the
    ///   metadata store's own directory (that resolution rule is this
    ///   crate's *standalone*-attach convention only, documented in
    ///   `crates/moraine-duckdb/README.md`'s "Path resolution" — a
    ///   different attach, a different base path).
    /// - `table.path` is fixed at `CREATE TABLE` time (here, `t_old/`) and
    ///   is untouched by a later rename — matches real DuckLake semantics
    ///   (renaming a catalog entry never moves its files on disk), and
    ///   moraine's own `rename_table` verb likewise only touches the name.
    ///
    /// Returns `(file_size_bytes, footer_size)` for the written file, per
    /// the Parquet spec's fixed trailer: the last 4 bytes are the magic
    /// `PAR1`, and the 4 bytes before that are the footer's thrift-encoded
    /// length as a little-endian `u32` — exactly the `footer_size` DuckLake
    /// itself registers when it authors a data file (see
    /// `DuckLakeMetadataManager`'s data-file-append call sites, which read
    /// this same trailer through DuckDB's own Parquet writer metadata
    /// rather than recomputing it, but the on-disk encoding is identical).
    fn write_parquet(data_path: &Path) -> (u64, u64) {
        let table_dir = data_path.join("main").join("t_old");
        std::fs::create_dir_all(&table_dir).expect("test setup: create table dir");
        let file = table_dir.join("data.parquet");

        let status = Command::new(cli_path())
            .arg("-c")
            .arg(format!(
                "COPY (SELECT i::BIGINT AS id, (i * 1.5)::DOUBLE AS amount FROM range({ROW_COUNT}) t(i)) \
                 TO '{}' (FORMAT PARQUET);",
                file.display()
            ))
            .status()
            .expect("failed to spawn duckdb CLI to write the fixture Parquet file");
        assert!(status.success(), "writing fixture Parquet file failed");

        let bytes = std::fs::read(&file).expect("test setup: read back fixture Parquet file");
        let file_size_bytes = u64::try_from(bytes.len()).expect("test setup: file size fits u64");
        assert!(
            bytes.ends_with(b"PAR1"),
            "fixture Parquet file missing trailing PAR1 magic"
        );
        let footer_len_offset = bytes.len() - 8;
        let footer_size = u64::from(u32::from_le_bytes(
            bytes[footer_len_offset..footer_len_offset + 4]
                .try_into()
                .expect("test setup: 4-byte footer length slice"),
        ));
        (file_size_bytes, footer_size)
    }

    /// Runs one CLI session: caches `ducklake` under
    /// `target/duckdb-extensions`, loads both extensions, attaches the
    /// nested `ducklake:moraine:` chain, then runs `sql`.
    ///
    /// Pinned single-threaded. DuckLake's catalog re-read after a rename
    /// is racy under multiple threads — a fresh attach sometimes returns
    /// an empty table list. The race reproduces against a plain
    /// duckdb-file-backed DuckLake with no moraine in the chain (~75% of
    /// runs), so it is upstream, not a moraine translation defect; the
    /// store moraine writes is verified independently and deterministically
    /// through the standalone `moraine:` projections below. One thread
    /// closes the upstream race so these tests exercise moraine's
    /// translation, not DuckLake's cache concurrency.
    fn run_ducklake_sql(store_dir: &Path, data_path: &Path, sql: &str) -> String {
        let output = Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg("SET threads=1;")
            .arg("-c")
            .arg(format!(
                "SET extension_directory='{}';",
                extension_directory().display()
            ))
            .arg("-c")
            .arg("INSTALL ducklake;")
            .arg("-c")
            .arg("LOAD ducklake;")
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!(
                "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');",
                store_dir.display(),
                data_path.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            output.status.success(),
            "duckdb CLI failed for `{sql}`:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
    }

    fn csv_rows(output: &str) -> Vec<Vec<String>> {
        output
            .lines()
            .skip(1)
            .filter(|line| !line.is_empty())
            .map(|line| line.split(',').map(str::to_owned).collect())
            .collect()
    }

    /// `moraine:` prefix dispatch (no `TYPE moraine` needed) — DuckDB's own
    /// `PhysicalAttach`/`DBPathAndType::ExtractExtensionPrefix` resolves it
    /// before this shim ever sees the path (see the report's "prefix
    /// dispatch" finding); this proves it standalone, independent of
    /// DuckLake.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI and packaged extension; run via `cargo xtask e2e`"]
    fn moraine_prefix_attach_without_type_clause() {
        let dir = TempDir::new("prefix");
        // Never scanned by this test — placeholder stats are fine.
        seed(dir.path(), 0, 0);

        let output = Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!("ATTACH 'moraine:{}' AS m;", dir.path().display()))
            .arg("-c")
            .arg("SELECT database_name FROM duckdb_databases() WHERE database_name = 'm';")
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            output.status.success(),
            "moraine: prefix attach failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            csv_rows(&String::from_utf8_lossy(&output.stdout)),
            vec![vec!["m".to_string()]]
        );
    }

    /// The full `ducklake:moraine:` chain: attach, read through DuckLake's
    /// own reader, count (pushdown), time travel, `ducklake_snapshots()`.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_attach_reads_through_moraine_metadata() {
        let dir = TempDir::new("store");
        let data_dir = TempDir::new("data");
        // Written first: `seed` needs the real file's size/footer stats
        // (see `write_parquet`'s doc comment) to register a data file
        // DuckLake's own reader — not this crate's — can actually open.
        let (file_size_bytes, footer_size) = write_parquet(data_dir.path());
        seed(dir.path(), file_size_bytes, footer_size);
        let store = dir.path();
        let data_path = data_dir.path();

        let select = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT * FROM lake.main.t ORDER BY id;",
        ));
        assert_eq!(
            u64::try_from(select.len()).expect("row count fits u64"),
            ROW_COUNT
        );
        assert_eq!(select[0], vec!["0".to_string(), "0.0".to_string()]);
        assert_eq!(select[4], vec!["4".to_string(), "6.0".to_string()]);

        let count = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.t;",
        ));
        assert_eq!(count, vec![vec![ROW_COUNT.to_string()]]);

        let snapshots = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM ducklake_snapshots('lake');",
        ));
        assert_eq!(snapshots.len(), 1);

        // Time travel: `t` is created, gets its data file, and is renamed
        // all within bootstrap's snapshot 1 (`seed`'s one `commit` call);
        // snapshot 0 is bootstrap's own `main`-minting snapshot, before `t`
        // exists at all. `AT (VERSION => 1)` must see it; `AT (VERSION =>
        // 0)` must not — proving version-scoped resolution runs through
        // this shim's synthesized `ducklake_table`/`ducklake_snapshot`
        // rows, not just "whatever the head happens to be".
        let at_v1 = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT count(*) FROM lake.main.t AT (VERSION => 1);",
        ));
        assert_eq!(at_v1, vec![vec![ROW_COUNT.to_string()]]);

        let output = Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg(format!(
                "SET extension_directory='{}';",
                extension_directory().display()
            ))
            .arg("-c")
            .arg("INSTALL ducklake;")
            .arg("-c")
            .arg("LOAD ducklake;")
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!(
                "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');",
                store.display(),
                data_path.display()
            ))
            .arg("-c")
            .arg("SELECT count(*) FROM lake.main.t AT (VERSION => 0);")
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            !output.status.success(),
            "querying `t` AT (VERSION => 0), before it existed, unexpectedly succeeded"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("Catalog Error") || stderr.contains("does not exist"),
            "expected a catalog error naming the missing table; got: {stderr}"
        );
    }

    /// A helper mirroring `run_ducklake_sql` for the standalone
    /// metadata-only attach: reads the same store through this crate's own
    /// dump ABI + metadata-table scan, not DuckLake's reader — the
    /// independent verification surface for what the staged writes landed.
    fn run_standalone_sql(store_dir: &Path, sql: &str) -> String {
        let output = Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!("ATTACH 'moraine:{}' AS m;", store_dir.display()))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            output.status.success(),
            "standalone moraine: attach verification failed for `{sql}`:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
    }

    /// The staged-row write path, driven end to end by DuckLake's own SQL:
    ///
    /// - `CREATE TABLE` **completes** — its metadata batch (INSERT INTO
    ///   `ducklake_table`/`ducklake_column`/stats/`ducklake_snapshot`/
    ///   `ducklake_snapshot_changes`) translates through `PlanInsert` and
    ///   lands as one atomic staged commit. The batch stops short of any
    ///   `ducklake_inlined_data_tables` registration because the
    ///   synthesized `ducklake_metadata` serves
    ///   `data_inlining_row_limit = 0` (see `metadata_tables.cpp`): pinned
    ///   from the DuckLake source, `WriteNewInlinedTables` skips a table
    ///   whose `DataInliningRowLimit(...)` is 0, and that limit's only
    ///   inputs are catalog config options — with a default of 10, so
    ///   without the served row inlining is ON and CREATE TABLE demands a
    ///   table this catalog cannot store (discovered live in this test's
    ///   previous incarnation).
    /// - `ALTER TABLE ... RENAME TO` drives DuckLake's
    ///   `UPDATE ducklake_table SET end_snapshot = {SNAPSHOT_ID} WHERE
    ///   end_snapshot IS NULL AND table_id IN (...)` — the live proof of
    ///   `PlanUpdate`'s pinned sink-chunk layout (SET result column, rowid
    ///   last) and of the update-set-end lifecycle translation: the old
    ///   version must land in hist, the renamed one in cur.
    /// - `DROP TABLE` drives the same UPDATE convention for the drop.
    ///
    /// Every step is verified through two independent surfaces: DuckLake's
    /// own catalog in a fresh CLI session (fresh attach, fresh metadata
    /// read), and the standalone `moraine:` attach's row-faithful
    /// projections.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_create_rename_drop_round_trip_through_staged_writes() {
        let dir = TempDir::new("write-store");
        let data_dir = TempDir::new("write-data");
        // No fixture seed: bootstrap alone (an empty attach mints `main`)
        // is enough for a CREATE TABLE.
        let store = dir.path();
        let data_path = data_dir.path();

        // CREATE TABLE completes.
        run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.x (i BIGINT);");

        // A fresh session (fresh DuckLake attach re-reading the store) sees
        // the table DuckLake itself believes it created.
        let tables = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
        ));
        assert_eq!(tables, vec![vec!["x".to_string()]]);

        // Row-faithful check through the other surface: exactly one live
        // ducklake_table row, name `x`, its column typed in DuckLake's own
        // vocabulary.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT table_name, column_name, column_type FROM m.ducklake_table t \
             JOIN m.ducklake_column c USING (table_id) \
             WHERE t.end_snapshot IS NULL AND c.end_snapshot IS NULL;",
        ));
        assert_eq!(
            rows,
            vec![vec!["x".to_string(), "i".to_string(), "int64".to_string()]]
        );

        // RENAME: DuckLake ends the live ducklake_table row (UPDATE ... SET
        // end_snapshot) and inserts the renamed version.
        run_ducklake_sql(store, data_path, "ALTER TABLE lake.main.x RENAME TO y;");

        let tables = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
        ));
        assert_eq!(tables, vec![vec!["y".to_string()]]);

        // Lifecycle stitching, row-faithfully: one hist row `x` whose
        // end_snapshot equals the cur row `y`'s begin_snapshot, same
        // table_id.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT h.table_name, c.table_name, CAST(h.end_snapshot = c.begin_snapshot AS VARCHAR), \
                    CAST(h.table_id = c.table_id AS VARCHAR) \
             FROM m.ducklake_table h, m.ducklake_table c \
             WHERE h.end_snapshot IS NOT NULL AND c.end_snapshot IS NULL;",
        ));
        assert_eq!(
            rows,
            vec![vec![
                "x".to_string(),
                "y".to_string(),
                "true".to_string(),
                "true".to_string()
            ]]
        );

        // DROP: ends the remaining live version.
        run_ducklake_sql(store, data_path, "DROP TABLE lake.main.y;");

        let tables = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT name FROM (SHOW ALL TABLES) WHERE database = 'lake';",
        ));
        assert!(tables.is_empty(), "dropped table still listed: {tables:?}");

        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_table WHERE end_snapshot IS NULL;",
        ));
        assert_eq!(rows, vec![vec!["0".to_string()]]);
    }
}
