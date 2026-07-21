//! Drives a real, pinned DuckDB CLI + the `ducklake` extension against a
//! store pre-seeded through the `moraine` API, proving the whole nested
//! attach chain: `ATTACH 'ducklake:moraine:<dir>' AS lake (DATA_PATH
//! '<dir2>')` resolves DuckLake's metadata connection through this shim's
//! `moraine:` prefix dispatch and synthesized `ducklake_*` tables, and
//! DuckLake's own reader — not this crate's scan — serves the data back.
//!
//! Ignored by default: needs the downloaded DuckDB CLI, the packaged
//! `.duckdb_extension`, and network access to `INSTALL ducklake` (cached
//! under `target/duckdb-extensions/`, gitignored). Run manually after
//! `cargo xtask e2e` has produced the CLI/extension artifacts once:
//!
//! ```text
//! MORAINE_DUCKDB_CLI=target/duckdb-cli/cli/duckdb \
//! MORAINE_DUCKDB_EXT=build/release/extension/moraine/moraine.duckdb_extension \
//! cargo test -p moraine-duckdb --release --test ducklake_load -- --ignored
//! ```

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::{Path, PathBuf},
        process::Command,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use moraine::{Catalog, CatalogOptions, ColumnDef, DataFile, OptionScope};
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

    /// Cache root for `INSTALL ducklake`'s downloaded artifact, gitignored
    /// under `target/`.
    fn extension_directory() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/duckdb-extensions")
    }

    const ROW_COUNT: u64 = 5;

    /// Seeds a store via the `moraine` API: `main` from bootstrap (no
    /// explicit `create_schema` call), one table `t` with a relative-path
    /// data file, then a rename to give the table row history depth (two
    /// `ducklake_table` versions). `file_size_bytes`/`footer_size` must be
    /// the real Parquet file's stats: DuckLake's own reader uses the
    /// registered `footer_size` to seek to the file's metadata footer, so a
    /// placeholder `0` throws `Invalid Input Error: Invalid footer length`.
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
                            encryption_key: None,
                            column_stats: vec![],
                        },
                        &[],
                    )?;
                    // History depth: `t_old`'s current row ends, a history row is
                    // written, and a new current row for `t` begins.
                    tx.rename_table(t, "t")?;

                    Ok(())
                })
                .await
                .expect("test setup: commit fixtures");

            catalog.close().await.expect("test setup: close catalog");
        });
    }

    /// Writes `<data_path>/main/t_old/data.parquet`. Two path facts drive
    /// this:
    ///
    /// - DuckLake resolves a relative data-file path against `<DATA_PATH
    ///   from ATTACH>/<schema.path>/<table.path>/`, never against the
    ///   metadata store's own directory.
    /// - `table.path` is fixed at `CREATE TABLE` time (here, `t_old/`) and
    ///   is untouched by a later rename.
    ///
    /// Returns `(file_size_bytes, footer_size)` for the written file, per
    /// the Parquet spec's fixed trailer: the last 4 bytes are the magic
    /// `PAR1`, and the 4 bytes before that are the footer's thrift-encoded
    /// length as a little-endian `u32` — the same `footer_size` DuckLake
    /// registers when it authors a data file.
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
    /// Pinned single-threaded: DuckLake's catalog re-read after a rename is
    /// racy under multiple threads — a fresh attach sometimes returns an
    /// empty table list. The race reproduces with no moraine in the chain,
    /// so it is upstream; one thread closes it so these tests exercise
    /// moraine's translation, not DuckLake's cache concurrency.
    fn run_ducklake_sql(store_dir: &Path, data_path: &Path, sql: &str) -> String {
        run_ducklake_sql_with_options(store_dir, data_path, "", sql)
    }

    /// Runs `sql` against a fresh CLI + attach, returning the raw process
    /// output without asserting success — for statements expected to fail.
    fn run_ducklake_sql_output(
        store_dir: &Path,
        data_path: &Path,
        attach_options: &str,
        sql: &str,
    ) -> std::process::Output {
        Command::new(cli_path())
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
                "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}'{attach_options});",
                store_dir.display(),
                data_path.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI")
    }

    /// As [`run_ducklake_sql`], with extra ATTACH options appended after
    /// `DATA_PATH` (e.g. `", ENCRYPTED, META_ENCRYPTED true"`).
    fn run_ducklake_sql_with_options(
        store_dir: &Path,
        data_path: &Path,
        attach_options: &str,
        sql: &str,
    ) -> String {
        let output = run_ducklake_sql_output(store_dir, data_path, attach_options, sql);
        assert!(
            output.status.success(),
            "duckdb CLI failed for `{sql}`:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
    }

    /// As [`run_ducklake_sql`], but against a stock DuckLake catalog —
    /// DuckDB-file metadata at `<meta_dir>/meta.ducklake`, no moraine in
    /// the chain. The reference oracle for change-feed semantics: an
    /// identical statement stream mints identical snapshot ids and rowids
    /// on both catalogs, so outputs are comparable row-for-row.
    fn run_reference_ducklake_sql(meta_dir: &Path, data_path: &Path, sql: &str) -> String {
        let output = Command::new(cli_path())
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
            .arg(format!(
                "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}');",
                meta_dir.join("meta.ducklake").display(),
                data_path.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            output.status.success(),
            "reference duckdb CLI failed for `{sql}`:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
    }

    /// As [`run_ducklake_sql`], but for a statement that must fail:
    /// returns the CLI's combined output for the caller to assert on. The
    /// CLI exits nonzero when any statement errors; a success here means
    /// the statement unexpectedly worked.
    fn run_ducklake_sql_expect_err(store_dir: &Path, data_path: &Path, sql: &str) -> String {
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
            !output.status.success(),
            "`{sql}` unexpectedly succeeded:\nstdout: {}",
            String::from_utf8_lossy(&output.stdout),
        );
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    /// As [`run_reference_ducklake_sql`], but for a statement that must
    /// fail — the reference twin of [`run_ducklake_sql_expect_err`], so a
    /// refusal can be asserted on both catalogs.
    fn run_reference_ducklake_sql_expect_err(meta_dir: &Path, data_path: &Path, sql: &str) {
        let output = Command::new(cli_path())
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
            .arg(format!(
                "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}');",
                meta_dir.join("meta.ducklake").display(),
                data_path.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            !output.status.success(),
            "reference: `{sql}` unexpectedly succeeded:\nstdout: {}",
            String::from_utf8_lossy(&output.stdout),
        );
    }

    fn csv_rows(output: &str) -> Vec<Vec<String>> {
        output
            .lines()
            .skip(1)
            .filter(|line| !line.is_empty())
            .map(|line| line.split(',').map(str::to_owned).collect())
            .collect()
    }

    /// `moraine:` prefix dispatch (no `TYPE moraine` needed): DuckDB
    /// resolves the prefix before this shim ever sees the path. Proven
    /// standalone here, independent of DuckLake.
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
        // Written first: `seed` needs the real file's size/footer stats to
        // register a data file DuckLake's own reader can open.
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
        // all within `seed`'s one commit (snapshot 1); snapshot 0 is
        // bootstrap's own `main`-minting snapshot, before `t` exists at
        // all. `AT (VERSION => 1)` must see it; `AT (VERSION => 0)` must
        // not — proving version-scoped resolution runs through this shim's
        // synthesized `ducklake_table`/`ducklake_snapshot` rows.
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

    /// Mirrors `run_ducklake_sql` for the standalone metadata-only attach:
    /// reads the same store through this crate's own metadata-table scan,
    /// not DuckLake's reader — the independent verification surface for what
    /// the staged writes landed.
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

    /// Like [`run_standalone_sql`] but attaches `moraine:` **read-only**
    /// (`READ_ONLY`), so moraine opens a `DbReader` rather than the writer
    /// `Db`.
    fn run_standalone_read_only_sql(store_dir: &Path, sql: &str) -> String {
        let output = Command::new(cli_path())
            .arg("-unsigned")
            .arg("-csv")
            .arg("-c")
            .arg(format!("LOAD '{}';", ext_path().display()))
            .arg("-c")
            .arg(format!(
                "ATTACH 'moraine:{}' AS m (READ_ONLY);",
                store_dir.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            output.status.success(),
            "standalone read-only moraine: attach failed for `{sql}`:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
    }

    /// Like [`run_ducklake_sql`] but attaches the DuckLake chain **read-only**
    /// (`READ_ONLY` on the outer attach).
    fn run_ducklake_read_only_sql(store_dir: &Path, data_path: &Path, sql: &str) -> String {
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
                "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}', READ_ONLY);",
                store_dir.display(),
                data_path.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI");
        assert!(
            output.status.success(),
            "read-only ducklake attach failed for `{sql}`:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("duckdb CLI stdout is not UTF-8")
    }

    /// The staged-row write path, driven end to end by DuckLake's own SQL:
    ///
    /// - `CREATE TABLE` **completes** — its metadata INSERT batch
    ///   translates through `PlanInsert` and lands as one atomic staged
    ///   commit. Row inlining is on (the synthesized `ducklake_metadata`
    ///   serves `data_inlining_row_limit = 10`, DuckLake's default), so
    ///   `CREATE TABLE` also provisions the dynamic
    ///   `ducklake_inlined_data_<t>_<v>` entry this shim recognizes and
    ///   routes into the `inline/*` keyspace rather than materializing.
    /// - `ALTER TABLE ... RENAME TO` drives DuckLake's `UPDATE
    ///   ducklake_table SET end_snapshot ... WHERE end_snapshot IS NULL AND
    ///   table_id IN (...)` — the old version must land in history, the
    ///   renamed one in current.
    /// - `DROP TABLE` drives the same UPDATE convention for the drop.
    ///
    /// Every step is verified through two independent surfaces: DuckLake's
    /// own catalog in a fresh CLI session, and the standalone `moraine:`
    /// attach's row-faithful projections.
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

        // Lifecycle stitching, row-faithfully: one history row `x` whose
        // end_snapshot equals the current row `y`'s begin_snapshot, same
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

    /// Column/name mapping end to end: a Parquet file written by plain
    /// DuckDB `COPY` (no DuckLake field ids), one column short and sitting
    /// under a hive path, registers through `ducklake_add_data_files` with
    /// `hive_partitioning`. DuckLake writes the `ducklake_column_mapping` /
    /// `ducklake_name_mapping` rows (folded into one mapping record) and
    /// the file row carries its `mapping_id`; reads resolve the body
    /// column by name and the partition column from the path, time travel
    /// included; the standalone attach serves the row-faithful
    /// projections.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_add_data_files_maps_foreign_parquet() {
        let dir = TempDir::new("mapping-store");
        let data_dir = TempDir::new("mapping-data");
        let store = dir.path();
        let data_path = data_dir.path();

        let foreign_dir = data_path.join("foreign").join("region=east");
        std::fs::create_dir_all(&foreign_dir).expect("test setup: create hive dir");
        let foreign_file = foreign_dir.join("part.parquet");

        // Create, write the foreign file, register it.
        run_ducklake_sql(
            store,
            data_path,
            &format!(
                "CREATE TABLE lake.main.t (id BIGINT, region VARCHAR);\
                 COPY (SELECT range AS id FROM range(3)) TO '{f}' (FORMAT parquet);\
                 CALL ducklake_add_data_files('lake', 't', '{f}', hive_partitioning => true);",
                f = foreign_file.display(),
            ),
        );

        // A fresh attach re-reads the mapping from the store: the body
        // column resolves by name, the partition column from the path.
        let out = run_ducklake_sql(
            store,
            data_path,
            "SELECT id, region FROM lake.main.t ORDER BY id;",
        );
        assert_eq!(
            csv_rows(&out),
            vec![
                vec!["0".to_string(), "east".to_string()],
                vec!["1".to_string(), "east".to_string()],
                vec!["2".to_string(), "east".to_string()],
            ]
        );

        // A later write advances the head; the registered snapshot stays
        // readable through the mapping.
        let out = run_ducklake_sql(
            store,
            data_path,
            "USE lake;\
             SELECT CAST(max(snapshot_id) AS VARCHAR) FROM snapshots();",
        );
        let registered_snapshot = csv_rows(&out)[0][0].clone();
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.t VALUES (9, 'west');",
        );
        let out = run_ducklake_sql(
            store,
            data_path,
            &format!(
                "SELECT count(*), max(id) FROM lake.main.t AT (VERSION => {registered_snapshot});"
            ),
        );
        assert_eq!(csv_rows(&out), vec![vec!["3".to_string(), "2".to_string()]]);

        // Row-faithful projections through the standalone attach: one
        // mapping, map_by_name.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT mapping_id, table_id, type FROM m.ducklake_column_mapping;",
        ));
        assert_eq!(rows.len(), 1);
        let mapping_id = rows[0][0].clone();
        assert_eq!(rows[0][2], "map_by_name");

        // The name rows: `id` resolved from the file body, `region` a
        // hive-path virtual column; roots carry no parent.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT column_id, source_name, \
                    CAST(parent_column IS NULL AS VARCHAR), \
                    CAST(is_partition AS VARCHAR) \
             FROM m.ducklake_name_mapping ORDER BY column_id;",
        ));
        let sources: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r[1].as_str(), r[3].as_str()))
            .collect();
        assert!(sources.contains(&("id", "false")), "{rows:?}");
        assert!(sources.contains(&("region", "true")), "{rows:?}");
        assert!(rows.iter().all(|r| r[2] == "true"), "roots only: {rows:?}");

        // The registered file row carries the mapping id; nothing else
        // does (the inlined INSERT wrote no file).
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT CAST(mapping_id AS VARCHAR) FROM m.ducklake_data_file \
             WHERE mapping_id IS NOT NULL;",
        ));
        assert_eq!(rows, vec![vec![mapping_id]]);
    }

    /// Scalar and table macros end to end, driven entirely through
    /// DuckLake's own SQL: `CREATE MACRO` (arity overloads, a defaulted
    /// parameter, a table macro) folds its `ducklake_macro_impl` /
    /// `ducklake_macro_parameters` inserts into one macro record;
    /// `CREATE OR REPLACE` re-binds under a fresh `macro_id`;
    /// `DROP MACRO` ends the row; a `SNAPSHOT_VERSION` attach still calls
    /// the dropped definition; and the standalone attach serves the
    /// row-faithful `ducklake_macro*` projections.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    #[allow(clippy::too_many_lines)]
    fn ducklake_macros_round_trip_through_staged_writes() {
        let dir = TempDir::new("macro-store");
        let data_dir = TempDir::new("macro-data");
        let store = dir.path();
        let data_path = data_dir.path();

        // Create all three macros, then call every shape in one SELECT.
        let out = run_ducklake_sql(
            store,
            data_path,
            "USE lake;\
             CREATE MACRO add_num(a) AS a + 1, (a, b) AS a + b;\
             CREATE MACRO defaulted(a, b := 5) AS a + b;\
             CREATE MACRO pick(x) AS TABLE SELECT x AS v;\
             SELECT add_num(1), add_num(1, 2), defaulted(1), (SELECT v FROM pick(7));",
        );
        assert_eq!(
            csv_rows(&out),
            vec![vec![
                "2".to_string(),
                "3".to_string(),
                "6".to_string(),
                "7".to_string()
            ]]
        );

        // A fresh session re-reads the macros from the store and captures
        // the pre-replace snapshot for the time-travel attach below.
        let out = run_ducklake_sql(
            store,
            data_path,
            "USE lake;\
             SELECT CAST(max(snapshot_id) AS VARCHAR) || ':' || \
                    CAST(add_num(1, 2) AS VARCHAR) FROM snapshots();",
        );
        let combined = csv_rows(&out);
        let (pre_replace_snapshot, add_result) = combined[0][0]
            .trim_matches('"')
            .split_once(':')
            .expect("snapshot:result pair");
        assert_eq!(add_result, "3");
        let pre_replace_snapshot = pre_replace_snapshot.to_owned();

        // Replace re-binds under a fresh macro_id; drop removes the
        // defaulted macro from the live catalog set.
        let out = run_ducklake_sql(
            store,
            data_path,
            "USE lake;\
             CREATE OR REPLACE MACRO add_num(a) AS a + 10;\
             DROP MACRO defaulted;\
             SELECT add_num(1), \
                    (SELECT count(*) FROM duckdb_functions() \
                     WHERE database_name = 'lake' AND function_name = 'defaulted');",
        );
        assert_eq!(
            csv_rows(&out),
            vec![vec!["11".to_string(), "0".to_string()]]
        );

        // Time travel: at the pre-replace snapshot the old overloads and
        // the since-dropped macro both still bind and compute.
        let out = run_ducklake_sql_with_options(
            store,
            data_path,
            &format!(", SNAPSHOT_VERSION {pre_replace_snapshot}"),
            "USE lake; SELECT add_num(1), add_num(1, 2), defaulted(1);",
        );
        assert_eq!(
            csv_rows(&out),
            vec![vec!["2".to_string(), "3".to_string(), "6".to_string()]]
        );

        // Row-faithful projections through the standalone attach. Four
        // macro rows: the replaced and dropped ones ended, the replacement
        // and the table macro live.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT macro_name, CAST(end_snapshot IS NULL AS VARCHAR) \
             FROM m.ducklake_macro ORDER BY macro_id;",
        ));
        assert_eq!(
            rows,
            vec![
                vec!["add_num".to_string(), "false".to_string()],
                vec!["defaulted".to_string(), "false".to_string()],
                vec!["pick".to_string(), "true".to_string()],
                vec!["add_num".to_string(), "true".to_string()],
            ]
        );

        // Impl rows keep serving for ended macros (time travel reads
        // them); ordinals and types are verbatim.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT i.macro_id = m.macro_id, i.impl_id, i.type \
             FROM m.ducklake_macro_impl i \
             JOIN m.ducklake_macro m USING (macro_id) \
             WHERE m.macro_name = 'add_num' AND m.end_snapshot IS NOT NULL \
             ORDER BY i.impl_id;",
        ));
        assert_eq!(
            rows,
            vec![
                vec!["true".to_string(), "0".to_string(), "scalar".to_string()],
                vec!["true".to_string(), "1".to_string(), "scalar".to_string()],
            ]
        );
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT i.type FROM m.ducklake_macro_impl i \
             JOIN m.ducklake_macro m USING (macro_id) \
             WHERE m.macro_name = 'pick';",
        ));
        assert_eq!(rows, vec![vec!["table".to_string()]]);

        // The defaulted parameter row carries the default verbatim.
        let rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT p.column_id, p.parameter_name, p.default_value, p.default_value_type \
             FROM m.ducklake_macro_parameters p \
             JOIN m.ducklake_macro m USING (macro_id) \
             WHERE m.macro_name = 'defaulted' ORDER BY p.column_id;",
        ));
        assert_eq!(
            rows,
            vec![
                vec![
                    "0".to_string(),
                    "a".to_string(),
                    "NULL".to_string(),
                    "unknown".to_string()
                ],
                vec![
                    "1".to_string(),
                    "b".to_string(),
                    "5".to_string(),
                    "int32".to_string()
                ],
            ]
        );
    }

    /// Data inlining end to end, driven entirely through DuckLake's own
    /// SQL: small `INSERT`s land in the `inline/*` keyspace (never
    /// materialized as a real table) and read back through DuckLake's own
    /// inlined-data reader, not this crate's scan.
    ///
    /// - `INSERT` (two statements, two chunks) of mixed types (`BIGINT`,
    ///   `VARCHAR`, `DOUBLE`, `BOOLEAN`) and `NULL`s inlines; `SELECT`
    ///   returns every row with the right values and types.
    /// - `DELETE` of one row stages an `inline/inline_delete`; a follow-up `SELECT`
    ///   no longer sees it.
    /// - `CALL ducklake_flush_inlined_data('lake')` moves the remaining
    ///   rows to a real Parquet file; `SELECT` afterward is still correct
    ///   (now served by DuckLake's Parquet reader plus its delete-file join
    ///   for the pre-flush `DELETE`), and the standalone `moraine:`
    ///   attach's row-faithful projections confirm the `inline/insert` chunk
    ///   is gone (0 remaining rows in the now-empty
    ///   `ducklake_inlined_data_<t>_<v>` entry) and a `ducklake_data_file`
    ///   is registered.
    ///
    /// The full DuckLake scalar type matrix — every scalar moraine maps —
    /// created, inlined, and round-tripped live through DuckLake's own SQL,
    /// both before flush (served from the `inline/*` keyspace via Arrow IPC)
    /// and after (transcoded to Parquet and read by DuckLake's own reader).
    ///
    /// Covers every integer width (signed and unsigned), `FLOAT`/`DOUBLE`,
    /// `DECIMAL(w,s)` (width/scale preserved through the type round trip),
    /// `VARCHAR`/`BLOB`/`BOOLEAN`, the temporal types
    /// (`DATE`/`TIME`/`TIMESTAMP`/`TIMESTAMPTZ`/`INTERVAL`), `UUID`, and
    /// `JSON` (VARCHAR-backed, aliased — stored as DuckLake's `json`). A
    /// second all-`NULL` row proves null handling for each. The stored
    /// `ducklake_column.column_type` is checked in DuckLake's own vocabulary
    /// through the standalone projection, so a type that reads back but
    /// mis-names itself (a dropped `DECIMAL` suffix) is caught too.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    #[allow(clippy::too_many_lines)]
    fn ducklake_scalar_type_matrix_round_trip_through_flush() {
        let dir = TempDir::new("scalars-store");
        let data_dir = TempDir::new("scalars-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(
            store,
            data_path,
            "CREATE TABLE lake.main.t (\
             c_tinyint TINYINT, c_smallint SMALLINT, c_integer INTEGER, c_bigint BIGINT, \
             c_hugeint HUGEINT, c_utinyint UTINYINT, c_usmallint USMALLINT, c_uinteger UINTEGER, \
             c_ubigint UBIGINT, c_float FLOAT, c_double DOUBLE, c_decimal DECIMAL(18,4), \
             c_varchar VARCHAR, c_blob BLOB, c_boolean BOOLEAN, c_date DATE, c_time TIME, \
             c_timestamp TIMESTAMP, c_timestamptz TIMESTAMPTZ, c_interval INTERVAL, c_uuid UUID, \
             c_json JSON);",
        );
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.t VALUES (\
             1, 2, 3, 4, 5, 6, 7, 8, 9, 1.5, 2.5, 12345.6789, 'hello', '\\x01\\x02'::BLOB, true, \
             DATE '2020-01-02', TIME '03:04:05', TIMESTAMP '2020-01-02 03:04:05', \
             TIMESTAMPTZ '2020-01-02 03:04:05+00', INTERVAL '1' MONTH, \
             '12345678-1234-5678-1234-567812345678'::UUID, '[1]'::JSON), \
             (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
        );

        // TIMESTAMPTZ renders in the session zone; pin UTC so it is stable.
        let select = "SET TimeZone='UTC'; \
             SELECT c_tinyint::VARCHAR, c_smallint::VARCHAR, c_integer::VARCHAR, \
             c_bigint::VARCHAR, c_hugeint::VARCHAR, c_utinyint::VARCHAR, c_usmallint::VARCHAR, \
             c_uinteger::VARCHAR, c_ubigint::VARCHAR, c_float::VARCHAR, c_double::VARCHAR, \
             c_decimal::VARCHAR, c_varchar, c_blob::VARCHAR, c_boolean::VARCHAR, c_date::VARCHAR, \
             c_time::VARCHAR, c_timestamp::VARCHAR, c_timestamptz::VARCHAR, c_interval::VARCHAR, \
             c_uuid::VARCHAR, c_json::VARCHAR FROM lake.main.t ORDER BY c_bigint NULLS LAST;";
        let values_row = vec![
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "1.5",
            "2.5",
            "12345.6789",
            "hello",
            "\\x01\\x02",
            "true",
            "2020-01-02",
            "03:04:05",
            "2020-01-02 03:04:05",
            "2020-01-02 03:04:05+00",
            "1 month",
            "12345678-1234-5678-1234-567812345678",
            "[1]",
        ];
        let null_row = vec!["NULL"; 22];
        let want = vec![values_row.clone(), null_row.clone()];

        // Pre-flush: served from the inline keyspace, no Parquet file yet.
        assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
            )),
            vec![vec!["0".to_string()]]
        );

        // The stored type names round-trip in DuckLake's own vocabulary.
        // `decimal(18,4)` is checked separately below: its comma would split
        // under `csv_rows`, and it is the one type whose parameters must
        // survive the round trip.
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT column_type FROM m.ducklake_column WHERE end_snapshot IS NULL \
                 AND column_name <> 'c_decimal' ORDER BY column_order;",
            )),
            vec![
                vec!["int8"],
                vec!["int16"],
                vec!["int32"],
                vec!["int64"],
                vec!["int128"],
                vec!["uint8"],
                vec!["uint16"],
                vec!["uint32"],
                vec!["uint64"],
                vec!["float32"],
                vec!["float64"],
                vec!["varchar"],
                vec!["blob"],
                vec!["boolean"],
                vec!["date"],
                vec!["time"],
                vec!["timestamp"],
                vec!["timestamptz"],
                vec!["interval"],
                vec!["uuid"],
                vec!["json"],
            ]
        );
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT column_type = 'decimal(18,4)' FROM m.ducklake_column \
                 WHERE column_name = 'c_decimal' AND end_snapshot IS NULL;",
            )),
            vec![vec!["true".to_string()]]
        );

        // Post-flush: the same values, now read through DuckLake's Parquet
        // reader after the transcode.
        run_ducklake_sql(
            store,
            data_path,
            "CALL ducklake_flush_inlined_data('lake');",
        );
        assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);
    }

    /// As [`run_ducklake_sql`], but returns combined stdout+stderr without
    /// asserting success — for statements expected to raise a moraine error.
    fn run_ducklake_sql_capturing(store_dir: &Path, data_path: &Path, sql: &str) -> String {
        let output = Command::new(cli_path())
            .arg("-unsigned")
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
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    /// A `GEOMETRY` column round-trips through moraine + DuckLake with the
    /// `spatial` extension loaded: DuckDB's Arrow inline encoding supports
    /// geometry (spatial registers it), the stored `column_type` reads back as
    /// DuckLake's `geometry`, and values survive both the inline keyspace and
    /// the Parquet flush. A `NULL` row proves null handling.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake/spatial"]
    fn ducklake_geometry_round_trip_through_flush() {
        let dir = TempDir::new("geom-store");
        let data_dir = TempDir::new("geom-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(
            store,
            data_path,
            "INSTALL spatial; LOAD spatial; CREATE TABLE lake.main.g (id BIGINT, geom GEOMETRY);",
        );
        run_ducklake_sql(
            store,
            data_path,
            "LOAD spatial; INSERT INTO lake.main.g VALUES (1, ST_Point(1, 2)), (2, NULL);",
        );

        let select = "LOAD spatial; SELECT id::VARCHAR, coalesce(ST_AsText(geom), 'NULL') \
             FROM lake.main.g ORDER BY id;";
        let want = vec![vec!["1", "POINT (1 2)"], vec!["2", "NULL"]];

        // Pre-flush: served from the inline keyspace.
        assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);

        // The stored type name round-trips in DuckLake's vocabulary.
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT column_type FROM m.ducklake_column WHERE column_name = 'geom' \
                 AND end_snapshot IS NULL;",
            )),
            vec![vec!["geometry".to_string()]]
        );

        // Post-flush: read back through DuckLake's Parquet reader.
        run_ducklake_sql(
            store,
            data_path,
            "CALL ducklake_flush_inlined_data('lake');",
        );
        assert_eq!(csv_rows(&run_ducklake_sql(store, data_path, select)), want);
    }

    /// A `VARIANT` column is rejected with an actionable moraine error: its
    /// inline data is serialized through Arrow, and DuckDB's Arrow format has
    /// no VARIANT support (unlike GEOMETRY, which spatial registers). Vanilla
    /// DuckLake accepts VARIANT, so the error names the moraine-specific cause.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_variant_column_rejected_with_clear_error() {
        let dir = TempDir::new("variant-store");
        let data_dir = TempDir::new("variant-data");
        let combined = run_ducklake_sql_capturing(
            dir.path(),
            data_dir.path(),
            "CREATE TABLE lake.main.t (id BIGINT, v VARIANT);",
        );
        assert!(
            combined.contains("moraine")
                && combined.contains("VARIANT")
                && combined.contains("Arrow"),
            "expected an actionable moraine VARIANT error, got:\n{combined}"
        );
    }

    /// The extended scalar types DuckLake can name — `uint128` (UHUGEINT) and
    /// the sub-second / tz temporals (`timestamp_s`/`_ms`/`_ns`, `time_ns`,
    /// `timetz`) — map through moraine, so a table using them creates and its
    /// `ducklake_column.column_type` reads back in DuckLake's vocabulary. This
    /// is the metadata probe that previously failed with "unsupported DuckLake
    /// column type". `uint128` data round-trips exactly through the inline
    /// (Arrow) keyspace, so the data check here stays inline: once flushed,
    /// DuckDB's Parquet writer stores 128-bit integers — `int128` and
    /// `uint128` alike — as `DOUBLE`, losing precision beyond ~17 significant
    /// digits (a DuckDB limitation, not moraine's, unchanged by this mapping).
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_extended_scalar_types_map_through_probe() {
        let dir = TempDir::new("exttypes-store");
        let data_dir = TempDir::new("exttypes-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(
            store,
            data_path,
            "CREATE TABLE lake.main.t (\
             c_uint128 UHUGEINT, c_ts_s TIMESTAMP_S, c_ts_ms TIMESTAMP_MS, \
             c_ts_ns TIMESTAMP_NS, c_time_ns TIME_NS, c_timetz TIMETZ);",
        );

        // The stored type names round-trip in DuckLake's vocabulary — the probe
        // that regressed for each of these.
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT column_type FROM m.ducklake_column WHERE end_snapshot IS NULL \
                 ORDER BY column_order;",
            )),
            vec![
                vec!["uint128"],
                vec!["timestamp_s"],
                vec!["timestamp_ms"],
                vec!["timestamp_ns"],
                vec!["time_ns"],
                vec!["timetz"],
            ]
        );

        // uint128 data round-trips through the inline keyspace for values within
        // its Arrow (`DECIMAL(38,0)`) range.
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.t (c_uint128) VALUES (12345), (NULL);",
        );
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT coalesce(c_uint128::VARCHAR, 'NULL') FROM lake.main.t \
                 ORDER BY c_uint128 NULLS LAST;",
            )),
            vec![vec!["12345"], vec!["NULL"]],
        );
    }

    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_inline_data_round_trip_through_flush() {
        let dir = TempDir::new("inline-store");
        let data_dir = TempDir::new("inline-data");
        // No fixture seed: bootstrap alone (an empty attach mints `main`)
        // is enough for a CREATE TABLE; row inlining is on by default
        // (`data_inlining_row_limit = 10`), so these small inserts inline.
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(
            store,
            data_path,
            "CREATE TABLE lake.main.t (i BIGINT, s VARCHAR, d DOUBLE, b BOOLEAN);",
        );
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.t VALUES (1, 'a', 1.5, true), (2, NULL, NULL, false), \
             (3, 'c', 3.25, NULL);",
        );
        // A second statement is a second chunk: proves multi-chunk decode.
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.t VALUES (4, 'd', 4.5, true), (5, 'e', 5.5, false);",
        );

        let select = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT * FROM lake.main.t ORDER BY i;",
        ));
        assert_eq!(
            select,
            vec![
                vec!["1", "a", "1.5", "true"],
                vec!["2", "NULL", "NULL", "false"],
                vec!["3", "c", "3.25", "NULL"],
                vec!["4", "d", "4.5", "true"],
                vec!["5", "e", "5.5", "false"],
            ]
        );
        // Every inlined row is served through the dynamic
        // `ducklake_inlined_data_<t>_<v>` entry, not a real materialized
        // table: no Parquet file is registered yet.
        let pre_flush_files = csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        ));
        assert_eq!(pre_flush_files, vec![vec!["0".to_string()]]);

        run_ducklake_sql(store, data_path, "DELETE FROM lake.main.t WHERE i = 3;");
        let after_delete = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT i FROM lake.main.t ORDER BY i;",
        ));
        assert_eq!(
            after_delete,
            vec![vec!["1"], vec!["2"], vec!["4"], vec!["5"]]
        );

        run_ducklake_sql(
            store,
            data_path,
            "CALL ducklake_flush_inlined_data('lake');",
        );
        let after_flush = csv_rows(&run_ducklake_sql(
            store,
            data_path,
            "SELECT * FROM lake.main.t ORDER BY i;",
        ));
        assert_eq!(
            after_flush,
            vec![
                vec!["1", "a", "1.5", "true"],
                vec!["2", "NULL", "NULL", "false"],
                vec!["4", "d", "4.5", "true"],
                vec!["5", "e", "5.5", "false"],
            ]
        );

        // Row-faithful check through the standalone surface: the `t`
        // table's inline entry is drained (the flush's `inline/*` deletes
        // landed) and exactly one live `ducklake_data_file` now backs it.
        let table_id = csv_rows(&run_standalone_sql(
            store,
            "SELECT table_id FROM m.ducklake_table WHERE table_name = 't' AND end_snapshot IS NULL;",
        ));
        assert_eq!(table_id, vec![vec!["1".to_string()]]);
        let remaining_inline_rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_inlined_data_1_1;",
        ));
        assert_eq!(remaining_inline_rows, vec![vec!["0".to_string()]]);
        let post_flush_files = csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*), sum(record_count) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        ));
        assert_eq!(
            post_flush_files,
            vec![vec!["1".to_string(), "5".to_string()]]
        );
    }

    /// Nested user-column types (`LIST`, `STRUCT`, `MAP`) create, inline, and
    /// round-trip end to end. DuckLake stores a nested column as a marker
    /// row (`list`/`struct`/`map`) plus child `ducklake_column` rows; moraine
    /// stores those verbatim and passes the marker through its metadata
    /// projection, and the Arrow IPC inline path carries the values. Read
    /// back through scalar extractors so the comma-splitting `csv_rows`
    /// never sees a nested value's internal commas.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_inline_nested_types_round_trip_through_flush() {
        let dir = TempDir::new("inline-nested-store");
        let data_dir = TempDir::new("inline-nested-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(
            store,
            data_path,
            "CREATE TABLE lake.main.n \
             (id BIGINT, tags BIGINT[], pt STRUCT(x INTEGER, y INTEGER), mp MAP(VARCHAR, INTEGER));",
        );
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.n VALUES \
             (1, [10, 20, 30], {'x': 1, 'y': 2}, MAP {'a': 7}), \
             (2, [], {'x': 3, 'y': 4}, MAP {}), \
             (3, NULL, NULL, NULL);",
        );

        let extracted = "SELECT id, len(tags), tags[1], pt.x, pt.y, map_extract(mp, 'a')[1], cardinality(mp) \
                         FROM lake.main.n ORDER BY id;";
        let want = vec![
            vec!["1", "3", "10", "1", "2", "7", "1"],
            vec!["2", "0", "NULL", "3", "4", "NULL", "0"],
            vec!["3", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"],
        ];
        assert_eq!(
            csv_rows(&run_ducklake_sql(store, data_path, extracted)),
            want
        );

        // Served through the inline entry, not a materialized Parquet file.
        let pre_flush_files = csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file WHERE end_snapshot IS NULL;",
        ));
        assert_eq!(pre_flush_files, vec![vec!["0".to_string()]]);

        // Flush transcodes the inlined nested rows through the shim's decode
        // into Parquet; the read afterward is DuckLake's Parquet reader.
        run_ducklake_sql(
            store,
            data_path,
            "CALL ducklake_flush_inlined_data('lake');",
        );
        assert_eq!(
            csv_rows(&run_ducklake_sql(store, data_path, extracted)),
            want
        );

        let remaining_inline_rows = csv_rows(&run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_inlined_data_1_1;",
        ));
        assert_eq!(remaining_inline_rows, vec![vec!["0".to_string()]]);
    }

    /// Column-level schema evolution through DuckLake's own `ALTER TABLE`:
    /// ADD / RENAME / DROP COLUMN. DuckLake expresses each as
    /// `ducklake_column` version transitions over the staged-write path,
    /// so this exercises no dedicated schema-mutation path in
    /// moraine — the generic staged commit carries it. Verified through the
    /// standalone row-faithful projection (live columns, ordered) and
    /// DuckLake's own reflection in a fresh session.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_column_schema_evolution_through_staged_writes() {
        let dir = TempDir::new("evolve-store");
        let data_dir = TempDir::new("evolve-data");
        let store = dir.path();
        let data_path = data_dir.path();

        let live_columns = "SELECT column_name, column_type FROM m.ducklake_column \
                            WHERE end_snapshot IS NULL ORDER BY column_order;";

        run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
        run_ducklake_sql(
            store,
            data_path,
            "ALTER TABLE lake.main.t ADD COLUMN b VARCHAR;",
        );
        assert_eq!(
            csv_rows(&run_standalone_sql(store, live_columns)),
            vec![vec!["a", "int64"], vec!["b", "varchar"]]
        );
        // DuckLake's own reflection in a fresh attach agrees.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT column_name FROM (DESCRIBE lake.main.t) ORDER BY column_name;",
            )),
            vec![vec!["a"], vec!["b"]]
        );

        run_ducklake_sql(
            store,
            data_path,
            "ALTER TABLE lake.main.t RENAME COLUMN b TO c;",
        );
        assert_eq!(
            csv_rows(&run_standalone_sql(store, live_columns)),
            vec![vec!["a", "int64"], vec!["c", "varchar"]]
        );

        run_ducklake_sql(store, data_path, "ALTER TABLE lake.main.t DROP COLUMN c;");
        assert_eq!(
            csv_rows(&run_standalone_sql(store, live_columns)),
            vec![vec!["a", "int64"]]
        );

        // The evolved schema is functional: a fresh session inserts and reads.
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (42);");
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t;"
            )),
            vec![vec!["42"]]
        );
    }

    /// Type promotion through DuckLake's `ALTER TABLE ... ALTER COLUMN ...
    /// TYPE` — the remaining column-level op. The load-bearing
    /// case is data that predates the change: rows inlined under the old type
    /// live in their own schema version's chunk (decoded against that
    /// version's `inline/schema`), and must still read back — coerced to the
    /// new type by DuckLake — after the column is retyped and newer rows
    /// inline under the new version.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_column_type_promotion_over_inlined_data() {
        let dir = TempDir::new("promote-store");
        let data_dir = TempDir::new("promote-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a INTEGER);");
        // Inlined under the INTEGER (int32) schema version.
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (1), (2);");

        run_ducklake_sql(
            store,
            data_path,
            "ALTER TABLE lake.main.t ALTER COLUMN a TYPE BIGINT;",
        );
        // The retyped column is int64 in the live catalog projection.
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT column_type FROM m.ducklake_column WHERE end_snapshot IS NULL;",
            )),
            vec![vec!["int64"]]
        );
        // Rows inlined before the change still read, coerced to the new type.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t ORDER BY a;"
            )),
            vec![vec!["1"], vec!["2"]]
        );
        // New rows inline under the new version and coexist with the old.
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (3);");
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t ORDER BY a;"
            )),
            vec![vec!["1"], vec!["2"], vec!["3"]]
        );
    }

    /// Time travel through DuckLake's `AT (VERSION => N)`: a query at a past
    /// snapshot sees exactly that snapshot's data *and* schema. moraine adds
    /// no time-travel logic — it serves every `ducklake_*` row (current and
    /// history) row-faithfully with begin/end snapshots, and DuckLake filters
    /// by version in its own SQL, reconstructing the past schema from the
    /// `ducklake_column` versions moraine hands it. Each commit is one
    /// snapshot: 1 = CREATE, 2 = first INSERT, 3 = ADD COLUMN, 4 = second
    /// INSERT.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_time_travel_reads_past_data_and_schema() {
        let dir = TempDir::new("tt-store");
        let data_dir = TempDir::new("tt-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (1);");
        run_ducklake_sql(
            store,
            data_path,
            "ALTER TABLE lake.main.t ADD COLUMN b VARCHAR;",
        );
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (2, 'x');");

        // Present: both columns, both rows.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT * FROM lake.main.t ORDER BY a;"
            )),
            vec![vec!["1", "NULL"], vec!["2", "x"]]
        );
        // At v2 (after the first insert, before ADD COLUMN): schema is just
        // `a`, and only the first row exists.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT column_name FROM (DESCRIBE SELECT * FROM lake.main.t AT (VERSION => 2));",
            )),
            vec![vec!["a"]]
        );
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT * FROM lake.main.t AT (VERSION => 2) ORDER BY a;",
            )),
            vec![vec!["1"]]
        );
        // At v3 (after ADD COLUMN, before the second insert): both columns,
        // the pre-existing row back-filled with a NULL `b`.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT * FROM lake.main.t AT (VERSION => 3) ORDER BY a;",
            )),
            vec![vec!["1", "NULL"]]
        );
    }

    /// Time travel survives flush: rows inlined before a flush read back at a
    /// pre-flush version from the **backdated** Parquet file DuckLake writes
    /// (its `ducklake_data_file` record carries the minimum per-row snapshot),
    /// so a past-snapshot scan is served the Parquet with a per-row filter —
    /// never double-counted, never lost.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_time_travel_survives_flush() {
        let dir = TempDir::new("ttf-store");
        let data_dir = TempDir::new("ttf-data");
        let store = dir.path();
        let data_path = data_dir.path();

        run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (10);"); // v2
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (20);"); // v3
        run_ducklake_sql(
            store,
            data_path,
            "CALL ducklake_flush_inlined_data('lake');",
        );

        // Present: both rows, now served from Parquet.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t ORDER BY a;"
            )),
            vec![vec!["10"], vec!["20"]]
        );
        // Pre-flush versions still read the right subset, from the backdated file.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t AT (VERSION => 2) ORDER BY a;",
            )),
            vec![vec!["10"]]
        );
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t AT (VERSION => 3) ORDER BY a;",
            )),
            vec![vec!["10"], vec!["20"]]
        );
    }

    /// Read-only attach: `READ_ONLY` opens moraine's `DbReader`
    /// (never the writer `Db`, so it never fences a live writer), and reads
    /// flow through it end to end. The standalone `moraine: (READ_ONLY)`
    /// surface is the reference case — DuckDB sets the access mode directly
    /// from the flag and the shim reads it; a read-only DuckLake chain reads
    /// the committed data the same way a read-write one does. Write rejection
    /// and no-fencing are pinned by the core `tests/catalog.rs` suite, and
    /// DuckDB enforces the outer `READ_ONLY` at the SQL layer.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_read_only_attach_reads_through_a_reader() {
        let dir = TempDir::new("ro-store");
        let data_dir = TempDir::new("ro-data");
        let store = dir.path();
        let data_path = data_dir.path();

        // Seed through a read-write attach.
        run_ducklake_sql(store, data_path, "CREATE TABLE lake.main.t (a BIGINT);");
        run_ducklake_sql(store, data_path, "INSERT INTO lake.main.t VALUES (1), (2);");

        // Standalone read-only (moraine's DbReader) serves the metadata
        // projection — the reference case for the shim's access-mode wiring.
        assert_eq!(
            csv_rows(&run_standalone_read_only_sql(
                store,
                "SELECT count(*) FROM m.ducklake_table WHERE end_snapshot IS NULL;",
            )),
            vec![vec!["1"]]
        );

        // A read-only DuckLake chain reads the committed rows.
        assert_eq!(
            csv_rows(&run_ducklake_read_only_sql(
                store,
                data_path,
                "SELECT a FROM lake.main.t ORDER BY a;",
            )),
            vec![vec!["1"], vec!["2"]]
        );
    }

    /// Multi-statement, cross-table ACID transactions through DuckLake's own
    /// `BEGIN`/`COMMIT`/`ROLLBACK`, driven end to end.
    ///
    /// Every write a DuckDB transaction makes stages into one moraine
    /// staged tx (opened lazily on the first write, reused across every
    /// statement), committed atomically at `COMMIT` — so a transaction that
    /// writes two different tables mints exactly one `ducklake_snapshot`, and
    /// both tables' rows appear together or not at all.
    ///
    /// - `BEGIN; INSERT a; INSERT b; COMMIT;` across two tables lands both
    ///   rows and advances the snapshot by exactly one (not one per
    ///   statement) — the batching proof.
    /// - `BEGIN; INSERT; ROLLBACK;` discards the write and mints no snapshot.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_multi_statement_transaction_commits_atomically() {
        let dir = TempDir::new("tx-store");
        let data_dir = TempDir::new("tx-data");
        let store = dir.path();
        let data_path = data_dir.path();

        // Two tables to span in one transaction.
        run_ducklake_sql(
            store,
            data_path,
            "CREATE TABLE lake.main.accounts (id BIGINT); \
             CREATE TABLE lake.main.ledger (amount BIGINT);",
        );

        // The two CREATEs above minted snapshots 1 and 2 (bootstrap is 0):
        // head is now 2.
        let head_before = csv_rows(&run_standalone_sql(
            store,
            "SELECT max(snapshot_id) FROM m.ducklake_snapshot;",
        ));
        assert_eq!(head_before, vec![vec!["2".to_string()]]);

        // One transaction, two tables, two writes — committed atomically.
        run_ducklake_sql(
            store,
            data_path,
            "BEGIN; \
             INSERT INTO lake.main.accounts VALUES (1); \
             INSERT INTO lake.main.ledger VALUES (100); \
             COMMIT;",
        );

        // Both rows are visible: the transaction landed as a whole.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT (SELECT count(*) FROM lake.main.accounts), \
                        (SELECT count(*) FROM lake.main.ledger);",
            )),
            vec![vec!["1".to_string(), "1".to_string()]]
        );

        // The two writes advanced the head by exactly one snapshot: they
        // shared one moraine staged tx, not one per statement.
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT max(snapshot_id) FROM m.ducklake_snapshot;",
            )),
            vec![vec!["3".to_string()]]
        );

        // ROLLBACK discards the write and mints no snapshot.
        run_ducklake_sql(
            store,
            data_path,
            "BEGIN; \
             INSERT INTO lake.main.accounts VALUES (2); \
             ROLLBACK;",
        );
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                store,
                data_path,
                "SELECT count(*) FROM lake.main.accounts;",
            )),
            vec![vec!["1".to_string()]]
        );
        assert_eq!(
            csv_rows(&run_standalone_sql(
                store,
                "SELECT max(snapshot_id) FROM m.ducklake_snapshot;",
            )),
            vec![vec!["3".to_string()]]
        );
    }

    /// Every `.parquet` file under `dir`, recursively.
    fn parquet_files_under(dir: &Path) -> Vec<PathBuf> {
        let mut result = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(current) = stack.pop() {
            for entry in std::fs::read_dir(&current).expect("read data dir") {
                let path = entry.expect("read dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "parquet") {
                    result.push(path);
                }
            }
        }
        result
    }

    /// `FLUSH_INTERVAL_MS` end to end: `ATTACH (META_FLUSH_INTERVAL_MS
    /// 5)` → DuckLake's `META_` passthrough → this shim's inner attach →
    /// the store's WAL flush cadence. The setting is visible only as
    /// commit latency, so the assertion is that the option is accepted,
    /// commits land, and a plain re-attach (default cadence) reads them
    /// back.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_attach_flush_interval_option_is_applied() {
        let dir = TempDir::new("flush-store");
        let data_dir = TempDir::new("flush-data");

        run_ducklake_sql_with_options(
            dir.path(),
            data_dir.path(),
            ", META_FLUSH_INTERVAL_MS 5",
            "CREATE TABLE lake.main.t(id BIGINT); \
             INSERT INTO lake.main.t VALUES (1), (2);",
        );

        assert_eq!(
            csv_rows(&run_ducklake_sql(
                dir.path(),
                data_dir.path(),
                "SELECT count(*) FROM lake.main.t;",
            )),
            vec![vec!["2".to_string()]]
        );
    }

    /// `ENCRYPTED` end to end. The flag travels `ATTACH (ENCRYPTED,
    /// META_ENCRYPTED true)` → DuckLake's `META_` passthrough → this
    /// shim's inner attach → the store's creation-time flag, which the
    /// synthesized `ducklake_metadata` serves back. DuckLake then writes
    /// Parquet-encrypted data files and records their keys in catalog
    /// rows; a later plain attach adopts the stored flag and decrypts.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_encrypted_writes_encrypted_files_and_reads_back() {
        let dir = TempDir::new("enc-store");
        let data_dir = TempDir::new("enc-data");

        // Create and load in one encrypted session; 100 rows overflows
        // the inlining limit, forcing a real data file.
        run_ducklake_sql_with_options(
            dir.path(),
            data_dir.path(),
            ", ENCRYPTED, META_ENCRYPTED true",
            "CREATE TABLE lake.main.t(id BIGINT); \
             INSERT INTO lake.main.t SELECT range FROM range(100);",
        );

        // A plain re-attach adopts the stored flag and reads through
        // decryption.
        assert_eq!(
            csv_rows(&run_ducklake_sql(
                dir.path(),
                data_dir.path(),
                "SELECT count(*) FROM lake.main.t;",
            )),
            vec![vec!["100".to_string()]]
        );

        // The bytes at rest are not plaintext Parquet: an
        // encrypted-footer file does not end with the plaintext `PAR1`
        // trailer.
        let files = parquet_files_under(data_dir.path());
        assert!(
            !files.is_empty(),
            "no data files written under the data path"
        );
        for file in &files {
            let bytes = std::fs::read(file).expect("read data file");
            assert!(
                !bytes.ends_with(b"PAR1"),
                "{} is plaintext Parquet despite ENCRYPTED",
                file.display()
            );
        }

        // The catalog rows carry the stored flag and per-file key
        // material.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test setup: build tokio runtime");
        rt.block_on(async {
            let store =
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("open local store"));
            let catalog = Catalog::open(store, CatalogOptions::default())
                .await
                .expect("open catalog");
            let head = catalog.snapshot().await.expect("snapshot");
            assert_eq!(
                head.option(OptionScope::Global, "encrypted").as_deref(),
                Some("true")
            );

            let schema = head.schema_by_name("main").expect("main schema");
            let table = head.table_by_name(schema.id, "t").expect("table t");
            let data_files = head.data_files_of(table.id);
            assert!(!data_files.is_empty());
            for file in &data_files {
                assert!(
                    file.encryption_key
                        .as_deref()
                        .is_some_and(|k| !k.is_empty()),
                    "data file {} carries no encryption key",
                    file.path
                );
            }
            catalog.close().await.expect("close catalog");
        });
    }

    /// Partitioning end to end: `SET PARTITIONED BY` lands a spec through
    /// the staged-row path, inserted files carry the spec id and their
    /// partition values, DuckLake's planner prunes by the served values,
    /// repartitioning ends the old spec while files keep the spec they
    /// were written under, and `RESET PARTITIONED BY` clears it.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_partitioning_specs_values_and_pruning() {
        let dir = TempDir::new("partition-store");
        let data_dir = TempDir::new("partition-data");
        let (store, data) = (dir.path(), data_dir.path());

        run_ducklake_sql(
            store,
            data,
            "CREATE TABLE lake.main.events(part_key INTEGER, ts TIMESTAMP, v VARCHAR);",
        );
        run_ducklake_sql(
            store,
            data,
            "ALTER TABLE lake.main.events SET PARTITIONED BY (part_key);",
        );

        // 100 rows exceeds the inlining limit, so this lands as real
        // Parquet, split by partition value.
        run_ducklake_sql(
            store,
            data,
            "INSERT INTO lake.main.events \
             SELECT i % 2, TIMESTAMP '2024-06-01 00:00:00', concat('v', i) FROM range(100) t(i);",
        );

        // One live spec with one identity column on part_key (field id 1).
        let live_specs = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_partition_info WHERE end_snapshot IS NULL;",
        );
        assert_eq!(csv_rows(&live_specs), vec![vec!["1".to_string()]]);
        let spec_columns = run_standalone_sql(
            store,
            "SELECT partition_key_index, column_id FROM m.ducklake_partition_column;",
        );
        assert_eq!(
            csv_rows(&spec_columns),
            vec![vec!["0".to_string(), "1".to_string()]]
        );

        // Every live file names the spec and carries one value per spec
        // column; two distinct partition values exist.
        let files = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file \
             WHERE end_snapshot IS NULL AND partition_id IS NOT NULL;",
        );
        let file_count: u64 = csv_rows(&files)[0][0].parse().expect("file count");
        assert!(
            file_count >= 2,
            "expected at least one file per partition value, got {file_count}"
        );
        let values = run_standalone_sql(
            store,
            "SELECT count(DISTINCT partition_value) FROM m.ducklake_file_partition_value;",
        );
        assert_eq!(csv_rows(&values), vec![vec!["2".to_string()]]);

        // DuckLake's planner prunes by the served values.
        let plan = run_ducklake_sql(
            store,
            data,
            "EXPLAIN ANALYZE SELECT count(*) FROM lake.main.events WHERE part_key = 1;",
        );
        assert!(plan.contains("Total Files Read: 1"), "not pruned:\n{plan}");

        // Repartition: the old spec ends, files keep the spec they were
        // written under.
        run_ducklake_sql(
            store,
            data,
            "ALTER TABLE lake.main.events SET PARTITIONED BY (year(ts));",
        );
        let ended = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_partition_info WHERE end_snapshot IS NOT NULL;",
        );
        assert_eq!(csv_rows(&ended), vec![vec!["1".to_string()]]);
        let stale_files = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_data_file \
             WHERE end_snapshot IS NULL AND partition_id IS NOT NULL;",
        );
        assert_eq!(
            csv_rows(&stale_files)[0][0].parse::<u64>().expect("count"),
            file_count
        );

        // Clear: DuckLake writes RESET PARTITIONED BY as a set-to-empty —
        // the old spec ends and a new spec with zero columns lands live.
        run_ducklake_sql(
            store,
            data,
            "ALTER TABLE lake.main.events RESET PARTITIONED BY;",
        );
        let cleared = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_partition_info WHERE end_snapshot IS NULL;",
        );
        assert_eq!(csv_rows(&cleared), vec![vec!["1".to_string()]]);
        let cleared_columns = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_partition_column pc \
             JOIN m.ducklake_partition_info pi USING (partition_id) \
             WHERE pi.end_snapshot IS NULL;",
        );
        assert_eq!(csv_rows(&cleared_columns), vec![vec!["0".to_string()]]);

        // Time travel still reads data written under the first spec
        // (snapshots: 1 = CREATE, 2 = SET PARTITIONED BY, 3 = INSERT).
        let travel = csv_rows(&run_ducklake_sql(
            store,
            data,
            "SELECT count(*) FROM lake.main.events AT (VERSION => 3);",
        ));
        assert_eq!(travel, vec![vec!["100".to_string()]]);
    }

    /// Sorting end to end: `SET SORTED BY` lands a spec whose expression,
    /// direction, and null order are stored verbatim; inserts under a
    /// live spec succeed (DuckLake's writer sorts — moraine only serves
    /// the spec); changing the spec ends the old one; `RESET SORTED BY`
    /// clears it; and dropping a table with historical sort specs is
    /// clean.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_sort_specs_round_trip_and_reset() {
        let dir = TempDir::new("sort-store");
        let data_dir = TempDir::new("sort-data");
        let (store, data) = (dir.path(), data_dir.path());

        run_ducklake_sql(
            store,
            data,
            "CREATE TABLE lake.main.items(k INTEGER, v VARCHAR);",
        );
        run_ducklake_sql(
            store,
            data,
            "ALTER TABLE lake.main.items SET SORTED BY (v DESC NULLS FIRST);",
        );

        // Direction and null order stored verbatim.
        let expressions = run_standalone_sql(
            store,
            "SELECT sort_key_index, sort_direction, null_order FROM m.ducklake_sort_expression;",
        );
        assert_eq!(
            csv_rows(&expressions),
            vec![vec![
                "0".to_string(),
                "DESC".to_string(),
                "NULLS_FIRST".to_string()
            ]]
        );

        // Inserts under a live spec succeed.
        run_ducklake_sql(
            store,
            data,
            "INSERT INTO lake.main.items SELECT i, concat('v', i % 7) FROM range(100) t(i);",
        );
        let count = csv_rows(&run_ducklake_sql(
            store,
            data,
            "SELECT count(*) FROM lake.main.items;",
        ));
        assert_eq!(count, vec![vec!["100".to_string()]]);

        // Change ends the old spec; reset ends the replacement.
        run_ducklake_sql(
            store,
            data,
            "ALTER TABLE lake.main.items SET SORTED BY (k ASC);",
        );
        run_ducklake_sql(store, data, "ALTER TABLE lake.main.items RESET SORTED BY;");
        let live = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_sort_info WHERE end_snapshot IS NULL;",
        );
        assert_eq!(csv_rows(&live), vec![vec!["0".to_string()]]);
        let ended = run_standalone_sql(
            store,
            "SELECT count(*) FROM m.ducklake_sort_info WHERE end_snapshot IS NOT NULL;",
        );
        assert_eq!(csv_rows(&ended), vec![vec!["2".to_string()]]);

        // DROP TABLE with historical sort specs is clean.
        run_ducklake_sql(store, data, "DROP TABLE lake.main.items;");
    }

    /// The change data feed over inlined (unflushed) data, differential
    /// against a stock DuckLake catalog fed the identical statements:
    /// insert attribution to the minting snapshot with stable rowids,
    /// inline deletes, `UPDATE` pairing `update_preimage`/`update_postimage`
    /// on one rowid, sub-range and full-range queries, agreement of
    /// `ducklake_table_insertions`/`_deletions` with `ducklake_table_changes`,
    /// and the `TIMESTAMPTZ` bound form agreeing with the version form.
    /// moraine adds no feed logic — DuckLake computes everything from the
    /// served projections; the reference catalog is the semantic oracle.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    #[allow(clippy::too_many_lines)]
    fn ducklake_change_feed_attributes_inline_changes() {
        let store = TempDir::new("cdf-store");
        let data = TempDir::new("cdf-data");
        let reference_meta = TempDir::new("cdf-ref-meta");
        let reference_data = TempDir::new("cdf-ref-data");

        // Runs the same statements on both catalogs, discarding output.
        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        // Runs the same probe on both catalogs, asserts moraine matches
        // the reference, and returns the shared rows.
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };

        // One snapshot per autocommitted statement: 1 create, 2 insert
        // rowids 0-1, 3 insert rowid 2, 4 delete rowid 1, 5 update rowid 0.
        apply(
            "CREATE TABLE lake.main.t (i BIGINT, v VARCHAR);\
             INSERT INTO lake.main.t VALUES (1, 'a'), (2, 'b');\
             INSERT INTO lake.main.t VALUES (3, 'c');\
             DELETE FROM lake.main.t WHERE i = 2;\
             UPDATE lake.main.t SET v = 'z' WHERE i = 1;",
        );

        let changes = |start: &str, end: &str| {
            format!(
                "SELECT snapshot_id, rowid, change_type, i, v \
                 FROM ducklake_table_changes('lake', 'main', 't', {start}, {end}) \
                 ORDER BY snapshot_id, rowid, change_type;"
            )
        };

        // Insert-only sub-range: each row attributed to its minting
        // snapshot, rowids stable.
        assert_eq!(
            probe(&changes("2", "3")),
            vec![
                vec!["2", "0", "insert", "1", "a"],
                vec!["2", "1", "insert", "2", "b"],
                vec!["3", "2", "insert", "3", "c"],
            ]
        );

        // Delete + update sub-range: the delete carries the deleted row's
        // values; the update pairs pre/postimage on one rowid in one
        // snapshot.
        assert_eq!(
            probe(&changes("4", "5")),
            vec![
                vec!["4", "1", "delete", "2", "b"],
                vec!["5", "0", "update_postimage", "1", "z"],
                vec!["5", "0", "update_preimage", "1", "a"],
            ]
        );

        // Full range: the update pair coexists with rowid 0's original
        // insert — separate events, not merged.
        let full_range = probe(&changes("0", "5"));
        assert_eq!(
            full_range,
            vec![
                vec!["2", "0", "insert", "1", "a"],
                vec!["2", "1", "insert", "2", "b"],
                vec!["3", "2", "insert", "3", "c"],
                vec!["4", "1", "delete", "2", "b"],
                vec!["5", "0", "update_postimage", "1", "z"],
                vec!["5", "0", "update_preimage", "1", "a"],
            ]
        );

        // The convenience functions are exactly the corresponding subsets.
        assert_eq!(
            probe(
                "SELECT snapshot_id, rowid, i, v \
                 FROM ducklake_table_insertions('lake', 'main', 't', 2, 3) \
                 ORDER BY snapshot_id, rowid;"
            ),
            vec![
                vec!["2", "0", "1", "a"],
                vec!["2", "1", "2", "b"],
                vec!["3", "2", "3", "c"],
            ]
        );
        assert_eq!(
            probe(
                "SELECT snapshot_id, rowid, i, v \
                 FROM ducklake_table_deletions('lake', 'main', 't', 4, 4) \
                 ORDER BY snapshot_id, rowid;"
            ),
            vec![vec!["4", "1", "2", "b"]]
        );

        // Timestamp bounds resolve through each catalog's own served
        // snapshot times (wall clocks differ, so this is per catalog, not
        // differential): the full time window equals the full version
        // range. Inclusive bounds make the window unambiguous even if
        // neighboring snapshots share a timestamp.
        for run in [
            &(|sql: &str| run_ducklake_sql(store.path(), data.path(), sql))
                as &dyn Fn(&str) -> String,
            &(|sql: &str| {
                run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql)
            }),
        ] {
            let window = csv_rows(&run("SET TimeZone='UTC'; \
                 SELECT min(snapshot_time)::VARCHAR, max(snapshot_time)::VARCHAR \
                 FROM ducklake_snapshots('lake');"));
            let by_time = csv_rows(&run(&format!(
                "SET TimeZone='UTC'; \
                 SELECT snapshot_id, rowid, change_type, i, v \
                 FROM ducklake_table_changes('lake', 'main', 't', \
                 TIMESTAMPTZ '{}', TIMESTAMPTZ '{}') \
                 ORDER BY snapshot_id, rowid, change_type;",
                window[0][0], window[0][1]
            )));
            assert_eq!(by_time, full_range);
        }
    }

    /// The change data feed across `ducklake_flush_inlined_data` and over
    /// file-based deletes, differential against a stock DuckLake catalog:
    /// a range spanning the flush answers identically before and after it
    /// (flushed files are backdated; `partial_max` filters per row), the
    /// flush snapshot itself contributes nothing, and post-flush the three
    /// delete flavors — delete file, a later delete superseding it (the
    /// previous-delete subtraction), and update pairing — attribute
    /// correctly.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    #[allow(clippy::too_many_lines)]
    fn ducklake_change_feed_survives_flush_and_file_deletes() {
        let store = TempDir::new("cdf-flush-store");
        let data = TempDir::new("cdf-flush-data");
        let reference_meta = TempDir::new("cdf-flush-ref-meta");
        let reference_data = TempDir::new("cdf-flush-ref-data");

        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };
        let changes = |start: &str, end: &str| {
            format!(
                "SELECT snapshot_id, rowid, change_type, i, v \
                 FROM ducklake_table_changes('lake', 'main', 't', {start}, {end}) \
                 ORDER BY snapshot_id, rowid, change_type;"
            )
        };

        // 1 create, 2 insert rowids 0-1, 3 insert rowids 2-3, 4 delete
        // rowid 1 — all inlined.
        apply(
            "CREATE TABLE lake.main.t (i BIGINT, v VARCHAR);\
             INSERT INTO lake.main.t VALUES (1, 'a'), (2, 'b');\
             INSERT INTO lake.main.t VALUES (3, 'c'), (4, 'd');\
             DELETE FROM lake.main.t WHERE i = 2;",
        );

        // Owned so it can be compared against post-flush output and
        // extended below; the literal pins the expected content.
        let pre_flush_history = probe(&changes("1", "4"));
        assert_eq!(
            pre_flush_history,
            vec![
                vec!["2", "0", "insert", "1", "a"],
                vec!["2", "1", "insert", "2", "b"],
                vec!["3", "2", "insert", "3", "c"],
                vec!["3", "3", "insert", "4", "d"],
                vec!["4", "1", "delete", "2", "b"],
            ]
        );
        let pre_flush_middle = probe(&changes("3", "3"));
        assert_eq!(
            pre_flush_middle,
            vec![
                vec!["3", "2", "insert", "3", "c"],
                vec!["3", "3", "insert", "4", "d"],
            ]
        );

        // Flush mints snapshot 5; the same ranges must answer identically
        // (files backdated, per-row filtering through partial_max), and
        // the flush snapshot contributes no rows.
        apply("CALL ducklake_flush_inlined_data('lake');");
        assert_eq!(probe(&changes("1", "4")), pre_flush_history);
        assert_eq!(probe(&changes("3", "3")), pre_flush_middle);
        assert_eq!(probe(&changes("5", "5")), Vec::<Vec<String>>::new());

        // Post-flush file operations: 6 update rowid 2, 7 delete rowid 3,
        // 8 delete the remaining rowids 0 and 2. Snapshot 8's delete state
        // supersedes 7's on the same data file, so [8, 8] must not
        // re-report rowid 3.
        apply(
            "UPDATE lake.main.t SET v = 'z' WHERE i = 3;\
             DELETE FROM lake.main.t WHERE i = 4;\
             DELETE FROM lake.main.t;",
        );

        assert_eq!(
            probe(&changes("6", "6")),
            vec![
                vec!["6", "2", "update_postimage", "3", "z"],
                vec!["6", "2", "update_preimage", "3", "c"],
            ]
        );
        assert_eq!(
            probe(&changes("7", "7")),
            vec![vec!["7", "3", "delete", "4", "d"]]
        );
        assert_eq!(
            probe(&changes("8", "8")),
            vec![
                vec!["8", "0", "delete", "1", "a"],
                vec!["8", "2", "delete", "3", "z"],
            ]
        );

        // Full history stays coherent across the flush boundary, and the
        // deletions function agrees with the feed's delete subset.
        let mut full = pre_flush_history.clone();
        full.extend(probe(&changes("6", "8")));
        assert_eq!(probe(&changes("1", "8")), full);
        assert_eq!(
            probe(
                "SELECT snapshot_id, rowid, i, v \
                 FROM ducklake_table_deletions('lake', 'main', 't', 6, 8) \
                 ORDER BY snapshot_id, rowid;"
            ),
            vec![
                vec!["6", "2", "3", "c"],
                vec!["7", "3", "4", "d"],
                vec!["8", "0", "1", "a"],
                vec!["8", "2", "3", "z"],
            ]
        );
    }

    /// Table and column comments, differential against a stock DuckLake
    /// catalog fed the identical statements: `COMMENT ON` lands
    /// `ducklake_tag` / `ducklake_column_tag` rows through the staged-row
    /// path, DuckLake reads them back, a re-comment ends the old entry
    /// and inserts the new one, the served rows carry both lifecycles
    /// row-for-row identical to the reference, and comments survive a
    /// column rename.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_table_and_column_comments_round_trip() {
        let store = TempDir::new("tags-store");
        let data = TempDir::new("tags-data");
        let reference_meta = TempDir::new("tags-ref-meta");
        let reference_data = TempDir::new("tags-ref-data");

        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };

        apply(
            "CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);\
             COMMENT ON TABLE lake.main.t IS 'first';\
             COMMENT ON COLUMN lake.main.t.a IS 'col comment';",
        );
        assert_eq!(
            probe("SELECT comment FROM duckdb_tables() WHERE table_name = 't';"),
            vec![vec!["first".to_string()]]
        );
        assert_eq!(
            probe(
                "SELECT comment FROM duckdb_columns() \
                 WHERE table_name = 't' AND column_name = 'a';"
            ),
            vec![vec!["col comment".to_string()]]
        );

        // A re-comment is a set-end + insert pair: the old entry ends,
        // the new one lands live, both rows lifecycle-identical to stock.
        apply("COMMENT ON TABLE lake.main.t IS 'second';");
        assert_eq!(
            probe("SELECT comment FROM duckdb_tables() WHERE table_name = 't';"),
            vec![vec!["second".to_string()]]
        );
        assert_eq!(
            probe(
                "SELECT object_id, begin_snapshot, end_snapshot, key, value \
                 FROM __ducklake_metadata_lake.ducklake_tag ORDER BY begin_snapshot;"
            ),
            vec![
                vec![
                    "1".to_string(),
                    "2".to_string(),
                    "4".to_string(),
                    "comment".to_string(),
                    "first".to_string(),
                ],
                vec![
                    "1".to_string(),
                    "4".to_string(),
                    "NULL".to_string(),
                    "comment".to_string(),
                    "second".to_string(),
                ],
            ]
        );
        assert_eq!(
            probe(
                "SELECT table_id, column_id, begin_snapshot, end_snapshot, key, value \
                 FROM __ducklake_metadata_lake.ducklake_column_tag;"
            ),
            vec![vec![
                "1".to_string(),
                "1".to_string(),
                "3".to_string(),
                "NULL".to_string(),
                "comment".to_string(),
                "col comment".to_string(),
            ]]
        );

        // Column comments survive a column rename (a version transition
        // carries the entries forward), identically to stock.
        apply("ALTER TABLE lake.main.t RENAME COLUMN a TO a2;");
        assert_eq!(
            probe(
                "SELECT comment FROM duckdb_columns() \
                 WHERE table_name = 't' AND column_name = 'a2';"
            ),
            vec![vec!["col comment".to_string()]]
        );
    }

    /// Snapshot expiry and file cleanup, differential against a stock
    /// DuckLake catalog fed the identical statements: a dropped table's
    /// snapshots expire (all but head), its rows vanish from every
    /// metadata table identically to stock, its Parquet lands on the
    /// deletion schedule with the bytes intact, and
    /// `ducklake_cleanup_old_files` then deletes the bytes and drains the
    /// schedule on both catalogs.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_expire_and_cleanup_reclaims_files() {
        let store = TempDir::new("expire-store");
        let data = TempDir::new("expire-data");
        let reference_meta = TempDir::new("expire-ref-meta");
        let reference_data = TempDir::new("expire-ref-data");

        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };

        apply(
            "CREATE TABLE lake.main.t(a BIGINT);\
             INSERT INTO lake.main.t SELECT i FROM range(100) t(i);",
        );
        assert_eq!(parquet_files_under(data.path()).len(), 1);
        assert_eq!(parquet_files_under(reference_data.path()).len(), 1);
        apply("DROP TABLE lake.main.t;");

        // Expire everything below head: snapshots 1 (create) and 2
        // (insert) go; 3 (drop) survives. The dropped table's whole row
        // set is now dead, and both catalogs agree on the aftermath.
        apply("CALL ducklake_expire_snapshots('lake', older_than => now());");
        assert_eq!(
            probe("SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot;"),
            vec![vec!["3".to_string()]]
        );
        assert_eq!(
            probe(
                "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_table UNION ALL \
                 SELECT count(*) FROM __ducklake_metadata_lake.ducklake_column UNION ALL \
                 SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file UNION ALL \
                 SELECT count(*) FROM __ducklake_metadata_lake.ducklake_table_stats;"
            ),
            vec![
                vec!["0".to_string()],
                vec!["0".to_string()],
                vec!["0".to_string()],
                vec!["0".to_string()],
            ]
        );

        // Logical expiry deletes no bytes: the Parquet is scheduled, not
        // gone (paths carry catalog-unique names, so counts compare).
        assert_eq!(
            probe(
                "SELECT count(*), bool_and(path_is_relative) \
                 FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
            ),
            vec![vec!["1".to_string(), "true".to_string()]]
        );
        assert_eq!(parquet_files_under(data.path()).len(), 1);
        assert_eq!(parquet_files_under(reference_data.path()).len(), 1);

        // Time travel below the horizon no longer resolves — on either.
        run_ducklake_sql_expect_err(
            store.path(),
            data.path(),
            "SELECT count(*) FROM lake.main.t AT (VERSION => 2);",
        );
        run_reference_ducklake_sql_expect_err(
            reference_meta.path(),
            reference_data.path(),
            "SELECT count(*) FROM lake.main.t AT (VERSION => 2);",
        );

        apply("CALL ducklake_cleanup_old_files('lake', cleanup_all => true);");
        assert!(parquet_files_under(data.path()).is_empty());
        assert!(parquet_files_under(reference_data.path()).is_empty());
        assert_eq!(
            probe(
                "SELECT count(*) \
                 FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
            ),
            vec![vec!["0".to_string()]]
        );
    }

    /// Orphaned-file deletion, differential against a stock DuckLake
    /// catalog: a stray Parquet no catalog row ever referenced is deleted
    /// on both, while every catalogued file survives and both catalogs
    /// still answer identically.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_delete_orphaned_files_ignores_catalogued_paths() {
        let store = TempDir::new("orphan-store");
        let data = TempDir::new("orphan-data");
        let reference_meta = TempDir::new("orphan-ref-meta");
        let reference_data = TempDir::new("orphan-ref-data");

        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };

        apply(
            "CREATE TABLE lake.main.t(a BIGINT);\
             INSERT INTO lake.main.t SELECT i FROM range(100) t(i);",
        );
        let catalogued = parquet_files_under(data.path());
        assert_eq!(catalogued.len(), 1);

        // Plant a stray file under each table's data prefix: never
        // catalogued, so nothing references it.
        for base in [data.path(), reference_data.path()] {
            std::fs::write(
                base.join("main").join("t").join("stray.parquet"),
                b"not parquet",
            )
            .expect("plant stray file");
        }

        apply("CALL ducklake_delete_orphaned_files('lake', cleanup_all => true);");

        assert_eq!(parquet_files_under(data.path()), catalogued);
        assert_eq!(parquet_files_under(reference_data.path()).len(), 1);
        assert!(
            !data
                .path()
                .join("main")
                .join("t")
                .join("stray.parquet")
                .exists()
        );
        assert!(
            !reference_data
                .path()
                .join("main")
                .join("t")
                .join("stray.parquet")
                .exists()
        );
        assert_eq!(
            probe("SELECT count(*) FROM lake.main.t;"),
            vec![vec!["100".to_string()]]
        );
    }

    /// Merge compaction, differential against a stock DuckLake catalog
    /// fed the identical statements: three small files merge into one,
    /// rows and row ids are identical to the reference before and after,
    /// time travel to a pre-merge snapshot still answers pre-merge, the
    /// sources land on the deletion schedule (bytes intact until
    /// cleanup), `next_row_id` is untouched, and an UPDATE after the
    /// merge still hits the right row on both catalogs (lineage held).
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    #[allow(clippy::too_many_lines)]
    fn ducklake_merge_adjacent_files_preserves_rows_and_time_travel() {
        let store = TempDir::new("merge-store");
        let data = TempDir::new("merge-data");
        let reference_meta = TempDir::new("merge-ref-meta");
        let reference_data = TempDir::new("merge-ref-data");

        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };

        apply("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
        for batch in 0..3 {
            apply(&format!(
                "INSERT INTO lake.main.t \
                 SELECT i + {}, concat('v', i) FROM range(100) t(i);",
                batch * 100
            ));
        }
        assert_eq!(
            probe(
                "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
                 WHERE end_snapshot IS NULL;"
            ),
            vec![vec!["3".to_string()]]
        );
        let rows_before = probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;");
        let next_row_id_before =
            probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;");
        let pre_merge = probe("SELECT count(*) FROM lake.main.t AT (VERSION => 3);");

        apply("CALL ducklake_merge_adjacent_files('lake');");

        assert_eq!(
            probe(
                "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
                 WHERE end_snapshot IS NULL;"
            ),
            vec![vec!["1".to_string()]]
        );
        assert_eq!(
            probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;"),
            rows_before,
            "rows and row ids must survive the merge"
        );
        assert_eq!(
            probe("SELECT next_row_id FROM __ducklake_metadata_lake.ducklake_table_stats;"),
            next_row_id_before,
            "compaction never allocates row ids"
        );

        // The sources are scheduled, bytes intact until cleanup.
        assert_eq!(
            probe(
                "SELECT count(*) \
                 FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
            ),
            vec![vec!["3".to_string()]]
        );
        assert_eq!(parquet_files_under(data.path()).len(), 4);
        assert_eq!(parquet_files_under(reference_data.path()).len(), 4);

        // Time travel to a pre-merge snapshot answers exactly as before.
        assert_eq!(
            probe("SELECT count(*) FROM lake.main.t AT (VERSION => 3);"),
            pre_merge
        );

        // Row lineage holds through the merge.
        apply("UPDATE lake.main.t SET b = 'updated' WHERE a = 150;");
        assert_eq!(
            probe("SELECT b FROM lake.main.t WHERE a = 150;"),
            vec![vec!["updated".to_string()]]
        );

        apply("CALL ducklake_cleanup_old_files('lake', cleanup_all => true);");
        assert_eq!(
            probe(
                "SELECT count(*) \
                 FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion;"
            ),
            vec![vec!["0".to_string()]]
        );
        assert_eq!(
            probe("SELECT count(*) FROM lake.main.t;"),
            vec![vec!["300".to_string()]]
        );
    }

    /// Delete-rewrite compaction, differential against a stock DuckLake
    /// catalog fed the identical statements: after a DELETE, the rewrite
    /// leaves one live data file and no live delete file, survivors keep
    /// their row ids row-for-row with the reference, and time travel to
    /// the pre-rewrite snapshot still shows the deleted rows.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn ducklake_rewrite_data_files_materializes_deletes() {
        let store = TempDir::new("rewrite-store");
        let data = TempDir::new("rewrite-data");
        let reference_meta = TempDir::new("rewrite-ref-meta");
        let reference_data = TempDir::new("rewrite-ref-data");

        let apply = |sql: &str| {
            run_ducklake_sql(store.path(), data.path(), sql);
            run_reference_ducklake_sql(reference_meta.path(), reference_data.path(), sql);
        };
        let probe = |sql: &str| -> Vec<Vec<String>> {
            let moraine_rows = csv_rows(&run_ducklake_sql(store.path(), data.path(), sql));
            let reference_rows = csv_rows(&run_reference_ducklake_sql(
                reference_meta.path(),
                reference_data.path(),
                sql,
            ));
            assert_eq!(
                moraine_rows, reference_rows,
                "moraine diverges from stock DuckLake for `{sql}`"
            );
            moraine_rows
        };

        apply(
            "CREATE TABLE lake.main.t(a BIGINT);\
             INSERT INTO lake.main.t SELECT i FROM range(100) t(i);\
             DELETE FROM lake.main.t WHERE a % 2 = 0;",
        );
        assert_eq!(
            probe(
                "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file \
                 WHERE end_snapshot IS NULL;"
            ),
            vec![vec!["1".to_string()]]
        );
        let survivors_before = probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;");

        apply("CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.1);");

        assert_eq!(
            probe(
                "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file \
                 WHERE end_snapshot IS NULL;"
            ),
            vec![vec!["1".to_string()]]
        );
        assert_eq!(
            probe(
                "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file \
                 WHERE end_snapshot IS NULL;"
            ),
            vec![vec!["0".to_string()]],
            "the rewrite consumes the delete file"
        );
        assert_eq!(
            probe("SELECT rowid, a FROM lake.main.t ORDER BY rowid;"),
            survivors_before,
            "survivors keep their row ids"
        );

        // The ended rows stay in history: time travel to the pre-delete
        // snapshot still sees all 100 rows.
        assert_eq!(
            probe("SELECT count(*) FROM lake.main.t AT (VERSION => 2);"),
            vec![vec!["100".to_string()]]
        );
    }

    /// The equality-index SQL surface end to end: `moraine_index_create`
    /// backfills an index over existing data by scoped-reading the table's
    /// Parquet from `DATA_PATH` (autonomous commit), `moraine_indexes` lists it,
    /// `moraine_index_lookup` resolves a value to its row, and
    /// `moraine_index_drop` removes it — each through a fresh attach, so the
    /// definition and entries round-trip through the persisted store.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_functions_create_list_lookup_and_drop() {
        let store = TempDir::new("index-fns-store");
        let data = TempDir::new("index-fns-data");
        // DATA_PATH is declared once, at attach: DuckLake's own `DATA_PATH`
        // plus `META_DATA_PATH` (the passthrough that reaches moraine), so the
        // index functions source it from the handle — no per-call argument.
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

        // A 100-row insert writes a Parquet data file under DATA_PATH.
        run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
        run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");

        // Create a unique index over the existing rows — backfilled by the
        // scoped read of the DATA_PATH files, no DATA_PATH argument.
        run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

        let listed = csv_rows(&run(
            "SELECT index_name, is_unique FROM moraine_indexes('lake', 'main', 't');",
        ));
        assert_eq!(
            listed,
            vec![vec!["by_a".to_string(), "true".to_string()]],
            "the created unique index is listed"
        );

        // A value that exists resolves to exactly one row; one that does not
        // resolves to none.
        let hit = csv_rows(&run("SELECT count(*) FROM \
             moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"));
        assert_eq!(hit, vec![vec!["1".to_string()]], "value 42 is indexed");
        let miss = csv_rows(&run("SELECT count(*) FROM \
             moraine_index_lookup('lake', 'main', 't', 'by_a', 9999);"));
        assert_eq!(miss, vec![vec!["0".to_string()]], "value 9999 is absent");

        run("CALL moraine_index_drop('lake', 'main', 't', 'by_a');");
        let after = csv_rows(&run(
            "SELECT count(*) FROM moraine_indexes('lake', 'main', 't');",
        ));
        assert_eq!(
            after,
            vec![vec!["0".to_string()]],
            "the index is gone after drop"
        );
    }

    /// Write-path coverage: a bulk INSERT *after* the index exists is
    /// maintained by the staged commit scoped-reading the new Parquet from
    /// `DATA_PATH`, and a duplicate INSERT is rejected on the unique index.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_maintained_on_bulk_insert() {
        let store = TempDir::new("index-maint-store");
        let data = TempDir::new("index-maint-data");
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

        run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
        run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
        run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

        // A bulk INSERT after create is maintained by the staged commit.
        run("INSERT INTO lake.main.t SELECT i, 'y' FROM range(100, 200) t(i);");
        let post = csv_rows(&run("SELECT count(*) FROM \
             moraine_index_lookup('lake', 'main', 't', 'by_a', 150);"));
        assert_eq!(
            post,
            vec![vec!["1".to_string()]],
            "value 150 from the post-create INSERT is indexed"
        );
        // The backfilled rows are still resolvable too.
        let pre = csv_rows(&run("SELECT count(*) FROM \
             moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"));
        assert_eq!(
            pre,
            vec![vec!["1".to_string()]],
            "the backfilled value 42 is indexed"
        );

        // A small INSERT (one row, under the 10-row inline limit) is inlined
        // as an Arrow chunk, not a Parquet file, and is maintained too.
        run("INSERT INTO lake.main.t VALUES (500, 'z');");
        let inline = csv_rows(&run("SELECT count(*) FROM \
             moraine_index_lookup('lake', 'main', 't', 'by_a', 500);"));
        assert_eq!(
            inline,
            vec![vec!["1".to_string()]],
            "the inlined value 500 is indexed"
        );

        // Duplicates are rejected on both write paths: a bulk (Parquet) INSERT
        // and a small (inline) INSERT of an already-indexed value.
        let reject = |sql: &str| {
            let out = run_ducklake_sql_output(store.path(), data.path(), &meta, sql);
            assert!(
                !out.status.success(),
                "`{sql}` should fail; stdout: {}",
                String::from_utf8_lossy(&out.stdout)
            );
            let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
            assert!(
                stderr.contains("duplicate value violates equality index"),
                "expected the equality-index constraint error for `{sql}`, got: {stderr}"
            );
            // The message must avoid DuckLake's retry substrings, or its
            // commit loop spins instead of surfacing the error.
            for retry_word in ["conflict", "concurrent", "unique", "primary key"] {
                assert!(
                    !stderr.contains(retry_word),
                    "the constraint message must not contain `{retry_word}`: {stderr}"
                );
            }
        };
        reject("INSERT INTO lake.main.t SELECT i, 'dup' FROM range(20) t(i);");
        reject("INSERT INTO lake.main.t VALUES (500, 'dup');");
    }

    /// Delete-path coverage: entries are live-only, so a DELETE frees the
    /// value it killed and the same key can be written again. Covers both
    /// residences — an inlined row and a row in a flushed Parquet file —
    /// and the replace-in-one-transaction shape a writer depends on.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_entries_are_removed_by_delete() {
        let store = TempDir::new("index-delete-store");
        let data = TempDir::new("index-delete-data");
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
        let count = |sql: &str| csv_rows(&run(sql));

        run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
        run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
        run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

        // A row in the flushed Parquet file: deleting it frees its value.
        run("DELETE FROM lake.main.t WHERE a = 42;");
        assert_eq!(
            count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"),
            vec![vec!["0".to_string()]],
            "the deleted row's entry is gone"
        );
        run("INSERT INTO lake.main.t VALUES (42, 'again');");
        assert_eq!(
            count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 42);"),
            vec![vec!["1".to_string()]],
            "the freed value is insertable again"
        );

        // A small INSERT stays inline; deleting it frees its value too.
        run("INSERT INTO lake.main.t VALUES (500, 'z');");
        run("DELETE FROM lake.main.t WHERE a = 500;");
        assert_eq!(
            count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 500);"),
            vec![vec!["0".to_string()]],
            "the inlined row's entry is gone"
        );
        run("INSERT INTO lake.main.t VALUES (500, 'again');");
        assert_eq!(
            count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 500);"),
            vec![vec!["1".to_string()]],
            "the freed inlined value is insertable again"
        );

        // The replace shape: delete and reinsert one key in one transaction.
        run("BEGIN; DELETE FROM lake.main.t WHERE a = 7; \
             INSERT INTO lake.main.t VALUES (7, 'replaced'); COMMIT;");
        assert_eq!(
            count("SELECT b FROM lake.main.t WHERE a = 7;"),
            vec![vec!["replaced".to_string()]],
            "the replacement row is the live one"
        );
        assert_eq!(
            count("SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', 7);"),
            vec![vec!["1".to_string()]],
            "the replaced key resolves to exactly one row"
        );

        // Every lookup still resolves to a row that exists: no entry outlives
        // its row, which is what a non-unique leak would show as.
        assert_eq!(
            count("SELECT count(*) FROM lake.main.t;"),
            vec![vec!["101".to_string()]],
            "100 original rows, plus the extra key 500; every delete was reinserted"
        );
    }

    /// Runs `sql` against a fresh CLI attaching the moraine lake with **no**
    /// data-path option at all — DuckLake must read the data root from the
    /// `data_path` metadata moraine serves. Returns the raw output.
    fn run_ducklake_sql_bare(store_dir: &Path, sql: &str) -> std::process::Output {
        Command::new(cli_path())
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
                "ATTACH 'ducklake:moraine:{}' AS lake;",
                store_dir.display()
            ))
            .arg("-c")
            .arg(sql)
            .output()
            .expect("failed to spawn duckdb CLI")
    }

    /// The data root, given once at creation via `META_DATA_PATH`, is
    /// persisted at bootstrap and served back — so later attaches maintain
    /// the index with `DATA_PATH` alone, and even with no data-path option at
    /// all (DuckLake reads the root moraine serves).
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_data_path_persists_across_attach() {
        let store = TempDir::new("index-persist-store");
        let data = TempDir::new("index-persist-data");
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());

        // Creation: DATA_PATH + META_DATA_PATH seeds and persists the root.
        let create =
            |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
        create("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
        create("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
        create("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

        // Re-attach with DATA_PATH only (no META_DATA_PATH): a bulk INSERT
        // (a Parquet file, not inline) is maintained by scoped-reading it from
        // the data store moraine resolves off the *recorded* root — the value
        // never came over this attach.
        let dp = |sql: &str| run_ducklake_sql(store.path(), data.path(), sql);
        dp("INSERT INTO lake.main.t SELECT i, 'y' FROM range(100, 120) t(i);");
        assert_eq!(
            csv_rows(&dp("SELECT count(*) FROM \
                 moraine_index_lookup('lake','main','t','by_a',110);")),
            vec![vec!["1".to_string()]],
            "a bulk value indexed through a DATA_PATH-only attach is found"
        );
        let dup_dp = run_ducklake_sql_output(
            store.path(),
            data.path(),
            "",
            "INSERT INTO lake.main.t SELECT i, 'dup' FROM range(100, 120) t(i);",
        );
        assert!(
            !dup_dp.status.success(),
            "a bulk duplicate is rejected on a DATA_PATH-only attach; stdout: {}",
            String::from_utf8_lossy(&dup_dp.stdout)
        );

        // Re-attach with no data-path option: DuckLake reads the served root,
        // and maintenance still works.
        let bare_count = run_ducklake_sql_bare(store.path(), "SELECT count(*) FROM lake.main.t;");
        assert!(
            bare_count.status.success(),
            "a bare attach reads the served data root; stderr: {}",
            String::from_utf8_lossy(&bare_count.stderr)
        );
        assert_eq!(
            csv_rows(&String::from_utf8_lossy(&bare_count.stdout)),
            vec![vec!["120".to_string()]],
            "the lake is readable through a bare attach (100 seed + 20 bulk)"
        );
        let dup_bare =
            run_ducklake_sql_bare(store.path(), "INSERT INTO lake.main.t VALUES (110, 'z');");
        assert!(
            !dup_bare.status.success(),
            "a duplicate is rejected on a bare attach; stdout: {}",
            String::from_utf8_lossy(&dup_bare.stdout)
        );
    }

    /// A catalog name that is not a moraine-backed lake is refused with a
    /// clear error, not a crash — the handle downcast behind the index
    /// functions is unchecked, so the kind is verified first.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_functions_reject_a_non_moraine_catalog() {
        let store = TempDir::new("index-badcat-store");
        let data = TempDir::new("index-badcat-data");
        // `memory` is DuckDB's built-in in-memory catalog — present, but not
        // a moraine catalog, and not a DuckLake lake either.
        let out = run_ducklake_sql_output(
            store.path(),
            data.path(),
            "",
            "CALL moraine_index_create('memory', 'main', 't', 'by_a', ['a'], true);",
        );
        assert!(
            !out.status.success(),
            "a non-moraine catalog must be refused; stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        // The clean error proves it did not crash: a SIGSEGV would carry no
        // such message.
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("not a moraine-backed lake"),
            "expected a clear rejection, got: {stderr}"
        );
    }

    /// A unique index over a `UUID` column works across write paths and is
    /// queryable by UUID: a UUID written to a Parquet file backfills and
    /// looks up, and an inline duplicate of it is rejected — proving the
    /// inline Arrow encoding and the Parquet encoding derive the same key.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_uuid_across_paths_and_lookup() {
        const KNOWN: &str = "550e8400-e29b-41d4-a716-446655440000";
        let store = TempDir::new("index-uuid-store");
        let data = TempDir::new("index-uuid-data");
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

        run("CREATE TABLE lake.main.t(id UUID, name VARCHAR);");
        // A bulk insert (>10 rows → a Parquet file) that includes the known
        // UUID at row 0, the rest random.
        run(&format!(
            "INSERT INTO lake.main.t SELECT \
                 CASE WHEN i = 0 THEN '{KNOWN}'::UUID ELSE gen_random_uuid() END, 'x' \
             FROM range(20) t(i);"
        ));
        run("CALL moraine_index_create('lake', 'main', 't', 'by_id', ['id'], true);");

        // The Parquet-stored UUID is found by an equality lookup.
        let hit = csv_rows(&run(&format!(
            "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_id', '{KNOWN}'::UUID);"
        )));
        assert_eq!(
            hit,
            vec![vec!["1".to_string()]],
            "the UUID is indexed and found"
        );
        let miss = csv_rows(&run(
            "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_id', \
                 '00000000-0000-0000-0000-000000000000'::UUID);",
        ));
        assert_eq!(
            miss,
            vec![vec!["0".to_string()]],
            "an absent UUID resolves to none"
        );

        // An inline (single-row) INSERT of the same UUID is rejected: inline
        // maintenance derives the same 16-byte key the Parquet path stored.
        let dup = run_ducklake_sql_output(
            store.path(),
            data.path(),
            &meta,
            &format!("INSERT INTO lake.main.t VALUES ('{KNOWN}'::UUID, 'dup');"),
        );
        assert!(
            !dup.status.success(),
            "a cross-path UUID duplicate must be rejected; stdout: {}",
            String::from_utf8_lossy(&dup.stdout)
        );
    }

    /// A unique index over a `TIMESTAMP_MS` column: the scoped read must read
    /// the millisecond array (not misread it as microseconds), and the inline
    /// path must derive the same millisecond count — so backfill succeeds and
    /// a cross-path duplicate is rejected.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_millisecond_timestamp() {
        let store = TempDir::new("index-ts-store");
        let data = TempDir::new("index-ts-data");
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());
        let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);

        run("CREATE TABLE lake.main.t(ts TIMESTAMP_MS, name VARCHAR);");
        // A bulk insert (>10 rows → a Parquet file) of 20 distinct sub-second
        // millisecond timestamps, row 0 at the epoch base.
        run("INSERT INTO lake.main.t SELECT \
                 '2023-01-01 00:00:00'::TIMESTAMP_MS + to_milliseconds(i * 137), 'x' \
             FROM range(20) t(i);");
        // Before the fix this failed: the scoped read downcast the millisecond
        // column as microseconds.
        run("CALL moraine_index_create('lake', 'main', 't', 'by_ts', ['ts'], true);");

        // An inline (single-row) INSERT duplicating row 0's timestamp is
        // rejected: the inline path derives the same millisecond key.
        let dup = run_ducklake_sql_output(
            store.path(),
            data.path(),
            &meta,
            "INSERT INTO lake.main.t VALUES ('2023-01-01 00:00:00'::TIMESTAMP_MS, 'dup');",
        );
        assert!(
            !dup.status.success(),
            "a cross-path millisecond-timestamp duplicate must be rejected; stdout: {}",
            String::from_utf8_lossy(&dup.stdout)
        );
    }

    /// A `HUGEINT` column is refused at index creation with a clear reason:
    /// DuckDB stores it as a lossy double in Parquet, so it cannot be a
    /// faithful equality index (a silently wrong one would be worse).
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_rejects_hugeint() {
        let store = TempDir::new("index-hugeint-store");
        let data = TempDir::new("index-hugeint-data");
        let meta = format!(", META_DATA_PATH '{}'", data.path().display());
        run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &meta,
            "CREATE TABLE lake.main.t(big HUGEINT, name VARCHAR);",
        );
        let out = run_ducklake_sql_output(
            store.path(),
            data.path(),
            &meta,
            "CALL moraine_index_create('lake', 'main', 't', 'by_big', ['big'], true);",
        );
        assert!(
            !out.status.success(),
            "a HUGEINT index must be refused; stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
        assert!(
            stderr.contains("lossy double"),
            "expected the lossy-double reason, got: {stderr}"
        );
    }

    /// With no data-path store resolvable (a lake attached with neither a
    /// recorded nor a supplied data path), an index can still be created on
    /// an empty table — but a later bulk INSERT is refused rather than
    /// silently leaving the index under-covered.
    #[test]
    #[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
    fn moraine_index_bulk_insert_without_a_data_store_is_refused() {
        let store = TempDir::new("index-nostore-store");
        let data = TempDir::new("index-nostore-data");
        // DATA_PATH only (no META_DATA_PATH) on a fresh lake: moraine records
        // and resolves no data store.
        let run = |sql: &str| run_ducklake_sql(store.path(), data.path(), sql);

        run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
        // An index on the still-empty table needs no scoped read, so it is
        // created fine.
        run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

        // A bulk INSERT registers a Parquet file that would need scoped-reading
        // to maintain the index; with no store, the commit is refused.
        let out = run_ducklake_sql_output(
            store.path(),
            data.path(),
            "",
            "INSERT INTO lake.main.t SELECT i, 'x' FROM range(20) t(i);",
        );
        assert!(
            !out.status.success(),
            "a bulk insert with no data store must be refused; stdout: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
        assert!(
            stderr.contains("no data-path store"),
            "expected the missing-store reason, got: {stderr}"
        );
    }
}
