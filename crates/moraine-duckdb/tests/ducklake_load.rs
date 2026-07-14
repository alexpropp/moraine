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
//! MORAINE_DUCKDB_EXT=target/duckdb-cli/artifact/moraine_duckdb.duckdb_extension \
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

    /// As [`run_ducklake_sql`], with extra ATTACH options appended after
    /// `DATA_PATH` (e.g. `", ENCRYPTED, META_ENCRYPTED true"`).
    fn run_ducklake_sql_with_options(
        store_dir: &Path,
        data_path: &Path,
        attach_options: &str,
        sql: &str,
    ) -> String {
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
                "ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}'{attach_options});",
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
    /// (`DATE`/`TIME`/`TIMESTAMP`/`TIMESTAMPTZ`/`INTERVAL`), and `UUID`. A
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
             c_timestamp TIMESTAMP, c_timestamptz TIMESTAMPTZ, c_interval INTERVAL, c_uuid UUID);",
        );
        run_ducklake_sql(
            store,
            data_path,
            "INSERT INTO lake.main.t VALUES (\
             1, 2, 3, 4, 5, 6, 7, 8, 9, 1.5, 2.5, 12345.6789, 'hello', '\\x01\\x02'::BLOB, true, \
             DATE '2020-01-02', TIME '03:04:05', TIMESTAMP '2020-01-02 03:04:05', \
             TIMESTAMPTZ '2020-01-02 03:04:05+00', INTERVAL '1' MONTH, \
             '12345678-1234-5678-1234-567812345678'::UUID), \
             (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
             NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
        );

        // TIMESTAMPTZ renders in the session zone; pin UTC so it is stable.
        let select = "SET TimeZone='UTC'; \
             SELECT c_tinyint::VARCHAR, c_smallint::VARCHAR, c_integer::VARCHAR, \
             c_bigint::VARCHAR, c_hugeint::VARCHAR, c_utinyint::VARCHAR, c_usmallint::VARCHAR, \
             c_uinteger::VARCHAR, c_ubigint::VARCHAR, c_float::VARCHAR, c_double::VARCHAR, \
             c_decimal::VARCHAR, c_varchar, c_blob::VARCHAR, c_boolean::VARCHAR, c_date::VARCHAR, \
             c_time::VARCHAR, c_timestamp::VARCHAR, c_timestamptz::VARCHAR, c_interval::VARCHAR, \
             c_uuid::VARCHAR FROM lake.main.t ORDER BY c_bigint NULLS LAST;";
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
        ];
        let null_row = vec!["NULL"; 21];
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
}
