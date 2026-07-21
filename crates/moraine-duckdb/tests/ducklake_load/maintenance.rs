use crate::helpers::*;

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
