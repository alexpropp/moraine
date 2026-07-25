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

/// `moraine_maintenance` with nothing configured at attach is a
/// moraine-only pass: every DuckLake step reports `skipped`, the
/// orphaned-index sweep runs, and no data moves.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_without_configuration_runs_only_the_sweep() {
    let store = TempDir::new("maint-bare-store");
    let data = TempDir::new("maint-bare-data");

    let rows = csv_rows(&run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(20) t(i);\
         SELECT step, status FROM moraine_maintenance('lake');",
    ));
    let by_step: std::collections::HashMap<_, _> = rows
        .iter()
        .map(|row| (row[0].as_str(), row[1].as_str()))
        .collect();

    for step in [
        "expire_snapshots",
        "flush_inlined_data",
        "merge_adjacent_files",
        "rewrite_data_files",
        "cleanup_old_files",
        "delete_orphaned_files",
    ] {
        assert_eq!(
            by_step.get(step),
            Some(&"skipped"),
            "unconfigured `{step}` must not run: {rows:?}"
        );
    }
    assert_eq!(by_step.get("sweep_indexes"), Some(&"ran"), "{rows:?}");

    // Nothing the pass did is observable in the data.
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            "SELECT count(*), sum(a) FROM lake.main.t;"
        )),
        vec![vec!["20".to_string(), "190".to_string()]]
    );
}

/// The sweep reclaims a dropped index's entries and nothing else: a live
/// index is untouched, the drop orphans its range, and the next pass
/// reports exactly that range reclaimed. A second pass finds nothing.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_sweeps_a_dropped_index() {
    let store = TempDir::new("maint-sweep-store");
    let data = TempDir::new("maint-sweep-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    let detail = |sql: &str| -> String {
        let rows = csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &meta,
            sql,
        ));
        rows.into_iter()
            .next()
            .map(|row| row[0].clone())
            .unwrap_or_default()
    };

    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(25) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);",
    );

    // A live index is spared.
    assert!(
        detail("SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_indexes';")
            .contains("0 entries"),
        "a live index must not be swept"
    );

    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "SELECT * FROM moraine_index_drop('lake','main','t','by_a');",
    );

    let swept =
        detail("SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_indexes';");
    assert!(
        swept.contains("25 entries") && swept.contains("1 dropped index"),
        "expected the whole range reclaimed, got: {swept}"
    );

    // Idempotent: the range is gone.
    assert!(
        detail("SELECT detail FROM moraine_maintenance('lake') WHERE step = 'sweep_indexes';")
            .contains("0 entries"),
        "a second pass must find nothing"
    );
}

/// A configured pass runs DuckLake's own functions in sequence order and
/// leaves the lake's contents unchanged.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_runs_configured_ducklake_steps_in_order() {
    let store = TempDir::new("maint-full-store");
    let data = TempDir::new("maint-full-data");
    let options = format!(
        ", META_DATA_PATH '{}', META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN now(), \
         META_MAINTENANCE_FLUSH_INLINED_DATA true, META_MAINTENANCE_MERGE_ADJACENT_FILES true, \
         META_MAINTENANCE_CLEANUP_OLD_FILES_CLEANUP_ALL true",
        data.path().display()
    );

    let rows = csv_rows(&run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(10) t(i);\
         INSERT INTO lake.main.t VALUES (99);\
         SELECT step, status FROM moraine_maintenance('lake');",
    ));

    // Reported in sequence order, with the configured steps run.
    let order: Vec<_> = rows.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(
        order,
        vec![
            "expire_snapshots",
            "flush_inlined_data",
            "merge_adjacent_files",
            "rewrite_data_files",
            "cleanup_old_files",
            "delete_orphaned_files",
            "sweep_indexes",
        ],
        "steps must report in sequence order"
    );
    let by_step: std::collections::HashMap<_, _> = rows
        .iter()
        .map(|row| (row[0].as_str(), row[1].as_str()))
        .collect();
    for step in [
        "expire_snapshots",
        "flush_inlined_data",
        "merge_adjacent_files",
        "cleanup_old_files",
        "sweep_indexes",
    ] {
        assert_eq!(by_step.get(step), Some(&"ran"), "{step} in {rows:?}");
    }
    // Unconfigured steps stay skipped even in a full pass.
    assert_eq!(by_step.get("rewrite_data_files"), Some(&"skipped"));
    assert_eq!(by_step.get("delete_orphaned_files"), Some(&"skipped"));

    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            store.path(),
            data.path(),
            &options,
            "SELECT count(*), sum(a) FROM lake.main.t;"
        )),
        vec![vec!["11".to_string(), "144".to_string()]],
        "a maintenance pass must not change what the lake contains"
    );
}

/// The trigger refuses inside an explicit transaction rather than
/// deadlocking against the pass's own connection.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_refuses_inside_an_explicit_transaction() {
    let store = TempDir::new("maint-tx-store");
    let data = TempDir::new("maint-tx-data");
    let error = run_ducklake_sql_expect_err(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         BEGIN;\
         SELECT * FROM moraine_maintenance('lake');",
    );
    assert!(
        error.contains("explicit transaction"),
        "expected a transaction refusal, got: {error}"
    );
}

/// A misconfigured attach fails at bind with a message naming the
/// problem, rather than starting a scheduler that silently does the
/// wrong thing.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_rejects_unknown_and_contradictory_options() {
    let store = TempDir::new("maint-badopt-store");
    let data = TempDir::new("maint-badopt-data");

    let unknown = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        ", META_MAINTENANCE_NONSENSE true",
        "SELECT 1;",
    );
    assert!(
        unknown.contains("unknown maintenance option"),
        "got: {unknown}"
    );

    let contradictory = run_ducklake_sql_expect_err_with_options(
        store.path(),
        data.path(),
        ", META_MAINTENANCE_EXPIRE_SNAPSHOTS false, META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN now()",
        "SELECT 1;",
    );
    assert!(
        contradictory.contains("but one of its parameters was supplied"),
        "got: {contradictory}"
    );
}

/// Nesting the catalog store inside `DATA_PATH` is refused at attach:
/// orphan cleanup lists `DATA_PATH` and would delete the catalog itself.
///
/// The guard fires on the data path moraine is actually told about —
/// `META_DATA_PATH`, or a value already recorded for the lake. DuckLake
/// keeps its own unprefixed `DATA_PATH` for the data layer and does not
/// forward it to this metadata attach, so an attach that names only that
/// leaves moraine nothing to check.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn attach_refuses_a_data_path_containing_the_catalog() {
    let root = TempDir::new("maint-overlap");
    let store_dir = root.path().join("catalog");
    std::fs::create_dir_all(&store_dir).expect("create catalog dir");

    // DATA_PATH is the catalog's own parent, so orphan cleanup would
    // sweep the catalog's own objects.
    let nested = format!(", META_DATA_PATH '{}'", root.path().display());
    let error =
        run_ducklake_sql_expect_err_with_options(&store_dir, root.path(), &nested, "SELECT 1;");
    assert!(
        error.contains("nested on the same object store"),
        "expected the overlap refusal, got: {error}"
    );

    // A sibling data path attaches normally.
    let sibling_data = TempDir::new("maint-overlap-data");
    let safe = format!(", META_DATA_PATH '{}'", sibling_data.path().display());
    assert_eq!(
        csv_rows(&run_ducklake_sql_with_options(
            &store_dir,
            sibling_data.path(),
            &safe,
            "SELECT 1;"
        )),
        vec![vec!["1".to_string()]],
        "sibling locations must attach"
    );
}

/// The status surface retains more than the newest pass, so a pass that
/// did something stays visible after a later one that did not — the
/// property that makes a failing schedule findable rather than erased by
/// the next success.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_status_retains_earlier_passes() {
    let store = TempDir::new("maint-status-store");
    let data = TempDir::new("maint-status-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());

    // The retained window is per-attach and in memory, so setup and the
    // status query must share one session. Earlier statements emit rows
    // of their own, so the status rows carry a marker to select on.
    let output = run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &meta,
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(7) t(i);\
         SELECT * FROM moraine_index_create('lake','main','t','by_a',['a'],false);\
         SELECT * FROM moraine_index_drop('lake','main','t','by_a');\
         SELECT count(*) FROM moraine_maintenance('lake');\
         SELECT count(*) FROM moraine_maintenance('lake');\
         SELECT 'PASS' AS marker, trigger, detail FROM moraine_maintenance_status('lake') \
           WHERE step = 'sweep_indexes' ORDER BY started_at DESC;",
    );
    let passes: Vec<Vec<String>> = csv_rows(&output)
        .into_iter()
        .filter(|row| row.first().is_some_and(|marker| marker == "PASS"))
        .collect();

    // Both passes are retained, newest first, and each records what drove
    // it. The reclaiming pass survives the empty one that followed.
    assert_eq!(passes.len(), 2, "both passes must be retained: {passes:?}");
    assert_eq!(passes[0][1], "manual");
    assert!(
        passes[0][2].contains("0 entries"),
        "newest pass reclaimed nothing: {passes:?}"
    );
    assert!(
        passes[1][2].contains("7 entries"),
        "the earlier reclaiming pass must still be visible: {passes:?}"
    );
}

/// Maintenance mutates, so a read-only attach neither schedules a pass
/// nor runs one on demand: the trigger refuses, and the status surface
/// stays empty because no thread ever started.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn maintenance_never_runs_on_a_read_only_attach() {
    let store = TempDir::new("maint-ro-store");
    let data = TempDir::new("maint-ro-data");

    // Bootstrap read-write, then reattach read-only with a schedule that
    // would otherwise start a thread.
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT);\
         INSERT INTO lake.main.t SELECT i FROM range(5) t(i);",
    );

    let output = run_ducklake_read_only_sql(
        store.path(),
        data.path(),
        "SELECT count(*) FROM moraine_maintenance_status('lake');",
    );
    assert_eq!(
        csv_rows(&output),
        vec![vec!["0".to_string()]],
        "a read-only attach must start no scheduler"
    );
}
