use crate::helpers::*;

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
