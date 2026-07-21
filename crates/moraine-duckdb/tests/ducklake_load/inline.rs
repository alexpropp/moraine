use crate::helpers::*;

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
