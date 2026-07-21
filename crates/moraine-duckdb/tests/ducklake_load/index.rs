use crate::helpers::*;

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
    let create = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
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

/// UPDATE and both compaction shapes write per-row-id files; the index
/// tracks every move, and a rebuilt index backfills them.
#[test]
#[ignore = "needs the downloaded DuckDB CLI, packaged extension, and network access to INSTALL ducklake"]
fn moraine_index_survives_update_and_compaction() {
    let store = TempDir::new("index-rewrite-store");
    let data = TempDir::new("index-rewrite-data");
    let meta = format!(", META_DATA_PATH '{}'", data.path().display());
    let run = |sql: &str| run_ducklake_sql_with_options(store.path(), data.path(), &meta, sql);
    let count = |sql: &str| csv_rows(&run(sql));
    let lookup_count = |key: i64| {
        count(&format!(
            "SELECT count(*) FROM moraine_index_lookup('lake', 'main', 't', 'by_a', {key});"
        ))
    };

    run("CREATE TABLE lake.main.t(a BIGINT, b VARCHAR);");
    run("INSERT INTO lake.main.t SELECT i, 'x' FROM range(100) t(i);");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");

    // UPDATE writes a per-row-id file (delete plus re-insert under
    // preserved ids); the unchanged key still resolves exactly once.
    run("UPDATE lake.main.t SET b = 'updated' WHERE a = 7;");
    assert_eq!(lookup_count(7), vec![vec!["1".to_string()]]);

    // Deletes then rewrite: the compacted replacement re-derives its
    // surviving rows' entries as no-ops.
    run("DELETE FROM lake.main.t WHERE a IN (1, 2, 3);");
    run("CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.01);");
    assert_eq!(
        lookup_count(1),
        vec![vec!["0".to_string()]],
        "deleted keys stay gone after the rewrite"
    );
    assert_eq!(
        lookup_count(50),
        vec![vec!["1".to_string()]],
        "survivors stay found after the rewrite"
    );

    // A delete against the rewritten (per-row-id) file.
    run("DELETE FROM lake.main.t WHERE a = 50;");
    assert_eq!(lookup_count(50), vec![vec!["0".to_string()]]);

    // Merge-adjacent over the mixed file set.
    run("INSERT INTO lake.main.t SELECT i, 'y' FROM range(100, 200) t(i);");
    run("CALL ducklake_merge_adjacent_files('lake');");
    assert_eq!(lookup_count(150), vec![vec!["1".to_string()]]);
    assert_eq!(lookup_count(7), vec![vec!["1".to_string()]]);

    // Rebuild: backfill must read the per-row-id files.
    run("CALL moraine_index_drop('lake', 'main', 't', 'by_a');");
    run("CALL moraine_index_create('lake', 'main', 't', 'by_a', ['a'], true);");
    assert_eq!(lookup_count(150), vec![vec!["1".to_string()]]);
    assert_eq!(
        lookup_count(1),
        vec![vec!["0".to_string()]],
        "the rebuilt index omits rows deleted before compaction"
    );
}
