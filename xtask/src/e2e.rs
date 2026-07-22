//! The `e2e` task: downloads/caches the pinned DuckDB CLI, builds and
//! packages the extension, and runs
//! `crates/moraine-duckdb/tests/duckdb_load.rs` and
//! `crates/moraine-duckdb/tests/ducklake_load.rs` un-ignored against the
//! real binaries (plus a real `INSTALL ducklake`) as a hard assertion.

use std::process::Command;

use crate::duckdb;

/// The single `#[ignore]`d integration test `e2e` un-ignores and runs by
/// exact name. Deleting or renaming it fails `e2e` instead of silently
/// matching zero tests.
const DUCKDB_LOAD_TEST_NAME: &str = "attach_lists_and_scans_through_real_duckdb";

/// Every `#[ignore]`d test in `ducklake_load.rs`, run together (not
/// `--exact`, since there are many). Needs network access to `INSTALL
/// ducklake`. Adding a test there means bumping this count, or `e2e`
/// fails — deliberate, so a silently-filtered test can never pass.
const DUCKLAKE_LOAD_TEST_COUNT: &str = "38 passed";

/// Downloads/caches the pinned DuckDB CLI, builds and packages the
/// extension, runs the crate's test suite, then runs `duckdb_load.rs`
/// un-ignored against the real CLI and packaged artifact.
pub fn e2e() -> anyhow::Result<()> {
    let cli = duckdb::ensure_duckdb_cli()?;
    println!("ok: duckdb CLI at {}", cli.display());

    let extension = duckdb::build_and_package_extension()?;
    println!("ok: packaged {}", extension.display());

    duckdb::run(Command::new("cargo").args(["test", "-p", "moraine-duckdb", "--release"]))?;

    let envs: &[(&str, &std::ffi::OsStr)] = &[
        ("MORAINE_DUCKDB_CLI", cli.as_os_str()),
        ("MORAINE_DUCKDB_EXT", extension.as_os_str()),
    ];

    duckdb::run_ignored_suite(
        "moraine-duckdb",
        "duckdb_load",
        true,
        &["--exact", DUCKDB_LOAD_TEST_NAME],
        envs,
        "1 passed",
    )?;
    println!("ok: real DuckDB loaded moraine_duckdb and drove attach/listing/scan");

    duckdb::run_ignored_suite(
        "moraine-duckdb",
        "ducklake_load",
        true,
        &[],
        envs,
        DUCKLAKE_LOAD_TEST_COUNT,
    )?;
    println!(
        "ok: real DuckDB + ducklake attached through moraine:'s metadata catalog and read the lake"
    );

    Ok(())
}
