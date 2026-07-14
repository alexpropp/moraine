//! The `e2e` task: downloads/caches the pinned DuckDB CLI, builds and
//! packages the extension, and runs
//! `crates/moraine-duckdb/tests/duckdb_load.rs` and
//! `crates/moraine-duckdb/tests/ducklake_load.rs` un-ignored against the
//! real binaries (plus a real `INSTALL ducklake`) as a hard assertion.

use std::process::Command;

use anyhow::{Context, ensure};

use crate::duckdb;

/// The single `#[ignore]`d integration test `e2e` un-ignores and runs by
/// exact name (including its `tests::` module path, required for
/// `--exact` to match). Deleting or renaming it fails `e2e` instead of
/// silently matching zero tests.
const DUCKDB_LOAD_TEST_NAME: &str = "tests::attach_lists_and_scans_through_real_duckdb";

/// Every `#[ignore]`d test in `ducklake_load.rs`, run together (not
/// `--exact`, since there are many). Needs network access to `INSTALL
/// ducklake`. Adding a test there means bumping this count, or `e2e`
/// fails — deliberate, so a silently-filtered test can never pass.
const DUCKLAKE_LOAD_TEST_COUNT: &str = "24 passed";

/// Downloads/caches the pinned DuckDB CLI, builds and packages the
/// extension, runs the crate's test suite, then runs `duckdb_load.rs`
/// un-ignored against the real CLI and packaged artifact.
pub fn e2e() -> anyhow::Result<()> {
    let cli = duckdb::ensure_duckdb_cli()?;
    println!("ok: duckdb CLI at {}", cli.display());

    let extension = duckdb::build_and_package_extension()?;
    println!("ok: packaged {}", extension.display());

    duckdb::run(Command::new("cargo").args(["test", "-p", "moraine-duckdb", "--release"]))?;

    let output = Command::new("cargo")
        .args([
            "test",
            "-p",
            "moraine-duckdb",
            "--release",
            "--test",
            "duckdb_load",
            "--",
            "--ignored",
            "--exact",
            DUCKDB_LOAD_TEST_NAME,
        ])
        .env("MORAINE_DUCKDB_CLI", &cli)
        .env("MORAINE_DUCKDB_EXT", &extension)
        .output()
        .context("spawning the duckdb_load integration test")?;
    // Stdio isn't inherited here (unlike `run`), so output can be checked
    // below; echo both streams so failures stay visible on the console.
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    ensure!(
        output.status.success(),
        "duckdb_load integration test failed"
    );
    // An exact-name filter matching nothing still exits 0 ("0 passed; 0
    // filtered out"), which would let a deleted/renamed #[test] pass
    // vacuously.
    let stdout = String::from_utf8_lossy(&output.stdout);
    ensure!(
        stdout.contains("1 passed"),
        "expected `{DUCKDB_LOAD_TEST_NAME}` to report `1 passed`; the test may have been \
         deleted, renamed, or its #[ignore] removed/changed. Got:\n{stdout}"
    );
    println!("ok: real DuckDB loaded moraine_duckdb and drove attach/listing/scan");

    let ducklake_output = Command::new("cargo")
        .args([
            "test",
            "-p",
            "moraine-duckdb",
            "--release",
            "--test",
            "ducklake_load",
            "--",
            "--ignored",
        ])
        .env("MORAINE_DUCKDB_CLI", &cli)
        .env("MORAINE_DUCKDB_EXT", &extension)
        .output()
        .context("spawning the ducklake_load integration test")?;
    print!("{}", String::from_utf8_lossy(&ducklake_output.stdout));
    eprint!("{}", String::from_utf8_lossy(&ducklake_output.stderr));
    ensure!(
        ducklake_output.status.success(),
        "ducklake_load integration test failed"
    );
    let ducklake_stdout = String::from_utf8_lossy(&ducklake_output.stdout);
    ensure!(
        ducklake_stdout.contains(DUCKLAKE_LOAD_TEST_COUNT),
        "expected ducklake_load.rs to report `{DUCKLAKE_LOAD_TEST_COUNT}`; a test may have been \
         deleted or its #[ignore] removed/changed. Got:\n{ducklake_stdout}"
    );
    println!(
        "ok: real DuckDB + ducklake attached through moraine:'s metadata catalog and read the lake"
    );

    Ok(())
}
