//! Repo automation. Invoked as `cargo xtask <command>`.

use std::{path::PathBuf, process::Command};

use anyhow::{Context, bail, ensure};

fn main() -> anyhow::Result<()> {
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("e2e") => e2e(),
        Some(other) => bail!("unknown task `{other}`; available: e2e"),
        None => bail!("usage: cargo xtask <task>; available: e2e"),
    }
}

/// Tier 3: build the extension cdylib and run its test suite.
///
/// Loading into a real DuckDB is added once the extension has entry points;
/// until then this validates the artifact and packaging plumbing.
fn e2e() -> anyhow::Result<()> {
    run(Command::new("cargo").args(["build", "-p", "moraine-duckdb", "--release"]))?;

    let lib = release_dir().join(cdylib_name());
    ensure!(lib.exists(), "expected cdylib at {}", lib.display());
    println!("ok: built {}", lib.display());

    run(Command::new("cargo").args(["test", "-p", "moraine-duckdb", "--release"]))?;
    println!("note: DuckDB load test pending extension entry points (see ROADMAP.md)");
    Ok(())
}

fn run(cmd: &mut Command) -> anyhow::Result<()> {
    let status = cmd.status().with_context(|| format!("spawning {cmd:?}"))?;
    ensure!(status.success(), "{cmd:?} exited with {status}");
    Ok(())
}

fn release_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/release")
}

fn cdylib_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libmoraine_duckdb.dylib"
    } else if cfg!(target_os = "windows") {
        "moraine_duckdb.dll"
    } else {
        "libmoraine_duckdb.so"
    }
}
