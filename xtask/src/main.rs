//! Repo automation. Invoked as `cargo xtask <command>`.
//!
//! - `e2e` packages the extension and drives it through a real DuckDB CLI
//!   (see `e2e.rs`).
//! - `bench` compares DuckLake metadata catalogs — moraine's SlateDB
//!   store, a stock DuckDB file, and Postgres — on identical workloads
//!   (see `bench.rs`).

use anyhow::bail;

mod bench;
mod duckdb;
mod e2e;

fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(2).collect();
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("e2e") => e2e::e2e(),
        Some("bench") => bench::bench(&arguments),
        Some(other) => bail!("unknown task `{other}`; available: e2e, bench"),
        None => bail!("usage: cargo xtask <task>; available: e2e, bench"),
    }
}
