//! Repo automation. Invoked as `cargo xtask <command>`.
//!
//! - `e2e` packages the extension and drives it through a real DuckDB CLI
//!   (see `e2e.rs`).

use anyhow::bail;

mod duckdb;
mod e2e;

fn main() -> anyhow::Result<()> {
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("e2e") => e2e::e2e(),
        Some(other) => bail!("unknown task `{other}`; available: e2e"),
        None => bail!("usage: cargo xtask <task>; available: e2e"),
    }
}
