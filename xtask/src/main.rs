//! Repo automation. Invoked as `cargo xtask <command>`.
//!
//! - `e2e` packages the extension and drives it through a real DuckDB CLI
//!   (see `e2e.rs`).
//! - `bench` compares DuckLake metadata catalogs — moraine's SlateDB
//!   store, a stock DuckDB file, and Postgres — on identical workloads
//!   (see `bench.rs`).
//! - `s3` runs the catalog's object storage suite against a pinned MinIO
//!   server (see `s3.rs`).
//! - `package` builds and packages the extension into a distributable
//!   `dist/<duckdb_version>/<platform>/` tree (the directory shape DuckDB
//!   extension repositories serve).

use std::path::Path;

use anyhow::bail;

mod bench;
mod duckdb;
mod e2e;
mod s3;

fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(2).collect();
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("e2e") => e2e::e2e(),
        Some("bench") => bench::bench(&arguments),
        Some("s3") => s3::s3(),
        Some("package") => {
            let out = arguments.first().map_or("dist", String::as_str);
            duckdb::package(Path::new(out))
        }
        Some(other) => bail!("unknown task `{other}`; available: e2e, bench, s3, package"),
        None => bail!("usage: cargo xtask <task>; available: e2e, bench, s3, package"),
    }
}
