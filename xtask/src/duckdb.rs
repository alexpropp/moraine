//! Shared DuckDB plumbing for xtask commands: downloading and caching the
//! pinned CLI, and building the loadable `.duckdb_extension` through
//! DuckDB's own extension toolchain (`make release`).

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

/// The pinned DuckDB version: the extension is built against the `duckdb`
/// submodule at this tag, and xtask downloads the matching CLI to load the
/// artifact against. Keep in lockstep with the submodule ref.
pub fn duckdb_pin() -> &'static str {
    "v1.5.4"
}

const DUCKDB_RELEASE_BASE_URL: &str = "https://github.com/duckdb/duckdb/releases/download";

/// Runs `cmd`, failing with its exact invocation and exit status if it
/// didn't succeed. Stdio is inherited, so the child's own output (e.g.
/// `curl`'s error text) reaches the caller.
pub fn run(cmd: &mut Command) -> anyhow::Result<()> {
    let status = cmd.status().with_context(|| format!("spawning {cmd:?}"))?;
    ensure!(status.success(), "{cmd:?} exited with {status}");
    Ok(())
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// Cache root for the downloaded CLI, gitignored (`/target`) and never
/// committed.
fn duckdb_cli_root() -> PathBuf {
    workspace_root().join("target/duckdb-cli")
}

/// Cache root for `INSTALL ducklake`-style downloaded extensions,
/// gitignored under `target/`.
pub fn extension_install_directory() -> PathBuf {
    workspace_root().join("target/duckdb-extensions")
}

fn cli_binary_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "duckdb.exe"
    } else {
        "duckdb"
    }
}

/// Downloads and unpacks the pinned DuckDB CLI for the host platform into
/// the gitignored cache, skipping both the download and the unpack if the
/// binary is already cached there. Returns the CLI's path.
pub fn ensure_duckdb_cli() -> anyhow::Result<PathBuf> {
    let duckdb_root = duckdb_cli_root();
    let cli_dir = duckdb_root.join("cli");
    let cli_path = cli_dir.join(cli_binary_name());
    if cli_path.exists() {
        return Ok(cli_path);
    }

    let pin = duckdb_pin();
    let platform = cli_download_platform(pin)?;
    let zip_name = format!("duckdb_cli-{platform}.zip");
    let zip_path = duckdb_root.join(&zip_name);
    if !zip_path.exists() {
        fs::create_dir_all(&duckdb_root)
            .with_context(|| format!("creating {}", duckdb_root.display()))?;
        let url = format!("{DUCKDB_RELEASE_BASE_URL}/{pin}/{zip_name}");
        download(&url, &zip_path)?;
    }

    fs::create_dir_all(&cli_dir).with_context(|| format!("creating {}", cli_dir.display()))?;
    run(Command::new("unzip")
        .args(["-o", "-q"])
        .arg(&zip_path)
        .arg("-d")
        .arg(&cli_dir))?;
    ensure!(
        cli_path.exists(),
        "unzipped {} but {} is still missing",
        zip_path.display(),
        cli_path.display()
    );

    #[cfg(unix)]
    make_executable(&cli_path)?;

    Ok(cli_path)
}

/// Builds the extension through DuckDB's extension toolchain (`make
/// release`) and returns the path to the loadable `.duckdb_extension`. The
/// toolchain statically links DuckDB, links moraine's Rust core, and writes
/// the metadata footer; this replaces the old cdylib-plus-hand-written-
/// footer packaging. Requires `ninja` on PATH (the toolchain generator).
pub fn build_and_package_extension() -> anyhow::Result<PathBuf> {
    run(Command::new("make")
        .args(["release", "GEN=ninja"])
        .current_dir(workspace_root()))?;
    let artifact =
        workspace_root().join("build/release/extension/moraine/moraine.duckdb_extension");
    ensure!(
        artifact.exists(),
        "`make release` finished but the loadable extension is missing at {}",
        artifact.display()
    );
    Ok(artifact)
}

/// Downloads `url` to `dest` via the system `curl`, removing any partial
/// file on failure. Fails with a message naming the URL and hinting at
/// connectivity issues when offline; curl's own diagnostics still reach
/// stderr.
pub fn download(url: &str, dest: &Path) -> anyhow::Result<()> {
    println!("downloading {url}");
    let outcome = Command::new("curl")
        .args(["--fail", "--location", "--show-error", "-o"])
        .arg(dest)
        .arg(url)
        .status();
    match outcome {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let _ = fs::remove_file(dest);
            bail!(
                "curl exited with {status} downloading {url}; if this machine is offline, \
                 cache the zip at that path manually and rerun"
            )
        }
        Err(e) => {
            let _ = fs::remove_file(dest);
            bail!("failed to run `curl` (required to download the DuckDB CLI): {e}")
        }
    }
}

#[cfg(unix)]
pub fn make_executable(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = fs::metadata(path)
        .with_context(|| format!("reading permissions of {}", path.display()))?
        .permissions();
    perms.set_mode(perms.mode() | 0o111);
    fs::set_permissions(path, perms)
        .with_context(|| format!("marking {} executable", path.display()))?;
    Ok(())
}

/// The `duckdb_cli-<platform>.zip` platform tag for the host, matching
/// the URL scheme in `crates/moraine-duckdb/README.md`. Scoped to macOS
/// and Linux, the platforms the extension supports.
fn cli_download_platform(pin: &str) -> anyhow::Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("osx-arm64"),
        // No standalone macOS x86_64 asset is published; the universal
        // binary covers both architectures.
        ("macos", "x86_64") => Ok("osx-universal"),
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("linux", "aarch64") => Ok("linux-arm64"),
        (os, arch) => bail!(
            "no pinned DuckDB {pin} CLI mapping for {os}/{arch}; add one in \
             xtask/src/duckdb.rs (see crates/moraine-duckdb/README.md's CLI URL list)"
        ),
    }
}
