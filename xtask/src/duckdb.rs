//! Shared DuckDB plumbing for xtask commands: downloading/caching the
//! pinned CLI, building the extension cdylib, and packaging it into a
//! loadable `.duckdb_extension` (rename + 512-byte metadata footer — see
//! `crates/moraine-duckdb/README.md`'s "Extension entry-point contract").

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

/// The DuckDB version xtask downloads and the extension is packaged
/// against. The packaged footer's DuckDB-version field must equal this
/// exactly for the loader to accept it (see the README's "Extension
/// entry-point contract").
pub const DUCKDB_PIN: &str = "v1.5.4";

const DUCKDB_RELEASE_BASE_URL: &str = "https://github.com/duckdb/duckdb/releases/download";

const EXTENSION_NAME: &str = "moraine_duckdb";

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

fn release_dir() -> PathBuf {
    // Honor a redirected target directory; the cdylib lands there, not in
    // the workspace tree.
    std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| workspace_root().join("target"), PathBuf::from)
        .join("release")
}

/// Cache root for the downloaded CLI and the packaged extension artifact,
/// gitignored (`/target`) and never committed.
fn duckdb_cli_root() -> PathBuf {
    workspace_root().join("target/duckdb-cli")
}

/// Cache root for `INSTALL ducklake`-style downloaded extensions,
/// gitignored under `target/`.
pub fn extension_install_directory() -> PathBuf {
    workspace_root().join("target/duckdb-extensions")
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

    let platform = cli_download_platform()?;
    let zip_name = format!("duckdb_cli-{platform}.zip");
    let zip_path = duckdb_root.join(&zip_name);
    if !zip_path.exists() {
        fs::create_dir_all(&duckdb_root)
            .with_context(|| format!("creating {}", duckdb_root.display()))?;
        let url = format!("{DUCKDB_RELEASE_BASE_URL}/{DUCKDB_PIN}/{zip_name}");
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

/// Builds the release cdylib and packages it into a loadable
/// `.duckdb_extension` in the cache, returning the artifact's path.
pub fn build_and_package_extension() -> anyhow::Result<PathBuf> {
    run(Command::new("cargo").args(["build", "-p", "moraine-duckdb", "--release"]))?;
    let lib = release_dir().join(cdylib_name());
    ensure!(lib.exists(), "expected cdylib at {}", lib.display());
    println!("ok: built {}", lib.display());

    package_extension(&lib, &duckdb_cli_root())
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
/// and Linux, matching `build.rs`'s own linkage support.
fn cli_download_platform() -> anyhow::Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("osx-arm64"),
        // No standalone macOS x86_64 asset is published; the universal
        // binary covers both architectures.
        ("macos", "x86_64") => Ok("osx-universal"),
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("linux", "aarch64") => Ok("linux-arm64"),
        (os, arch) => bail!(
            "no pinned DuckDB {DUCKDB_PIN} CLI mapping for {os}/{arch}; add one in \
             xtask/src/duckdb.rs (see crates/moraine-duckdb/README.md's CLI URL list)"
        ),
    }
}

/// The metadata footer's platform field, matching DuckDB's own
/// `DuckDBPlatform()` (`<os>_<arch>`) — a different spelling than the
/// CLI download's hyphenated platform tag above.
fn footer_platform() -> anyhow::Result<String> {
    let (os, arch) = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => ("osx", "arm64"),
        ("macos", "x86_64") => ("osx", "amd64"),
        ("linux", "x86_64") => ("linux", "amd64"),
        ("linux", "aarch64") => ("linux", "arm64"),
        (os, arch) => bail!("no metadata-footer platform mapping for {os}/{arch}"),
    };
    Ok(format!("{os}_{arch}"))
}

const FOOTER_SIZE: usize = 512;

/// Free-form (only ever compared byte-for-byte against itself); mirrors
/// this workspace's `moraine-duckdb` version for traceability.
const EXTENSION_VERSION: &str = "0.1.0";

/// Writes `value` into `footer[offset..offset + value.len()]`, UTF-8,
/// NUL-padded on the right since `footer` starts zeroed. Every caller
/// passes a value well under the 32-byte field width, but overflow is a
/// hard error, never a silent truncation.
fn write_footer_field(
    footer: &mut [u8; FOOTER_SIZE],
    offset: usize,
    value: &str,
) -> anyhow::Result<()> {
    let bytes = value.as_bytes();
    ensure!(
        bytes.len() <= 32,
        "metadata field {value:?} is {} bytes, must fit in 32",
        bytes.len()
    );
    footer[offset..offset + bytes.len()].copy_from_slice(bytes);
    Ok(())
}

/// Builds the 512-byte extension metadata footer DuckDB v1.5.4 expects
/// appended to a loadable extension file (byte layout and field order in
/// `crates/moraine-duckdb/README.md`'s "Extension entry-point contract").
/// The signature region `[256, 512)` stays zero: unsigned extensions
/// only, matching this project's `-unsigned` CLI contract.
fn metadata_footer() -> anyhow::Result<[u8; FOOTER_SIZE]> {
    let mut footer = [0u8; FOOTER_SIZE];
    write_footer_field(&mut footer, 96, "CPP")?;
    write_footer_field(&mut footer, 128, EXTENSION_VERSION)?;
    write_footer_field(&mut footer, 160, DUCKDB_PIN)?;
    write_footer_field(&mut footer, 192, &footer_platform()?)?;
    write_footer_field(&mut footer, 224, "4")?;
    Ok(footer)
}

/// Packages the built cdylib at `lib` into a loadable
/// `<EXTENSION_NAME>.duckdb_extension` under `<duckdb_root>/artifact/`:
/// copies the bytes, then appends the 512-byte metadata footer. The
/// filename carries exactly one `.`, required for DuckDB's loader to
/// derive the right init-symbol name (`FileSystem::ExtractBaseName`
/// splits on the first `.`).
fn package_extension(lib: &Path, duckdb_root: &Path) -> anyhow::Result<PathBuf> {
    let artifact_dir = duckdb_root.join("artifact");
    fs::create_dir_all(&artifact_dir)
        .with_context(|| format!("creating {}", artifact_dir.display()))?;
    let extension_path = artifact_dir.join(format!("{EXTENSION_NAME}.duckdb_extension"));

    let mut bytes =
        fs::read(lib).with_context(|| format!("reading built cdylib at {}", lib.display()))?;
    bytes.extend_from_slice(&metadata_footer()?);
    fs::write(&extension_path, &bytes)
        .with_context(|| format!("writing packaged extension to {}", extension_path.display()))?;

    Ok(extension_path)
}
