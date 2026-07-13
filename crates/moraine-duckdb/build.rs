//! Compiles the C++ shim (`cpp/*.cpp`) against DuckDB v1.5.4's full
//! source-tree headers (`src/include/`, sparse-checked-out from the pinned
//! git tag and cached under `target/duckdb-src/`) and links it into this
//! crate's cdylib.
//!
//! The shim needs DuckDB's C++ internal headers at compile time but links
//! against no DuckDB library: a loadable extension is `dlopen`'d into a
//! process with every DuckDB symbol already resolved, so unresolved symbols
//! in the shim are left for the dynamic loader (`-undefined dynamic_lookup`
//! on macOS; ELF's default `-shared` behavior permits this on Linux).
//!
//! Three linker interventions are required on every platform:
//!
//! 1. Nothing in the Rust crate calls the C++ entry point, so the archive
//!    member must be force-loaded or the linker drops it.
//! 2. Rust's cdylib link restricts the dynamic-symbol table to symbols
//!    rustc knows about, so the C++ entry point must be explicitly exported.
//! 3. The archive must appear on the link line exactly once. `cc`'s default
//!    cargo metadata adds a second, lazy `-l` mention ahead of the
//!    force-load one, making lld define every cross-referenced symbol
//!    twice; so `cc`'s metadata is suppressed and the C++ standard library
//!    it carried is linked by hand.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

/// The DuckDB tag whose `src/include/` tree this shim compiles against.
/// Matches the artifact-footer version pin in `xtask/src/main.rs`
/// (`DUCKDB_PIN`) — both must name the same release.
const DUCKDB_PIN: &str = "v1.5.4";

const DUCKDB_GIT_URL: &str = "https://github.com/duckdb/duckdb.git";

fn main() -> anyhow::Result<()> {
    let include_dir = ensure_duckdb_headers()?;

    let archive_name = "moraine_duckdb_cpp";

    cc::Build::new()
        .cpp(true)
        .flag_if_supported("-std=c++17")
        .include(&include_dir)
        .include("cpp")
        .file("cpp/extension.cpp")
        .file("cpp/storage_extension.cpp")
        .file("cpp/catalog.cpp")
        .file("cpp/inline_tables.cpp")
        .file("cpp/metadata_tables.cpp")
        .file("cpp/scan.cpp")
        .file("cpp/staged_write.cpp")
        .file("cpp/transaction_manager.cpp")
        .warnings(false)
        // Suppress `cargo:rustc-link-lib=static=…`, which would put the
        // archive on the link line a second time ahead of the force-load
        // below and make lld define cross-referenced symbols twice. This
        // also drops the C++ standard-library link, re-added per platform.
        .cargo_metadata(false)
        .compile(archive_name);

    let out_dir = std::env::var("OUT_DIR").context("cargo sets OUT_DIR for every build script")?;
    // Uses the *target* OS (cargo always sets this), not `cfg!(target_os)`
    // which is the host and would be wrong under cross-compilation.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")
        .context("cargo sets CARGO_CFG_TARGET_OS for every build script")?;

    let archive_path = Path::new(&out_dir).join(format!("lib{archive_name}.a"));

    match target_os.as_str() {
        "macos" => {
            // `-force_load` pulls in the whole archive despite nothing
            // referencing it.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,-force_load,{}",
                archive_path.display()
            );
            // Defers DuckDB symbol resolution to `dlopen` time; the host
            // process already has them.
            println!("cargo:rustc-cdylib-link-arg=-undefined");
            println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
            // Rust's auto-generated `-exported_symbols_list` lists only
            // `#[no_mangle]` symbols, dropping the C++ entry point;
            // `-exported_symbol` adds to that list. Leading underscore is
            // Mach-O's C decoration.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,-exported_symbol,_moraine_duckdb_duckdb_cpp_init"
            );
            // The C++ standard library, after the archive so it can satisfy
            // the shim's references.
            println!("cargo:rustc-cdylib-link-arg=-lc++");
        }
        "linux" => {
            // GNU ld equivalents of the macOS flags. `--whole-archive` must
            // be switched off again so it doesn't leak onto later archives.
            // ELF `-shared` tolerates undefined symbols, so no
            // `dynamic_lookup` analog is needed.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,--whole-archive,{},--no-whole-archive",
                archive_path.display()
            );
            // Rust restricts the dynamic-symbol list on ELF via a version
            // script; `--export-dynamic-symbol` (GNU ld >= 2.35, also lld)
            // adds the C++ entry point. No leading underscore: ELF doesn't
            // decorate C symbols.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,--export-dynamic-symbol=moraine_duckdb_duckdb_cpp_init"
            );
            // The C++ standard library, after the archive so it can satisfy
            // the shim's references.
            println!("cargo:rustc-cdylib-link-arg=-lstdc++");
        }
        other => {
            println!(
                "cargo:warning=moraine-duckdb: extension linkage is unverified on target OS \
                 `{other}` — the DuckDB entry symbol may be dropped or unexported; only macOS \
                 and Linux link flags are wired up"
            );
        }
    }

    println!("cargo:rerun-if-changed=cpp");

    Ok(())
}

/// Returns the path to a local `src/include/` tree from the DuckDB
/// `DUCKDB_PIN` tag, downloading and caching it under
/// `target/duckdb-src/<pin>/` if it isn't already cached.
///
/// Only `src/include/` is fetched, via a blobless partial clone plus a
/// sparse checkout of that one path: the shim compiles cleanly against
/// `src/include/` alone with zero `third_party/` headers, and that
/// directory is ~9 MB versus the full tree's 100+ MB.
fn ensure_duckdb_headers() -> anyhow::Result<PathBuf> {
    let checkout_root = duckdb_src_cache_root()?;
    let include_dir = checkout_root.join("src/include");
    if full_tree_marker(&include_dir).exists() {
        return Ok(include_dir);
    }

    // A prior attempt may have left a partial checkout; start clean rather
    // than risk `git clone` failing into a non-empty directory.
    if checkout_root.exists() {
        fs::remove_dir_all(&checkout_root)
            .with_context(|| format!("clearing stale checkout at {}", checkout_root.display()))?;
    }
    fs::create_dir_all(&checkout_root)
        .with_context(|| format!("creating {}", checkout_root.display()))?;

    println!(
        "cargo:warning=moraine-duckdb: fetching DuckDB {DUCKDB_PIN} headers (src/include/, ~9 \
         MB) into {} — one-time cost, cached under target/ after this",
        checkout_root.display()
    );

    run_git(
        &checkout_root,
        [
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            "--depth",
            "1",
            "--branch",
            DUCKDB_PIN,
            DUCKDB_GIT_URL,
            ".",
        ],
    )?;
    run_git(&checkout_root, ["sparse-checkout", "set", "src/include"])?;
    run_git(&checkout_root, ["checkout", DUCKDB_PIN])?;

    ensure!(
        full_tree_marker(&include_dir).exists(),
        "checked out DuckDB {DUCKDB_PIN} into {} but {} is still missing",
        checkout_root.display(),
        full_tree_marker(&include_dir).display()
    );

    Ok(include_dir)
}

/// A file present in the full `src/include/` tree but absent from the
/// single-file `duckdb.hpp` amalgamation. Used as the cache sanity check
/// so an amalgamation-shaped cache is rejected here rather than at compile
/// time.
fn full_tree_marker(include_dir: &Path) -> PathBuf {
    include_dir.join("duckdb/storage/storage_extension.hpp")
}

/// The shared cache root for the DuckDB headers, `target/duckdb-src/<pin>/`.
/// This crate lives at `<workspace_root>/crates/moraine-duckdb`, so the
/// workspace `target/` is two levels up from `CARGO_MANIFEST_DIR`.
fn duckdb_src_cache_root() -> anyhow::Result<PathBuf> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .context("cargo sets CARGO_MANIFEST_DIR for every build script")?;
    Ok(Path::new(&manifest_dir).join(format!("../../target/duckdb-src/{DUCKDB_PIN}")))
}

/// Runs `git <args>` with `cwd` as the working directory, failing with a
/// message naming the command on non-zero exit. `git`'s own diagnostics
/// reach stderr since stdio is inherited.
fn run_git<I, S>(cwd: &Path, args: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(cwd);
    let outcome = cmd.status();
    match outcome {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => bail!(
            "{cmd:?} exited with {status}; if this machine is offline, pre-populate \
             target/duckdb-src/{DUCKDB_PIN}/src/include/ manually with the `src/include/` \
             directory of a duckdb/duckdb checkout at tag {DUCKDB_PIN} (e.g. \
             `git clone --depth 1 --branch {DUCKDB_PIN} {DUCKDB_GIT_URL} /tmp/duckdb && \
             cp -r /tmp/duckdb/src/include target/duckdb-src/{DUCKDB_PIN}/src/`) and rerun. \
             The `libduckdb-src.zip` release asset is NOT a substitute: it is the single-file \
             amalgamation, not this header tree"
        ),
        Err(e) => bail!("failed to run `git` (required to fetch DuckDB headers): {e}"),
    }
}
