//! Compiles the C++ shim (`cpp/*.cpp`) against DuckDB v1.5.4's full
//! source-tree headers (`src/include/`, sparse-checked-out from the pinned
//! git tag and cached under `target/duckdb-src/`), compiles DuckDB itself
//! from the pinned release's single-file amalgamation
//! (`libduckdb-src.zip`, cached and built once per target under
//! `target/duckdb-src/`), and links both into this crate's cdylib.
//!
//! DuckDB is linked *statically* because the stock Linux release CLI is a
//! statically linked executable that exports none of its C++ internals —
//! an extension that leaves `duckdb::` symbols undefined for the dynamic
//! loader can never resolve them there. Official DuckDB C++ extensions are
//! shaped the same way: each carries its own copy of DuckDB's internals,
//! and objects cross the boundary by pointer between ABI-identical builds
//! of the same pinned version.
//!
//! The extension entry point is a Rust `#[no_mangle]` function (see
//! `src/entrypoint.rs`) that forwards to the C++ shim, so rustc lists it
//! among the cdylib's exported symbols on every platform — the C++ side
//! exports nothing. Two linker interventions remain:
//!
//! 1. Only the C++ translation units the entry point transitively references
//!    would otherwise be pulled from the shim archive; force-loading the
//!    whole archive keeps every translation unit present regardless. The
//!    DuckDB archive needs no force-load — the shim's references pull it in.
//! 2. Each archive must appear on the link line exactly once. `cc`'s default
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
    let duckdb_archive = ensure_duckdb_static_archive()?;

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
            // `-force_load` pulls in the whole archive, keeping every C++
            // translation unit present, not just those the entry point
            // transitively references.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,-force_load,{}",
                archive_path.display()
            );
            // DuckDB itself, after the shim so the shim's references pull
            // it in; lazy (no force-load) so nothing unused is kept.
            println!("cargo:rustc-cdylib-link-arg={}", duckdb_archive.display());
            // The C++ standard library, after both archives so it can
            // satisfy their references.
            println!("cargo:rustc-cdylib-link-arg=-lc++");
        }
        "linux" => {
            // GNU ld equivalents of the macOS flags. `--whole-archive` must
            // be switched off again so it doesn't leak onto later archives.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,--whole-archive,{},--no-whole-archive",
                archive_path.display()
            );
            // DuckDB itself, after the shim so the shim's references pull
            // it in; lazy (no whole-archive) so nothing unused is kept.
            println!("cargo:rustc-cdylib-link-arg={}", duckdb_archive.display());
            // The C++ standard library, after both archives so it can
            // satisfy their references.
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
    let checkout_root = duckdb_src_root()?.join(DUCKDB_PIN);
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

/// Returns the path to a static archive of DuckDB `DUCKDB_PIN` built from
/// the release's single-file amalgamation, compiling and caching it under
/// `target/duckdb-src/<pin>-lib/<target>/` if it isn't already cached.
///
/// Built once per target triple with fixed flags (`-O1`, `NDEBUG`, no debug
/// info) regardless of cargo profile, so debug and release builds share the
/// compile — a single translation unit that takes minutes. `NDEBUG` matches
/// the release CLI the extension loads into. `-O1`, not `-O2`: gcc at `-O2`
/// on this one giant translation unit peaks past the memory of a 16 GB CI
/// runner running parallel rustc jobs (the runner agent gets killed), and
/// optimization level does not affect the ABI; the linked-in code serves
/// only the shim's metadata-catalog calls, not the host's query execution.
fn ensure_duckdb_static_archive() -> anyhow::Result<PathBuf> {
    let target = std::env::var("TARGET").context("cargo sets TARGET for every build script")?;
    let lib_dir = duckdb_src_root()?.join(format!("{DUCKDB_PIN}-lib/{target}"));
    let archive_path = lib_dir.join("libduckdb_amalgamation.a");
    if archive_path.exists() {
        return Ok(archive_path);
    }

    let amalgamation_dir = ensure_duckdb_amalgamation_source()?;

    println!(
        "cargo:warning=moraine-duckdb: compiling the DuckDB {DUCKDB_PIN} amalgamation into a \
         static archive (one translation unit, takes minutes) — one-time cost per target, \
         cached under target/ after this"
    );

    // Compile into a scratch directory and rename into place, so an
    // interrupted compile can't leave a partial archive at the final path.
    let scratch_dir = lib_dir.with_extension("partial");
    if scratch_dir.exists() {
        fs::remove_dir_all(&scratch_dir)
            .with_context(|| format!("clearing stale scratch at {}", scratch_dir.display()))?;
    }
    fs::create_dir_all(&scratch_dir)
        .with_context(|| format!("creating {}", scratch_dir.display()))?;

    cc::Build::new()
        .cpp(true)
        .flag_if_supported("-std=c++17")
        .include(&amalgamation_dir)
        .file(amalgamation_dir.join("duckdb.cpp"))
        .define("NDEBUG", None)
        .opt_level(1)
        .debug(false)
        .warnings(false)
        .cargo_metadata(false)
        .out_dir(&scratch_dir)
        .compile("duckdb_amalgamation");

    if lib_dir.exists() {
        fs::remove_dir_all(&lib_dir)
            .with_context(|| format!("clearing stale lib dir at {}", lib_dir.display()))?;
    }
    fs::rename(&scratch_dir, &lib_dir).with_context(|| {
        format!(
            "moving compiled archive from {} to {}",
            scratch_dir.display(),
            lib_dir.display()
        )
    })?;

    ensure!(
        archive_path.exists(),
        "compiled the DuckDB amalgamation but {} is still missing",
        archive_path.display()
    );

    Ok(archive_path)
}

/// Returns the path to a directory holding the `DUCKDB_PIN` release's
/// single-file amalgamation (`duckdb.cpp` + `duckdb.hpp`), downloading and
/// caching it under `target/duckdb-src/<pin>-amalgamation/` if it isn't
/// already cached. This is the `libduckdb-src.zip` release asset — the
/// library *source*, distinct from the `src/include/` header tree the shim
/// compiles against.
fn ensure_duckdb_amalgamation_source() -> anyhow::Result<PathBuf> {
    let amalgamation_dir = duckdb_src_root()?.join(format!("{DUCKDB_PIN}-amalgamation"));
    if amalgamation_dir.join("duckdb.cpp").exists() {
        return Ok(amalgamation_dir);
    }

    if amalgamation_dir.exists() {
        fs::remove_dir_all(&amalgamation_dir).with_context(|| {
            format!("clearing stale download at {}", amalgamation_dir.display())
        })?;
    }
    fs::create_dir_all(&amalgamation_dir)
        .with_context(|| format!("creating {}", amalgamation_dir.display()))?;

    let url = format!(
        "https://github.com/duckdb/duckdb/releases/download/{DUCKDB_PIN}/libduckdb-src.zip"
    );
    let zip_path = amalgamation_dir.join("libduckdb-src.zip");

    println!(
        "cargo:warning=moraine-duckdb: downloading the DuckDB {DUCKDB_PIN} amalgamation \
         ({url}) into {} — one-time cost, cached under target/ after this",
        amalgamation_dir.display()
    );

    run_tool(
        Command::new("curl")
            .arg("--fail")
            .arg("--location")
            .arg("--output")
            .arg(&zip_path)
            .arg(&url),
        &format!(
            "downloading {url}; if this machine is offline, pre-populate {} manually with the \
             unzipped `libduckdb-src.zip` asset of the DuckDB {DUCKDB_PIN} release",
            amalgamation_dir.display()
        ),
    )?;
    run_tool(
        Command::new("unzip")
            .arg("-o")
            .arg(&zip_path)
            .arg("-d")
            .arg(&amalgamation_dir),
        "unzipping the DuckDB amalgamation",
    )?;
    fs::remove_file(&zip_path)
        .with_context(|| format!("removing the unpacked zip at {}", zip_path.display()))?;

    ensure!(
        amalgamation_dir.join("duckdb.cpp").exists(),
        "unzipped the DuckDB amalgamation into {} but duckdb.cpp is still missing",
        amalgamation_dir.display()
    );

    Ok(amalgamation_dir)
}

/// A file present in the full `src/include/` tree but absent from the
/// single-file `duckdb.hpp` amalgamation. Used as the cache sanity check
/// so an amalgamation-shaped cache is rejected here rather than at compile
/// time.
fn full_tree_marker(include_dir: &Path) -> PathBuf {
    include_dir.join("duckdb/storage/storage_extension.hpp")
}

/// The shared cache root for everything fetched or built from DuckDB
/// sources, `target/duckdb-src/`. This crate lives at
/// `<workspace_root>/crates/moraine-duckdb`, so the workspace `target/` is
/// two levels up from `CARGO_MANIFEST_DIR`.
fn duckdb_src_root() -> anyhow::Result<PathBuf> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .context("cargo sets CARGO_MANIFEST_DIR for every build script")?;
    Ok(Path::new(&manifest_dir).join("../../target/duckdb-src"))
}

/// Runs `cmd` with inherited stdio, failing with `what` on non-zero exit
/// or spawn failure.
fn run_tool(cmd: &mut Command, what: &str) -> anyhow::Result<()> {
    match cmd.status() {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => bail!("{cmd:?} exited with {status} while {what}"),
        Err(e) => bail!("failed to run {cmd:?} while {what}: {e}"),
    }
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
