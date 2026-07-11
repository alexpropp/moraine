//! Compiles the C++ shim (`cpp/*.cpp`) against the vendored DuckDB v1.5.4
//! internal header and links it into this crate's cdylib.
//!
//! The shim only needs DuckDB's C++ internals header (`duckdb.hpp`,
//! vendored under `vendor/duckdb-v1.5.4/`) at compile time — it links
//! against no DuckDB library. A loadable extension is `dlopen`'d into a
//! process with every DuckDB symbol already resolved, so unresolved
//! symbols in the shim are left for the dynamic loader to resolve
//! (`-undefined dynamic_lookup` on macOS; ELF's default `-shared`
//! behavior already permits this on Linux, no equivalent flag needed).
//!
//! Two linker interventions are required on every platform:
//!
//! 1. Nothing in the Rust crate calls the C++ entry point, so the linker
//!    drops the archive member as unreferenced unless force-loaded.
//! 2. Rust's cdylib link restricts the dynamic-symbol table to symbols
//!    rustc knows about, so the C++ entry point must be explicitly added
//!    to the export list.

use std::path::Path;

fn main() {
    let vendor_dir = Path::new("vendor/duckdb-v1.5.4");
    let archive_name = "moraine_duckdb_cpp";

    cc::Build::new()
        .cpp(true)
        .flag_if_supported("-std=c++17")
        .include(vendor_dir)
        .include("cpp")
        .file("cpp/extension.cpp")
        .file("cpp/storage_extension.cpp")
        .file("cpp/catalog.cpp")
        .file("cpp/scan.cpp")
        .file("cpp/transaction_manager.cpp")
        .warnings(false)
        .compile(archive_name);

    // Cargo always sets OUT_DIR for a build script; not a fallible lookup.
    #[allow(clippy::expect_used)]
    let out_dir = std::env::var("OUT_DIR").expect("cargo sets OUT_DIR for every build script");
    // Uses the *target* OS (cargo always sets this), not `cfg!(target_os)`
    // which is the host and would be wrong under cross-compilation.
    #[allow(clippy::expect_used)]
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")
        .expect("cargo sets CARGO_CFG_TARGET_OS for every build script");

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
            // Rust's cdylib link auto-generates an `-exported_symbols_list`
            // containing only `#[no_mangle]` symbols, silently dropping
            // the C++ entry point even though `-force_load` pulled its
            // object in. `-exported_symbol` adds to that list rather than
            // replacing it. Leading underscore is Mach-O's C decoration.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,-exported_symbol,_moraine_duckdb_duckdb_cpp_init"
            );
        }
        "linux" => {
            // GNU ld equivalents of the macOS flags above. `--whole-archive`
            // must be switched off again so it doesn't leak onto later
            // archives on the link line. ELF `-shared` links tolerate
            // undefined symbols by default, so no `dynamic_lookup` analog
            // is needed. One `-Wl,` list: the driver comma-splits
            // everything after `-Wl,` into successive ld arguments.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,--whole-archive,{},--no-whole-archive",
                archive_path.display()
            );
            // Rust restricts the dynamic-symbol list on ELF too, via a
            // version script; `--export-dynamic-symbol` (GNU ld >= 2.35,
            // also lld) adds the C++ entry point. No leading underscore:
            // ELF doesn't decorate C symbols.
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,--export-dynamic-symbol=moraine_duckdb_duckdb_cpp_init"
            );
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
    println!("cargo:rerun-if-changed=vendor/duckdb-v1.5.4");
}
