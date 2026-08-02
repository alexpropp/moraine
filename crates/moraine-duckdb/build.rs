//! Generates `cpp/moraine_abi.h` from the crate's `extern "C"` surface
//! with cbindgen. The committed header is build output kept in-tree so
//! the C++ shim's own build needs no cargo step; edit the Rust
//! definitions, not the header.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    if building_packaged_copy() {
        return;
    }

    if let Err(message) = generate() {
        eprintln!("generating cpp/moraine_abi.h failed: {message}");
        std::process::exit(1);
    }
}

/// Whether this build is of a `cargo package` tarball rather than the
/// working tree. Cargo extracts those to `target/package/<name>-<version>`,
/// which is the only signal it offers — there is no environment variable
/// saying "you are being packaged".
fn building_packaged_copy() -> bool {
    let Some(dir) = std::env::var_os("CARGO_MANIFEST_DIR") else {
        return false;
    };
    let dir = Path::new(&dir);
    let mut ancestors = dir.ancestors().skip(1);
    let parent = ancestors.next().and_then(Path::file_name);
    let grandparent = ancestors.next().and_then(Path::file_name);
    parent == Some("package".as_ref()) && grandparent == Some("target".as_ref())
}

fn generate() -> Result<(), String> {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|e| e.to_string())?;
    let crate_dir = Path::new(&crate_dir);

    let config = cbindgen::Config::from_file(crate_dir.join("cbindgen.toml"))?;
    let bindings = cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .map_err(|e| e.to_string())?;
    bindings.write_to_file(crate_dir.join("cpp/moraine_abi.h"));
    Ok(())
}
