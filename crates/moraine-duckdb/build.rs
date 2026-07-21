//! Generates `cpp/moraine_abi.h` from the crate's `extern "C"` surface
//! with cbindgen. The committed header is build output kept in-tree so
//! the C++ shim's own build needs no cargo step; edit the Rust
//! definitions, not the header.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    if let Err(message) = generate() {
        eprintln!("generating cpp/moraine_abi.h failed: {message}");
        std::process::exit(1);
    }
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
