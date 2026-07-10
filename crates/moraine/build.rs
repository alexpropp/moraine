//! Compiles the store's protobuf value schemas with `protox` (a pure-Rust
//! protobuf front-end feeding `prost-build`).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/moraine.proto");
    let descriptors = protox::compile(["proto/moraine.proto"], ["proto/"])?;
    let mut config = prost_build::Config::new();
    // Test builds derive proptest strategies for every message, so the
    // per-message roundtrip property tests need no hand-written strategies.
    config.type_attribute(".", "#[cfg_attr(test, derive(proptest_derive::Arbitrary))]");
    config.compile_fds(descriptors)?;
    Ok(())
}
