//! Generated protobuf value messages (see `proto/moraine.proto` and
//! `build.rs`). One message type per key kind.

// dead_code: generates the full set of `ducklake_*` value messages; several
// are only exercised by the codec's proptest roundtrips until the catalog
// features that write them land.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::doc_markdown,
    clippy::module_name_repetitions,
    rustdoc::invalid_html_tags,
    dead_code
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/moraine.store.rs"));
}

// unused_imports: consumers arrive with `catalog`/`transaction`
#[allow(unused_imports)]
pub(crate) use generated::*;
