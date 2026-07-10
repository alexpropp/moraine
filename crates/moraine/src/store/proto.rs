//! Generated protobuf value messages (see `proto/moraine.proto` and
//! `build.rs`). One message type per key kind.

#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::doc_markdown,
    clippy::module_name_repetitions,
    rustdoc::invalid_html_tags
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/moraine.store.rs"));
}

// unused_imports: consumers arrive with `catalog`/`txn`; tests use it now.
#[allow(unused_imports)]
pub(crate) use generated::*;
