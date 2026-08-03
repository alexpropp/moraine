//! Generated protobuf wire messages (see `proto/wal.proto` and `build.rs`).

#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::doc_markdown,
    clippy::module_name_repetitions
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/moraine.wal.rs"));
}

// `FoldValue` is public so a cursor store can persist it under its own
// framing; every other message stays internal to the envelope codec.
pub use generated::FoldValue;
pub(crate) use generated::{
    CommitValue, EnvelopeValue, LeaderAdvertValue, SlotPayloadValue, SlotWriteValue,
};
