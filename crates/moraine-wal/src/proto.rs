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

// `FoldValue` is the one message that crosses the crate boundary: a cursor
// store persists it under its own framing, so it must be a public
// `prost::Message`. Every other message is an implementation detail of the
// envelope codec.
pub use generated::FoldValue;
pub(crate) use generated::{CommitValue, EnvelopeValue, SlotPayloadValue, SlotWriteValue};
