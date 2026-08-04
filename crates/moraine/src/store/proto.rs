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
// `pub` so `ffi_support` can re-export them as the ABI crate's row types;
// reachable only through that `#[doc(hidden)]` seam.
#[allow(unused_imports)]
pub(crate) use generated::*;
pub use generated::{
    ColumnValue, DataFileValue, DeleteFileValue, FileColumnStatsValue, GcFileValue, MacroValue,
    MappingValue, PartitionValue, SchemaValue, SnapshotValue, SortValue, TableColumnStatsValue,
    TableStatsValue, TableValue, ViewValue,
};
