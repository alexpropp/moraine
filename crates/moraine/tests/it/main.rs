//! The integration suite: the public API against real SlateDB on
//! in-memory object storage. Object-storage-backed runs stay in the
//! separate `object_storage` target, which automation invokes by name.

mod fixtures;

mod catalog;
mod coalescer_bench;
mod commit_concurrency;
mod data_files;
mod index_backfill;
mod macros;
mod multi_writer;
mod staged_index_build;
mod views_options;
