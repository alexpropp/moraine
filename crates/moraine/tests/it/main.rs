//! The integration suite: the public API against real SlateDB on
//! in-memory object storage. Object-storage-backed runs stay in the
//! separate `object_storage` target, which automation invokes by name.

mod fixtures;

mod cache;
mod catalog;
mod commit_concurrency;
mod crash_recovery;
mod data_files;
mod index_backfill;
mod macros;
mod measure;
mod staged_index_build;
mod views_options;
