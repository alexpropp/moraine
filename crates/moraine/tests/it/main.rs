//! The integration suite: the public API against real SlateDB on
//! in-memory object storage. Object-storage-backed runs stay in the
//! separate `object_storage` target, which automation invokes by name.

mod fixtures;

mod catalog;
mod commit_concurrency;
mod data_files;
mod macros;
mod views_options;
