//! The commit protocol: turns catalog mutations into one atomic store
//! write, with conflict classification and bounded benign-race retry.

pub(crate) mod commit;
pub(crate) mod operations;
pub(crate) mod staged;
mod verbs;

pub use verbs::Transaction;
