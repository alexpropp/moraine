//! The SlateDB layer: key layout and value codecs.
//!
//! Knows nothing about DuckLake semantics.

// dead_code: consumed by `catalog`/`txn` in later slices; drop the allow
// once the first caller lands.
#[allow(dead_code)]
pub(crate) mod frame;
#[allow(dead_code)]
pub(crate) mod key;
#[allow(dead_code)]
pub(crate) mod open;
#[allow(dead_code)]
pub(crate) mod proto;
#[allow(dead_code)]
pub(crate) mod segment;
#[allow(dead_code)]
pub(crate) mod value;
