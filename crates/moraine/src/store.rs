//! The SlateDB layer: key layout and value codecs.
//!
//! Knows nothing about DuckLake semantics.

pub(crate) mod frame;
pub(crate) mod inline;
pub(crate) mod key;
pub(crate) mod open;
pub(crate) mod proto;
pub(crate) mod read;
pub(crate) mod segment;
pub(crate) mod value;
