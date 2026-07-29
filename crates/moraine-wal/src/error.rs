//! The crate's error type: two failure domains, transport and corruption.

/// Errors returned by commit-slot log operations.
///
/// Neither message contains the substrings a SQL-shaped retry loop scans
/// for (`conflict`, `concurrent`, `unique`, `primary key`): losing a race
/// is an outcome, not an error, so nothing here is meant to be retried
/// blindly.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The put or get could not complete; a put's outcome is unknown.
    #[error("commit-slot log unavailable: {0}")]
    Transport(String),

    /// The slot's bytes are not a valid envelope.
    #[error("commit-slot log corruption: {0}")]
    Corruption(String),
}
