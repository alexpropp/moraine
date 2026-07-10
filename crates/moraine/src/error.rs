//! Crate error types: one enum, variants per failure domain.

/// Errors returned by moraine operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Another writer committed a conflicting change; the transaction can be
    /// retried against the new state.
    #[error("commit conflict: {0}")]
    CommitConflict(String),

    /// Stored bytes failed to decode: corrupt, truncated, wrong-kind, or
    /// written by a newer encoding than this binary understands.
    #[error("corruption: {0}")]
    Corruption(String),

    /// The underlying store failed (SlateDB / object-store I/O).
    #[error("store error")]
    Store(#[source] Box<slatedb::Error>),
}

impl From<slatedb::Error> for Error {
    fn from(err: slatedb::Error) -> Self {
        Self::Store(Box::new(err))
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;
