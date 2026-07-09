//! Crate error types: one enum, variants per failure domain.

/// Errors returned by moraine operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Another writer committed a conflicting change; the transaction can be
    /// retried against the new state.
    #[error("commit conflict: {0}")]
    CommitConflict(String),
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;
