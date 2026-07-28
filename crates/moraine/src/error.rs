//! Crate error types: one enum, variants per failure domain.
use tracing::warn;

/// Errors returned by moraine operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Another writer committed a conflicting change; the transaction can be
    /// retried against the new state.
    #[error("commit conflict: {0}")]
    CommitConflict(String),

    /// A commit spent its whole internal retry budget on benign races
    /// without settling; the caller must re-drive the work itself, usually
    /// as smaller commits.
    ///
    /// The text carries none of the four substrings DuckLake's commit loop
    /// keys its retry decision on (`conflict`, `concurrent`, `unique`,
    /// `primary key`), so an exhausted budget surfaces at once instead of
    /// being re-run against a premise that already failed to settle ten
    /// times. That wording is part of the wire contract, not incidental
    /// diagnostics.
    #[error("retry budget exhausted: {0}")]
    RetryBudgetExhausted(String),

    /// Stored bytes failed to decode: corrupt, truncated, wrong-kind, or
    /// written by a newer encoding than this binary understands.
    #[error("corruption: {0}")]
    Corruption(String),

    /// An operation referenced an entity that does not exist (or is not
    /// live in the transaction's view).
    #[error("not found: {0}")]
    NotFound(String),

    /// An operation would violate name uniqueness.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// An operation would violate a structural constraint (e.g. dropping
    /// a schema that still contains tables).
    #[error("constraint violation: {0}")]
    Constraint(String),

    /// A lookup targeted an index whose staged backfill has not completed;
    /// it serves no reads until it flips ready.
    #[error("index building: {0}")]
    IndexBuilding(String),

    /// An environment or option value could not be parsed or is out of
    /// range.
    #[error("configuration: {0}")]
    Configuration(String),

    /// This writer has been fenced: another process opened the store
    /// read-write, and the newest writer wins.
    #[error("writer fenced: {0}")]
    Fenced(String),

    /// The underlying store failed (SlateDB / object-store I/O).
    #[error("store error")]
    Store(#[source] Box<slatedb::Error>),
}

impl From<slatedb::Error> for Error {
    fn from(err: slatedb::Error) -> Self {
        // Every store error crosses here, so this is the one place fencing
        // can be told apart from ordinary I/O failure.
        if err.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) {
            warn!("another process attached this catalog read-write; this writer is fenced");
            return Self::Fenced(
                "another process attached this catalog read-write and took over as \
                 the writer; this handle can no longer commit — re-attach to write"
                    .to_string(),
            );
        }
        Self::Store(Box::new(err))
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;
