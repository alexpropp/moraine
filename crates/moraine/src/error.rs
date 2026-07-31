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

    /// A DuckLake feature moraine does not implement (e.g. inlining a
    /// `VARIANT` column, RFC 0005). Terminal: re-running cannot help.
    ///
    /// Like every non-conflict variant, the message avoids the four
    /// substrings DuckLake's commit loop retries on (`conflict`,
    /// `concurrent`, `unique`, `primary key`) — a caller must not word a
    /// payload so it trips a pointless re-drive.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A held or requested snapshot fell below the retention horizon and
    /// its record is gone; the reader must re-resolve from head rather
    /// than dereference reclaimed files. Not a conflict — the message
    /// stays clear of DuckLake's retry substrings.
    #[error("snapshot expired: {0}")]
    SnapshotExpired(String),

    /// A host interrupt cancelled the operation before its point of no
    /// return. Distinct from a store failure so the bridge can raise
    /// DuckDB's interrupt, and free of retry substrings — an interrupted
    /// commit whose durability is ambiguous must never be re-driven as a
    /// conflict.
    #[error("interrupted: {0}")]
    Interrupted(String),

    /// The store requires, is undergoing, or was written by a structural
    /// format this binary does not support (RFC 0015). Terminal, and free
    /// of retry substrings: a migration is never a commit conflict.
    #[error("migration required: {0}")]
    Migration(String),

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

#[cfg(test)]
mod tests {
    use super::Error;

    /// The substrings DuckLake's commit loop lowercases the error message
    /// and scans for, retrying the commit if any is present.
    const RETRY_SUBSTRINGS: [&str; 4] = ["conflict", "concurrent", "unique", "primary key"];

    /// Only `CommitConflict` may carry a retry substring: it is the one
    /// error DuckLake should re-drive. Every other variant that can surface
    /// from a commit must be free of them, or an unretryable failure gets
    /// retried until the budget is spent. This pins the wording as the wire
    /// contract it is.
    #[test]
    fn only_commit_conflict_carries_a_retry_substring() {
        let sample = "index \"unique_by_primary key\" saw a concurrent conflict";
        let non_retryable = [
            Error::RetryBudgetExhausted(sample.into()),
            Error::Corruption(sample.into()),
            Error::NotFound(sample.into()),
            Error::AlreadyExists(sample.into()),
            Error::Constraint(sample.into()),
            Error::IndexBuilding(sample.into()),
            Error::Configuration(sample.into()),
            Error::Fenced(sample.into()),
            Error::Unsupported(sample.into()),
            Error::SnapshotExpired(sample.into()),
            Error::Interrupted(sample.into()),
            Error::Migration(sample.into()),
        ];

        // The variants' own prefixes carry no retry substring — a payload
        // that does is the caller's responsibility, so this asserts the
        // prefix alone by stripping the shared sample.
        for err in non_retryable {
            let rendered = err.to_string();
            let prefix = rendered
                .strip_suffix(sample)
                .expect("every variant renders as `<prefix>{sample}`")
                .to_lowercase();
            for needle in RETRY_SUBSTRINGS {
                assert!(
                    !prefix.contains(needle),
                    "{prefix:?} carries retry substring {needle:?}"
                );
            }
        }

        assert!(
            Error::CommitConflict(String::new())
                .to_string()
                .contains("conflict"),
            "CommitConflict must stay retryable"
        );
    }
}
