//! Integration tests: exercise the public API only.

use moraine::Error;

#[test]
fn commit_conflict_displays_context() {
    let err = Error::CommitConflict("snapshot 42".to_string());
    assert_eq!(err.to_string(), "commit conflict: snapshot 42");
}

/// DuckLake's commit loop re-runs a failed commit whenever the error text
/// contains one of four substrings. A true conflict is retryable and keeps
/// them; an exhausted retry budget is terminal and must carry none, or
/// DuckLake spends its own budget re-running a commit that cannot settle.
#[test]
fn retry_budget_exhausted_avoids_ducklake_retry_substrings() {
    let text = Error::RetryBudgetExhausted("spent 10 attempts".to_string()).to_string();
    for substring in ["conflict", "concurrent", "unique", "primary key"] {
        assert!(
            !text.contains(substring),
            "{text:?} contains DuckLake's retry substring {substring:?}"
        );
    }
}

/// The transient counterpart still carries `conflict`: DuckLake retrying a
/// genuine race is correct, and the text is the wire contract that asks it to.
#[test]
fn commit_conflict_keeps_the_retry_substring() {
    assert!(
        Error::CommitConflict("concurrent commit 7 touched the same state".to_string())
            .to_string()
            .contains("conflict")
    );
}

#[test]
fn logical_errors_display_context() {
    assert_eq!(
        Error::NotFound("table 9".to_string()).to_string(),
        "not found: table 9"
    );
    assert_eq!(
        Error::AlreadyExists("schema sales".to_string()).to_string(),
        "already exists: schema sales"
    );
    assert_eq!(
        Error::Constraint("cannot drop the last column".to_string()).to_string(),
        "constraint violation: cannot drop the last column"
    );
}
