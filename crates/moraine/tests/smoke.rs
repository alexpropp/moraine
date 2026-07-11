//! Integration tests: exercise the public API only.

use moraine::Error;

#[test]
fn commit_conflict_displays_context() {
    let err = Error::CommitConflict("snapshot 42".to_string());
    assert_eq!(err.to_string(), "commit conflict: snapshot 42");
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
