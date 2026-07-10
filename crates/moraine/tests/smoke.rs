//! Integration tests: exercise the public API only.

use moraine::Error;

#[test]
fn commit_conflict_displays_context() {
    let err = Error::CommitConflict("snapshot 42".to_string());
    assert_eq!(err.to_string(), "commit conflict: snapshot 42");
}
