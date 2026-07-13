//! Commit-advance evidence carried by successful append acknowledgements.

use super::response::successful_ack_can_advance_commit;
use crate::LogIndex;

#[test]
fn commit_advance_requires_new_evidence() {
    assert!(successful_ack_can_advance_commit(
        LogIndex(2),
        LogIndex(5),
        LogIndex(4),
    ));
    assert!(!successful_ack_can_advance_commit(
        LogIndex(5),
        LogIndex(5),
        LogIndex(4),
    ));
    assert!(!successful_ack_can_advance_commit(
        LogIndex(2),
        LogIndex(4),
        LogIndex(4),
    ));
}
