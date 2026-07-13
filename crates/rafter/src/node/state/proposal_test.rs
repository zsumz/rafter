//! Ordered local proposal correlation independent of replicated protocol state.

use super::proposal::{LocalProposal, LocalProposalTracker};
use crate::{LocalProposalId, LogIndex, Term};

fn proposal(id: u64) -> LocalProposal {
    LocalProposal {
        term: Term(1),
        id: LocalProposalId(id),
    }
}

#[test]
fn local_proposal_tracker_keeps_index_order() {
    let mut tracker = LocalProposalTracker::default();

    tracker.insert(LogIndex(3), proposal(3));
    tracker.insert(LogIndex(1), proposal(1));
    tracker.insert(LogIndex(2), proposal(2));
    tracker.insert(LogIndex(2), proposal(20));

    let entries = tracker.into_iter().collect::<Vec<_>>();
    assert_eq!(
        entries,
        vec![
            (LogIndex(1), proposal(1)),
            (LogIndex(2), proposal(20)),
            (LogIndex(3), proposal(3)),
        ],
    );
}

#[test]
fn local_proposal_tracker_removes_by_index() {
    let mut tracker = LocalProposalTracker::default();
    tracker.insert(LogIndex(1), proposal(1));
    tracker.insert(LogIndex(2), proposal(2));
    tracker.insert(LogIndex(3), proposal(3));

    assert_eq!(tracker.remove(LogIndex(1)), Some(proposal(1)));
    assert_eq!(tracker.remove(LogIndex(3)), Some(proposal(3)));
    assert_eq!(tracker.remove(LogIndex(9)), None);
    assert_eq!(
        tracker.into_iter().collect::<Vec<_>>(),
        vec![(LogIndex(2), proposal(2))],
    );
}

#[test]
fn local_proposal_tracker_split_off_returns_suffix() {
    let mut tracker = LocalProposalTracker::default();
    tracker.insert(LogIndex(1), proposal(1));
    tracker.insert(LogIndex(2), proposal(2));
    tracker.insert(LogIndex(4), proposal(4));

    let suffix = tracker.split_off(LogIndex(3));

    assert_eq!(
        tracker.into_iter().collect::<Vec<_>>(),
        vec![(LogIndex(1), proposal(1)), (LogIndex(2), proposal(2))],
    );
    assert_eq!(
        suffix.into_iter().collect::<Vec<_>>(),
        vec![(LogIndex(4), proposal(4))],
    );
}
