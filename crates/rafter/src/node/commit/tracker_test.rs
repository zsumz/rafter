//! Stable and joint quorum thresholds for leader commit advancement.

use super::super::state::ProgressSet;
use super::tracker::CommitTracker;
use crate::{LogIndex, MembershipConfig, MembershipSet, NodeId};

fn progress(membership: &MembershipConfig, indexes: &[(u64, u64)]) -> ProgressSet {
    let mut progress = ProgressSet::default();
    progress.reset(membership, NodeId(1), LogIndex(1), LogIndex::ZERO);
    for (node_id, match_index) in indexes {
        if let Some(replica) = progress.get_mut(NodeId(*node_id)) {
            replica.match_index = LogIndex(*match_index);
            replica.next_index = LogIndex(match_index.saturating_add(1));
        }
    }
    progress
}

#[test]
fn stable_candidate_is_quorum_threshold() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)],
            Vec::new(),
        )
        .expect("membership is valid"),
    );
    let progress = progress(&membership, &[(1, 10), (2, 8), (3, 6), (4, 4), (5, 2)]);

    assert_eq!(
        CommitTracker::new(&progress).committable_index(),
        Some(LogIndex(6)),
    );
}

#[test]
fn joint_candidate_is_min_of_both_thresholds() {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(3), NodeId(4), NodeId(5)], Vec::new())
        .expect("new membership is valid");
    let membership = MembershipConfig::joint(old, new);
    let progress = progress(&membership, &[(1, 10), (2, 9), (3, 1), (4, 8), (5, 7)]);

    assert_eq!(
        CommitTracker::new(&progress).committable_index(),
        Some(LogIndex(7)),
    );
}

#[test]
fn unmatched_voter_progress_bounds_threshold_to_zero() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("membership is valid"),
    );
    let progress = progress(&membership, &[(1, 10)]);

    assert_eq!(
        CommitTracker::new(&progress).committable_index(),
        Some(LogIndex::ZERO),
    );
}
