//! Stable and joint quorum thresholds for leader commit advancement.

use super::super::state::ProgressSet;
use super::advance::quorum_replicated_index;
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
fn stable_quorum_replicated_index_is_quorum_threshold() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)],
            Vec::new(),
        )
        .expect("membership is valid"),
    );
    let progress = progress(&membership, &[(1, 10), (2, 8), (3, 6), (4, 4), (5, 2)]);

    assert_eq!(quorum_replicated_index(&progress), Some(LogIndex(6)),);
}

#[test]
fn joint_quorum_replicated_index_is_minimum_of_both_quorum_indexes() {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(3), NodeId(4), NodeId(5)], Vec::new())
        .expect("new membership is valid");
    let membership = MembershipConfig::joint(old, new);
    let progress = progress(&membership, &[(1, 10), (2, 9), (3, 1), (4, 8), (5, 7)]);

    assert_eq!(quorum_replicated_index(&progress), Some(LogIndex(7)),);
}

#[test]
fn unmatched_voter_progress_bounds_quorum_replicated_index_to_zero() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("membership is valid"),
    );
    let progress = progress(&membership, &[(1, 10)]);

    assert_eq!(quorum_replicated_index(&progress), Some(LogIndex::ZERO),);
}

/// Deliberately direct reference: sort each declared voter set independently.
/// No acknowledgement means zero, including a voter initialized by reset.
fn sorted_reference(membership: &MembershipConfig, observations: &[(u64, u64)]) -> LogIndex {
    let threshold = |voters: &[NodeId]| {
        let mut indexes = voters
            .iter()
            .map(|voter| {
                observations
                    .iter()
                    .find(|(id, _)| *id == voter.0)
                    .map_or(LogIndex::ZERO, |(_, index)| LogIndex(*index))
            })
            .collect::<Vec<_>>();
        indexes.sort_unstable();
        indexes[(indexes.len() - 1) / 2]
    };
    match membership {
        MembershipConfig::Stable(stable) => threshold(stable.voters()),
        MembershipConfig::Joint(joint) => {
            threshold(joint.old().voters()).min(threshold(joint.new_membership().voters()))
        }
    }
}

#[test]
fn selection_matches_sorted_reference_with_missing_and_duplicate_acknowledgements() {
    let set = |ids: &[u64]| {
        MembershipSet::new(ids.iter().copied().map(NodeId).collect(), vec![NodeId(6)]).unwrap()
    };
    let memberships = [
        MembershipConfig::stable(set(&[1])),
        MembershipConfig::stable(set(&[1, 2])),
        MembershipConfig::stable(set(&[1, 2, 3])),
        MembershipConfig::stable(set(&[1, 2, 3, 4, 5])),
        MembershipConfig::joint(set(&[1, 2, 3]), set(&[3, 4, 5])),
        MembershipConfig::joint(set(&[1, 2]), set(&[4, 5])),
    ];
    for membership in memberships {
        // Exhaust all five voters' choices: no report, zero, 3, or 9.
        for mut pattern in 0..4_u64.pow(5) {
            let mut observations = vec![(6, 100)]; // learner never contributes
            for voter in 1..=5 {
                let choice = pattern % 4;
                pattern /= 4;
                if choice != 0 {
                    observations.push((voter, [0, 0, 3, 9][choice as usize]));
                }
            }
            assert_eq!(
                quorum_replicated_index(&progress(&membership, &observations)),
                Some(sorted_reference(&membership, &observations)),
                "membership={membership:?}, observations={observations:?}"
            );
        }
    }
    // A completely absent progress layout establishes no quorum at all.
    assert_eq!(quorum_replicated_index(&ProgressSet::default()), None);
}
