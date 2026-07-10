use crate::LogIndex;

use super::state::{ProgressSet, SlotSet};

/// Derived leader-side commit threshold.
///
/// `CommitTracker` is intentionally rebuilt from effective membership and
/// progress whenever the leader tries to advance commit. It owns no protocol
/// state; it only names the Raft quorum rule in the shape the hot path needs:
/// the quorum-th largest voter match index.
pub(super) struct CommitTracker<'a> {
    progress: &'a ProgressSet,
}

impl<'a> CommitTracker<'a> {
    pub(super) const fn new(progress: &'a ProgressSet) -> Self {
        Self { progress }
    }

    /// Returns the highest index known on a valid stable or joint quorum.
    pub(super) fn candidate(&self) -> Option<LogIndex> {
        let membership = self.progress.index();
        let old_candidate = self.quorum_candidate(membership.old_voters())?;
        let Some(new_voters) = membership.new_voters() else {
            return Some(old_candidate);
        };
        let new_candidate = self.quorum_candidate(new_voters)?;
        Some(old_candidate.min(new_candidate))
    }

    fn quorum_candidate(&self, voters: &SlotSet) -> Option<LogIndex> {
        let quorum = (voters.count() / 2) + 1;
        let mut match_indexes = self.progress.match_indexes_for(voters);
        if match_indexes.len() < quorum {
            return None;
        }
        let threshold = quorum - 1;
        let (_, candidate, _) =
            match_indexes.select_nth_unstable_by(threshold, |left, right| right.cmp(left));
        Some(*candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::ProgressSet;
    use super::*;
    use crate::{MembershipConfig, MembershipSet, NodeId};

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

        assert_eq!(CommitTracker::new(&progress).candidate(), Some(LogIndex(6)));
    }

    #[test]
    fn joint_candidate_is_min_of_both_thresholds() {
        let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("old membership is valid");
        let new = MembershipSet::new(vec![NodeId(3), NodeId(4), NodeId(5)], Vec::new())
            .expect("new membership is valid");
        let membership = MembershipConfig::joint(old, new);
        let progress = progress(&membership, &[(1, 10), (2, 9), (3, 1), (4, 8), (5, 7)]);

        assert_eq!(CommitTracker::new(&progress).candidate(), Some(LogIndex(7)));
    }

    #[test]
    fn unmatched_voter_progress_bounds_threshold_to_zero() {
        let membership = MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
                .expect("membership is valid"),
        );
        let progress = progress(&membership, &[(1, 10)]);

        assert_eq!(
            CommitTracker::new(&progress).candidate(),
            Some(LogIndex::ZERO)
        );
    }
}
