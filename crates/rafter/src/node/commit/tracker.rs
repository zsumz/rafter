//! Quorum-derived leader commit thresholds.

use crate::LogIndex;

use super::super::state::{ProgressSet, SlotSet};

/// Derived leader-side commit threshold.
///
/// `CommitTracker` is rebuilt from effective membership and replication
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
    pub(super) fn committable_index(&self) -> Option<LogIndex> {
        let membership = self.progress.index();
        let old_quorum_index = self.quorum_index(membership.old_voters())?;
        let Some(new_voters) = membership.new_voters() else {
            return Some(old_quorum_index);
        };
        let new_quorum_index = self.quorum_index(new_voters)?;
        Some(old_quorum_index.min(new_quorum_index))
    }

    fn quorum_index(&self, voters: &SlotSet) -> Option<LogIndex> {
        let quorum = (voters.count() / 2) + 1;
        let mut match_indexes = self.progress.match_indexes_for(voters);
        if match_indexes.len() < quorum {
            return None;
        }

        let threshold = quorum - 1;
        let (_, quorum_index, _) =
            match_indexes.select_nth_unstable_by(threshold, |left, right| right.cmp(left));
        Some(*quorum_index)
    }
}
