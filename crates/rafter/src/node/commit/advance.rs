//! Leader commit authorization beside its stable and joint quorum calculation.

use crate::LogIndex;

use super::super::state::{ProgressSet, SlotSet};
use super::super::{Node, Output};

impl Node {
    pub(in crate::node) fn advance_commit_index(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.advance_commit_index_into(&mut outputs);
        outputs
    }

    pub(in crate::node) fn advance_commit_index_into(&mut self, outputs: &mut Vec<Output>) {
        self.reconcile_replication_progress();

        let Some(candidate) = quorum_replicated_index(&self.leader.progress) else {
            return;
        };
        if candidate <= self.volatile.commit_index {
            return;
        }
        // CM-03: replication alone cannot commit an older-term candidate.
        // A current-term entry establishes commitment for its whole prefix.
        if self.term_at(candidate) != Some(self.current_term()) {
            return;
        }

        self.volatile.commit_index = candidate;
        self.emit_committed_outputs(outputs);
    }
}

/// Highest index acknowledged by a valid quorum; not yet permission to commit.
pub(super) fn quorum_replicated_index(progress: &ProgressSet) -> Option<LogIndex> {
    let membership = progress.index();
    let old_quorum_index = quorum_index(progress, membership.old_voters())?;
    let Some(new_voters) = membership.new_voters() else {
        return Some(old_quorum_index);
    };
    // CM-02 / MB-02: both constituent majorities must cover the candidate.
    // A majority of their union does not establish either individual quorum.
    let new_quorum_index = quorum_index(progress, new_voters)?;
    Some(old_quorum_index.min(new_quorum_index))
}

fn quorum_index(progress: &ProgressSet, voters: &SlotSet) -> Option<LogIndex> {
    let quorum = (voters.count() / 2) + 1;
    let mut match_indexes = progress.match_indexes_for(voters);
    if match_indexes.len() < quorum {
        return None;
    }

    let threshold = quorum - 1;
    let (_, quorum_index, _) =
        match_indexes.select_nth_unstable_by(threshold, |left, right| right.cmp(left));
    Some(*quorum_index)
}
