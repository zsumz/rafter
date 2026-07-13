//! Commit-index advancement and ordered application output.

use crate::{ConfigurationEntry, LogEntryKind, MembershipConfig};

use super::super::{Node, Output, Role};
use super::tracker::CommitTracker;

impl Node {
    /// Drains application outputs for committed log entries that have not yet
    /// been applied in this process.
    ///
    /// This is the recovery companion to
    /// [`Node::from_bootstrap_applied_through`](crate::Node::from_bootstrap_applied_through):
    /// after constructing from durable state, call this once to replay
    /// committed entries above the application's durable applied floor without
    /// waiting for a later commit-index advance.
    ///
    /// # Panics
    ///
    /// Panics only if the committed index points beyond the retained log — a
    /// kernel bug or invalid bootstrap state, since bootstrap validation and
    /// log mutation maintain that invariant.
    #[must_use]
    pub fn drain_committed_outputs(&mut self) -> Vec<Output> {
        self.apply_committed()
    }

    pub(in crate::node) fn advance_commit_index(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.advance_commit_index_into(&mut outputs);
        outputs
    }

    pub(in crate::node) fn advance_commit_index_into(&mut self, outputs: &mut Vec<Output>) {
        self.refresh_leader_progress_index();

        let Some(committable_index) = CommitTracker::new(&self.leader.progress).committable_index()
        else {
            return;
        };
        if committable_index <= self.volatile.commit_index {
            return;
        }
        if self.term_at(committable_index) != Some(self.current_term()) {
            return;
        }

        self.volatile.commit_index = committable_index;
        self.apply_committed_into(outputs);
    }

    fn apply_committed(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.apply_committed_into(&mut outputs);
        outputs
    }

    pub(in crate::node) fn apply_committed_into(&mut self, outputs: &mut Vec<Output>) {
        while self.volatile.applied_index < self.volatile.commit_index {
            let index = self.volatile.applied_index.next();
            let Some(entry) = self.entry_at(index) else {
                break;
            };

            let entry_term = entry.term;
            let application_payload = match &entry.kind {
                LogEntryKind::Application(payload) => Some(payload.clone()),
                LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
            };
            let membership = entry
                .configuration_entry()
                .map(ConfigurationEntry::membership_config);

            self.volatile.applied_index = index;
            let local_proposal_id = self
                .volatile
                .local_proposals
                .remove(index)
                .and_then(|proposal| (proposal.term == entry_term).then_some(proposal.id));

            if let Some(payload) = application_payload {
                outputs.push(Output::Apply {
                    index,
                    term: entry_term,
                    payload,
                    local_proposal_id,
                });
            } else if let Some(membership) = membership {
                self.step_down_if_removed(&membership, outputs);
            }
        }
    }

    fn step_down_if_removed(
        &mut self,
        committed_membership: &MembershipConfig,
        outputs: &mut Vec<Output>,
    ) {
        if self.role() == Role::Leader && !committed_membership.contains_voter(self.id()) {
            outputs.extend(self.become_follower(self.current_term()));
        }
    }
}
