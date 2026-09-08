//! Ordered effects for the committed prefix, without executing the application.

use crate::{LogEntryKind, LogIndex, MembershipConfig};

use super::super::{Node, Output, Role};

impl Node {
    /// Drains outputs for committed entries above the core dispatch cursor.
    /// This prepares effects; the embedding executes them after persistence.
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
        let mut outputs = Vec::new();
        self.emit_committed_outputs(&mut outputs);
        outputs
    }

    pub(in crate::node) fn emit_committed_outputs(&mut self, outputs: &mut Vec<Output>) {
        while self.volatile.dispatched_index < self.volatile.commit_index {
            let index = self.volatile.dispatched_index.next();
            let Some(entry) = self.entry_at(index) else {
                break;
            };

            let entry_term = entry.term;
            let application_payload = match &entry.kind {
                LogEntryKind::Application(payload) => Some(payload.clone()),
                LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
            };
            // The entry itself, not just the membership it resolves to. The
            // configuration's own identity travels to the embedder, so a
            // consumer can tell two configurations apart that happen to name the
            // same replicas.
            let configuration = entry.configuration_entry().cloned();

            self.volatile.dispatched_index = index;
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
            } else if let Some(configuration) = configuration {
                // Announced before the step-down it may cause, because the
                // commit is the fact and the step-down is its consequence: an
                // embedder reading these in order sees why it lost leadership
                // after it has seen the configuration that took it away.
                let membership = configuration.membership_config();
                outputs.push(Output::ConfigurationCommitted {
                    index,
                    term: entry_term,
                    previous: self.membership_before(index),
                    configuration,
                });
                self.step_down_if_removed(&membership, outputs);
            }
        }
    }

    /// Historical predecessor for this entry, resolved from the retained log,
    /// snapshot membership, or bootstrap membership. Comparing a replayed
    /// transition against the consumer's later membership can retire live IDs.
    fn membership_before(&self, index: LogIndex) -> MembershipConfig {
        self.membership_at_index(LogIndex(index.0.saturating_sub(1)))
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
