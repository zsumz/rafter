//! Election-round state owned by the local node.
//!
//! This state is entirely volatile. It tracks the local election timer and the
//! vote or pre-vote grants collected by this node while campaigning. Durable
//! term and vote ownership remain in [`PersistentState`](super::PersistentState).

use std::collections::BTreeSet;

use crate::NodeId;

/// Volatile timer and grant sets for election and pre-vote rounds.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct ElectionState {
    elapsed: u64,
    votes: BTreeSet<NodeId>,
    pre_votes: BTreeSet<NodeId>,
}

impl ElectionState {
    /// Advances the local election timer, saturating at the integer limit.
    pub(in crate::node) fn advance_timeout(&mut self) -> u64 {
        self.elapsed = self.elapsed.saturating_add(1);
        self.elapsed
    }

    /// Returns the ticks since the last accepted authority or round reset.
    pub(in crate::node) const fn elapsed(&self) -> u64 {
        self.elapsed
    }

    /// Restarts the election timeout without changing collected grants.
    pub(in crate::node) fn reset_timeout(&mut self) {
        self.elapsed = 0;
    }

    /// Begins a pre-vote round with the local node's non-binding self-grant.
    pub(in crate::node) fn begin_pre_vote(&mut self, self_id: NodeId) {
        self.reset_timeout();
        self.pre_votes.clear();
        self.pre_votes.insert(self_id);
    }

    /// Begins a binding election with the local node's durable self-vote.
    pub(in crate::node) fn begin_election(&mut self, self_id: NodeId) {
        self.reset_timeout();
        self.votes.clear();
        self.votes.insert(self_id);
        self.pre_votes.clear();
    }

    /// Records one binding vote grant for the active election.
    pub(in crate::node) fn record_vote(&mut self, voter_id: NodeId) {
        self.votes.insert(voter_id);
    }

    /// Records one non-binding grant for the active pre-vote round.
    pub(in crate::node) fn record_pre_vote(&mut self, voter_id: NodeId) {
        self.pre_votes.insert(voter_id);
    }

    /// Returns the unique binding grants collected in this election.
    pub(in crate::node) fn votes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.votes.iter().copied()
    }

    /// Returns the unique grants collected in this pre-vote round.
    pub(in crate::node) fn pre_votes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.pre_votes.iter().copied()
    }

    /// Clears all campaign state when the node becomes a follower.
    pub(in crate::node) fn reset_for_follower(&mut self) {
        self.reset_timeout();
        self.votes.clear();
        self.pre_votes.clear();
    }

    /// Clears pre-vote state when this term's leadership begins.
    ///
    /// Binding election grants remain until step-down, matching their previous
    /// diagnostic state while the node is leader.
    pub(in crate::node) fn enter_leadership(&mut self) {
        self.reset_timeout();
        self.pre_votes.clear();
    }

    #[cfg(test)]
    pub(in crate::node) fn set_elapsed(&mut self, elapsed: u64) {
        self.elapsed = elapsed;
    }
}
