//! Leader authority evidence used by leases and check-quorum.
//!
//! This module interprets same-term acknowledgements as authority evidence; it
//! does not advance follower log progress.

use crate::NodeId;

use super::super::{Node, Output};

impl Node {
    /// Feeds a follower acknowledgement to the lease checkpoint machine.
    ///
    /// A quorum confirms the pending basis and re-arms the next checkpoint at
    /// the current leader tick.
    pub(super) fn acknowledge_read_lease(&mut self, follower_id: NodeId, sequence: u64) {
        if !self.config.lease_reads() {
            return;
        }

        let membership = self.effective_membership();
        let self_id = self.id();
        if !self
            .leader
            .lease
            .record_ack(follower_id, sequence, &membership, self_id)
        {
            return;
        }
        if !self
            .leader
            .lease
            .acks
            .has_quorum_with_self(&membership, self_id)
        {
            return;
        }

        let current_tick = self.leader.ticks;
        let next_heartbeat_sequence = self.leader.heartbeat_sequence + 1;
        self.leader
            .lease
            .confirm_and_rearm(current_tick, next_heartbeat_sequence);
    }

    /// Re-arms a lease checkpoint whose basis has aged past the lease window.
    pub(in crate::node) fn tick_read_lease(&mut self) {
        if !self.config.lease_reads() {
            return;
        }

        let basis_age = self
            .leader
            .ticks
            .saturating_sub(self.leader.lease.pending_basis_tick);
        if basis_age < self.config.read_lease_ticks() {
            return;
        }

        let current_tick = self.leader.ticks;
        let next_heartbeat_sequence = self.leader.heartbeat_sequence + 1;
        self.leader
            .lease
            .rearm(current_tick, next_heartbeat_sequence);
    }

    /// Returns step-down outputs when a full election timeout passes without
    /// hearing from an effective quorum (thesis 6.2).
    pub(in crate::node) fn tick_check_quorum(&mut self) -> Option<Vec<Output>> {
        if !self.config.check_quorum() {
            return None;
        }

        self.leader.quorum_check_elapsed += 1;
        if self.leader.quorum_check_elapsed < self.config.election_timeout_ticks() {
            return None;
        }

        let membership = self.effective_membership();
        let self_id = self.id();
        if self
            .leader
            .quorum_acks
            .has_quorum_with_self(&membership, self_id)
        {
            self.leader.quorum_acks.clear();
            self.leader.quorum_check_elapsed = 0;
            return None;
        }

        // This same-term step-down deliberately forgets the self-hint. The
        // node has just proved that no current leader can reach a quorum
        // through it, so it may grant a new pre-vote immediately.
        let outputs = self.become_follower(self.current_term());
        self.volatile.leader_hint = None;
        Some(outputs)
    }

    pub(super) fn record_quorum_ack(&mut self, follower_id: NodeId) {
        let self_id = self.id();
        if self.derived.configuration.is_empty() {
            let membership = self
                .persistent
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.metadata.committed_membership())
                .unwrap_or_else(|| self.config.static_membership_ref());
            self.leader
                .quorum_acks
                .insert(follower_id, membership, self_id);
            return;
        }

        let membership = self.effective_membership();
        self.leader
            .quorum_acks
            .insert(follower_id, &membership, self_id);
    }
}
