//! Client and administrative inputs accepted by a simulated cluster.
//!
//! Each entry point steps exactly one node and hands the resulting outputs to
//! the recorder, so every externally driven transition is witnessed the same
//! way regardless of which caller issued it.

use rafter::{ConfigurationEntry, Input, MembershipSet, NodeId, PromotionBarrier, ReadId};

use crate::{Cluster, ReadRegistered, SimTick};

impl Cluster {
    /// Delivers one logical tick to `node_id`.
    pub fn tick(&mut self, node_id: NodeId) {
        self.clock.advance();
        let outputs = self.node_mut(node_id).step(Input::Tick);
        self.record_outputs(node_id, outputs);
    }

    /// Advances the simulator clock without stepping any node.
    pub fn advance_clock(&mut self) -> SimTick {
        self.clock.advance()
    }

    /// Submits an application proposal to `node_id`.
    pub fn propose(&mut self, node_id: NodeId, payload: Vec<u8>) {
        let outputs = self
            .node_mut(node_id)
            .step(Input::ClientProposal { payload });
        self.record_outputs(node_id, outputs);
    }

    /// Registers a read barrier on `node_id`.
    pub fn read_index(&mut self, node_id: NodeId, request_id: u64) -> ReadRegistered {
        // Record the cluster-wide committed floor at registration: the
        // freshness bar any eventual grant must clear.
        let committed_floor = self.committed_floor();
        let operation_id = self.read_registrations.len() as u64;
        let registration = ReadRegistered {
            node_id,
            operation_id,
            request_id,
            committed_floor,
        };
        self.read_registrations.push(registration.clone());
        let outputs = self.node_mut(node_id).step(Input::ReadIndex {
            read_id: ReadId(request_id),
        });
        self.record_outputs(node_id, outputs);
        registration
    }

    /// Asks `node_id` to transfer leadership to `target`.
    pub fn transfer_leadership(&mut self, node_id: NodeId, target: NodeId) {
        let outputs = self
            .node_mut(node_id)
            .step(Input::TransferLeadership { target });
        self.record_outputs(node_id, outputs);
    }

    /// Submits a raw configuration entry directly to `node_id`.
    pub fn dangerous_raw_configuration_proposal(
        &mut self,
        node_id: NodeId,
        configuration: ConfigurationEntry,
        promotion_barriers: Vec<PromotionBarrier>,
    ) {
        let outputs = self
            .node_mut(node_id)
            .step(Input::DangerousRawConfigurationProposal {
                configuration,
                promotion_barriers,
            });
        self.record_outputs(node_id, outputs);
    }

    /// Proposes adding `learner_id` as a learner through `node_id`.
    pub fn add_learner(&mut self, node_id: NodeId, learner_id: NodeId) {
        let outputs = self
            .node_mut(node_id)
            .step(Input::AddLearner { learner_id });
        self.record_outputs(node_id, outputs);
    }

    /// Proposes promoting `learner_id` to a voter through `node_id`.
    pub fn promote_learner(
        &mut self,
        node_id: NodeId,
        learner_id: NodeId,
        promotion_barrier: PromotionBarrier,
    ) {
        let outputs = self.node_mut(node_id).step(Input::PromoteLearner {
            learner_id,
            promotion_barrier,
        });
        self.record_outputs(node_id, outputs);
    }

    /// Proposes removing `voter_id` from the voter set through `node_id`.
    pub fn remove_voter(&mut self, node_id: NodeId, voter_id: NodeId) {
        let outputs = self.node_mut(node_id).step(Input::RemoveVoter { voter_id });
        self.record_outputs(node_id, outputs);
    }

    /// Proposes entering joint consensus toward `target` through `node_id`.
    pub fn enter_joint(
        &mut self,
        node_id: NodeId,
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    ) {
        let outputs = self.node_mut(node_id).step(Input::EnterJoint {
            target,
            promotion_barriers,
        });
        self.record_outputs(node_id, outputs);
    }

    /// Proposes leaving joint consensus through `node_id`.
    pub fn leave_joint(&mut self, node_id: NodeId) {
        let outputs = self.node_mut(node_id).step(Input::LeaveJoint);
        self.record_outputs(node_id, outputs);
    }

    /// Proposes a direct membership change through `node_id`.
    pub fn change_membership(
        &mut self,
        node_id: NodeId,
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    ) {
        let outputs = self.node_mut(node_id).step(Input::ChangeMembership {
            target,
            promotion_barriers,
        });
        self.record_outputs(node_id, outputs);
    }
}
