//! Deterministic simulation harness for the Rafter consensus stack.
//!
//! This crate owns in-memory cluster simulation, scheduled message delivery,
//! restart/fault modeling, and model-check/soak support for the deterministic
//! kernel and integration layers. It does not provide production storage,
//! networking, runtime APIs, or application embedding contracts; those remain
//! in `rafter-storage`, `rafter-runtime-api`, `rafter-runtime`, `rafter-app`,
//! and `rafter-service`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rafter::{
    BootstrapState, ConfigurationEntry, InMemorySnapshotChunkSource, Input, LogIndex,
    MembershipSet, Node, NodeConfig, NodeId, PromotionBarrier, ReadId,
};

/// Disk fault models used by the deterministic protocol simulator.
pub mod disk_fault;
mod inspection;
/// Bounded model-checking, replay, soak, and TLA projection helpers.
pub mod model_check;
mod network;
mod records;
mod restart;
mod snapshot;
mod time;

pub use network::Envelope;
use network::QueuedEnvelope;
pub use records::{
    Applied, DurableSnapshotDigest, DurableStateDigest, ExecutedLogEntry, ExecutionWitness,
    ReadGranted, ReadRegistered, ReferenceState, SnapshotInstalled,
};
use records::{ExecutionCursor, StagedSnapshotTransfer};
use time::SimRng;
pub use time::{SimClock, SimSeed, SimTick};

/// Deterministic in-memory Raft cluster used by tests, soaks, and explorers.
#[derive(Clone, Debug, Hash)]
pub struct Cluster {
    clock: SimClock,
    configs: BTreeMap<NodeId, NodeConfig>,
    nodes: BTreeMap<NodeId, Node>,
    network: VecDeque<QueuedEnvelope>,
    rng: SimRng,
    applied: Vec<Applied>,
    /// Append-only exact application/configuration execution history used by
    /// the AP-02 reference-contract oracle.
    execution_history: Vec<ExecutionWitness>,
    /// Per-node live cursor used only to derive the next immutable execution
    /// witness. Restart and snapshot transitions replace the cursor without
    /// deleting prior history.
    execution_cursors: BTreeMap<NodeId, ExecutionCursor>,
    /// Canonical empty application/configuration state for each static node
    /// configuration. Application-state loss without a snapshot resets here.
    initial_reference_states: BTreeMap<NodeId, ReferenceState>,
    /// Per-node application incarnation. Explicit application-state-loss
    /// restarts advance this while ordinary process restarts preserve it.
    application_epochs: BTreeMap<NodeId, u64>,
    /// Per-node durable application floor. Plain restarts preserve this
    /// state-machine durability and replay only committed entries above it.
    durable_applied: BTreeMap<NodeId, LogIndex>,
    snapshot_installs: Vec<SnapshotInstalled>,
    /// Per-node durable snapshot payload stores. The kernel holds only
    /// descriptors; leader chunk directives resolve against the sending
    /// node's store, and installed snapshots are registered in the
    /// receiving node's store so it can serve them if it later leads.
    snapshot_sources: BTreeMap<NodeId, InMemorySnapshotChunkSource>,
    /// Per-node staging areas for in-flight inbound snapshot transfers.
    /// Volatile like the kernel's transfer bookkeeping: dropped on plain
    /// restarts unless the harness explicitly models a durable resume.
    snapshot_staging: BTreeMap<NodeId, StagedSnapshotTransfer>,
    read_grants: Vec<ReadGranted>,
    read_registrations: Vec<ReadRegistered>,
    /// Directional blocked pairs: a sustained partition drops traffic at
    /// enqueue until healed, so it holds across elections.
    blocked_pairs: BTreeSet<(NodeId, NodeId)>,
    /// The highest match index each follower has CONFIRMED to a leader — the
    /// acknowledgement envelope was actually delivered, not merely sent. A
    /// legal lossy restart also preserves the node's local committed prefix;
    /// everything above those floors is loss the protocol must tolerate.
    delivered_ack_floor: BTreeMap<NodeId, LogIndex>,
    /// Bootstrap states captured by [`Cluster::mark_synced`] for the
    /// design-note lossy restart shape.
    synced_marks: BTreeMap<NodeId, BootstrapState>,
}

impl Cluster {
    /// Builds a cluster with the default deterministic seed.
    #[must_use]
    pub fn new(configs: Vec<NodeConfig>) -> Self {
        Self::new_with_seed(configs, SimSeed::default())
    }

    /// Builds a cluster with an explicit deterministic seed.
    #[must_use]
    pub fn new_with_seed(configs: Vec<NodeConfig>, seed: SimSeed) -> Self {
        let configs_by_id = configs
            .iter()
            .cloned()
            .map(|config| (config.id(), config))
            .collect();
        let nodes: BTreeMap<NodeId, Node> = configs
            .into_iter()
            .map(|config| (config.id(), Node::new(config)))
            .collect();
        let snapshot_sources = nodes
            .keys()
            .map(|node_id| (*node_id, InMemorySnapshotChunkSource::new()))
            .collect();
        let durable_applied = nodes
            .keys()
            .map(|node_id| (*node_id, LogIndex::ZERO))
            .collect();
        let application_epochs = nodes.keys().map(|node_id| (*node_id, 0)).collect();
        let initial_reference_states: BTreeMap<_, _> = nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    ReferenceState {
                        application_value: Vec::new().into(),
                        committed_membership: node.committed_membership(),
                        committed_configuration: None,
                    },
                )
            })
            .collect();
        let execution_cursors = initial_reference_states
            .iter()
            .map(|(node_id, state)| {
                (
                    *node_id,
                    ExecutionCursor {
                        application_epoch: 0,
                        applied_through: LogIndex::ZERO,
                        state: state.clone(),
                    },
                )
            })
            .collect();

        Self {
            clock: SimClock::default(),
            configs: configs_by_id,
            nodes,
            network: VecDeque::new(),
            rng: SimRng::new(seed),
            applied: Vec::new(),
            execution_history: Vec::new(),
            execution_cursors,
            initial_reference_states,
            application_epochs,
            durable_applied,
            snapshot_installs: Vec::new(),
            snapshot_sources,
            snapshot_staging: BTreeMap::new(),
            read_grants: Vec::new(),
            blocked_pairs: BTreeSet::new(),
            delivered_ack_floor: BTreeMap::new(),
            synced_marks: BTreeMap::new(),
            read_registrations: Vec::new(),
        }
    }

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
    pub fn read_index(&mut self, node_id: NodeId, request_id: u64) {
        // Record the cluster-wide committed floor at registration: the
        // freshness bar any eventual grant must clear.
        let committed_floor = self.committed_floor();
        self.read_registrations.push(ReadRegistered {
            node_id,
            request_id,
            committed_floor,
        });
        let outputs = self.node_mut(node_id).step(Input::ReadIndex {
            read_id: ReadId(request_id),
        });
        self.record_outputs(node_id, outputs);
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

#[cfg(test)]
mod tests;
