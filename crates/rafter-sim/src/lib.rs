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
    BootstrapLogEntry, BootstrapState, BootstrapValidationError, CommittedConfiguration,
    ConfigurationEntry, InMemorySnapshotChunkSource, Input, LogEntry, LogIndex, MembershipConfig,
    MembershipSet, Node, NodeConfig, NodeId, PromotionBarrier, RaftSnapshot, RaftSnapshotMetadata,
    ReadId, ReplicationProgress, Role, SharedPayload, SnapshotTransferId, StagedSnapshotChunk,
    Term,
};

/// Disk fault models used by the deterministic protocol simulator.
pub mod disk_fault;
/// Bounded model-checking, replay, soak, and TLA projection helpers.
pub mod model_check;
mod network;

pub use network::Envelope;
use network::QueuedEnvelope;

/// Monotonic logical time used by the deterministic simulator.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimTick(pub u64);

/// Simulator clock advanced explicitly by ticks and message scheduling.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct SimClock {
    now: SimTick,
}

impl SimClock {
    /// Returns the current logical tick.
    #[must_use]
    pub fn now(&self) -> SimTick {
        self.now
    }

    /// Advances the clock by one logical tick and returns the new value.
    pub fn advance(&mut self) -> SimTick {
        self.now.0 += 1;
        self.now
    }
}

impl SimTick {
    /// Returns a tick value `ticks` after this one.
    #[must_use]
    pub fn after(self, ticks: u64) -> Self {
        Self(self.0 + ticks)
    }
}

/// Seed for deterministic simulator randomness.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SimSeed(pub u64);

impl Default for SimSeed {
    fn default() -> Self {
        Self(0x5041_4e47_4541)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SimRng {
    state: u64,
}

impl SimRng {
    fn new(seed: SimSeed) -> Self {
        Self { state: seed.0 }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);
        let bounded = self.next_u64() % upper_bound as u64;
        usize::try_from(bounded).unwrap_or(upper_bound - 1)
    }
}

/// One application payload applied by a simulated node.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Applied {
    pub node_id: NodeId,
    pub index: LogIndex,
    pub payload: SharedPayload,
}

/// A snapshot installation observed on a node, recorded alongside the
/// position it occupies in the applied stream so invariants can reason
/// about ordering between installs and entry applies.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotInstalled {
    pub node_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub payload: Vec<u8>,
    pub applied_records_before_install: usize,
}

/// A node's in-progress staging area for one inbound snapshot transfer: the
/// simulated snapshot store accumulating [`rafter::Output::StageSnapshotChunk`]
/// bytes until the matching [`rafter::Output::ApplySnapshot`] promotes them.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StagedSnapshotTransfer {
    leader_id: NodeId,
    transfer_id: SnapshotTransferId,
    metadata: RaftSnapshotMetadata,
    total_payload_len: u64,
    application_payload_crc32: u32,
    bytes: Vec<u8>,
}

/// Deterministic in-memory Raft cluster used by tests, soaks, and explorers.
#[derive(Clone, Debug, Hash)]
pub struct Cluster {
    clock: SimClock,
    configs: BTreeMap<NodeId, NodeConfig>,
    nodes: BTreeMap<NodeId, Node>,
    network: VecDeque<QueuedEnvelope>,
    rng: SimRng,
    applied: Vec<Applied>,
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

/// A read barrier granted by a node, recorded for scenario assertions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReadGranted {
    pub node_id: NodeId,
    pub request_id: u64,
    pub read_index: LogIndex,
    pub local_applied_index: LogIndex,
}

/// A read-barrier registration, recorded with the highest commit index any
/// node had reached at registration time: the committed-floor freshness bar the
/// eventual grant must not undercut.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReadRegistered {
    pub node_id: NodeId,
    pub request_id: u64,
    pub committed_floor: LogIndex,
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

        Self {
            clock: SimClock::default(),
            configs: configs_by_id,
            nodes,
            network: VecDeque::new(),
            rng: SimRng::new(seed),
            applied: Vec::new(),
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

    /// Returns the simulator clock.
    #[must_use]
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Returns the nodes currently in the leader role.
    #[must_use]
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| (node.role() == Role::Leader).then_some(*node_id))
            .collect()
    }

    /// Returns the nodes currently leading in `term`.
    #[must_use]
    pub fn leaders_in_term(&self, term: Term) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| {
                (node.role() == Role::Leader && node.current_term() == term).then_some(*node_id)
            })
            .collect()
    }

    /// Returns the snapshot installations observed by the simulator.
    #[must_use]
    pub fn snapshot_installs(&self) -> &[SnapshotInstalled] {
        &self.snapshot_installs
    }

    /// Returns the read barriers granted by simulated nodes.
    #[must_use]
    pub fn read_grants(&self) -> &[ReadGranted] {
        &self.read_grants
    }

    /// Returns the read barriers registered by simulated clients.
    #[must_use]
    pub fn read_registrations(&self) -> &[ReadRegistered] {
        &self.read_registrations
    }

    /// Returns the highest commit index reported by any simulated node.
    #[must_use]
    pub fn committed_floor(&self) -> LogIndex {
        self.nodes
            .values()
            .map(Node::commit_index)
            .max()
            .unwrap_or_default()
    }

    /// Returns `node_id`'s local applied index.
    #[must_use]
    pub fn local_applied_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).applied_index()
    }

    /// Returns the simulator-wide stream of applied application payloads.
    #[must_use]
    pub fn applied(&self) -> &[Applied] {
        &self.applied
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

    /// Returns the promotion barrier currently required for `learner_id`.
    #[must_use]
    pub fn promotion_barrier(
        &self,
        node_id: NodeId,
        learner_id: NodeId,
    ) -> Option<PromotionBarrier> {
        self.node(node_id).promotion_barrier(learner_id)
    }

    /// Replaces a simulated node with a freshly hydrated Raft node.
    ///
    /// This models a process restart at the deterministic protocol boundary:
    /// hard state and log entries come from the supplied bootstrap state while
    /// volatile Raft state such as role, commit index, and in-flight progress is
    /// reset. The node's durable snapshot payload store survives the restart;
    /// its volatile staging area for a partially received transfer does not.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] when the supplied state is not
    /// valid for the node's static configuration.
    ///
    /// # Panics
    ///
    /// Panics when `node_id` is not part of this simulated cluster, or when
    /// the bootstrap state carries a snapshot descriptor whose payload was
    /// never registered in the node's snapshot store (see
    /// [`Cluster::seed_snapshot_payload`]): durable metadata without durable
    /// content models a persistence bug, not a restart.
    pub fn restart_node_from_bootstrap(
        &mut self,
        node_id: NodeId,
        bootstrap: BootstrapState,
    ) -> Result<(), BootstrapValidationError> {
        let config = self
            .configs
            .get(&node_id)
            .expect("simulated node config must exist in cluster")
            .clone();
        if let Some(snapshot) = bootstrap.snapshot.as_ref() {
            assert!(
                self.snapshot_payload(node_id, snapshot).is_some(),
                "snapshot payload for transfer {} must be seeded in {node_id}'s snapshot store \
                 before restarting with its descriptor",
                snapshot.transfer_id()
            );
        }
        let mut node = Node::from_bootstrap_applied_through(
            config,
            bootstrap,
            self.durable_applied_floor(node_id),
        )?;
        let outputs = node.drain_committed_outputs();
        self.nodes.insert(node_id, node);
        // A process restart loses the volatile staging area; explicit resume
        // paths (the model checker's durable-transfer restart) reinstate it
        // alongside the kernel's pending-transfer record.
        self.snapshot_staging.remove(&node_id);
        self.record_outputs(node_id, outputs);
        Ok(())
    }

    fn durable_applied_floor(&self, node_id: NodeId) -> LogIndex {
        self.durable_applied
            .get(&node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Restarts `node_id` from `bootstrap` after losing the simulated
    /// application state for that node.
    ///
    /// This is for durability-violation and storage-repair scenarios. Normal
    /// process restarts should use [`Cluster::restart_node_from_bootstrap`],
    /// which preserves the simulator's durable application floor.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] when the supplied state is not
    /// valid for the node's static configuration.
    ///
    /// # Panics
    ///
    /// Panics when `node_id` is not part of this simulated cluster, or when
    /// the bootstrap state carries a snapshot descriptor whose payload was
    /// never registered in the node's snapshot store.
    pub fn restart_node_from_bootstrap_losing_application_state(
        &mut self,
        node_id: NodeId,
        bootstrap: BootstrapState,
    ) -> Result<(), BootstrapValidationError> {
        let config = self
            .configs
            .get(&node_id)
            .expect("simulated node config must exist in cluster")
            .clone();
        if let Some(snapshot) = bootstrap.snapshot.as_ref() {
            assert!(
                self.snapshot_payload(node_id, snapshot).is_some(),
                "snapshot payload for transfer {} must be seeded in {node_id}'s snapshot store \
                 before restarting with its descriptor",
                snapshot.transfer_id()
            );
        }
        let snapshot_boundary = bootstrap
            .snapshot
            .as_ref()
            .map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            });

        let mut node = Node::from_bootstrap_applied_through(config, bootstrap, snapshot_boundary)?;
        let outputs = node.drain_committed_outputs();
        self.nodes.insert(node_id, node);
        self.snapshot_staging.remove(&node_id);
        self.durable_applied.insert(node_id, snapshot_boundary);
        self.record_outputs(node_id, outputs);
        Ok(())
    }

    /// Captures this node's current bootstrap state as its "last synced"
    /// point for [`Cluster::restart_node_from_mark`].
    ///
    /// The caller owns legality: restarting from a mark that predates entries
    /// the node has acknowledged models a durability violation, which the
    /// leader's match floor deliberately never rewinds until a protocol-level
    /// rejection hint can prove the follower's lower durable tail.
    pub fn mark_synced(&mut self, node_id: NodeId) {
        let mark = self.bootstrap_state(node_id);
        self.synced_marks.insert(node_id, mark);
    }

    /// Restarts `node_id` from its [`Cluster::mark_synced`] point with its
    /// LIVE term/vote and lost application state — a crash that lost the log
    /// tail written after the mark while term and vote survived. This is the
    /// explicit amnesia model for assumption-violating durability tests;
    /// ordinary restarts preserve the simulator's durable application floor.
    ///
    /// # Panics
    ///
    /// Panics when no mark was captured or the composed state fails
    /// bootstrap validation.
    pub fn restart_node_from_mark(&mut self, node_id: NodeId) {
        let mark = self
            .synced_marks
            .get(&node_id)
            .cloned()
            .expect("a synced mark must be captured before a marked restart");
        let live = self.bootstrap_state(node_id);
        let bootstrap = BootstrapState {
            current_term: live.current_term,
            voted_for: live.voted_for,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: mark.snapshot,
            log: mark.log,
        };
        self.restart_node_from_bootstrap_losing_application_state(node_id, bootstrap)
            .expect("marked lossy restart composes a valid bootstrap state");
    }

    /// Restarts `node_id` losing exactly the log tail no leader ever heard
    /// acknowledged and that the node has not locally committed: the log
    /// rewinds to the delivered-acknowledgement floor, the local commit floor,
    /// or the snapshot boundary, whichever is highest. Hard state, the local
    /// commit floor, the committed configuration identity, and the snapshot
    /// survive. Legal by construction — every lost entry's acknowledgement
    /// envelope was still in flight or dropped, and no local committed prefix
    /// is erased — so schedules may apply it freely without weakening the
    /// safety invariants.
    ///
    /// # Panics
    ///
    /// Panics when the truncated state fails bootstrap validation, which
    /// would be a harness bug.
    pub fn restart_node_lossy(&mut self, node_id: NodeId) {
        let live = self.bootstrap_state(node_id);
        let floor = self
            .delivered_ack_floor
            .get(&node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO)
            .max(live.commit_index)
            .max(live.snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            }));
        let bootstrap = BootstrapState {
            current_term: live.current_term,
            voted_for: live.voted_for,
            commit_index: live.commit_index,
            committed_configuration: live.committed_configuration,
            snapshot: live.snapshot,
            log: live
                .log
                .into_iter()
                .filter(|entry| entry.index <= floor)
                .collect(),
        };
        self.restart_node_from_bootstrap(node_id, bootstrap)
            .expect("floor-truncated lossy restart composes a valid bootstrap state");
    }

    /// The highest match index `node_id` has confirmed to any leader via a
    /// DELIVERED acknowledgement.
    #[must_use]
    pub fn delivered_ack_floor(&self, node_id: NodeId) -> LogIndex {
        self.delivered_ack_floor
            .get(&node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Registers `payload` as `snapshot`'s content in `node_id`'s store.
    ///
    /// # Panics
    ///
    /// Panics when the payload length does not match the descriptor.
    pub fn seed_snapshot_payload(
        &mut self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) {
        assert!(
            self.configs.contains_key(&node_id),
            "simulated node {node_id} must exist in cluster"
        );
        self.snapshot_sources
            .entry(node_id)
            .or_default()
            .insert(snapshot, payload)
            .expect("seeded snapshot payload must match its descriptor length");
    }

    /// Returns the payload bytes `node_id`'s snapshot store holds for
    /// `snapshot`, if any.
    #[must_use]
    pub fn snapshot_payload(&self, node_id: NodeId, snapshot: &RaftSnapshot) -> Option<&[u8]> {
        self.snapshot_sources
            .get(&node_id)?
            .payload(snapshot.transfer_id())
    }

    /// Appends one validated inbound chunk to `node_id`'s staging area,
    /// enforcing the kernel's staging contract: chunks arrive strictly in
    /// offset order within one transfer, and offset zero begins or replaces
    /// the staged transfer.
    fn stage_snapshot_chunk(&mut self, node_id: NodeId, chunk: StagedSnapshotChunk) {
        let StagedSnapshotChunk {
            leader_id,
            transfer_id,
            metadata,
            total_payload_len,
            application_payload_crc32,
            offset,
            bytes,
            done,
        } = chunk;

        if offset == 0 {
            self.snapshot_staging.insert(
                node_id,
                StagedSnapshotTransfer {
                    leader_id,
                    transfer_id,
                    metadata,
                    total_payload_len,
                    application_payload_crc32,
                    bytes,
                },
            );
        } else {
            let staged = self.snapshot_staging.get_mut(&node_id).unwrap_or_else(|| {
                panic!(
                    "kernel staging contract breach: {node_id} staged a chunk of transfer \
                     {transfer_id} at offset {offset} with no transfer in progress"
                )
            });
            assert!(
                staged.leader_id == leader_id
                    && staged.transfer_id == transfer_id
                    && staged.metadata == metadata
                    && staged.total_payload_len == total_payload_len
                    && staged.application_payload_crc32 == application_payload_crc32,
                "kernel staging contract breach: {node_id} staged a chunk of transfer \
                 {transfer_id} from {leader_id} while transfer {} from {} is in progress",
                staged.transfer_id,
                staged.leader_id
            );
            assert_eq!(
                offset,
                staged.bytes.len() as u64,
                "kernel staging contract breach: {node_id} staged an out-of-order chunk of \
                 transfer {transfer_id}"
            );
            staged.bytes.extend_from_slice(&bytes);
        }

        if done {
            let staged = &self.snapshot_staging[&node_id];
            assert_eq!(
                staged.bytes.len() as u64,
                staged.total_payload_len,
                "kernel staging contract breach: {node_id} finished transfer {transfer_id} \
                 with an incomplete staged payload"
            );
        }
    }

    /// Promotes the completed staged transfer backing `snapshot` into
    /// `node_id`'s durable snapshot store and returns its bytes.
    ///
    /// Cross-node content invariant: the bytes a follower assembled must be
    /// the bytes the transfer's leader registered for the same descriptor.
    /// The comparison is skipped only when the leader's store no longer
    /// holds that transfer's payload.
    fn take_installed_snapshot_payload(
        &mut self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
    ) -> Vec<u8> {
        let transfer_id = snapshot.transfer_id();
        let staged = self.snapshot_staging.remove(&node_id).unwrap_or_else(|| {
            panic!(
                "kernel staging contract breach: {node_id} applied snapshot transfer \
                 {transfer_id} with no staged transfer"
            )
        });
        assert_eq!(
            staged.transfer_id, transfer_id,
            "kernel staging contract breach: {node_id} applied snapshot transfer \
             {transfer_id} while transfer {} is staged",
            staged.transfer_id
        );
        assert_eq!(
            staged.bytes.len() as u64,
            snapshot.application_payload_len,
            "kernel staging contract breach: {node_id} applied snapshot transfer \
             {transfer_id} with an incomplete staged payload"
        );
        assert_eq!(
            staged.application_payload_crc32, snapshot.application_payload_crc32,
            "kernel staging contract breach: {node_id} applied snapshot transfer \
             {transfer_id} with a mismatched payload checksum"
        );

        if let Some(expected) = self
            .snapshot_sources
            .get(&staged.leader_id)
            .and_then(|source| source.payload(transfer_id))
        {
            assert!(
                staged.bytes == expected,
                "snapshot content invariant violated: {node_id} installed bytes for transfer \
                 {transfer_id} that differ from the payload leader {} serves for the same \
                 descriptor",
                staged.leader_id
            );
        }

        self.snapshot_sources
            .entry(node_id)
            .or_default()
            .insert(snapshot, staged.bytes.clone())
            .expect("completed staged payload length was validated against the descriptor");
        staged.bytes
    }

    /// Returns `node_id`'s committed index.
    #[must_use]
    pub fn commit_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).commit_index()
    }

    /// Returns `node_id`'s last local log index.
    #[must_use]
    pub fn last_log_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).last_log_index()
    }

    /// Returns `node_id`'s log entries starting at `first_index`.
    #[must_use]
    pub fn log_entries_from(&self, node_id: NodeId, first_index: LogIndex) -> Vec<LogEntry> {
        self.node(node_id).log_entries_from(first_index)
    }

    /// Returns `node_id`'s effective membership configuration.
    #[must_use]
    pub fn effective_membership(&self, node_id: NodeId) -> MembershipConfig {
        self.node(node_id).effective_membership()
    }

    /// Returns `node_id`'s committed configuration identity, if known.
    #[must_use]
    pub fn committed_configuration_state(&self, node_id: NodeId) -> Option<CommittedConfiguration> {
        self.node(node_id).committed_configuration_state()
    }

    /// Captures `node_id` as a restart bootstrap state.
    #[must_use]
    pub fn bootstrap_state(&self, node_id: NodeId) -> BootstrapState {
        let node = self.node(node_id);
        let first_log_index = node.snapshot_index().next();
        BootstrapState {
            current_term: node.current_term(),
            voted_for: node.voted_for(),
            commit_index: node.commit_index(),
            committed_configuration: node.committed_configuration_state(),
            snapshot: node.snapshot().cloned(),
            log: node
                .log_entries_from(first_log_index)
                .into_iter()
                .enumerate()
                .map(|(offset, entry)| BootstrapLogEntry {
                    index: LogIndex(first_log_index.0 + offset as u64),
                    term: entry.term,
                    kind: entry.kind,
                })
                .collect(),
        }
    }

    /// Returns whether `node_id` currently has an active read lease.
    #[must_use]
    pub fn read_lease_active(&self, node_id: NodeId) -> bool {
        self.node(node_id).read_lease_active()
    }

    /// Returns `node_id`'s current role.
    #[must_use]
    pub fn role(&self, node_id: NodeId) -> Role {
        self.node(node_id).role()
    }

    /// Returns `node_id`'s per-follower replication progress as reported by
    /// the kernel's leader observability; empty unless the node leads.
    #[must_use]
    pub fn leader_replication_progress(&self, node_id: NodeId) -> Vec<ReplicationProgress> {
        self.node(node_id).leader_replication_progress()
    }

    /// Returns `node_id`'s current term.
    #[must_use]
    pub fn current_term(&self, node_id: NodeId) -> Term {
        self.node(node_id).current_term()
    }

    fn node(&self, node_id: NodeId) -> &Node {
        self.nodes
            .get(&node_id)
            .expect("simulated node must exist in cluster")
    }

    fn node_mut(&mut self, node_id: NodeId) -> &mut Node {
        self.nodes
            .get_mut(&node_id)
            .expect("simulated node must exist in cluster")
    }
}

#[cfg(test)]
mod tests;
