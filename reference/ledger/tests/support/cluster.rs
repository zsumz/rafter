//! Consumer-owned deterministic three-node driver over Rafter's public APIs.
//!
//! The driver owns every choice a real embedder owns: when a node ticks, which
//! envelopes are delivered, which links are cut, and when a replica restarts.
//! It uses no simulator, no internal hooks, and no privileged observation; an
//! external user with the published crates can write the same thing.
//!
//! Two things are deliberately modeled rather than real at this slice. Durable
//! Raft state lives in shared in-memory media that outlive a node incarnation,
//! and application state survives a restart because the state machine carries
//! its applied index with its data. A transactional application backend and
//! application crash points arrive with the durable slices.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rafter::{
    LocalProposalId, LogEntryKind, LogIndex, NodeConfig, NodeId, ProposalRejection, ReadId,
    ReadIndexCancelReason, ReadIndexRejection, Role, Term,
};
use rafter_app::group::{GroupInput, GroupStepReport, RaftGroup};
use rafter_app::proposal::{Proposal, ProposalBegin, ProposalEvent};
use rafter_app::read::{ReadBarrierRequest, ReadEvent, ReadProof};
use rafter_app::state_machine::{ReadBarrier, ReplicatedStateMachine};
use rafter_app::transport::PeerEnvelope;
use rafter_reference_ledger::{
    ApplyDisposition, ApplyOutcome, Command, HistoryEvent, LedgerConfig, LedgerQuery,
    LedgerQueryResult, LedgerResponse, LedgerStateMachine, OperationId,
};
use rafter_runtime::DurableRaftNode;

use crate::storage::{NodeStorage, SharedHardStateStore, SharedLogSegment, SharedSnapshotStore};

/// Bound on driver rounds spent waiting for one outcome.
///
/// Every wait is bounded so a stalled protocol fails the test instead of
/// hanging, and so a client that runs out of rounds observes the same unknown
/// outcome a real client observes when it stops waiting.
const MAX_ROUNDS: usize = 32;

/// Caller-defined group identity for the single ledger group.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LedgerGroupId(pub u64);

/// The one group every node in this driver serves.
pub const GROUP_ID: LedgerGroupId = LedgerGroupId(1);

type LedgerRuntime = DurableRaftNode<SharedHardStateStore, SharedLogSegment, SharedSnapshotStore>;
type LedgerGroup = RaftGroup<LedgerGroupId, LedgerStateMachine, LedgerRuntime>;
type LedgerReport = GroupStepReport<LedgerGroupId, ApplyOutcome>;

/// Terminal client-visible outcome of one submitted command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProposalOutcome {
    /// The command committed and applied; this is its replicated result.
    Committed {
        index: LogIndex,
        term: Term,
        outcome: ApplyOutcome,
    },
    /// The local node refused the command before replication.
    Rejected {
        reason: ProposalRejection,
        leader_hint: Option<NodeId>,
    },
    /// The client cannot tell whether the command committed, and must retry
    /// under the same request identity.
    Unknown,
}

impl ProposalOutcome {
    /// Returns the replicated response when the command committed.
    pub fn response(&self) -> Option<&LedgerResponse> {
        match self {
            Self::Committed { outcome, .. } => Some(&outcome.response),
            Self::Rejected { .. } | Self::Unknown => None,
        }
    }

    /// Returns how the ledger classified a committed command.
    pub fn disposition(&self) -> Option<ApplyDisposition> {
        match self {
            Self::Committed { outcome, .. } => Some(outcome.disposition),
            Self::Rejected { .. } | Self::Unknown => None,
        }
    }
}

/// Terminal outcome of one linearizable query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadOutcome {
    /// The barrier was granted and the query ran against fresh state.
    Ready(LedgerQueryResult),
    /// The local node refused the read barrier.
    Rejected {
        reason: ReadIndexRejection,
        leader_hint: Option<NodeId>,
    },
    /// Local runtime lifecycle canceled the barrier, usually leadership loss.
    Canceled {
        reason: ReadIndexCancelReason,
        leader_hint: Option<NodeId>,
    },
    /// The barrier did not resolve within the driver's round budget.
    Unresolved,
}

#[derive(Debug)]
struct ClusterNode {
    node_id: NodeId,
    election_timeout_ticks: u64,
    storage: NodeStorage,
    group: LedgerGroup,
}

/// Deterministic three-node ledger cluster with explicit delivery control.
#[derive(Debug)]
pub struct LedgerCluster {
    config: LedgerConfig,
    nodes: Vec<ClusterNode>,
    network: VecDeque<PeerEnvelope<LedgerGroupId>>,
    isolated: BTreeSet<NodeId>,
    proposal_outcomes: BTreeMap<LocalProposalId, ProposalOutcome>,
    read_proofs: BTreeMap<ReadId, ReadProof<LedgerGroupId>>,
    read_failures: BTreeMap<ReadId, ReadOutcome>,
    runtime_unknown_outcomes: usize,
    next_local_proposal_id: u64,
    next_read_id: u64,
    next_operation_id: u64,
    history: Vec<HistoryEvent>,
}

impl LedgerCluster {
    /// Builds three replicas whose election timeouts make every uncontested
    /// election deterministic: the lowest-numbered reachable node wins.
    pub fn new(config: LedgerConfig) -> Self {
        let nodes = [
            (NodeId(1), &[2, 3][..], 4),
            (NodeId(2), &[1, 3][..], 6),
            (NodeId(3), &[1, 2][..], 8),
        ]
        .into_iter()
        .map(|(node_id, peers, election_timeout_ticks)| {
            let storage = NodeStorage::new();
            let (group, _) = open_group(
                node_id,
                peers,
                election_timeout_ticks,
                &storage,
                LedgerStateMachine::new(config),
            );
            ClusterNode {
                node_id,
                election_timeout_ticks,
                storage,
                group,
            }
        })
        .collect();

        Self {
            config,
            nodes,
            network: VecDeque::new(),
            isolated: BTreeSet::new(),
            proposal_outcomes: BTreeMap::new(),
            read_proofs: BTreeMap::new(),
            read_failures: BTreeMap::new(),
            runtime_unknown_outcomes: 0,
            next_local_proposal_id: 1,
            next_read_id: 1,
            next_operation_id: 1,
            history: Vec::new(),
        }
    }

    /// Returns the configured ledger bounds shared by every replica.
    pub fn config(&self) -> LedgerConfig {
        self.config
    }

    /// Returns the recorded client history.
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns how many proposals the runtime itself declared unresolvable.
    ///
    /// A client observes an unknown outcome whenever it stops waiting. This
    /// counts the narrower case where the app layer reported that it lost the
    /// proposal's outcome, which is what a former leader's dropped proposals
    /// produce.
    pub fn runtime_unknown_outcomes(&self) -> usize {
        self.runtime_unknown_outcomes
    }

    /// Returns every node ID in the cluster.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }

    /// Returns the reachable leader with the highest term, if one exists.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|node| !self.isolated.contains(&node.node_id))
            .filter_map(|node| {
                let metrics = node.group.metrics();
                (metrics.role == Role::Leader).then_some((metrics.term, node.node_id))
            })
            .max()
            .map(|(_, node_id)| node_id)
    }

    /// Returns the state machine owned by one replica.
    pub fn state_machine(&self, node_id: NodeId) -> &LedgerStateMachine {
        self.node(node_id).group.state_machine()
    }

    /// Returns mutable access to one replica's state machine.
    ///
    /// This is the maintenance hook the group layer documents. It is used here
    /// only to build application snapshots, which read state rather than move
    /// the durable applied floor.
    pub fn state_machine_mut(&mut self, node_id: NodeId) -> &mut LedgerStateMachine {
        self.node_mut(node_id).group.state_machine_mut()
    }

    /// Returns the replica's applied index.
    pub fn applied_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id)
            .group
            .state_machine()
            .applied_index()
            .expect("ledger state machines always report an applied index")
    }

    /// Returns the commands committed on one replica, in log order.
    ///
    /// This reads the durable log through the public runtime accessor and
    /// decodes it with the same adapter the replica applies with, so the
    /// checker can replay a real replicated history through the oracle.
    pub fn committed_commands(&self, node_id: NodeId) -> Vec<Command> {
        let node = self.node(node_id);
        self.committed_application_entries(node_id)
            .into_iter()
            .map(|(_, payload)| {
                node.group
                    .state_machine()
                    .decode_command(&payload)
                    .expect("replicas only append frames this adapter encoded")
            })
            .collect()
    }

    /// Returns the highest committed index the state machine can ever apply.
    ///
    /// Elections and membership changes commit entries the state machine never
    /// sees, so a replica's applied index legitimately trails its commit index.
    /// Progress therefore has to be measured against committed application
    /// entries, which the app layer does not report on its own.
    fn committed_application_floor(&self, node_id: NodeId) -> LogIndex {
        self.committed_application_entries(node_id)
            .last()
            .map_or(LogIndex::ZERO, |(index, _)| *index)
    }

    fn committed_application_entries(&self, node_id: NodeId) -> Vec<(LogIndex, Vec<u8>)> {
        let runtime = self.node(node_id).group.runtime();
        let commit_index = runtime.commit_index();
        let first_index = runtime.snapshot_index().0 + 1;
        runtime
            .log_entries_from(LogIndex(first_index))
            .into_iter()
            .zip(first_index..)
            .take_while(|(_, index)| LogIndex(*index) <= commit_index)
            .filter_map(|(entry, index)| match entry.kind {
                LogEntryKind::Application(payload) => {
                    Some((LogIndex(index), payload.as_ref().to_vec()))
                }
                LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
            })
            .collect()
    }

    /// Drives ticks until a leader exists among the reachable nodes.
    pub fn elect_leader(&mut self) -> NodeId {
        for _ in 0..MAX_ROUNDS {
            if let Some(leader) = self.leader() {
                return leader;
            }
            self.tick_reachable();
            self.deliver_all();
        }
        panic!("no leader was elected within {MAX_ROUNDS} rounds");
    }

    /// Drives the cluster until every reachable replica has applied through
    /// its committed index.
    ///
    /// Convergence is a precondition for comparing replicas to each other and
    /// to the oracle, never something a test may assume.
    pub fn settle(&mut self) {
        for _ in 0..MAX_ROUNDS {
            if self.node_ids().into_iter().all(|node_id| {
                self.isolated.contains(&node_id)
                    || self.applied_index(node_id) >= self.committed_application_floor(node_id)
            }) {
                return;
            }
            self.tick_reachable();
            self.deliver_all();
        }
        panic!("replicas did not apply their committed commands within {MAX_ROUNDS} rounds");
    }

    /// Ticks every reachable node and drains the network, `rounds` times.
    pub fn run_rounds(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.tick_reachable();
            self.deliver_all();
        }
    }

    /// Ticks every reachable node once.
    pub fn tick_reachable(&mut self) {
        for node_id in self.node_ids() {
            if self.isolated.contains(&node_id) {
                continue;
            }
            let report = self
                .node_mut(node_id)
                .group
                .step(GroupInput::Tick)
                .expect("a healthy group accepts ticks");
            self.record_report(report);
        }
    }

    /// Delivers every queued envelope, including envelopes queued by delivery.
    pub fn deliver_all(&mut self) {
        while let Some(envelope) = self.network.pop_front() {
            if self.isolated.contains(&envelope.to) || self.isolated.contains(&envelope.from) {
                continue;
            }
            let report = self
                .node_mut(envelope.to)
                .group
                .step(GroupInput::PeerMessage { envelope })
                .expect("a healthy group accepts peer messages");
            self.record_report(report);
        }
    }

    /// Cuts every link to and from `node_id`.
    pub fn partition(&mut self, node_id: NodeId) {
        self.isolated.insert(node_id);
    }

    /// Restores every cut link.
    pub fn heal(&mut self) {
        self.isolated.clear();
    }

    /// Restarts one replica over its retained durable state.
    ///
    /// This follows the documented restart recipe: read the application's
    /// durable applied floor, recover the runtime through the same floor, then
    /// hand the recovery outputs to the new group before using it.
    pub fn restart(&mut self, node_id: NodeId) {
        let index = self.node_index(node_id);
        let election_timeout_ticks = self.nodes[index].election_timeout_ticks;
        let peers = self
            .node_ids()
            .into_iter()
            .filter(|peer| *peer != node_id)
            .map(|peer| peer.0)
            .collect::<Vec<_>>();
        // `RaftGroup` has no decomposition path, so the surviving application
        // state has to be taken through the state-machine accessor before the
        // old incarnation is dropped.
        let app = self.nodes[index].group.state_machine().clone();
        let storage = self.nodes[index].storage.reopen();

        let (group, report) = open_group(node_id, &peers, election_timeout_ticks, &storage, app);
        self.nodes[index].storage = storage;
        self.nodes[index].group = group;
        self.record_report(report);
    }

    /// Submits one command to `node_id` and waits for a terminal outcome.
    ///
    /// The command, its response, and any unknown outcome are recorded in the
    /// history under one operation identity.
    pub fn submit(&mut self, node_id: NodeId, command: Command) -> ProposalOutcome {
        let operation_id = self.record_invocation(command.clone());
        let local_proposal_id = self.allocate_local_proposal_id();
        let started = self
            .node_mut(node_id)
            .group
            .begin_proposal(Proposal {
                local_proposal_id,
                client_request_id: None,
                command,
            })
            .expect("a healthy group accepts proposals");
        self.record_report(started.report);
        if let Some(outcome) = immediate_outcome(&started.begin) {
            return self.record_completion(operation_id, outcome);
        }

        for _ in 0..MAX_ROUNDS {
            if let Some(outcome) = self.proposal_outcomes.get(&local_proposal_id) {
                let outcome = outcome.clone();
                return self.record_completion(operation_id, outcome);
            }
            self.deliver_all();
            self.tick_reachable();
            self.deliver_all();
        }
        self.record_completion(operation_id, ProposalOutcome::Unknown)
    }

    /// Runs one linearizable query against `node_id`.
    ///
    /// The driver assembles the barrier itself and reads the state machine
    /// under the granted proof, so no step report is lost while a read is in
    /// flight.
    pub fn read(&mut self, node_id: NodeId, query: LedgerQuery) -> ReadOutcome {
        let read_id = self.allocate_read_id();
        // The barrier's immediate outcome is derived from the same read events
        // this report carries, so recording the report records the outcome.
        let barrier = self
            .node_mut(node_id)
            .group
            .begin_read_barrier(ReadBarrierRequest {
                group_id: GROUP_ID,
                read_id,
                min_applied_index: None,
                context: Vec::new(),
            })
            .expect("a healthy group accepts read barriers");
        self.record_report(barrier.report);

        for _ in 0..MAX_ROUNDS {
            if let Some(failure) = self.read_failures.remove(&read_id) {
                return failure;
            }
            if let Some(proof) = self.read_proofs.remove(&read_id) {
                return ReadOutcome::Ready(self.read_under_proof(node_id, query, &proof));
            }
            self.deliver_all();
            self.tick_reachable();
            self.deliver_all();
        }
        self.node_mut(node_id).group.cancel_read(read_id);
        ReadOutcome::Unresolved
    }

    fn read_under_proof(
        &self,
        node_id: NodeId,
        query: LedgerQuery,
        proof: &ReadProof<LedgerGroupId>,
    ) -> LedgerQueryResult {
        self.node(node_id)
            .group
            .state_machine()
            .read(
                query,
                ReadBarrier {
                    required_applied_index: proof.required_applied_index,
                    local_applied_index: proof.local_applied_index,
                },
            )
            .expect("a granted proof proves the local replica is fresh enough")
    }

    fn record_report(&mut self, report: LedgerReport) {
        let LedgerReport {
            peer_messages,
            proposal_events,
            read_events,
            ..
        } = report;
        self.network.extend(peer_messages);
        for event in &proposal_events {
            self.record_proposal_event(event);
        }
        for event in &read_events {
            self.record_read_event(event);
        }
    }

    fn record_proposal_event(&mut self, event: &ProposalEvent<ApplyOutcome>) {
        match event {
            ProposalEvent::Applied {
                local_proposal_id,
                index,
                term,
                result,
            } => {
                self.proposal_outcomes.insert(
                    *local_proposal_id,
                    ProposalOutcome::Committed {
                        index: *index,
                        term: *term,
                        outcome: result.clone(),
                    },
                );
            }
            ProposalEvent::Rejected {
                local_proposal_id,
                reason,
                leader_hint,
            } => {
                self.proposal_outcomes.insert(
                    *local_proposal_id,
                    ProposalOutcome::Rejected {
                        reason: reason.clone(),
                        leader_hint: *leader_hint,
                    },
                );
            }
            ProposalEvent::UnknownOutcome {
                local_proposal_id, ..
            } => {
                self.runtime_unknown_outcomes += 1;
                self.proposal_outcomes
                    .insert(*local_proposal_id, ProposalOutcome::Unknown);
            }
            _ => {}
        }
    }

    fn record_read_event(&mut self, event: &ReadEvent<LedgerGroupId>) {
        match event {
            ReadEvent::Granted { read_id, proof } => {
                self.read_proofs.insert(*read_id, proof.clone());
            }
            ReadEvent::Rejected {
                read_id,
                reason,
                leader_hint,
            } => {
                self.read_failures.insert(
                    *read_id,
                    ReadOutcome::Rejected {
                        reason: *reason,
                        leader_hint: *leader_hint,
                    },
                );
            }
            ReadEvent::Canceled {
                read_id,
                reason,
                leader_hint,
            } => {
                self.read_failures.insert(
                    *read_id,
                    ReadOutcome::Canceled {
                        reason: *reason,
                        leader_hint: *leader_hint,
                    },
                );
            }
            _ => {}
        }
    }

    fn record_invocation(&mut self, command: Command) -> OperationId {
        let operation_id = OperationId::new(self.next_operation_id);
        self.next_operation_id += 1;
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        operation_id
    }

    fn record_completion(
        &mut self,
        operation_id: OperationId,
        outcome: ProposalOutcome,
    ) -> ProposalOutcome {
        match &outcome {
            ProposalOutcome::Committed {
                outcome: applied, ..
            } => {
                self.history.push(HistoryEvent::Completed {
                    operation_id,
                    response: applied.response.clone(),
                });
            }
            // A pre-replication rejection provably did not commit, but the
            // contract's history vocabulary has no weaker terminal event than
            // `Unknown`, and `Unknown` is the sound over-approximation.
            ProposalOutcome::Rejected { .. } | ProposalOutcome::Unknown => {
                self.history.push(HistoryEvent::Unknown { operation_id });
            }
        }
        outcome
    }

    fn allocate_local_proposal_id(&mut self) -> LocalProposalId {
        let id = LocalProposalId(self.next_local_proposal_id);
        self.next_local_proposal_id += 1;
        id
    }

    fn allocate_read_id(&mut self) -> ReadId {
        let id = ReadId(self.next_read_id);
        self.next_read_id += 1;
        id
    }

    fn node_index(&self, node_id: NodeId) -> usize {
        self.nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .expect("the driver only addresses its own nodes")
    }

    fn node(&self, node_id: NodeId) -> &ClusterNode {
        &self.nodes[self.node_index(node_id)]
    }

    fn node_mut(&mut self, node_id: NodeId) -> &mut ClusterNode {
        let index = self.node_index(node_id);
        &mut self.nodes[index]
    }
}

/// Returns the terminal outcome a proposal already reached while starting.
fn immediate_outcome(
    begin: &ProposalBegin<LedgerGroupId, ApplyOutcome>,
) -> Option<ProposalOutcome> {
    match begin {
        ProposalBegin::Completed {
            index,
            term,
            result,
            ..
        } => Some(ProposalOutcome::Committed {
            index: *index,
            term: *term,
            outcome: result.clone(),
        }),
        ProposalBegin::Rejected {
            reason,
            leader_hint,
            ..
        } => Some(ProposalOutcome::Rejected {
            reason: reason.clone(),
            leader_hint: *leader_hint,
        }),
        ProposalBegin::UnknownOutcome { .. } => Some(ProposalOutcome::Unknown),
        _ => None,
    }
}

fn open_group(
    node_id: NodeId,
    peers: &[u64],
    election_timeout_ticks: u64,
    storage: &NodeStorage,
    app: LedgerStateMachine,
) -> (LedgerGroup, LedgerReport) {
    let config = NodeConfig::new(
        node_id,
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("three-node static configuration is valid");
    let applied_index = app
        .applied_index()
        .expect("ledger state machines always report an applied index");
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        config,
        storage.hard_state.clone(),
        storage.log.clone(),
        storage.snapshots.reopen(),
        applied_index,
    )
    .expect("retained durable state reopens");
    let (raft, recovery_outputs) = recovered.into_parts();
    let mut group = RaftGroup::with_applied_index(GROUP_ID, node_id, raft, app, applied_index);
    let report = group
        .apply_raft_outputs(recovery_outputs)
        .expect("recovered outputs apply");
    (group, report)
}
