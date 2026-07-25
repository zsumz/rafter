//! Consumer-owned deterministic three-node lock cluster over `rafter-service`.
//!
//! Each replica is a [`TransportRaftDriver`] over its own group, its own
//! transport endpoint, and its own [`RaftHandle`]. The driver owns the waiter
//! tables, the ID allocators, the report routing, and the movable group slot; a
//! consumer supplies none of that. The cluster below owns only what a real
//! deployment owns too — when a node ticks, which frames are delivered, which
//! links are cut, when a replica retires one incarnation for another over the
//! same durable stores, and what the history records.
//!
//! The split matters for the histories. A client future here resolves when the
//! *driver* observes a terminal proposal or read outcome, not when the client
//! asks. That is what lets a test start an acquisition, cut the leader's links
//! while it is in flight, and then watch the outcome window close.
//!
//! No simulator, no internal hooks, no privileged observation: an external user
//! with the published crates can write the same thing. The one place that takes
//! effort is reading a running replica's state at all, which is
//! [`crate::observe`].

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use rafter::{
    LocalProposalId, LogEntryKind, LogIndex, NodeConfig, NodeId, Output as RaftOutput, ReadId, Role,
};
use rafter_app::{
    group::{RaftGroup, RaftGroupParts},
    state_machine::ReplicatedStateMachine,
};
use rafter_reference_fenced_lock::{
    unknown_outcome_reason, Command, HistoryEvent, LockClient, LockConfig, LockStateMachine,
    LogicalTime, OperationId, QueryOutcome, ResourceName, SubmitOutcome,
};
use rafter_runtime::{DurableRaftNode, PersistedRaftRuntime};
use rafter_service::{
    ErrorCause, InboundEnvelopeError, MetricsWatch, ReadAbandonReason, ReadError,
    TransportDriverOptions, TransportRaftDriver, UnknownOutcomeReason, WriteError,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

use crate::{
    observe::{LockStorage, SharedRuntime, SharedStateMachine},
    transport::{DeterministicNetwork, NodeTransport, PeerDirectory},
};

/// Bound on driver rounds spent waiting for one outcome.
///
/// Every wait is bounded so a stalled protocol fails the test instead of
/// hanging, and so a client that runs out of rounds observes the same unknown
/// outcome a real client observes when it stops waiting.
pub const MAX_ROUNDS: usize = 32;

/// Bound on drain passes inside one delivery, so a message storm fails loudly.
const MAX_DELIVERY_PASSES: usize = 64;

/// Caller-defined group identity for the single lock group.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LockGroupId(pub u64);

/// The one group every node in this driver serves.
pub const GROUP_ID: LockGroupId = LockGroupId(1);

/// One replica's managed driver over its own transport endpoint.
pub type NodeDriver = TransportRaftDriver<
    LockGroupId,
    SharedStateMachine,
    SharedRuntime,
    NodeTransport,
    PeerDirectory,
>;

type LockGroup = RaftGroup<LockGroupId, SharedStateMachine, SharedRuntime>;

/// Terminal client outcomes are carried by the application's own vocabulary,
/// so the cluster records history against these rather than raw receipts.
pub struct PendingSubmit {
    operation_id: OperationId,
    node_id: NodeId,
    future: SubmitFuture,
}

type SubmitFuture = Pin<Box<dyn Future<Output = SubmitOutcome>>>;

impl fmt::Debug for PendingSubmit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingSubmit")
            .field("operation_id", &self.operation_id)
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

/// One linearizable query in flight against a chosen replica.
pub struct PendingQuery {
    node_id: NodeId,
    future: Pin<Box<dyn Future<Output = QueryOutcome<LockGroupId>>>>,
}

impl fmt::Debug for PendingQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingQuery")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ClusterNode {
    node_id: NodeId,
    peers: Vec<NodeId>,
    election_timeout_ticks: u64,
    driver: NodeDriver,
    client: LockClient<LockGroupId, NodeDriver>,
    metrics: MetricsWatch<LockGroupId>,
    app: SharedStateMachine,
    runtime: SharedRuntime,
    /// Proposals the app layer declared unresolvable after their caller had
    /// already stopped waiting. See [`LockCluster::runtime_unknown_outcomes`].
    runtime_unknown_outcomes: usize,
    /// The most recent barrier this replica's driver could not carry forward.
    ///
    /// One slot is enough because this harness keeps at most one query in
    /// flight per replica, and the driver repeats the same refusal on every
    /// later pump. See [`LockCluster::drive_reads`].
    lost_barrier: Option<ReadError>,
}

/// Deterministic three-node lock cluster with explicit delivery control.
pub struct LockCluster {
    config: LockConfig,
    network: DeterministicNetwork,
    nodes: Vec<ClusterNode>,
    history: Vec<HistoryEvent>,
    next_operation_id: u64,
    /// Client futures whose caller stopped waiting.
    ///
    /// The driver removes a waiter only when its future is polled after the
    /// waiter resolved, and offers no way to abandon one, so a caller that
    /// walked away leaves a slot the driver will fill and nobody will read.
    /// Keeping the future is how this cluster still observes a late outcome —
    /// which is the stronger evidence anyway, because it arrives through the
    /// public client surface rather than through a private counter.
    abandoned: Vec<(NodeId, SubmitFuture)>,
}

impl fmt::Debug for LockCluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockCluster")
            .field("config", &self.config)
            .field("nodes", &self.nodes)
            .field("history", &self.history)
            .field("abandoned", &self.abandoned.len())
            .finish_non_exhaustive()
    }
}

impl LockCluster {
    /// Builds three replicas whose election timeouts make every uncontested
    /// election deterministic: the lowest-numbered reachable node wins.
    pub fn new(config: LockConfig) -> Self {
        let network = DeterministicNetwork::new();
        let all_nodes = [NodeId(1), NodeId(2), NodeId(3)];
        let nodes = [(NodeId(1), 4), (NodeId(2), 6), (NodeId(3), 8)]
            .into_iter()
            .map(|(node_id, election_timeout_ticks)| {
                let peers = all_nodes
                    .iter()
                    .copied()
                    .filter(|peer| *peer != node_id)
                    .collect::<Vec<_>>();
                let directory = PeerDirectory::new(&all_nodes, &peers);
                let transport = network.endpoint(node_id, directory.clone());
                let app = SharedStateMachine::new(LockStateMachine::new(config));
                let opened = open_group(
                    node_id,
                    &peers,
                    election_timeout_ticks,
                    empty_storage(),
                    app.clone(),
                );

                // `TransportRaftDriver::new` takes a group and no recovery
                // outputs, so a first incarnation has to apply them outside the
                // driver — unlike `adopt_group`, which routes them. That is
                // lossless only while the report they produce carries nothing to
                // route, which this asserts rather than assumes. A replica whose
                // first incarnation recovered a non-empty durable log would need
                // the restart path instead.
                let mut group = opened.group;
                let report = group
                    .apply_raft_outputs(opened.recovery_outputs)
                    .expect("recovered outputs apply");
                assert!(
                    report.peer_messages.is_empty(),
                    "a replica that has never started recovers nothing to route"
                );

                let driver = NodeDriver::new(
                    group,
                    Vec::new(),
                    transport,
                    directory,
                    TransportDriverOptions::default(),
                )
                .expect("a quiescent group is adoptable");
                let handle = driver.handle();
                let metrics = handle
                    .metrics()
                    .expect("a handle opens a metrics watch for its own group");
                ClusterNode {
                    node_id,
                    peers,
                    election_timeout_ticks,
                    client: LockClient::new(handle),
                    driver,
                    metrics,
                    app,
                    runtime: opened.runtime,
                    runtime_unknown_outcomes: 0,
                    lost_barrier: None,
                }
            })
            .collect();

        Self {
            config,
            network,
            nodes,
            history: Vec::new(),
            next_operation_id: 1,
            abandoned: Vec::new(),
        }
    }

    /// Returns the configured lock bounds shared by every replica.
    pub fn config(&self) -> LockConfig {
        self.config
    }

    /// Returns the recorded client history.
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns every node ID in the cluster.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }

    /// Returns one replica's managed lock client.
    pub fn client(&self, node_id: NodeId) -> &LockClient<LockGroupId, NodeDriver> {
        &self.node(node_id).client
    }

    /// Returns one replica's managed driver.
    pub fn driver(&self, node_id: NodeId) -> &NodeDriver {
        &self.node(node_id).driver
    }

    /// Returns the reachable leader with the highest term, if one exists.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|node| self.network.reaches(node.node_id))
            .filter_map(|node| {
                let metrics = node.metrics.current();
                (metrics.role == Role::Leader).then_some((metrics.term, node.node_id))
            })
            .max()
            .map(|(_, node_id)| node_id)
    }

    /// Returns a replica's role as its own metrics report it.
    ///
    /// A node with cut links keeps reporting whatever its last round told it,
    /// which is how an isolated former leader still believes it leads.
    pub fn believes_it_leads(&self, node_id: NodeId) -> bool {
        self.node(node_id).metrics.current().role == Role::Leader
    }

    /// Returns a copy of one replica's state machine.
    pub fn state_machine(&self, node_id: NodeId) -> LockStateMachine {
        self.node(node_id).app.observe()
    }

    /// Returns the replica's applied index.
    pub fn applied_index(&self, node_id: NodeId) -> LogIndex {
        self.state_machine(node_id)
            .applied_index()
            .expect("lock state machines always report an applied index")
    }

    /// Returns the index a replica's state machine must reach to have applied
    /// every application command that replica knows to be committed.
    ///
    /// This is the readiness half of [`LockCluster::applied_index`]: the two
    /// together say whether a replica has consumed everything it knows about.
    pub fn committed_application_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).runtime.committed_application_index()
    }

    /// Returns the commands committed on one replica, in log order.
    ///
    /// This reads the durable log through the public runtime accessor and
    /// decodes it with the same adapter the replica applies with, so the
    /// checker can replay a real replicated history through the oracle.
    pub fn committed_commands(&self, node_id: NodeId) -> Vec<Command> {
        let node = self.node(node_id);
        let commit_index = node.runtime.commit_index();
        let first_index = node.runtime.snapshot_index().0 + 1;
        node.runtime
            .log_entries_from(LogIndex(first_index))
            .into_iter()
            .zip(first_index..)
            .take_while(|(_, index)| LogIndex(*index) <= commit_index)
            .filter_map(|(entry, _)| match entry.kind {
                LogEntryKind::Application(payload) => Some(
                    node.app
                        .decode_command(payload.as_ref())
                        .expect("replicas only append frames this adapter encoded"),
                ),
                LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
            })
            .collect()
    }

    /// Returns how many of this replica's proposals the app layer itself
    /// declared unresolvable after their caller had stopped waiting.
    ///
    /// A driver never reports this on its own — it resolves the waiter and the
    /// fact goes wherever the client future went — so the cluster keeps those
    /// futures and reads the answer off the public client surface.
    pub fn runtime_unknown_outcomes(&self, node_id: NodeId) -> usize {
        self.node(node_id).runtime_unknown_outcomes
    }

    /// Drives ticks until a leader exists among the reachable nodes.
    pub fn elect_leader(&mut self) -> NodeId {
        for _ in 0..MAX_ROUNDS {
            if let Some(leader) = self.leader() {
                return leader;
            }
            self.run_rounds(1);
        }
        panic!("no leader was elected within {MAX_ROUNDS} rounds");
    }

    /// Drives the cluster until every reachable replica has applied every
    /// application command it knows to be committed.
    ///
    /// Elections and membership changes commit entries the state machine never
    /// sees, so the applied index legitimately trails the commit index and the
    /// gate is the group's committed application index instead. Convergence is
    /// a precondition for comparing replicas to each other and to the oracle,
    /// never something a test may assume.
    pub fn settle(&mut self) {
        for _ in 0..MAX_ROUNDS {
            let converged = self.node_ids().into_iter().all(|node_id| {
                !self.network.reaches(node_id)
                    || self.applied_index(node_id) >= self.committed_application_index(node_id)
            });
            if converged && self.network.is_idle() {
                return;
            }
            self.run_rounds(1);
        }
        panic!("replicas did not apply their committed commands within {MAX_ROUNDS} rounds");
    }

    /// Ticks every node, delivers every accepted frame, and retries every
    /// outstanding read barrier, `rounds` times.
    pub fn run_rounds(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.deliver_all();
            self.tick_all();
            self.deliver_all();
            self.drive_reads();
            self.poll_abandoned();
        }
    }

    /// Ticks every running node once, cut off or not.
    ///
    /// A partitioned replica is not a stopped replica. It keeps ticking, keeps
    /// offering frames its transport refuses, and keeps believing whatever its
    /// last successful round told it.
    pub fn tick_all(&mut self) {
        for node in &self.nodes {
            node.driver.tick().expect("a healthy group accepts ticks");
        }
    }

    /// Delivers every accepted frame, including frames delivery produces.
    pub fn deliver_all(&mut self) {
        for _ in 0..MAX_DELIVERY_PASSES {
            let batch = self.network.take_deliverable();
            if batch.is_empty() {
                return;
            }
            for envelope in batch {
                let driver = &self.node(envelope.raft_to).driver;
                // A frame that fails this replica's own authentication policy is
                // refused inside the driver, before the group is stepped, which
                // is exactly where a production embedder refuses it.
                match driver.deliver(envelope) {
                    Ok(()) | Err(InboundEnvelopeError::Rejected { .. }) => {}
                    Err(error) => panic!("a healthy group accepts validated frames: {error}"),
                }
            }
        }
        panic!("peer delivery did not quiesce within {MAX_DELIVERY_PASSES} passes");
    }

    /// Retries every outstanding read barrier on every replica.
    ///
    /// A driver error here is not a cluster failure, and catching it is a
    /// workaround rather than a design. `TransportRaftDriver` never inspects a
    /// step report's read events, so a barrier the cluster rejected or
    /// cancelled during a tick or a delivery is never reported to its client.
    /// The group has dropped that barrier's state, so the driver's next retry
    /// asks it to re-reserve a spent `ReadId` and the group refuses with
    /// `GroupError::NonMonotonicReadId`. That refusal arrives here as the only
    /// evidence the barrier is gone, and the driver leaves the client waiter
    /// unresolved forever, so the cluster records it and hands it to the caller
    /// in [`LockCluster::resolve_query`]. A driver that observed read events
    /// would resolve the waiter itself and this arm would be unreachable.
    pub fn drive_reads(&mut self) {
        for node in &mut self.nodes {
            if let Err(error) = node.driver.drive_pending_reads() {
                node.lost_barrier = Some(ReadError::Transport {
                    cause: ErrorCause::new(error),
                });
            }
        }
    }

    /// Cuts every link to and from `node_id`.
    pub fn isolate(&mut self, node_id: NodeId) {
        self.network.isolate(node_id);
    }

    /// Cuts only the links into `node_id`, leaving it able to send.
    pub fn isolate_inbound(&mut self, node_id: NodeId) {
        self.network.isolate_inbound(node_id);
    }

    /// Restores every cut link.
    pub fn heal(&mut self) {
        self.network.heal();
    }

    /// Restarts one replica over its retained durable media.
    ///
    /// The driver owns the movable slot, so the recipe is release, decompose,
    /// recover, adopt. `release_group` retires the running incarnation and
    /// resolves every outstanding waiter before it returns — writes as unknown
    /// outcomes, because an appended entry may still commit under the next
    /// incarnation, and reads as abandoned. From there this follows the
    /// documented decomposition recipe: the retiring group hands back its state
    /// machine, its runtime hands back all three durable stores, and the
    /// reopened runtime recovers through the application's own durable applied
    /// floor. `adopt_group` takes the recovery *outputs* rather than an
    /// already-applied group, so the recovery report's peer messages and
    /// snapshot directives are routed by the driver instead of being dropped
    /// outside it. The managed handle survives, because a handle names a
    /// service rather than a node incarnation.
    pub fn restart(&mut self, node_id: NodeId) {
        let index = self.node_index(node_id);
        let peers = self.nodes[index].peers.clone();
        let election_timeout_ticks = self.nodes[index].election_timeout_ticks;

        let group = self.nodes[index]
            .driver
            .release_group()
            .expect("a running node holds its group");
        let RaftGroupParts {
            state_machine,
            runtime,
            ..
        } = group.into_parts();
        // The returned ID watermarks are deliberately unused. They are
        // load-bearing only when the same runtime is carried into the new
        // group; this replica drops that runtime and rebuilds one from the
        // durable storage it returns, and a rebuilt runtime carries no local
        // proposal tracking, so a group over it may restart its IDs at zero.
        // The driver's own counters never restart anyway, which is stricter
        // than the contract requires.
        let storage = runtime.take_storage();

        let opened = open_group(
            node_id,
            &peers,
            election_timeout_ticks,
            storage,
            state_machine,
        );
        self.nodes[index].runtime = opened.runtime;
        self.nodes[index]
            .driver
            .adopt_group(opened.group, opened.recovery_outputs)
            .expect("the reopened incarnation installs and its outputs apply");
    }

    /// Invokes one command against `node_id` without waiting for it.
    ///
    /// The invocation is recorded immediately, so a command whose outcome is
    /// later lost still appears in the history with the exact bytes a retry
    /// must repeat.
    pub fn begin_submit(&mut self, node_id: NodeId, command: Command) -> PendingSubmit {
        let operation_id = self.record_invocation(command);
        let client = self.node(node_id).client.clone();
        let mut future: SubmitFuture =
            Box::pin(async move { client.submit_command(command).await });
        // The handle's methods are `async fn`s, so the driver's `write` — and
        // with it the waiter registration — does not run until this first poll.
        if let Poll::Ready(outcome) = poll_once(&mut future) {
            future = Box::pin(std::future::ready(outcome));
        }
        PendingSubmit {
            operation_id,
            node_id,
            future,
        }
    }

    /// Waits for a pending submission for at most `rounds` driver rounds.
    ///
    /// A caller that runs out of rounds stops waiting and closes its own
    /// outcome window as unknown. That is a real client's situation, not a test
    /// shortcut. The driver offers no way to say so — nothing abandons one
    /// waiter — so the outcome is authored here and the future is kept, because
    /// the driver may still resolve it with something worth observing.
    pub fn resolve(&mut self, mut pending: PendingSubmit, rounds: usize) -> SubmitOutcome {
        for _ in 0..rounds {
            if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
                self.observe_outcome(pending.node_id, &outcome);
                return self.record_completion(pending.operation_id, outcome);
            }
            self.run_rounds(1);
        }
        if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
            self.observe_outcome(pending.node_id, &outcome);
            return self.record_completion(pending.operation_id, outcome);
        }
        self.abandoned.push((pending.node_id, pending.future));
        self.record_completion(pending.operation_id, caller_stopped_waiting())
    }

    /// Submits one command and waits for it under the default round budget.
    pub fn submit(&mut self, node_id: NodeId, command: Command) -> SubmitOutcome {
        let pending = self.begin_submit(node_id, command);
        self.resolve(pending, MAX_ROUNDS)
    }

    /// Starts one linearizable `GetLock` against `node_id` without waiting.
    pub fn begin_query(&mut self, node_id: NodeId, resource: ResourceName) -> PendingQuery {
        let client = self.node(node_id).client.clone();
        let mut future: Pin<Box<dyn Future<Output = QueryOutcome<LockGroupId>>>> =
            Box::pin(async move { client.get_lock(resource).await });
        if let Poll::Ready(outcome) = poll_once(&mut future) {
            future = Box::pin(std::future::ready(outcome));
        }
        PendingQuery { node_id, future }
    }

    /// Waits for a pending query for at most `rounds` driver rounds.
    ///
    /// A barrier the driver lost (see [`LockCluster::drive_reads`]) is reported
    /// with the driver's own typed error, because that is the only account of
    /// it that exists. Otherwise the caller simply stopped waiting, and an
    /// abandoned read is a terminal non-answer rather than an unknown outcome:
    /// a read takes no effect, so there is nothing left to happen.
    pub fn resolve_query(
        &mut self,
        mut pending: PendingQuery,
        rounds: usize,
    ) -> QueryOutcome<LockGroupId> {
        for _ in 0..rounds {
            if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
                return outcome;
            }
            self.run_rounds(1);
        }
        if let Poll::Ready(outcome) = poll_once(&mut pending.future) {
            return outcome;
        }
        let index = self.node_index(pending.node_id);
        let error = self.nodes[index]
            .lost_barrier
            .take()
            .unwrap_or_else(caller_stopped_reading);
        QueryOutcome::Unavailable { error }
    }

    /// Runs one linearizable `GetLock` under the default round budget.
    pub fn get_lock(
        &mut self,
        node_id: NodeId,
        resource: ResourceName,
    ) -> QueryOutcome<LockGroupId> {
        let pending = self.begin_query(node_id, resource);
        self.resolve_query(pending, MAX_ROUNDS)
    }

    /// Returns how many accepted frames the network dropped before delivery.
    pub fn dropped_inbound(&self) -> u64 {
        self.network.dropped_inbound()
    }

    /// Returns replicated logical time as one replica has applied it.
    pub fn logical_time(&self, node_id: NodeId) -> LogicalTime {
        self.state_machine(node_id).service().logical_time()
    }

    /// Polls every future whose caller stopped waiting.
    ///
    /// Its terminal event is already in the history; this only observes what
    /// the driver eventually had to say, which is how a replica reports that it
    /// lost a proposal nobody was listening for any more.
    fn poll_abandoned(&mut self) {
        let mut retained = Vec::with_capacity(self.abandoned.len());
        for (node_id, mut future) in std::mem::take(&mut self.abandoned) {
            match poll_once(&mut future) {
                Poll::Ready(outcome) => self.observe_outcome(node_id, &outcome),
                Poll::Pending => retained.push((node_id, future)),
            }
        }
        self.abandoned = retained;
    }

    /// Records what a resolved write outcome said about the app layer.
    ///
    /// The driver hands the fact to whoever holds the client future and keeps
    /// no count of its own, so this is the one place it is seen — whether the
    /// caller was still waiting or had already walked away.
    fn observe_outcome(&mut self, node_id: NodeId, outcome: &SubmitOutcome) {
        if outcome_lost_its_proposal(outcome) {
            let index = self.node_index(node_id);
            self.nodes[index].runtime_unknown_outcomes += 1;
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
        outcome: SubmitOutcome,
    ) -> SubmitOutcome {
        self.history.push(outcome.history_event(operation_id));
        outcome
    }

    fn node(&self, node_id: NodeId) -> &ClusterNode {
        &self.nodes[self.node_index(node_id)]
    }

    fn node_index(&self, node_id: NodeId) -> usize {
        self.nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .expect("the cluster only addresses its own nodes")
    }
}

/// One replica's incarnation, before a driver takes it.
struct OpenedGroup {
    group: LockGroup,
    recovery_outputs: Vec<RaftOutput>,
    runtime: SharedRuntime,
}

/// Returns empty durable storage for a replica that has never started.
///
/// Only a replica that has never started gets this. Every later incarnation
/// recovers from the stores `DurableRaftNode::into_storage` returned, including
/// the snapshot store — which this slice's state machine never writes, because
/// it declares no snapshot support, but which is still its own medium rather
/// than something a restart may quietly replace.
fn empty_storage() -> LockStorage {
    LockStorage {
        hard_state_store: InMemoryRaftHardStateStore::default(),
        log_segment: InMemoryRaftLogSegment::default(),
        snapshot_store: InMemoryRaftSnapshotStore::new(),
    }
}

fn open_group(
    node_id: NodeId,
    peers: &[NodeId],
    election_timeout_ticks: u64,
    storage: LockStorage,
    app: SharedStateMachine,
) -> OpenedGroup {
    let config = NodeConfig::new(node_id, peers.to_vec(), election_timeout_ticks)
        .expect("three-node static configuration is valid");
    let applied_index = app
        .applied_index()
        .expect("lock state machines always report an applied index");
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        config,
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
        applied_index,
    )
    .expect("retained durable state reopens");
    let (node, recovery_outputs) = recovered.into_parts();
    let runtime = SharedRuntime::new(node);
    let group =
        RaftGroup::with_applied_index(GROUP_ID, node_id, runtime.clone(), app, applied_index);
    OpenedGroup {
        group,
        recovery_outputs,
        runtime,
    }
}

/// The outcome a client authors when it stops waiting for a write.
///
/// The driver has no term for this. `UnknownOutcomeReason::DriveBoundReached`
/// is the closest one it publishes, and it names a *driver's* bound rather than
/// a client's; the local proposal ID is unavailable for the same reason, since
/// nothing exposes which one the driver allocated.
fn caller_stopped_waiting() -> SubmitOutcome {
    SubmitOutcome::Unknown {
        error: WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(0),
            client_request_id: None,
            reason: UnknownOutcomeReason::DriveBoundReached,
        },
    }
}

/// The non-answer a client authors when it stops waiting for a barrier.
fn caller_stopped_reading() -> ReadError {
    ReadError::Abandoned {
        read_id: ReadId(0),
        reason: ReadAbandonReason::DriveBoundReached,
    }
}

/// Returns whether the app layer itself declared this proposal's outcome lost.
fn outcome_lost_its_proposal(outcome: &SubmitOutcome) -> bool {
    let (SubmitOutcome::Unknown { error } | SubmitOutcome::Refused { error }) = outcome else {
        return false;
    };
    unknown_outcome_reason(error) == Some(UnknownOutcomeReason::RuntimeDroppedProposal)
}

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}
