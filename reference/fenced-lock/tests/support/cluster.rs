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
//! with the published crates can write the same thing. A running replica's
//! state machine and durable log are read through
//! [`TransportRaftDriver::with_group`], which borrows the group under the
//! driver's own lock, so no replica is wrapped in anything to be observable.
//!
//! The driver is generic over the application each replica serves, because the
//! two lock state machines differ only in where their state lives. An in-memory
//! replica keeps its service in the value the retiring group hands back; a
//! durable replica drops that value and reopens its own slot files, which is
//! what a restarting process actually does. [`LockApps`] is the seam:
//! everything else in this file — delivery, isolation, elections, history
//! recording — is identical for both.
//!
//! One thing is still deliberately modeled rather than real. Durable Raft state
//! lives in in-memory stores that a retiring runtime hands back to the
//! incarnation replacing it, and every replica runs in one process. Durable
//! process composition arrives with a later slice.

use std::{
    collections::BTreeMap,
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
    unknown_outcome_reason, ApplyOutcome, Command, DurableLockStateMachine, HistoryEvent,
    LockClient, LockConfig, LockQuery, LockQueryResult, LockService, LockStateMachine, LogicalTime,
    OperationId, QueryOutcome, ResourceName, ResourceStatus, ServiceView, SubmitOutcome,
};
use rafter_runtime::{DurableRaftNode, DurableRaftNodeStorage};
use rafter_service::{
    InboundEnvelopeError, ManagedDriverError, MetricsWatch, TransportDriverOptions,
    TransportRaftDriver, UnknownOutcomeReason,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

use crate::transport::{DeterministicNetwork, NodeTransport, PeerDirectory};

/// The application contract every replica in this driver serves.
///
/// This names the lock service's application vocabulary without naming which
/// state machine provides it. It pins the associated types so the driver's
/// reports and history stay concrete, and it adds exactly one method.
///
/// That method exists because of a real property of the managed driver rather
/// than for convenience. [`TransportRaftDriver::with_group`] lends its group
/// only for the length of a closure and never returns a borrow that outlives
/// it, so unlike a hand-rolled group owner this cluster cannot hand a test back
/// a `&A::App`. Requiring one read accessor keeps every state comparison in
/// this file uniform across the two compositions instead of making the
/// in-memory machine `Clone` a precondition for being observable.
/// `Send + 'static` is not this file's requirement. It is
/// [`TransportRaftDriver`]'s, because a managed service resolves a client
/// waiter on a different task from the one that stepped the group.
pub trait LockApp:
    ReplicatedStateMachine<
        Command = Command,
        CommandResult = ApplyOutcome,
        Query = LockQuery,
        QueryResult = LockQueryResult,
    > + fmt::Debug
    + Send
    + 'static
{
    /// Returns the lock service this machine drives.
    fn lock_service(&self) -> &LockService;
}

impl LockApp for LockStateMachine {
    fn lock_service(&self) -> &LockService {
        self.service()
    }
}

impl LockApp for DurableLockStateMachine {
    fn lock_service(&self) -> &LockService {
        self.service()
    }
}

/// Supplies each replica's application and reopens it across a restart.
///
/// `reopen` is where the two compositions genuinely differ, and the difference
/// is the point: an in-memory replica's state survives because the value does,
/// while a durable replica's state — including every fencing high-water mark —
/// survives because its slot files do. A test that restarts a durable replica
/// must be dropping the value, or it is proving nothing about durability.
pub trait LockApps: fmt::Debug {
    /// The application this factory opens.
    type App: LockApp;

    /// Whether a replica's application can fail a driver step.
    ///
    /// An in-memory lock service cannot: it has no medium to fail on, so a
    /// refused step is a defect and this driver panics on it rather than
    /// recording a crash and carrying on with a quietly empty cluster. A
    /// durable composition can, and interrupting one is the whole point of its
    /// crash suite, so it opts in and the failure becomes an observation a test
    /// asserts on.
    const APPLICATIONS_CAN_FAIL: bool = false;

    /// Opens the application for a replica starting for the first time.
    fn open(&mut self, node_id: NodeId) -> Self::App;

    /// Reopens a restarting replica's application.
    fn reopen(&mut self, node_id: NodeId, retired: Self::App) -> Self::App;
}

/// Applications that keep their lock service in memory.
#[derive(Clone, Copy, Debug)]
pub struct InMemoryLockApps {
    config: LockConfig,
}

impl LockApps for InMemoryLockApps {
    type App = LockStateMachine;

    fn open(&mut self, _node_id: NodeId) -> Self::App {
        LockStateMachine::new(self.config)
    }

    /// An in-memory replica recovers because the retiring group handed its
    /// state machine back, applied index and all.
    fn reopen(&mut self, _node_id: NodeId, retired: Self::App) -> Self::App {
        retired
    }
}

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

/// Durable media one replica keeps across incarnations.
pub type LockStorage = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;

/// The durable runtime a replica runs, held by its group and nothing else.
type LockNode =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;

/// One replica's managed driver over its own transport endpoint.
///
/// Over the lock's own types rather than wrappers of them: the driver borrows
/// the group it owns, so nothing has to be shared with the cluster to stay
/// visible.
pub type NodeDriver<A> =
    TransportRaftDriver<LockGroupId, A, LockNode, NodeTransport, PeerDirectory>;

type LockGroup<A> = RaftGroup<LockGroupId, A, LockNode>;

/// Terminal client outcomes are carried by the application's own vocabulary,
/// so the cluster records history against these rather than raw receipts.
pub struct PendingSubmit {
    operation_id: OperationId,
    node_id: NodeId,
    /// The ID the driver allocated for this write, learned before the future
    /// resolved so this caller can retire exactly its own waiter.
    local_proposal_id: Option<LocalProposalId>,
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
    /// The ID the driver allocated for this barrier, learned the same way and
    /// for the same reason as [`PendingSubmit::local_proposal_id`].
    read_id: Option<ReadId>,
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
struct ClusterNode<A: LockApp> {
    node_id: NodeId,
    peers: Vec<NodeId>,
    election_timeout_ticks: u64,
    driver: NodeDriver<A>,
    client: LockClient<LockGroupId, NodeDriver<A>>,
    metrics: MetricsWatch<LockGroupId>,
    /// Proposals the app layer itself declared unresolvable.
    /// See [`LockCluster::runtime_unknown_outcomes`].
    runtime_unknown_outcomes: usize,
}

/// Deterministic three-node lock cluster with explicit delivery control.
pub struct LockCluster<A: LockApps = InMemoryLockApps> {
    config: LockConfig,
    network: DeterministicNetwork,
    nodes: Vec<ClusterNode<A::App>>,
    apps: A,
    /// Replicas whose application failed, with what each one reported.
    crashed: BTreeMap<NodeId, String>,
    history: Vec<HistoryEvent>,
    next_operation_id: u64,
}

impl<A: LockApps> fmt::Debug for LockCluster<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockCluster")
            .field("config", &self.config)
            .field("nodes", &self.nodes)
            .field("crashed", &self.crashed)
            .field("history", &self.history)
            .finish_non_exhaustive()
    }
}

impl LockCluster<InMemoryLockApps> {
    /// Builds three replicas whose lock services live in memory.
    pub fn new(config: LockConfig) -> Self {
        Self::with_apps(config, InMemoryLockApps { config })
    }
}

impl<A: LockApps> LockCluster<A> {
    /// Builds three replicas whose election timeouts make every uncontested
    /// election deterministic: the lowest-numbered reachable node wins.
    pub fn with_apps(config: LockConfig, mut apps: A) -> Self {
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
                let directory = PeerDirectory::new(&all_nodes);
                let transport = network.endpoint(node_id, directory.clone());
                let opened = open_group(
                    node_id,
                    &peers,
                    election_timeout_ticks,
                    empty_storage(),
                    apps.open(node_id),
                );
                // A first incarnation hands its recovery outputs to the driver
                // exactly as a restart does, so a replica that recovered a
                // non-empty durable log would route what it recovered rather
                // than needing the restart path.
                let driver = NodeDriver::new(
                    opened.group,
                    opened.recovery_outputs,
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
                    runtime_unknown_outcomes: 0,
                }
            })
            .collect();

        Self {
            config,
            network,
            nodes,
            apps,
            crashed: BTreeMap::new(),
            history: Vec::new(),
            next_operation_id: 1,
        }
    }

    /// Returns the configured lock bounds shared by every replica.
    pub fn config(&self) -> LockConfig {
        self.config
    }

    /// Returns the replicas whose durable application failed, with what each
    /// one reported.
    ///
    /// A test that does not expect a crash must assert this is empty. The
    /// driver records a refused step instead of panicking, so a regression that
    /// broke every replica would otherwise leave a suite green with an empty
    /// cluster quietly doing nothing.
    pub fn crashed(&self) -> Vec<(NodeId, &str)> {
        self.crashed
            .iter()
            .map(|(node_id, reason)| (*node_id, reason.as_str()))
            .collect()
    }

    /// Whether a replica's application has failed and it needs a restart.
    ///
    /// A crashed replica is not a partitioned one. Its process is still up and
    /// its transport still works, but its group is poisoned, so it neither
    /// ticks nor applies until a restart reopens it.
    pub fn is_crashed(&self, node_id: NodeId) -> bool {
        self.crashed.contains_key(&node_id)
    }

    /// Reads one replica's state machine under the driver's own lock.
    ///
    /// This is the general accessor, and it works on a crashed replica too:
    /// borrowing the group never checks poison, which is what lets a test ask a
    /// failed replica what it durably holds.
    pub fn with_state_machine<R>(&self, node_id: NodeId, read: impl FnOnce(&A::App) -> R) -> R {
        self.node(node_id)
            .driver
            .with_group(|group| read(group.state_machine()))
            .expect("a running replica holds its group")
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
    pub fn client(&self, node_id: NodeId) -> &LockClient<LockGroupId, NodeDriver<A::App>> {
        &self.node(node_id).client
    }

    /// Returns one replica's managed driver.
    pub fn driver(&self, node_id: NodeId) -> &NodeDriver<A::App> {
        &self.node(node_id).driver
    }

    /// Returns the reachable leader with the highest term, if one exists.
    ///
    /// A crashed replica is never a leader: its group stopped stepping, so
    /// whatever role its last successful round published is stale.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|node| self.network.reaches(node.node_id) && !self.is_crashed(node.node_id))
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

    /// Returns the replica's applied index.
    pub fn applied_index(&self, node_id: NodeId) -> LogIndex {
        self.with_state_machine(node_id, |app| {
            app.applied_index()
                .expect("lock state machines always report an applied index")
        })
    }

    /// Returns the canonical lock state one replica has applied.
    pub fn service_view(&self, node_id: NodeId) -> ServiceView {
        self.with_state_machine(node_id, |app| app.lock_service().view())
    }

    /// Returns one resource's status as one replica holds it.
    pub fn lock_status(&self, node_id: NodeId, resource: ResourceName) -> ResourceStatus {
        self.with_state_machine(node_id, |app| app.lock_service().status(resource))
    }

    /// Returns the index a replica's state machine must reach to have applied
    /// every application command that replica knows to be committed.
    ///
    /// This is the readiness half of [`LockCluster::applied_index`]: the two
    /// together say whether a replica has consumed everything it knows about.
    pub fn committed_application_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id)
            .driver
            .committed_application_index()
            .expect("a running replica holds its group")
    }

    /// Returns the commands committed on one replica, in log order.
    ///
    /// This reads the durable log through the public runtime accessor and
    /// decodes it with the same adapter the replica applies with, so the
    /// checker can replay a real replicated history through the oracle. Both
    /// halves come off the same borrowed group, which is why this is a closure
    /// rather than two forwarders.
    pub fn committed_commands(&self, node_id: NodeId) -> Vec<Command> {
        self.node(node_id)
            .driver
            .with_group(|group| {
                let runtime = group.runtime();
                let commit_index = runtime.commit_index();
                let first_index = runtime.snapshot_index().0 + 1;
                runtime
                    .log_entries_from(LogIndex(first_index))
                    .into_iter()
                    .zip(first_index..)
                    .take_while(|(_, index)| LogIndex(*index) <= commit_index)
                    .filter_map(|(entry, _)| match entry.kind {
                        LogEntryKind::Application(payload) => Some(
                            group
                                .state_machine()
                                .decode_command(payload.as_ref())
                                .expect("replicas only append frames this adapter encoded"),
                        ),
                        LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
                    })
                    .collect()
            })
            .expect("a running replica holds its group")
    }

    /// Returns how many of this replica's proposals the app layer itself
    /// declared unresolvable.
    ///
    /// A driver keeps no count of its own: it resolves the waiter, and the fact
    /// goes to whoever holds that client future. So this is read off the public
    /// client surface, which is the stronger evidence anyway — it is the same
    /// value a real client would see.
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
                    || self.is_crashed(node_id)
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
        }
    }

    /// Ticks every running node once, cut off or not.
    ///
    /// A partitioned replica is not a stopped replica. It keeps ticking, keeps
    /// offering frames its transport refuses, and keeps believing whatever its
    /// last successful round told it. A *crashed* replica is different: its
    /// application failed, its group is poisoned, and it is skipped until a
    /// restart reopens it.
    pub fn tick_all(&mut self) {
        for node_id in self.node_ids() {
            if self.is_crashed(node_id) {
                continue;
            }
            let outcome = self.node(node_id).driver.tick();
            self.record_step(node_id, outcome);
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
                let node_id = envelope.raft_to;
                if self.is_crashed(node_id) {
                    continue;
                }
                // A frame that fails this replica's own authentication policy is
                // refused inside the driver, before the group is stepped, which
                // is exactly where a production embedder refuses it.
                match self.node(node_id).driver.deliver(envelope) {
                    Ok(()) | Err(InboundEnvelopeError::Rejected { .. }) => {}
                    // An application that could not make a committed entry
                    // durable poisons its group. That is a crashed replica, not
                    // a malformed frame, and the driver reports it here.
                    Err(InboundEnvelopeError::Driver { source }) => self.crash(node_id, &source),
                    Err(error) => panic!("a healthy group accepts validated frames: {error}"),
                }
            }
        }
        panic!("peer delivery did not quiesce within {MAX_DELIVERY_PASSES} passes");
    }

    /// Collects every granted read proof on every replica.
    ///
    /// A granted barrier is consumed by a read call rather than announced to a
    /// client, so this is the third entry point beside tick and deliver. A
    /// barrier the cluster rejected or cancelled needs nothing from here: the
    /// step that observed it resolved its client already.
    pub fn drive_reads(&mut self) {
        for node_id in self.node_ids() {
            if self.is_crashed(node_id) {
                continue;
            }
            let outcome = self.node(node_id).driver.drive_pending_reads();
            self.record_step(node_id, outcome);
        }
    }

    /// Records a driver step's outcome, marking the replica crashed on failure.
    fn record_step(&mut self, node_id: NodeId, outcome: Result<(), ManagedDriverError>) {
        if let Err(error) = outcome {
            self.crash(node_id, &error);
        }
    }

    /// Marks a replica crashed, keeping the first failure it reported.
    ///
    /// The first one is the interesting one: every later step on a poisoned
    /// group reports the poison rather than what caused it.
    fn crash(&mut self, node_id: NodeId, error: &ManagedDriverError) {
        assert!(
            A::APPLICATIONS_CAN_FAIL,
            "replica {} failed a driver step in a composition whose application cannot fail: {error}",
            node_id.0
        );
        self.crashed
            .entry(node_id)
            .or_insert_with(|| error.to_string());
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

    /// Restarts one replica over its retained durable media, clearing a crash.
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
        // The application is reopened rather than carried across. For a durable
        // replica that means the retiring value is dropped and everything the
        // new incarnation knows — including every fencing high-water mark — is
        // read back from its slot files.
        let state_machine = self.apps.reopen(node_id, state_machine);
        // The returned ID watermarks are deliberately unused. They are
        // load-bearing only when the same runtime is carried into the new
        // group; this replica drops that runtime and rebuilds one from the
        // durable storage it returns, and a rebuilt runtime carries no local
        // proposal tracking, so a group over it may restart its IDs at zero.
        // The driver's own counters never restart anyway, which is stricter
        // than the contract requires.
        let storage = runtime.into_storage();

        let opened = open_group(
            node_id,
            &peers,
            election_timeout_ticks,
            storage,
            state_machine,
        );
        self.nodes[index]
            .driver
            .adopt_group(opened.group, opened.recovery_outputs)
            .expect("the reopened incarnation installs and its outputs apply");
        self.crashed.remove(&node_id);
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
            local_proposal_id: newest_pending_write(&self.node(node_id).driver),
            future,
        }
    }

    /// Waits for a pending submission for at most `rounds` driver rounds.
    ///
    /// A caller that runs out of rounds stops waiting and hands the write back
    /// to the driver, which resolves this client with its own vocabulary and its
    /// own allocated `LocalProposalId` — neither of which a caller could author.
    /// That is a real client's situation, not a test shortcut, and the window it
    /// closes is genuinely unknown: an appended entry may still commit.
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
        let local_proposal_id = pending
            .local_proposal_id
            .expect("an unresolved write is one the driver named at submission");
        assert!(
            self.node(pending.node_id)
                .driver
                .abandon_write(local_proposal_id),
            "an unresolved write is still the driver's to retire"
        );
        let Poll::Ready(outcome) = poll_once(&mut pending.future) else {
            panic!("an abandoned write resolves its client before this returns");
        };
        self.observe_outcome(pending.node_id, &outcome);
        self.record_completion(pending.operation_id, outcome)
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
        PendingQuery {
            node_id,
            read_id: newest_pending_read(&self.node(node_id).driver),
            future,
        }
    }

    /// Waits for a pending query for at most `rounds` driver rounds.
    ///
    /// A caller that runs out of rounds hands the barrier back to the driver,
    /// which cancels it through the group and resolves this client with its own
    /// terminal vocabulary — including the `ReadId`, which the caller learns
    /// from the driver rather than inventing. An abandoned read is a terminal
    /// non-answer rather than an unknown outcome: a read takes no effect, so
    /// there is nothing left to happen.
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
        let read_id = pending
            .read_id
            .expect("an unresolved barrier is one the driver named at submission");
        assert!(
            self.node(pending.node_id).driver.abandon_read(read_id),
            "an unresolved barrier is still the driver's to retire"
        );
        let Poll::Ready(outcome) = poll_once(&mut pending.future) else {
            panic!("an abandoned barrier resolves its client before this returns");
        };
        outcome
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
        self.with_state_machine(node_id, |app| app.lock_service().logical_time())
    }

    /// Records what a resolved write outcome said about the app layer.
    ///
    /// The driver hands the fact to whoever holds the client future and keeps
    /// no count of its own, so this is the one place it is seen.
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

    fn node(&self, node_id: NodeId) -> &ClusterNode<A::App> {
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
struct OpenedGroup<A: LockApp> {
    group: LockGroup<A>,
    recovery_outputs: Vec<RaftOutput>,
}

/// Returns empty durable storage for a replica that has never started.
///
/// Only a replica that has never started gets this. Every later incarnation
/// recovers from the stores `DurableRaftNode::into_storage` returned, including
/// the snapshot store, which is its own medium rather than something a restart
/// may quietly replace.
fn empty_storage() -> LockStorage {
    LockStorage {
        hard_state_store: InMemoryRaftHardStateStore::default(),
        log_segment: InMemoryRaftLogSegment::default(),
        snapshot_store: InMemoryRaftSnapshotStore::new(),
    }
}

fn open_group<A: LockApp>(
    node_id: NodeId,
    peers: &[NodeId],
    election_timeout_ticks: u64,
    storage: LockStorage,
    app: A,
) -> OpenedGroup<A> {
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
    let group = RaftGroup::with_applied_index(GROUP_ID, node_id, node, app, applied_index);
    OpenedGroup {
        group,
        recovery_outputs,
    }
}

/// Returns whether the app layer itself declared this proposal's outcome lost.
fn outcome_lost_its_proposal(outcome: &SubmitOutcome) -> bool {
    let (SubmitOutcome::Unknown { error } | SubmitOutcome::Refused { error }) = outcome else {
        return false;
    };
    unknown_outcome_reason(error) == Some(UnknownOutcomeReason::RuntimeDroppedProposal)
}

/// Returns the ID of the write a driver most recently admitted.
///
/// Local proposal IDs are strictly increasing for a driver's lifetime, so the
/// highest unresolved one is the write that was just started. `None` means the
/// write resolved inside its first poll and has no waiter left to name.
fn newest_pending_write<A: LockApp>(driver: &NodeDriver<A>) -> Option<LocalProposalId> {
    driver
        .pending_writes()
        .into_iter()
        .map(|write| write.local_proposal_id)
        .max()
}

/// Returns the ID of the barrier a driver most recently admitted, on the same
/// monotonicity as [`newest_pending_write`].
fn newest_pending_read<A: LockApp>(driver: &NodeDriver<A>) -> Option<ReadId> {
    driver.pending_reads().into_iter().max()
}

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}
