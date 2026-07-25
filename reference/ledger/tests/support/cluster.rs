//! Consumer-owned deterministic three-node driver over Rafter's public APIs.
//!
//! The driver owns every choice a real embedder owns: when a node ticks, which
//! envelopes are delivered, which links are cut, and when a replica restarts.
//! It uses no simulator, no internal hooks, and no privileged observation; an
//! external user with the published crates can write the same thing.
//!
//! The driver is generic over the application each replica serves, because the
//! two ledger state machines differ only in where their state lives. An
//! in-memory replica keeps its ledger in the value the retiring group hands
//! back; a durable replica drops that value and reopens its own journal, which
//! is what a restarting process actually does. [`LedgerApps`] is the seam:
//! everything else in this file — delivery, isolation, elections, history
//! recording — is identical for both.
//!
//! One thing here is deliberately modeled rather than real. Durable Raft state
//! lives in in-memory stores that a retiring runtime hands back to the
//! incarnation replacing it, so a restart in this driver is an in-process
//! decomposition rather than a new process reading a disk. That is not a gap
//! any more: `process_cluster.rs` runs the same application as real processes
//! over file-backed Raft stores. The two drivers answer different questions and
//! both are kept — this one decides *when* every node ticks and which envelope
//! is delivered, which is what makes its failures reproducible.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use rafter::{
    LocalProposalId, LogEntryKind, LogIndex, NodeConfig, NodeId, ProposalRejection, ReadId,
    ReadIndexCancelReason, ReadIndexRejection, Role, Term,
};
use rafter_app::group::{GroupInput, GroupStepReport, RaftGroup, ReadReport};
use rafter_app::proposal::{Proposal, ProposalBegin, ProposalEvent};
use rafter_app::read::{ReadEvent, ReadOutcome as GroupReadOutcome, ReadRequest};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_app::transport::PeerEnvelope;
use rafter_reference_ledger::{
    ApplyDisposition, ApplyOutcome, Command, HistoryEvent, LedgerConfig, LedgerQuery,
    LedgerQueryResult, LedgerResponse, LedgerStateMachine, OperationId,
};
use rafter_runtime::{DurableRaftNode, DurableRaftNodeStorage};
use rafter_storage::{InMemoryRaftHardStateStore, InMemoryRaftLogSegment};

use crate::storage::SharedSnapshotStore;

/// The application contract every replica in this driver serves.
///
/// This names the ledger's application vocabulary without naming which state
/// machine provides it. It adds no methods: the driver needs the associated
/// types pinned so its reports and history stay concrete, and everything a test
/// wants to inspect is an inherent accessor on whichever machine it built.
pub trait LedgerApp:
    ReplicatedStateMachine<
        Command = Command,
        CommandResult = ApplyOutcome,
        Query = LedgerQuery,
        QueryResult = LedgerQueryResult,
    > + fmt::Debug
{
}

impl<A> LedgerApp for A where
    A: ReplicatedStateMachine<
            Command = Command,
            CommandResult = ApplyOutcome,
            Query = LedgerQuery,
            QueryResult = LedgerQueryResult,
        > + fmt::Debug
{
}

/// Supplies each replica's application and reopens it across a restart.
///
/// `reopen` is where the two compositions genuinely differ, and the difference
/// is the point: an in-memory replica's state survives because the value does,
/// while a durable replica's state survives because its journal does. A test
/// that restarts a durable replica must be dropping the value, or it is
/// proving nothing about durability.
pub trait LedgerApps: fmt::Debug {
    /// The application this factory opens.
    type App: LedgerApp;

    /// Opens the application for a replica starting for the first time.
    fn open(&mut self, node_id: NodeId) -> Self::App;

    /// Reopens a restarting replica's application.
    fn reopen(&mut self, node_id: NodeId, retired: Self::App) -> Self::App;
}

/// Applications that keep their ledger in memory.
#[derive(Clone, Copy, Debug)]
pub struct InMemoryLedgerApps {
    config: LedgerConfig,
}

impl LedgerApps for InMemoryLedgerApps {
    type App = LedgerStateMachine;

    fn open(&mut self, _node_id: NodeId) -> Self::App {
        LedgerStateMachine::new(self.config)
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
const MAX_ROUNDS: usize = 32;

/// Caller-defined group identity for the single ledger group.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LedgerGroupId(pub u64);

/// The one group every node in this driver serves.
pub const GROUP_ID: LedgerGroupId = LedgerGroupId(1);

type LedgerStorage =
    DurableRaftNodeStorage<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, SharedSnapshotStore>;
type LedgerRuntime =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, SharedSnapshotStore>;
type LedgerGroup<App> = RaftGroup<LedgerGroupId, App, LedgerRuntime>;
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

/// A command the driver started and has not resolved.
///
/// Its history invocation is already recorded, so the operation's real-time
/// interval is open from the moment this handle exists.
#[derive(Debug)]
pub struct PendingProposal {
    operation_id: OperationId,
    local_proposal_id: LocalProposalId,
    /// Set when the proposal was already terminal on the way in.
    outcome: Option<ProposalOutcome>,
}

/// A linearizable query the driver issued and has not resolved.
#[derive(Debug)]
pub struct PendingRead {
    operation_id: OperationId,
    read_id: ReadId,
    node_id: NodeId,
    query: LedgerQuery,
    /// Set when the barrier answered on its first attempt.
    outcome: Option<ReadOutcome>,
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
struct ClusterNode<App> {
    node_id: NodeId,
    election_timeout_ticks: u64,
    group: LedgerGroup<App>,
}

/// Deterministic three-node ledger cluster with explicit delivery control.
#[derive(Debug)]
pub struct LedgerCluster<A: LedgerApps = InMemoryLedgerApps> {
    config: LedgerConfig,
    apps: A,
    nodes: Vec<ClusterNode<A::App>>,
    network: VecDeque<PeerEnvelope<LedgerGroupId>>,
    isolated: BTreeSet<NodeId>,
    /// Replicas whose group refused a step, keyed to what it reported.
    ///
    /// A durable application that cannot commit a transaction poisons its
    /// group, and a process in that state is finished: it serves no reads,
    /// replicates nothing, and answers no peer. Modeling it as unreachable
    /// until a restart is what a real deployment sees, and it lets the rest of
    /// the cluster carry on with its quorum while the dead replica's journal
    /// waits on disk for recovery.
    crashed: BTreeMap<NodeId, String>,
    proposal_outcomes: BTreeMap<LocalProposalId, ProposalOutcome>,
    /// Terminal read outcomes the report path has observed.
    ///
    /// A barrier can end in a report belonging to an unrelated tick or
    /// delivery, so the driver needs one place to hold that answer between the
    /// step that recorded it and the read that was waiting for it.
    read_failures: BTreeMap<ReadId, ReadOutcome>,
    runtime_unknown_outcomes: usize,
    next_local_proposal_id: u64,
    next_read_id: u64,
    next_operation_id: u64,
    history: Vec<HistoryEvent>,
}

impl LedgerCluster<InMemoryLedgerApps> {
    /// Builds three replicas whose ledgers live in memory.
    pub fn new(config: LedgerConfig) -> Self {
        Self::with_apps(config, InMemoryLedgerApps { config })
    }
}

impl<A: LedgerApps> LedgerCluster<A> {
    /// Builds three replicas whose election timeouts make every uncontested
    /// election deterministic: the lowest-numbered reachable node wins.
    pub fn with_apps(config: LedgerConfig, mut apps: A) -> Self {
        let nodes = [
            (NodeId(1), &[2, 3][..], 4),
            (NodeId(2), &[1, 3][..], 6),
            (NodeId(3), &[1, 2][..], 8),
        ]
        .into_iter()
        .map(|(node_id, peers, election_timeout_ticks)| {
            let app = apps.open(node_id);
            let (group, _) =
                open_group(node_id, peers, election_timeout_ticks, empty_storage(), app);
            ClusterNode {
                node_id,
                election_timeout_ticks,
                group,
            }
        })
        .collect();

        Self {
            config,
            apps,
            nodes,
            network: VecDeque::new(),
            isolated: BTreeSet::new(),
            crashed: BTreeMap::new(),
            proposal_outcomes: BTreeMap::new(),
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

    /// Whether a replica is currently unable to exchange messages.
    ///
    /// A partitioned replica and a crashed one are both unreachable; only the
    /// second one needs a restart to come back.
    fn unreachable(&self, node_id: NodeId) -> bool {
        self.isolated.contains(&node_id) || self.crashed.contains_key(&node_id)
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
            .filter(|node| !self.unreachable(node.node_id))
            .filter_map(|node| {
                let metrics = node.group.metrics();
                (metrics.role == Role::Leader).then_some((metrics.term, node.node_id))
            })
            .max()
            .map(|(_, node_id)| node_id)
    }

    /// Returns the state machine owned by one replica.
    pub fn state_machine(&self, node_id: NodeId) -> &A::App {
        self.node(node_id).group.state_machine()
    }

    /// Returns mutable access to one replica's state machine.
    ///
    /// This is the maintenance hook the group layer documents. It is used here
    /// only to build application snapshots, which read state rather than move
    /// the durable applied floor.
    pub fn state_machine_mut(&mut self, node_id: NodeId) -> &mut A::App {
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

    /// Returns the index a replica's state machine must reach to have applied
    /// every application command that replica knows to be committed.
    ///
    /// This is the readiness half of [`LedgerCluster::applied_index`]: the two
    /// together say whether a replica has consumed everything it knows about.
    pub fn committed_application_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).group.committed_application_index()
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

    /// Returns the log entries whose payloads the checker replays.
    ///
    /// This walks the log because it needs the encoded commands themselves.
    /// The convergence predicate does not: the group reports the committed
    /// application index directly.
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
            if self.node_ids().into_iter().all(|node_id| {
                self.unreachable(node_id)
                    || self.applied_index(node_id) >= self.committed_application_index(node_id)
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
            if self.unreachable(node_id) {
                continue;
            }
            let outcome = self.node_mut(node_id).group.step(GroupInput::Tick);
            self.absorb(node_id, outcome);
        }
    }

    /// Delivers every queued envelope, including envelopes queued by delivery.
    pub fn deliver_all(&mut self) {
        while let Some(envelope) = self.network.pop_front() {
            if self.unreachable(envelope.to) || self.unreachable(envelope.from) {
                continue;
            }
            let to = envelope.to;
            let outcome = self
                .node_mut(to)
                .group
                .step(GroupInput::PeerMessage { envelope });
            self.absorb(to, outcome);
        }
    }

    /// Records a step's report, or the failure that ended the replica.
    ///
    /// A group only refuses a step once something fatal happened to it, and for
    /// this application that means the durable backend could not commit. The
    /// replica stops here rather than the test stopping: what happens next —
    /// the surviving quorum carrying on, then a restart recovering the journal
    /// — is the behavior under test.
    fn absorb<E: fmt::Display>(&mut self, node_id: NodeId, outcome: Result<LedgerReport, E>) {
        match outcome {
            Ok(report) => self.record_report(report),
            Err(error) => {
                self.crashed
                    .entry(node_id)
                    .or_insert_with(|| error.to_string());
            }
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
    /// This is also how a crashed replica comes back: the poisoned group is
    /// decomposed, its application is reopened, and the new incarnation knows
    /// only what recovery could read back.
    ///
    /// Decomposition is the in-process restart path. The retiring group hands
    /// back its state machine, and its runtime hands back the durable stores
    /// the next incarnation recovers from, so nothing is cloned and the driver
    /// holds no parallel handle to a medium it is not currently driving. From
    /// there this follows the documented recipe: read the application's durable
    /// applied floor, recover through the same floor, then hand the recovery
    /// outputs to the new group before using it.
    pub fn restart(&mut self, node_id: NodeId) {
        self.crashed.remove(&node_id);
        let index = self.node_index(node_id);
        let peers = self
            .node_ids()
            .into_iter()
            .filter(|peer| *peer != node_id)
            .map(|peer| peer.0)
            .collect::<Vec<_>>();
        let retired = self.nodes.remove(index);
        let election_timeout_ticks = retired.election_timeout_ticks;
        let parts = retired.group.into_parts();
        // The returned ID watermarks are deliberately unused. They are
        // load-bearing only when the same runtime is carried into the new
        // group; this driver drops that runtime and rebuilds one from the
        // durable storage it returns, and a rebuilt runtime carries no local
        // proposal tracking, so a group over it may restart its IDs at zero.
        // The driver's own counters never restart anyway, which is stricter
        // than the contract requires.
        let storage = parts.runtime.into_storage();
        // The application is reopened rather than carried across. An in-memory
        // replica gets its own value back; a durable one drops it and recovers
        // from its journal, which is the only version of this that proves
        // anything about durability.
        let app = self.apps.reopen(node_id, parts.state_machine);

        let (group, report) = open_group(node_id, &peers, election_timeout_ticks, storage, app);
        self.nodes.insert(
            index,
            ClusterNode {
                node_id,
                election_timeout_ticks,
                group,
            },
        );
        self.record_report(report);
    }

    /// Submits one command to `node_id` and waits for a terminal outcome.
    ///
    /// The command, its response, and any unknown outcome are recorded in the
    /// history under one operation identity.
    pub fn submit(&mut self, node_id: NodeId, command: Command) -> ProposalOutcome {
        let pending = self.begin_submit(node_id, command);
        self.resolve_proposal(pending)
    }

    /// Starts one command without waiting for its outcome.
    ///
    /// Splitting invocation from resolution is what lets a test overlap
    /// operations: everything begun before the first resolution is genuinely
    /// concurrent in real time, and the recorded history says so. A client that
    /// waits is the special case, not the general one.
    #[must_use]
    pub fn begin_submit(&mut self, node_id: NodeId, command: Command) -> PendingProposal {
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
        // A proposal that is already terminal completed here, so its history
        // completion belongs at this position rather than wherever the caller
        // gets around to resolving it.
        let outcome = immediate_outcome(&started.begin)
            .map(|outcome| self.record_completion(operation_id, outcome));
        PendingProposal {
            operation_id,
            local_proposal_id,
            outcome,
        }
    }

    /// Drives the cluster until `pending` reaches a terminal outcome.
    pub fn resolve_proposal(&mut self, pending: PendingProposal) -> ProposalOutcome {
        if let Some(outcome) = pending.outcome {
            return outcome;
        }
        for _ in 0..MAX_ROUNDS {
            if let Some(outcome) = self.proposal_outcomes.get(&pending.local_proposal_id) {
                let outcome = outcome.clone();
                return self.record_completion(pending.operation_id, outcome);
            }
            self.deliver_all();
            self.tick_reachable();
            self.deliver_all();
        }
        self.record_completion(pending.operation_id, ProposalOutcome::Unknown)
    }

    /// Runs one linearizable query against `node_id` and waits for its answer.
    pub fn read(&mut self, node_id: NodeId, query: LedgerQuery) -> ReadOutcome {
        let pending = self.begin_read(node_id, query);
        self.resolve_read(pending)
    }

    /// Issues one linearizable query without waiting for its answer.
    #[must_use]
    pub fn begin_read(&mut self, node_id: NodeId, query: LedgerQuery) -> PendingRead {
        let operation_id = self.record_query_invocation(query);
        let read_id = self.allocate_read_id();
        let outcome = self
            .attempt_read(node_id, read_id, query)
            .map(|outcome| self.record_query_completion(operation_id, outcome));
        PendingRead {
            operation_id,
            read_id,
            node_id,
            query,
            outcome,
        }
    }

    /// Drives the cluster until `pending` answers or runs out of rounds.
    ///
    /// The group owns the barrier, the proof, and the state-machine read. The
    /// driver's job is to route the report each attempt produces through the
    /// same path as every other step, and to stop retrying once a terminal read
    /// event says the barrier ended.
    pub fn resolve_read(&mut self, pending: PendingRead) -> ReadOutcome {
        if let Some(outcome) = pending.outcome {
            return outcome;
        }
        for _ in 0..MAX_ROUNDS {
            self.deliver_all();
            self.tick_reachable();
            self.deliver_all();
            if let Some(outcome) =
                self.attempt_read(pending.node_id, pending.read_id, pending.query)
            {
                return self.record_query_completion(pending.operation_id, outcome);
            }
        }
        self.node_mut(pending.node_id)
            .group
            .cancel_read(pending.read_id);
        self.record_query_completion(pending.operation_id, ReadOutcome::Unresolved)
    }

    /// Makes one barrier attempt without driving the cluster.
    ///
    /// Returns `None` while the barrier is still in flight; every other state
    /// is terminal for this read.
    fn attempt_read(
        &mut self,
        node_id: NodeId,
        read_id: ReadId,
        query: LedgerQuery,
    ) -> Option<ReadOutcome> {
        // A terminal read event ends the barrier wherever it was observed,
        // including in the report of an unrelated tick or delivery. The group
        // drops its waiter with the event, so retrying the same read ID
        // afterwards is refused as non-monotonic instead of restating the
        // outcome.
        if let Some(terminal) = self.read_failures.remove(&read_id) {
            return Some(terminal);
        }
        let ReadReport { outcome, report } = self
            .node_mut(node_id)
            .group
            .read(ReadRequest::Linearizable {
                group_id: GROUP_ID,
                read_id,
                query,
                min_applied_index: None,
                context: Vec::new(),
            })
            .expect("a healthy group accepts linearizable reads");
        self.record_report(report);
        if let Some(terminal) = self.read_failures.remove(&read_id) {
            return Some(terminal);
        }
        match outcome {
            GroupReadOutcome::Ready { result, .. } => Some(ReadOutcome::Ready(result)),
            // The barrier is still in flight, or this replica has not applied
            // through it yet. Either way the contract is to keep driving and
            // retry with the same read ID, freshness, and context.
            GroupReadOutcome::Pending { .. }
            | GroupReadOutcome::LinearizableFreshnessUnavailable { .. } => None,
            // Rejections and cancellations are read events in the report above,
            // so the check that follows it has already answered them.
            outcome => unreachable!("a linearizable ledger read cannot produce {outcome:?}"),
        }
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
        let operation_id = self.allocate_operation_id();
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
            // The app layer emits this rejection only from the pre-append
            // admission check, so the command never entered this node's log and
            // never left it. That is the contract's provable-refusal criterion,
            // and it is strictly stronger than `Unknown`.
            ProposalOutcome::Rejected { .. } => {
                self.history
                    .push(HistoryEvent::NotCommitted { operation_id });
            }
            // A lost outcome proves nothing either way: the command may still
            // be in a log this client cannot see.
            ProposalOutcome::Unknown => {
                self.history.push(HistoryEvent::Unknown { operation_id });
            }
        }
        outcome
    }

    fn record_query_invocation(&mut self, query: LedgerQuery) -> OperationId {
        let operation_id = self.allocate_operation_id();
        self.history.push(HistoryEvent::QueryInvoked {
            operation_id,
            query,
        });
        operation_id
    }

    fn record_query_completion(
        &mut self,
        operation_id: OperationId,
        outcome: ReadOutcome,
    ) -> ReadOutcome {
        match &outcome {
            ReadOutcome::Ready(result) => self.history.push(HistoryEvent::QueryCompleted {
                operation_id,
                result: *result,
            }),
            // A refused barrier, a canceled barrier, and a client that stopped
            // waiting all delivered no value, so none of them constrains an
            // ordering. The history keeps the operation anyway: a query that
            // was issued and answered nothing is evidence about availability
            // even when it is not evidence about correctness.
            ReadOutcome::Rejected { .. }
            | ReadOutcome::Canceled { .. }
            | ReadOutcome::Unresolved => {
                self.history
                    .push(HistoryEvent::QueryAbandoned { operation_id });
            }
        }
        outcome
    }

    fn allocate_operation_id(&mut self) -> OperationId {
        let operation_id = OperationId::new(self.next_operation_id);
        self.next_operation_id += 1;
        operation_id
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

    fn node(&self, node_id: NodeId) -> &ClusterNode<A::App> {
        &self.nodes[self.node_index(node_id)]
    }

    fn node_mut(&mut self, node_id: NodeId) -> &mut ClusterNode<A::App> {
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

/// Returns empty durable storage for a replica that has never started.
fn empty_storage() -> LedgerStorage {
    LedgerStorage {
        hard_state_store: InMemoryRaftHardStateStore::default(),
        log_segment: InMemoryRaftLogSegment::default(),
        snapshot_store: SharedSnapshotStore::default(),
    }
}

fn open_group<App: LedgerApp>(
    node_id: NodeId,
    peers: &[u64],
    election_timeout_ticks: u64,
    storage: LedgerStorage,
    app: App,
) -> (LedgerGroup<App>, LedgerReport) {
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
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
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
