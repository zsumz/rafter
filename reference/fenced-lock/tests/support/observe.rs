//! Shared handles to the state machine and runtime a driver owns.
//!
//! [`TransportRaftDriver`] takes its `RaftGroup` by value and exposes no
//! accessor for what is inside it. `RaftGroupMetrics` covers role, term,
//! commit index, and applied index, and that is the whole observation surface:
//! a consumer cannot reach the application state machine to compare replicas,
//! cannot read the durable log to replay a real committed history through its
//! oracle, and cannot ask the runtime for `committed_application_index`, which
//! is the readiness gate the promoted decomposition recipe is written around.
//! `release_group` is not an escape, because it resolves every outstanding
//! waiter, so it cannot be used to look at a replica that is still running.
//!
//! Every type here exists for that reason and no other. Each is a shared handle
//! to a value the group owns, implementing the same public trait the group
//! requires, so the group is none the wiser and every assertion still reads the
//! replica's real state through published API. Nothing here is a simulator
//! hook: an external user with the published crates writes exactly this.
//!
//! Lock order is one handle at a time. No path here takes a second lock, and
//! no path calls back into a driver while holding one.
//!
//! [`TransportRaftDriver`]: rafter_service::TransportRaftDriver

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rafter::{
    ClientProposalInput, Input as RaftInput, LogEntry, LogIndex, MembershipConfig, NodeId,
    Output as RaftOutput, ReplicationProgress, Role, Term,
};
use rafter_app::state_machine::{
    ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine, SnapshotSupport,
};
use rafter_reference_fenced_lock::{
    ApplyOutcome, Command, LockAdapterError, LockQuery, LockQueryResult, LockStateMachine,
};
use rafter_runtime::{
    DurableRaftNode, DurableRaftNodeStorage, PersistedRaftRuntime, RaftRuntimeError,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

/// Durable media one replica keeps across incarnations.
pub type LockStorage = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;

/// The durable runtime a replica actually runs.
pub type LockNode =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;

/// The lock state machine, observable after a driver has taken it.
///
/// Delegation is total: this type decides nothing and caches nothing, so a
/// replica behaves exactly as it would over the bare
/// [`LockStateMachine`].
#[derive(Clone, Debug)]
pub struct SharedStateMachine {
    inner: Arc<Mutex<LockStateMachine>>,
}

impl SharedStateMachine {
    /// Wraps one replica's state machine in a handle the cluster keeps.
    pub fn new(app: LockStateMachine) -> Self {
        Self {
            inner: Arc::new(Mutex::new(app)),
        }
    }

    /// Returns a copy of the state machine as it currently stands.
    pub fn observe(&self) -> LockStateMachine {
        lock(&self.inner).clone()
    }
}

impl ReplicatedStateMachine for SharedStateMachine {
    type Command = Command;
    type CommandResult = ApplyOutcome;
    type Query = LockQuery;
    type QueryResult = LockQueryResult;
    type Error = LockAdapterError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = LockStateMachine::SNAPSHOT_SUPPORT;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        lock(&self.inner).applied_index()
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        lock(&self.inner).encode_command(command)
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        lock(&self.inner).decode_command(payload)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        lock(&self.inner).apply_batch(batch)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        lock(&self.inner).read(query, barrier)
    }
}

/// The durable runtime, observable after a driver has taken it.
///
/// The slot is emptied only by [`SharedRuntime::take_storage`], which is the
/// last thing a retiring incarnation does; the cluster installs the next
/// incarnation's handle in the same call.
#[derive(Clone, Debug)]
pub struct SharedRuntime {
    inner: Arc<Mutex<Option<LockNode>>>,
}

impl SharedRuntime {
    /// Wraps one replica's runtime in a handle the cluster keeps.
    pub fn new(node: LockNode) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(node))),
        }
    }

    /// Hands back the durable media this incarnation ran on.
    ///
    /// Takes `&self` rather than `self` because every clone of this handle
    /// names the same runtime, and the caller reaching decomposition holds only
    /// one of them. The retired runtime is dropped here, so nothing can step it
    /// afterwards, and the caller opens the next incarnation over the returned
    /// stores.
    pub fn take_storage(&self) -> LockStorage {
        lock(&self.inner)
            .take()
            .expect("a runtime is retired exactly once")
            .into_storage()
    }

    /// Returns the retained log entries from `first_index` onward.
    pub fn log_entries_from(&self, first_index: LogIndex) -> Vec<LogEntry> {
        self.with(|node| node.log_entries_from(first_index))
    }

    fn with<T>(&self, read: impl FnOnce(&LockNode) -> T) -> T {
        read(lock(&self.inner).as_ref().expect(RETIRED))
    }

    fn with_mut<T>(&self, step: impl FnOnce(&mut LockNode) -> T) -> T {
        step(lock(&self.inner).as_mut().expect(RETIRED))
    }
}

const RETIRED: &str = "a replica holds its runtime until the incarnation retires";

impl PersistedRaftRuntime for SharedRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        self.with(PersistedRaftRuntime::id)
    }

    fn leader_hint(&self) -> Option<NodeId> {
        self.with(PersistedRaftRuntime::leader_hint)
    }

    fn role(&self) -> Role {
        self.with(PersistedRaftRuntime::role)
    }

    fn current_term(&self) -> Term {
        self.with(PersistedRaftRuntime::current_term)
    }

    fn commit_index(&self) -> LogIndex {
        self.with(PersistedRaftRuntime::commit_index)
    }

    fn last_log_index(&self) -> LogIndex {
        self.with(PersistedRaftRuntime::last_log_index)
    }

    fn snapshot_index(&self) -> LogIndex {
        self.with(PersistedRaftRuntime::snapshot_index)
    }

    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        self.with(|node| node.committed_application_index_through(index))
    }

    fn membership(&self) -> MembershipConfig {
        self.with(PersistedRaftRuntime::membership)
    }

    fn committed_membership(&self) -> MembershipConfig {
        self.with(PersistedRaftRuntime::committed_membership)
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        self.with(PersistedRaftRuntime::replication)
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        self.with_mut(|node| node.step(input))
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        self.with_mut(|node| node.step_proposal_batch(proposals))
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        self.with_mut(|node| node.step_batch(inputs))
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        self.with(|node| node.term_at_index(index))
    }
}

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}
