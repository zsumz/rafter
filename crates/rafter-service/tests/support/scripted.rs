#![allow(dead_code)]

//! A replica whose two membership facts a test moves by hand.
//!
//! Shared rather than repeated, because three transport-driver suites need the
//! same thing and for the same reason: the driver's whole membership rule turns
//! on telling the effective configuration from the committed one, and
//! `DurableRaftNode` over a static config produces neither a configuration
//! change nor a divergence between them.
//!
//! The state is shared through an `Arc`, which is what lets a test move it while
//! the driver holds no group — the only way to reach a change this replica
//! observes across a release and re-adoption instead of through an event.

use std::sync::{Arc, Mutex, MutexGuard};

use rafter_service::{
    ManagedDriverError, PeerControlPlaneCheckpoint, TransportDriverOptions, TransportRaftDriver,
};

use super::transport::{Nameable, QueueTransport, Validator, GROUP};
use super::{
    ClientProposalInput, KvStateMachine, LogIndex, MembershipConfig, MembershipSet, NodeId,
    PersistedRaftRuntime, RaftGroup, RaftInput, RaftOutput, ReplicationProgress, Role, Term,
};

/// A runtime whose effective and committed memberships move independently.
#[derive(Clone, Debug)]
pub(crate) struct ScriptedMembershipRuntime {
    shared: Arc<Mutex<ScriptedMembership>>,
    node_id: NodeId,
}

/// Where a scripted replica's committed configuration starts.
///
/// Above zero so the frames these suites send, which name `last_log_index: 5`,
/// describe a replica with a log rather than one without.
const INITIAL_COMMIT_INDEX: LogIndex = LogIndex(5);

#[derive(Debug)]
pub(crate) struct ScriptedMembership {
    effective: Vec<u64>,
    committed: Vec<u64>,
    /// Where the committed configuration stands.
    ///
    /// **Advanced whenever `committed` moves, because a real runtime cannot do
    /// otherwise.** `committed_membership` is by definition the latest
    /// configuration entry at or below `commit_index`, and a committed log entry
    /// is never truncated — so one commit index names one committed membership,
    /// for good. A fixture that moved the membership and left the index standing
    /// would be stating a sequence no correct cluster produces, which is the
    /// failure mode this file's own neighbours were written against.
    ///
    /// It became load-bearing when the driver started reading it: a committed
    /// fact carries the position it stands at, and a driver skips one at or
    /// below the position it has already consumed. A frozen index makes every
    /// change after the first look like a replay of the first.
    commit_index: LogIndex,
    /// Applied from inside `step`, so the group observes the change as a
    /// membership event rather than as a value that was always this way.
    change_on_step: Option<(Vec<u64>, Vec<u64>)>,
    /// Outputs the next `step` releases beside the change it applies.
    ///
    /// What makes a *failing* step scriptable: the runtime moves its
    /// configuration and hands the group work that the state machine then
    /// refuses, which is the shape every membership-loss case has.
    outputs_on_step: Vec<RaftOutput>,
}

impl ScriptedMembership {
    /// Assigns both memberships, advancing the commit index if the committed one
    /// actually moved.
    ///
    /// The one place either membership is written, so the index cannot drift
    /// away from the fact it dates. Re-committing the same configuration
    /// advances nothing, which is what a cluster that commits no configuration
    /// change does.
    fn assign(&mut self, effective: Vec<u64>, committed: Vec<u64>) {
        if self.committed != committed {
            self.commit_index = LogIndex(self.commit_index.0 + 1);
        }
        self.effective = effective;
        self.committed = committed;
    }
}

impl ScriptedMembershipRuntime {
    pub(crate) fn new(effective: &[u64], committed: &[u64]) -> Self {
        Self::for_node(NodeId(1), effective, committed)
    }

    /// The same runtime under a different local replica, for a supervisor that
    /// rebuilds this node under a fresh identity.
    pub(crate) fn for_node(node_id: NodeId, effective: &[u64], committed: &[u64]) -> Self {
        Self::for_node_at(node_id, effective, committed, INITIAL_COMMIT_INDEX)
    }

    /// The same, opened at a chosen commit index.
    ///
    /// **A replacement incarnation must not claim an older position for a newer
    /// committed membership.** The committed membership at one log index is one
    /// set, so a driver holding a later observation reads the difference between
    /// the two as the removals that happened between them — which is how a
    /// checkpoint and a runtime jointly prove a removal neither witnessed. A
    /// fixture that rebuilds a replacement at the default index while the driver
    /// has already moved past it states a sequence no cluster produces, and
    /// measures that inference instead of the rule under test.
    pub(crate) fn for_node_at(
        node_id: NodeId,
        effective: &[u64],
        committed: &[u64],
        commit_index: LogIndex,
    ) -> Self {
        Self {
            shared: Arc::new(Mutex::new(ScriptedMembership {
                effective: effective.to_vec(),
                committed: committed.to_vec(),
                commit_index,
                change_on_step: None,
                outputs_on_step: Vec::new(),
            })),
            node_id,
        }
    }

    pub(crate) fn handle(&self) -> Arc<Mutex<ScriptedMembership>> {
        Arc::clone(&self.shared)
    }

    fn config(&self, committed: bool) -> MembershipConfig {
        let state = lock_membership(&self.shared);
        let source = if committed {
            &state.committed
        } else {
            &state.effective
        };
        config_of(source)
    }
}

/// The shared state a test moves while a driver holds the runtime.
pub(crate) type ScriptedMembershipHandle = Arc<Mutex<ScriptedMembership>>;

pub(crate) fn lock_membership(
    shared: &Arc<Mutex<ScriptedMembership>>,
) -> MutexGuard<'_, ScriptedMembership> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn config_of(voters: &[u64]) -> MembershipConfig {
    MembershipConfig::stable(
        MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("scripted membership is valid"),
    )
}

/// Moves the membership directly, for a change this replica learns about while
/// the driver holds no group.
pub(crate) fn set_membership(
    handle: &Arc<Mutex<ScriptedMembership>>,
    effective: &[u64],
    committed: &[u64],
) {
    lock_membership(handle).assign(effective.to_vec(), committed.to_vec());
}

/// Arms a change the runtime applies from inside its next `step`, so the group
/// reports it as a membership event.
pub(crate) fn change_on_step(
    handle: &Arc<Mutex<ScriptedMembership>>,
    effective: &[u64],
    committed: &[u64],
) {
    lock_membership(handle).change_on_step = Some((effective.to_vec(), committed.to_vec()));
}

/// Moves the committed membership *without* advancing the commit index, which is
/// a runtime breaking its own contract.
///
/// **Deliberately dishonest, and the only fixture here that is.** One commit
/// index names one committed membership for good — [`ScriptedMembership`] says so
/// and every other helper keeps it — so this produces the pair no correct runtime
/// can: two claims about the committed configuration at one position. It exists
/// because the driver's answer to that pair is a behaviour with no other
/// producer: the routing path cannot return a refusal, so it records one, and
/// what has to hold afterwards is that the driver stops serving and stops
/// publishing.
pub(crate) fn contradict_committed_in_place(
    handle: &Arc<Mutex<ScriptedMembership>>,
    committed: &[u64],
) {
    let mut state = lock_membership(handle);
    state.change_on_step = None;
    state.committed = committed.to_vec();
}

/// Arms outputs the runtime releases from its next `step`.
///
/// Paired with [`change_on_step`] to script the one shape a per-step membership
/// comparison could not survive: the configuration moves, and the work the same
/// step released then fails in the application.
pub(crate) fn outputs_on_step(handle: &Arc<Mutex<ScriptedMembership>>, outputs: Vec<RaftOutput>) {
    lock_membership(handle).outputs_on_step = outputs;
}

/// Pointer equality, because the state is shared: two handles to one scripted
/// membership are the same runtime, and two separate ones never are.
impl PartialEq for ScriptedMembershipRuntime {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared) && self.node_id == other.node_id
    }
}

impl Eq for ScriptedMembershipRuntime {}

impl PersistedRaftRuntime for ScriptedMembershipRuntime {
    type Error = rafter_runtime::RaftRuntimeError;

    fn id(&self) -> NodeId {
        self.node_id
    }
    fn leader_hint(&self) -> Option<NodeId> {
        Some(self.node_id)
    }
    fn role(&self) -> Role {
        Role::Leader
    }
    fn current_term(&self) -> Term {
        Term(1)
    }
    fn commit_index(&self) -> LogIndex {
        lock_membership(&self.shared).commit_index
    }
    /// The commit index, plus one while an uncommitted configuration is in
    /// effect — which is exactly when a replica's log stands above its commit
    /// floor.
    fn last_log_index(&self) -> LogIndex {
        let state = lock_membership(&self.shared);
        if state.effective == state.committed {
            state.commit_index
        } else {
            LogIndex(state.commit_index.0 + 1)
        }
    }
    fn snapshot_index(&self) -> LogIndex {
        LogIndex(0)
    }
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        std::cmp::min(index, self.commit_index())
    }
    fn membership(&self) -> MembershipConfig {
        self.config(false)
    }
    fn committed_membership(&self) -> MembershipConfig {
        self.config(true)
    }
    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }
    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= self.last_log_index()).then_some(Term(1))
    }

    fn step(&mut self, _input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut state = lock_membership(&self.shared);
        if let Some((effective, committed)) = state.change_on_step.take() {
            state.assign(effective, committed);
        }
        Ok(std::mem::take(&mut state.outputs_on_step))
    }

    fn step_proposal_batch(
        &mut self,
        _proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(Vec::new())
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs)
    }
}

pub(crate) type ScriptedDriver =
    TransportRaftDriver<u64, KvStateMachine, ScriptedMembershipRuntime, QueueTransport, Validator>;

pub(crate) fn scripted_driver(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
) -> (ScriptedDriver, QueueTransport) {
    scripted_driver_authorizing(runtime, nameable, &[NodeId(2), NodeId(3)])
}

/// The same driver over a deployment that allows a different set of replicas.
///
/// `authorized` is the validator's allowlist rather than the group's
/// membership, and the two are different questions: a deployment authorizes
/// every replica it has provisioned, and the cluster decides which of them are
/// members. A test that admits a replacement replica has to move the first.
pub(crate) fn scripted_driver_authorizing(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
    authorized: &[NodeId],
) -> (ScriptedDriver, QueueTransport) {
    scripted_driver_with_options(
        runtime,
        nameable,
        authorized,
        TransportDriverOptions::default(),
    )
}

pub(crate) fn scripted_driver_with_options(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
    authorized: &[NodeId],
    options: TransportDriverOptions,
) -> (ScriptedDriver, QueueTransport) {
    scripted_driver_with_app(
        runtime,
        nameable,
        authorized,
        options,
        KvStateMachine::default(),
    )
}

/// The same driver over a state machine the caller supplies.
///
/// The seam a losslessness test needs: every way a step can fail after the
/// runtime has already moved its configuration runs through the application, so
/// a fixture that cannot choose the state machine cannot produce one.
pub(crate) fn scripted_driver_with_app(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
    authorized: &[NodeId],
    options: TransportDriverOptions,
    app: KvStateMachine,
) -> (ScriptedDriver, QueueTransport) {
    build_scripted_driver(
        runtime,
        nameable,
        authorized,
        options,
        app,
        PeerControlPlaneCheckpoint::empty(GROUP),
    )
}

/// A driver rebuilt the way a restarted process rebuilds one: a fresh transport
/// that has accepted nothing, a runtime recovered from durable Raft state, and
/// the control-plane checkpoint the previous process persisted.
pub(crate) fn scripted_driver_with_checkpoint(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
    authorized: &[NodeId],
    options: TransportDriverOptions,
    checkpoint: PeerControlPlaneCheckpoint<u64>,
) -> (ScriptedDriver, QueueTransport) {
    build_scripted_driver(
        runtime,
        nameable,
        authorized,
        options,
        KvStateMachine::default(),
        checkpoint,
    )
}

/// A driver opened over a checkpoint it may refuse, with the link it would have
/// spoken to.
///
/// The seam every "refuses to open, and told the link layer nothing" case needs:
/// the refusal is the observation, so it must not be an `expect`, and the
/// transport has to outlive the attempt for its emptiness to be assertable.
pub(crate) fn try_scripted_driver_with_checkpoint(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
    authorized: &[NodeId],
    checkpoint: PeerControlPlaneCheckpoint<u64>,
) -> (Result<ScriptedDriver, ManagedDriverError>, QueueTransport) {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: authorized.iter().copied().collect(),
        nameable,
    };
    let node_id = runtime.node_id;
    let opened = TransportRaftDriver::with_control_plane_checkpoint(
        RaftGroup::new(GROUP, node_id, runtime, KvStateMachine::default()),
        Vec::new(),
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
        checkpoint,
    );
    (opened, transport)
}

fn build_scripted_driver(
    runtime: ScriptedMembershipRuntime,
    nameable: Nameable,
    authorized: &[NodeId],
    options: TransportDriverOptions,
    app: KvStateMachine,
    checkpoint: PeerControlPlaneCheckpoint<u64>,
) -> (ScriptedDriver, QueueTransport) {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: authorized.iter().copied().collect(),
        nameable,
    };
    let node_id = runtime.node_id;
    let driver = TransportRaftDriver::with_control_plane_checkpoint(
        RaftGroup::new(GROUP, node_id, runtime, app),
        Vec::new(),
        transport.clone(),
        validator,
        options,
        checkpoint,
    )
    .expect("a quiescent group is adoptable");
    (driver, transport)
}

/// A driver over `{1,2,3}`, with node 3's removal armed for the first tick.
pub(crate) fn shrink_driver(nameable: Nameable) -> (ScriptedDriver, QueueTransport) {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[1, 2], &[1, 2]);
    scripted_driver(runtime, nameable)
}
