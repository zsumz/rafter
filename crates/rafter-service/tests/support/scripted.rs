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

use rafter_service::{TransportDriverOptions, TransportRaftDriver};

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

#[derive(Debug)]
pub(crate) struct ScriptedMembership {
    effective: Vec<u64>,
    committed: Vec<u64>,
    /// Applied from inside `step`, so the group observes the change as a
    /// membership event rather than as a value that was always this way.
    change_on_step: Option<(Vec<u64>, Vec<u64>)>,
}

impl ScriptedMembershipRuntime {
    pub(crate) fn new(effective: &[u64], committed: &[u64]) -> Self {
        Self::for_node(NodeId(1), effective, committed)
    }

    /// The same runtime under a different local replica, for a supervisor that
    /// rebuilds this node under a fresh identity.
    pub(crate) fn for_node(node_id: NodeId, effective: &[u64], committed: &[u64]) -> Self {
        Self {
            shared: Arc::new(Mutex::new(ScriptedMembership {
                effective: effective.to_vec(),
                committed: committed.to_vec(),
                change_on_step: None,
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
    let mut state = lock_membership(handle);
    state.effective = effective.to_vec();
    state.committed = committed.to_vec();
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
        LogIndex(5)
    }
    fn last_log_index(&self) -> LogIndex {
        LogIndex(5)
    }
    fn snapshot_index(&self) -> LogIndex {
        LogIndex(0)
    }
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        std::cmp::min(index, LogIndex(5))
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
        (index <= LogIndex(5)).then_some(Term(1))
    }

    fn step(&mut self, _input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut state = lock_membership(&self.shared);
        if let Some((effective, committed)) = state.change_on_step.take() {
            state.effective = effective;
            state.committed = committed;
        }
        Ok(Vec::new())
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
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: authorized.iter().copied().collect(),
        nameable,
    };
    let node_id = runtime.node_id;
    let driver = TransportRaftDriver::new(
        RaftGroup::new(GROUP, node_id, runtime, KvStateMachine::default()),
        Vec::new(),
        transport.clone(),
        validator,
        options,
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
