use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    num::NonZeroUsize,
};

use rafter::{LocalProposalId, NodeId};
use rafter_app::{
    error::ErrorCause,
    group::{GroupFatalState, GroupInput, GroupStepReport, RaftGroup},
    proposal::ProposalEvent,
    transport::PeerEnvelope,
};
use rafter_multiraft::{
    managed::{
        AdmissionReceipt, AdmissionRejection, ArmPass, BeginDispatch, ManagedConfig,
        ManagedTypedMultiRaftHost, WorkClass,
    },
    MultiRaftErrorKind,
};
use rafter_runtime::DurableRaftNode;

use crate::{GroupId, GroupLifecycle};

use super::state_machine::{CounterApplyResult, CounterStateMachine, ReplicatedCounterCommand};

mod admission;

type CounterGroup = RaftGroup<GroupId, CounterStateMachine, DurableRaftNode>;
type CounterReport = GroupStepReport<GroupId, CounterApplyResult>;

/// Bounds owned by the deterministic consumer network and state machines.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkConfig {
    /// Peer envelopes retained before deterministic delivery.
    pub max_pending_messages: NonZeroUsize,
    /// Replicated client-session slots in each group.
    pub max_sessions_per_group: NonZeroUsize,
}

/// Queue receipt paired with the local proposal identity used at apply time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalReceipt {
    /// Managed queue admission.
    pub admission: AdmissionReceipt,
    /// Local proposal identity later attached to its apply result.
    pub proposal_id: LocalProposalId,
}

/// Why consumer policy or the managed mechanism refused a proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CounterAdmissionRejection {
    /// The group has not been registered in this consumer.
    UnknownGroup,
    /// The consumer lifecycle does not admit this proposal shape.
    Lifecycle {
        /// State observed at the gate, when the group exists.
        state: Option<GroupLifecycle>,
    },
    /// The real group has permanently poisoned.
    GroupPoisoned,
    /// The public scheduler refused the queue admission.
    Managed(AdmissionRejection<GroupId>),
}

/// Refusal returning the exact proposal input that took no queue slot.
#[derive(Debug)]
pub struct CounterAdmissionRejected {
    /// Typed consumer or managed refusal.
    pub reason: CounterAdmissionRejection,
    /// Unmodified input.
    pub input: GroupInput<GroupId, ReplicatedCounterCommand>,
}

/// Observable work performed while driving the deterministic cluster.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DriveReport {
    /// Immutable pass plans, in arm order.
    pub plans: Vec<Vec<GroupId>>,
    /// Group turns executed.
    pub opportunities: u64,
    /// Managed items whose group step succeeded.
    pub serviced: u64,
    /// Managed items whose group step failed explicitly.
    pub failed: u64,
    /// Remote-replica steps that failed after consuming an envelope.
    pub remote_failures: u64,
}

impl DriveReport {
    fn merge(&mut self, other: Self) {
        self.plans.extend(other.plans);
        self.opportunities += other.opportunities;
        self.serviced += other.serviced;
        self.failed += other.failed;
        self.remote_failures += other.remote_failures;
    }
}

/// Deterministic adapter/network failure.
#[derive(Debug)]
pub enum AdapterError {
    /// A lifecycle transition did not match the consumer contract.
    Lifecycle {
        group_id: GroupId,
        expected: GroupLifecycle,
        actual: Option<GroupLifecycle>,
    },
    /// A group slot was registered twice.
    GroupAlreadyRegistered(GroupId),
    /// A fresh in-memory driver could not be opened.
    OpenGroup {
        group_id: GroupId,
        kind: MultiRaftErrorKind,
    },
    /// A managed identity counter was exhausted.
    IdentityExhausted,
    /// The scheduler refused the internally generated recovery tick.
    RecoveryAdmission {
        group_id: GroupId,
        reason: AdmissionRejection<GroupId>,
    },
    /// The bounded deterministic transport refused an emitted envelope.
    NetworkFull {
        bound: usize,
        /// Every envelope that did not enter the bounded queue, in emission
        /// order.
        pending: Box<[PeerEnvelope<GroupId>]>,
    },
    /// A remote real-Raft replica failed while consuming a routed envelope.
    RemoteStep {
        group_id: GroupId,
        node_id: NodeId,
        cause: ErrorCause,
    },
    /// The explicit progress budget ended before the cluster quiesced.
    ProgressBudgetExhausted { rounds: usize },
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lifecycle {
                group_id,
                expected,
                actual,
            } => write!(
                formatter,
                "group {group_id:?} expected lifecycle {expected:?}, observed {actual:?}"
            ),
            Self::GroupAlreadyRegistered(group_id) => {
                write!(formatter, "group {group_id:?} is already registered")
            }
            Self::OpenGroup { group_id, kind } => {
                write!(formatter, "group {group_id:?} could not open: {kind:?}")
            }
            Self::IdentityExhausted => formatter.write_str("managed identity space is exhausted"),
            Self::RecoveryAdmission { group_id, reason } => write!(
                formatter,
                "group {group_id:?} recovery tick was refused: {reason:?}"
            ),
            Self::NetworkFull { bound, .. } => {
                write!(
                    formatter,
                    "deterministic network queue reached bound {bound}"
                )
            }
            Self::RemoteStep {
                group_id,
                node_id,
                cause,
            } => write!(
                formatter,
                "group {group_id:?} replica {node_id:?} failed a peer step: {cause}"
            ),
            Self::ProgressBudgetExhausted { rounds } => {
                write!(formatter, "cluster did not quiesce within {rounds} rounds")
            }
        }
    }
}

impl Error for AdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RemoteStep { cause, .. } => Some(cause.as_error()),
            _ => None,
        }
    }
}

/// Three-node-per-group real Rafter cluster under one managed local host.
#[derive(Debug)]
pub struct ManagedCounterCluster {
    host: ManagedTypedMultiRaftHost<GroupId, ReplicatedCounterCommand, CounterApplyResult>,
    network_config: NetworkConfig,
    peers: BTreeMap<(GroupId, NodeId), CounterGroup>,
    lifecycles: BTreeMap<GroupId, GroupLifecycle>,
    poisoned: BTreeSet<GroupId>,
    network: VecDeque<PeerEnvelope<GroupId>>,
    completed: BTreeMap<LocalProposalId, CounterApplyResult>,
    next_proposal_id: u64,
}

impl ManagedCounterCluster {
    /// Creates an empty managed counter cluster.
    #[must_use]
    pub fn new(managed: ManagedConfig, network: NetworkConfig) -> Self {
        Self {
            host: ManagedTypedMultiRaftHost::new(managed),
            network_config: network,
            peers: BTreeMap::new(),
            lifecycles: BTreeMap::new(),
            poisoned: BTreeSet::new(),
            network: VecDeque::new(),
            completed: BTreeMap::new(),
            next_proposal_id: 1,
        }
    }

    /// Returns a completed local proposal result, when its committed apply was
    /// observed.
    #[must_use]
    pub fn completed(&self, proposal_id: LocalProposalId) -> Option<CounterApplyResult> {
        self.completed.get(&proposal_id).copied()
    }

    /// Completed local proposal identities and results in identity order.
    pub fn completed_proposals(
        &self,
    ) -> impl Iterator<Item = (LocalProposalId, CounterApplyResult)> + '_ {
        self.completed.iter().map(|(id, result)| (*id, *result))
    }

    /// Managed queue/pass/occupancy metrics.
    #[must_use]
    pub fn metrics(&self) -> rafter_multiraft::managed::ManagedMetrics {
        self.host.managed_metrics()
    }

    /// Whether the real local group observed a permanent poison.
    #[must_use]
    pub fn is_poisoned(&self, group_id: GroupId) -> bool {
        self.poisoned.contains(&group_id)
    }

    /// Runs deterministic managed passes and peer delivery until quiescence.
    ///
    /// # Errors
    ///
    /// Returns the exact transport/remote failure or progress-budget boundary.
    pub fn drive_until_idle(&mut self, max_rounds: usize) -> Result<DriveReport, AdapterError> {
        let mut total = DriveReport::default();
        for _ in 0..max_rounds {
            self.restore_poisoned_for_explicit_drain();
            let mut round = DriveReport::default();
            self.route_network(&mut round)?;
            let progressed = self.run_one_pass(&mut round)?;
            let network_pending = !self.network.is_empty();
            total.merge(round);
            if !progressed && !network_pending && self.host.managed_metrics().queued == 0 {
                return Ok(total);
            }
        }
        Err(AdapterError::ProgressBudgetExhausted { rounds: max_rounds })
    }

    fn restore_poisoned_for_explicit_drain(&mut self) {
        for group_id in &self.poisoned {
            let _ = self.host.set_available(group_id, true);
        }
    }

    fn run_one_pass(&mut self, report: &mut DriveReport) -> Result<bool, AdapterError> {
        match self
            .host
            .arm_pass()
            .map_err(|_| AdapterError::IdentityExhausted)?
        {
            ArmPass::Armed(plan) => report.plans.push(plan.groups),
            ArmPass::AlreadyArmed(_) => {}
            ArmPass::Idle => return Ok(false),
        }
        loop {
            match self
                .host
                .begin_dispatch()
                .map_err(|_| AdapterError::IdentityExhausted)?
            {
                BeginDispatch::Dispatched(dispatch) => {
                    report.opportunities += 1;
                    let managed = self
                        .host
                        .execute_dispatch(dispatch)
                        .expect("the adapter executes only its own live dispatches");
                    for item in managed.items {
                        match item.result {
                            Ok(group_report) => {
                                report.serviced += 1;
                                self.collect_report(group_report)?;
                            }
                            Err(error) => {
                                report.failed += 1;
                                if error.kind() == MultiRaftErrorKind::DriverPoisoned {
                                    self.poisoned.insert(managed.group_id);
                                }
                            }
                        }
                    }
                }
                BeginDispatch::Skipped(_) => {}
                BeginDispatch::WorkersOccupied | BeginDispatch::PassComplete(_) => return Ok(true),
                BeginDispatch::NoPass => return Ok(false),
            }
        }
    }

    fn collect_report(&mut self, report: CounterReport) -> Result<(), AdapterError> {
        for event in report.proposal_events {
            if let ProposalEvent::Applied {
                local_proposal_id,
                result,
                ..
            } = event
            {
                self.completed.insert(local_proposal_id, result);
            }
        }
        for applied in report.applied {
            if let Some(proposal_id) = applied.local_proposal_id {
                self.completed.insert(proposal_id, applied.result);
            }
        }
        let mut envelopes = report.peer_messages.into_iter();
        while let Some(envelope) = envelopes.next() {
            let bound = self.network_config.max_pending_messages.get();
            if self.network.len() >= bound {
                let pending = std::iter::once(envelope)
                    .chain(envelopes)
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                return Err(AdapterError::NetworkFull { bound, pending });
            }
            self.network.push_back(envelope);
        }
        Ok(())
    }

    fn route_network(&mut self, report: &mut DriveReport) -> Result<(), AdapterError> {
        let mut remaining = self.network.len();
        while remaining != 0 {
            remaining -= 1;
            let envelope = self
                .network
                .pop_front()
                .expect("remaining counts queued envelopes");
            if envelope.to == NodeId(1) {
                let group_id = envelope.group_id;
                match self.host.admit(
                    &group_id,
                    WorkClass::Control,
                    GroupInput::PeerMessage { envelope },
                ) {
                    Ok(_) => {}
                    Err(rejected) => {
                        let GroupInput::PeerMessage { envelope } = rejected.payload else {
                            unreachable!("the network admitted a peer envelope");
                        };
                        self.network.push_back(envelope);
                        break;
                    }
                }
                continue;
            }
            let group_id = envelope.group_id;
            let node_id = envelope.to;
            let Some(peer) = self.peers.get_mut(&(group_id, node_id)) else {
                return Err(AdapterError::Lifecycle {
                    group_id,
                    expected: GroupLifecycle::Recovering,
                    actual: self.lifecycles.get(&group_id).copied(),
                });
            };
            match peer.step(GroupInput::PeerMessage { envelope }) {
                Ok(peer_report) => self.collect_report(peer_report)?,
                Err(error) => {
                    if matches!(peer.fatal_state(), GroupFatalState::Poisoned { .. }) {
                        report.remote_failures += 1;
                        self.poisoned.insert(group_id);
                    } else {
                        return Err(AdapterError::RemoteStep {
                            group_id,
                            node_id,
                            cause: ErrorCause::new(error),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}
