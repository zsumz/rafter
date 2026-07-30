use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    num::NonZeroUsize,
};

use rafter::{LocalProposalId, LogIndex, NodeId};
use rafter_app::{
    error::ErrorCause,
    group::{GroupInput, RaftGroup},
    transport::PeerEnvelope,
};
use rafter_multiraft::{
    managed::{
        AdmissionReceipt, AdmissionRejection, Dispatch, DispatchId, ManagedConfig,
        ManagedTypedMultiRaftHost, PassId, WorkClass, WorkId,
    },
    MultiRaftErrorKind,
};
use rafter_runtime::DurableRaftNode;

use crate::{
    AdmissionRejection as PolicyRejection, ClientId, CounterCommand, CounterResult, GroupId,
    GroupIncarnation, GroupLifecycle, RequestIdentity, Sequence, SessionEpoch, WorkQuota,
};

use super::state_machine::{CounterApplyResult, CounterStateMachine, ReplicatedCounterCommand};

mod admission;
mod checkpoint;
mod drive;
mod lifecycle;

pub use checkpoint::{
    CheckpointError, CheckpointOutstanding, CheckpointSession, CounterGroupCheckpoint,
    RestoredCounterGroup,
};

type CounterGroup = RaftGroup<GroupId, CounterStateMachine, DurableRaftNode>;
type CounterDispatch = Dispatch<GroupId, GroupInput<GroupId, ReplicatedCounterCommand>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompletedRequest {
    sequence: Sequence,
    command: CounterCommand,
    result: CounterResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OutstandingRequest {
    sequence: Sequence,
    command: CounterCommand,
    receipt: ProposalReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdapterSession {
    epoch: SessionEpoch,
    outstanding: Option<OutstandingRequest>,
    completed: Option<CompletedRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupSlot {
    incarnation: GroupIncarnation,
    lifecycle: GroupLifecycle,
    quota: WorkQuota,
    applied_index: LogIndex,
    value: i64,
    sessions: BTreeMap<ClientId, AdapterSession>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingProposal {
    OpenSession {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
        receipt: ProposalReceipt,
    },
    Counter {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        request: RequestIdentity,
        command: CounterCommand,
        receipt: ProposalReceipt,
    },
    Fault {
        group_id: GroupId,
        incarnation: GroupIncarnation,
        receipt: ProposalReceipt,
    },
}

impl PendingProposal {
    const fn group_id(self) -> GroupId {
        match self {
            Self::OpenSession { group_id, .. }
            | Self::Counter { group_id, .. }
            | Self::Fault { group_id, .. } => group_id,
        }
    }

    const fn receipt(self) -> ProposalReceipt {
        match self {
            Self::OpenSession { receipt, .. }
            | Self::Counter { receipt, .. }
            | Self::Fault { receipt, .. } => receipt,
        }
    }

    const fn incarnation(self) -> GroupIncarnation {
        match self {
            Self::OpenSession { incarnation, .. }
            | Self::Counter { incarnation, .. }
            | Self::Fault { incarnation, .. } => incarnation,
        }
    }
}

/// Peer envelope paired with the consumer incarnation that emitted it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedPeerEnvelope {
    /// Group incarnation whose Raft driver emitted the envelope.
    pub incarnation: GroupIncarnation,
    /// Exact Rafter peer envelope.
    pub envelope: PeerEnvelope<GroupId>,
}

#[derive(Debug)]
struct DelayedDispatch {
    remaining_rounds: usize,
    dispatch: CounterDispatch,
}

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

/// Stable outcome of real-adapter counter admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterSubmitOutcome {
    /// New work took one managed queue slot.
    Queued(ProposalReceipt),
    /// An exact retry names the already queued work and took no second slot.
    AlreadyQueued(ProposalReceipt),
    /// An exact retry was answered from the completed session cache.
    Replayed(CounterResult),
}

/// Stable outcome of replicated session establishment admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSubmitOutcome {
    /// New session work took one managed queue slot.
    Queued(ProposalReceipt),
    /// An exact retry names the already queued session proposal.
    AlreadyQueued(ProposalReceipt),
    /// The requested epoch is already active.
    AlreadyOpen,
}

/// Why consumer policy or the managed mechanism refused a proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CounterAdmissionRejection {
    /// Consumer-owned identity, lifecycle, session, or request gate refused.
    Policy(PolicyRejection),
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
    /// Exact dispatches in scheduler order.
    pub turns: Vec<DriveTurn>,
    /// Peer traffic refused by consumer identity/lifecycle policy.
    pub refused_peer_traffic: Vec<PeerTrafficRefusal>,
}

impl DriveReport {
    fn merge(&mut self, other: Self) {
        self.plans.extend(other.plans);
        self.opportunities += other.opportunities;
        self.serviced += other.serviced;
        self.failed += other.failed;
        self.remote_failures += other.remote_failures;
        self.turns.extend(other.turns);
        self.refused_peer_traffic.extend(other.refused_peer_traffic);
    }
}

/// One managed dispatch and every terminal item disposition it produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriveTurn {
    /// Immutable ready-set pass.
    pub pass_id: PassId,
    /// Exact dispatch occupancy.
    pub dispatch_id: DispatchId,
    /// Group receiving the opportunity.
    pub group_id: GroupId,
    /// Items selected within the group's quota, in service order.
    pub items: Vec<DrivenItem>,
}

/// One accepted managed item's terminal disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrivenItem {
    /// Stable managed work identity.
    pub work_id: WorkId,
    /// Class that selected the item.
    pub class: WorkClass,
    /// Whether the real group step succeeded or failed.
    pub disposition: DrivenDisposition,
}

/// Terminal real-group result for a managed item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrivenDisposition {
    /// The group returned its complete step report.
    Serviced,
    /// The real group refused the step after dispatch.
    Failed { kind: MultiRaftErrorKind },
}

/// Explicit refusal of late or otherwise invalid peer traffic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerTrafficRefusal {
    /// Addressed group.
    pub group_id: GroupId,
    /// Incarnation carried beside the envelope.
    pub incarnation: GroupIncarnation,
    /// Exact consumer policy refusal.
    pub reason: PolicyRejection,
}

/// Terminal failure of an accepted proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalFailure {
    /// Local proposal identity whose managed item failed.
    pub proposal_id: LocalProposalId,
    /// Group that owned the proposal.
    pub group_id: GroupId,
    /// Incarnation that admitted it.
    pub incarnation: GroupIncarnation,
    /// Permanent or transient managed driver classification.
    pub kind: MultiRaftErrorKind,
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
    /// A consumer quota could not be represented by the public scheduler.
    QuotaOutOfRange(usize),
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
        pending: Box<[RoutedPeerEnvelope]>,
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
            Self::QuotaOutOfRange(quota) => {
                write!(
                    formatter,
                    "group quota {quota} is outside the supported range"
                )
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
    groups: BTreeMap<GroupId, GroupSlot>,
    poisoned: BTreeSet<GroupId>,
    network: VecDeque<RoutedPeerEnvelope>,
    completed: BTreeMap<LocalProposalId, CounterApplyResult>,
    failures: BTreeMap<LocalProposalId, ProposalFailure>,
    pending: BTreeMap<LocalProposalId, PendingProposal>,
    work_proposals: BTreeMap<WorkId, LocalProposalId>,
    delayed: Vec<DelayedDispatch>,
    service_delays: BTreeMap<GroupId, usize>,
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
            groups: BTreeMap::new(),
            poisoned: BTreeSet::new(),
            network: VecDeque::new(),
            completed: BTreeMap::new(),
            failures: BTreeMap::new(),
            pending: BTreeMap::new(),
            work_proposals: BTreeMap::new(),
            delayed: Vec::new(),
            service_delays: BTreeMap::new(),
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

    /// Returns an explicit terminal failure for an accepted proposal.
    #[must_use]
    pub fn proposal_failure(&self, proposal_id: LocalProposalId) -> Option<ProposalFailure> {
        self.failures.get(&proposal_id).copied()
    }

    /// Returns one slot's current consumer-owned incarnation and lifecycle.
    #[must_use]
    pub fn group_identity(&self, group_id: GroupId) -> Option<(GroupIncarnation, GroupLifecycle)> {
        self.groups
            .get(&group_id)
            .map(|slot| (slot.incarnation, slot.lifecycle))
    }

    /// Configures explicit deterministic service occupancy for future turns.
    ///
    /// A value of zero restores immediate execution. A nonzero value holds a
    /// real dispatch—and therefore one public scheduler worker—for that many
    /// drive rounds before stepping it.
    pub fn set_service_delay(&mut self, group_id: GroupId, rounds: usize) {
        if rounds == 0 {
            self.service_delays.remove(&group_id);
        } else {
            self.service_delays.insert(group_id, rounds);
        }
    }

    /// Removes the oldest deterministic peer envelope for caller-controlled
    /// delay, duplication, or late-delivery tests.
    #[must_use]
    pub fn take_pending_peer(&mut self) -> Option<RoutedPeerEnvelope> {
        self.network.pop_front()
    }

    /// Requeues one exact peer envelope without changing its incarnation.
    ///
    /// # Errors
    ///
    /// Returns the envelope unchanged when the bounded network is full.
    pub fn enqueue_peer(
        &mut self,
        envelope: RoutedPeerEnvelope,
    ) -> Result<(), Box<RoutedPeerEnvelope>> {
        if self.network.len() >= self.network_config.max_pending_messages.get() {
            return Err(Box::new(envelope));
        }
        self.network.push_back(envelope);
        Ok(())
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

    fn record_failed_work(&mut self, work_id: WorkId, group_id: GroupId, kind: MultiRaftErrorKind) {
        let Some(proposal_id) = self.work_proposals.remove(&work_id) else {
            return;
        };
        let Some(pending) = self.pending.remove(&proposal_id) else {
            return;
        };
        self.failures.insert(
            proposal_id,
            ProposalFailure {
                proposal_id,
                group_id,
                incarnation: pending.incarnation(),
                kind,
            },
        );
        if let PendingProposal::Counter { request, .. } = pending {
            if let Some(session) = self
                .groups
                .get_mut(&group_id)
                .and_then(|slot| slot.sessions.get_mut(&request.client_id))
            {
                if session
                    .outstanding
                    .is_some_and(|outstanding| outstanding.receipt.proposal_id == proposal_id)
                {
                    session.outstanding = None;
                }
            }
        }
    }

    fn complete_pending(&mut self, proposal_id: LocalProposalId, result: CounterApplyResult) {
        let Some(pending) = self.pending.remove(&proposal_id) else {
            return;
        };
        self.work_proposals
            .retain(|_, pending_id| *pending_id != proposal_id);
        let group_id = pending.group_id();
        let Some(slot) = self.groups.get_mut(&group_id) else {
            return;
        };
        if slot.incarnation != pending.incarnation() {
            return;
        }
        match pending {
            PendingProposal::OpenSession {
                client_id, epoch, ..
            } => {
                if matches!(
                    result,
                    CounterApplyResult::Session(
                        super::state_machine::SessionApplyResult::Opened
                            | super::state_machine::SessionApplyResult::AlreadyOpen
                            | super::state_machine::SessionApplyResult::Replaced
                    )
                ) {
                    slot.sessions.insert(
                        client_id,
                        AdapterSession {
                            epoch,
                            outstanding: None,
                            completed: None,
                        },
                    );
                }
            }
            PendingProposal::Counter {
                request, command, ..
            } => {
                let Some(session) = slot.sessions.get_mut(&request.client_id) else {
                    return;
                };
                if session
                    .outstanding
                    .is_some_and(|outstanding| outstanding.receipt.proposal_id == proposal_id)
                {
                    session.outstanding = None;
                }
                if let CounterApplyResult::Counter(counter_result) = result {
                    session.completed = Some(CompletedRequest {
                        sequence: request.sequence,
                        command,
                        result: counter_result,
                    });
                }
            }
            PendingProposal::Fault { .. } => {}
        }
    }

    fn admit_group(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        class: crate::WorkClass,
    ) -> Result<(), PolicyRejection> {
        let Some(slot) = self.groups.get(&group_id) else {
            return Err(PolicyRejection::GroupUnknown);
        };
        if slot.lifecycle == GroupLifecycle::Tombstoned {
            return Err(PolicyRejection::GroupTombstoned);
        }
        if incarnation < slot.incarnation {
            return Err(PolicyRejection::StaleIncarnation {
                current: slot.incarnation,
            });
        }
        if incarnation > slot.incarnation {
            return Err(PolicyRejection::FutureIncarnation {
                current: slot.incarnation,
            });
        }
        if matches!(slot.lifecycle, GroupLifecycle::Removed) {
            return Err(PolicyRejection::GroupNotAcceptingWork {
                state: slot.lifecycle,
                class,
            });
        }
        Ok(())
    }

    fn accepted_work_remains(&self, group_id: GroupId) -> bool {
        self.pending
            .values()
            .any(|pending| pending.group_id() == group_id)
    }

    fn gate_protocol_continuation(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<(), PolicyRejection> {
        self.admit_group(group_id, incarnation, crate::WorkClass::Control)?;
        let slot = self
            .groups
            .get(&group_id)
            .ok_or(PolicyRejection::GroupUnknown)?;
        if !slot
            .lifecycle
            .permits_protocol_continuation(self.accepted_work_remains(group_id))
        {
            return Err(PolicyRejection::GroupNotAcceptingWork {
                state: slot.lifecycle,
                class: crate::WorkClass::Control,
            });
        }
        if self.poisoned.contains(&group_id) {
            return Err(PolicyRejection::GroupPoisoned);
        }
        Ok(())
    }
}
