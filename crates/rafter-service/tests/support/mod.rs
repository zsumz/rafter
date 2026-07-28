#![allow(dead_code, unused_imports)]

/// A replica whose two membership facts a test moves by hand.
pub(crate) mod scripted;
/// The transport, validator, and cluster the transport-driver suites share.
pub(crate) mod transport;

use std::{
    cmp::min,
    collections::BTreeMap,
    fmt,
    future::Future,
    task::{Context, Poll, Waker},
};

pub(crate) use rafter::{
    ClientProposalInput, Input as RaftInput, LeadershipTransferRejection, LocalProposalId,
    LogIndex, MembershipConfig, MembershipSet, Message, NodeConfig, NodeId, Output as RaftOutput,
    ProposalRejection, ReadId, ReadIndexCancelReason, ReadIndexRejection, ReplicationProgress,
    RequestVote, Role, SharedPayload, Term,
};
pub(crate) use rafter_app::{
    error::GroupError,
    group::RaftGroup,
    proposal::{Proposal, ProposalBegin},
    read::{ReadBarrierRequest, ReadOutcome, ReadProofOutcome, ReadRequest},
    state_machine::{
        ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine, SnapshotSupport,
    },
};
use rafter_runtime::{DurableRaftNode, RaftRuntimeError};
use rafter_runtime_api::PersistedRaftRuntime;
pub(crate) use rafter_service::{
    ErrorCause, InMemoryRaftDriver, ManagedDriverError, MetricsError, RaftHandle,
    ReadAbandonReason, ReadConsistency, ReadError, ReadErrorKind, ShutdownError,
    StateMachineOperation, TransferLeadershipError, UnknownOutcomeReason, WriteBatchEntry,
    WriteError, WriteErrorKind, WriteFate, WriteReceipt,
};
use rafter_storage::InMemoryRaftHardStateStore;

pub(crate) type KvGroup = RaftGroup<(), KvStateMachine, DurableRaftNode>;
pub(crate) type KvDriver = InMemoryRaftDriver<(), KvStateMachine, DurableRaftNode>;
pub(crate) type ScriptedReadGroup = RaftGroup<(), KvStateMachine, ScriptedReadRuntime>;
pub(crate) type ScriptedReadDriver = InMemoryRaftDriver<(), KvStateMachine, ScriptedReadRuntime>;
pub(crate) type ScriptedWriteGroup = RaftGroup<(), KvStateMachine, ScriptedWriteRuntime>;
pub(crate) type ScriptedWriteDriver = InMemoryRaftDriver<(), KvStateMachine, ScriptedWriteRuntime>;
pub(crate) type NumberedGroup = RaftGroup<u64, KvStateMachine, DurableRaftNode>;
pub(crate) type NumberedDriver = InMemoryRaftDriver<u64, KvStateMachine, DurableRaftNode>;

pub(crate) fn elected_driver() -> KvDriver {
    KvDriver::new_elected(NodeId(1), groups()).expect("primary elects")
}

pub(crate) fn groups() -> Vec<KvGroup> {
    vec![
        group(1, &[2, 3], 3),
        group(2, &[1, 3], 9),
        group(3, &[1, 2], 9),
    ]
}

pub(crate) fn group(id: u64, peers: &[u64], election_timeout_ticks: u64) -> KvGroup {
    group_with_app(id, peers, election_timeout_ticks, KvStateMachine::default())
}

pub(crate) fn poisoned_group(id: u64) -> KvGroup {
    let mut group = group_with_app(
        id,
        &[],
        3,
        KvStateMachine {
            fail_apply: true,
            ..KvStateMachine::default()
        },
    );
    let error = group
        .apply_raft_outputs(vec![RaftOutput::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: SharedPayload::from(&b"poison\nvalue"[..]),
            local_proposal_id: None,
        }])
        .expect_err("apply failure poisons the group");
    assert!(format!("{error:?}").contains("ApplyBatch"));
    assert!(matches!(
        group.fatal_state(),
        rafter_app::group::GroupFatalState::Poisoned { .. }
    ));
    group
}

pub(crate) fn group_with_app(
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
    app: KvStateMachine,
) -> KvGroup {
    let config = NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new((), NodeId(id), raft, app)
}

pub(crate) fn scripted_read_driver(mode: ScriptedReadMode) -> ScriptedReadDriver {
    ScriptedReadDriver::new(NodeId(1), vec![scripted_read_group(mode)])
        .expect("scripted read driver builds")
}

pub(crate) fn scripted_read_group(mode: ScriptedReadMode) -> ScriptedReadGroup {
    RaftGroup::new(
        (),
        NodeId(1),
        ScriptedReadRuntime {
            mode,
            metric_index: LogIndex::ZERO,
        },
        KvStateMachine::default(),
    )
}

pub(crate) fn scripted_write_driver(mode: ScriptedWriteMode) -> ScriptedWriteDriver {
    ScriptedWriteDriver::new(NodeId(1), vec![scripted_write_group(mode)])
        .expect("scripted write driver builds")
}

pub(crate) fn scripted_write_group(mode: ScriptedWriteMode) -> ScriptedWriteGroup {
    RaftGroup::new(
        (),
        NodeId(1),
        ScriptedWriteRuntime { mode },
        KvStateMachine::default(),
    )
}

pub(crate) fn numbered_group(
    group_id: u64,
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
) -> NumberedGroup {
    numbered_group_with_app(
        group_id,
        id,
        peers,
        election_timeout_ticks,
        KvStateMachine::default(),
    )
}

pub(crate) fn numbered_group_with_app(
    group_id: u64,
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
    app: KvStateMachine,
) -> NumberedGroup {
    let config = NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new(group_id, NodeId(id), raft, app)
}

/// A failure injected by, or detected in, the service test fake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KvStateMachineError {
    Apply,
    Encode,
    MalformedCommand,
    ReadBarrierUnsatisfied,
}

impl fmt::Display for KvStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Apply => "apply failed",
            Self::Encode => "encode failed",
            Self::MalformedCommand => "malformed command payload",
            Self::ReadBarrierUnsatisfied => "read barrier has not been reached",
        })
    }
}

impl std::error::Error for KvStateMachineError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct KvStateMachine {
    pub(crate) applied_index: LogIndex,
    pub(crate) values: BTreeMap<String, String>,
    pub(crate) fail_apply: bool,
    pub(crate) fail_encode: bool,
}

impl ReplicatedStateMachine for KvStateMachine {
    type Command = (String, String);
    type CommandResult = Option<String>;
    type Query = String;
    type QueryResult = Option<String>;
    type Error = KvStateMachineError;

    /// Declared `Unsupported`: this state machine has no snapshot
    /// representation, so it inherits the trait's provided bodies rather than
    /// answering a question it cannot answer. A group over it refuses a
    /// Raft-driven install before the state machine is touched.
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        if self.fail_encode {
            return Err(KvStateMachineError::Encode);
        }
        Ok(format!("{}\n{}", command.0, command.1).into_bytes())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        let text =
            std::str::from_utf8(payload).map_err(|_| KvStateMachineError::MalformedCommand)?;
        let (key, value) = text
            .split_once('\n')
            .ok_or(KvStateMachineError::MalformedCommand)?;
        Ok((key.to_owned(), value.to_owned()))
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        if self.fail_apply {
            return Err(KvStateMachineError::Apply);
        }
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let (key, value) = entry.command;
            let result = self.values.insert(key, value);
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result,
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err(KvStateMachineError::ReadBarrierUnsatisfied);
        }
        Ok(self.values.get(&query).cloned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedReadMode {
    Grant(LogIndex),
    /// Grants a read index the state machine has not reached *and* emits a peer
    /// message in the same step, which no non-`Pending` read outcome can carry.
    GrantWithPeerTraffic(LogIndex),
    /// Grants a read index that names an entry the state machine will never be
    /// told about — the `Noop` every new leader appends first. The application
    /// floor below it is `LogIndex::ZERO`, so a fresh state machine satisfies
    /// the barrier without any write ever committing.
    GrantAtNonApplicationIndex(LogIndex),
    Pending,
    Reject,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScriptedReadRuntime {
    pub(crate) mode: ScriptedReadMode,
    pub(crate) metric_index: LogIndex,
}

impl PersistedRaftRuntime for ScriptedReadRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }

    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }

    fn role(&self) -> Role {
        Role::Leader
    }

    fn current_term(&self) -> Term {
        Term(1)
    }

    fn commit_index(&self) -> LogIndex {
        match self.mode {
            ScriptedReadMode::Grant(index)
            | ScriptedReadMode::GrantWithPeerTraffic(index)
            | ScriptedReadMode::GrantAtNonApplicationIndex(index) => index,
            ScriptedReadMode::Pending | ScriptedReadMode::Reject | ScriptedReadMode::Cancel => {
                self.metric_index
            }
        }
    }

    fn last_log_index(&self) -> LogIndex {
        self.commit_index()
    }

    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    /// This fake models a log whose committed prefix is application entries,
    /// except in [`ScriptedReadMode::GrantAtNonApplicationIndex`], which models
    /// the post-election log a barrier actually lands on: a committed `Noop`
    /// with no application entry anywhere below it.
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        match self.mode {
            ScriptedReadMode::GrantAtNonApplicationIndex(_) => LogIndex::ZERO,
            ScriptedReadMode::Grant(_)
            | ScriptedReadMode::GrantWithPeerTraffic(_)
            | ScriptedReadMode::Pending
            | ScriptedReadMode::Reject
            | ScriptedReadMode::Cancel => min(index, self.commit_index()),
        }
    }

    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("scripted membership is valid"),
        )
    }

    /// This runtime proposes no configuration change, so it is never mid-change
    /// and its effective membership is its committed one. Written out rather
    /// than inherited, because that is a claim about this fixture.
    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        match (self.mode, input) {
            (
                ScriptedReadMode::Grant(read_index)
                | ScriptedReadMode::GrantAtNonApplicationIndex(read_index),
                RaftInput::ReadIndex { read_id },
            ) => Ok(vec![RaftOutput::ReadIndexGranted {
                read_id,
                read_index,
            }]),
            (
                ScriptedReadMode::GrantWithPeerTraffic(read_index),
                RaftInput::ReadIndex { read_id },
            ) => Ok(vec![
                RaftOutput::ReadIndexGranted {
                    read_id,
                    read_index,
                },
                missing_node_message(),
            ]),
            (ScriptedReadMode::Reject, RaftInput::ReadIndex { read_id }) => {
                self.metric_index = LogIndex(1);
                Ok(vec![RaftOutput::ReadIndexRejected {
                    read_id,
                    reason: ReadIndexRejection::NotLeader {
                        role: Role::Follower,
                        term: Term(1),
                    },
                }])
            }
            (ScriptedReadMode::Cancel, RaftInput::ReadIndex { read_id }) => {
                self.metric_index = LogIndex(1);
                Ok(vec![RaftOutput::ReadIndexCanceled {
                    read_id,
                    reason: ReadIndexCancelReason::LeaderStateReset,
                }])
            }
            _ => Ok(Vec::new()),
        }
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step_batch(proposal_inputs_from_client(proposals))
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs)
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= self.last_log_index()).then_some(Term(1))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedWriteMode {
    AppendThenIdle,
    AppendThenCycle,
    AppendThenMissingNode,
    PreAppendRuntimeError,
    PreAppendNoLifecycleMessage,
    /// A follower that knows its leader and refuses the write.
    RejectNotLeader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScriptedWriteRuntime {
    pub(crate) mode: ScriptedWriteMode,
}

impl PersistedRaftRuntime for ScriptedWriteRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }

    fn leader_hint(&self) -> Option<NodeId> {
        match self.mode {
            ScriptedWriteMode::RejectNotLeader => Some(NodeId(2)),
            _ => Some(NodeId(1)),
        }
    }

    fn role(&self) -> Role {
        match self.mode {
            ScriptedWriteMode::RejectNotLeader => Role::Follower,
            _ => Role::Leader,
        }
    }

    fn current_term(&self) -> Term {
        Term(1)
    }

    fn commit_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex(1)
    }

    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    /// This fake appends but never commits: `commit_index` stays at zero, so
    /// no application entry is committed for a state machine to reach at any
    /// bound.
    fn committed_application_index_through(&self, _index: LogIndex) -> LogIndex {
        LogIndex::ZERO
    }

    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("scripted membership is valid"),
        )
    }

    /// This runtime proposes no configuration change, so it is never mid-change
    /// and its effective membership is its committed one. Written out rather
    /// than inherited, because that is a claim about this fixture.
    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        match input {
            RaftInput::TrackedClientProposal {
                proposal_id,
                payload,
            } => {
                if self.mode == ScriptedWriteMode::PreAppendRuntimeError {
                    return Err(RaftRuntimeError::LogPrefixDiverged { index: LogIndex(1) });
                }
                if self.mode == ScriptedWriteMode::RejectNotLeader {
                    return Ok(vec![RaftOutput::RejectProposal {
                        proposal_id: Some(proposal_id),
                        reason: ProposalRejection::NotLeader {
                            role: Role::Follower,
                            term: Term(1),
                            payload_len: payload.len(),
                        },
                    }]);
                }
                let mut outputs = Vec::new();
                if self.mode != ScriptedWriteMode::PreAppendNoLifecycleMessage {
                    outputs.push(RaftOutput::LocalProposalAppended {
                        proposal_id,
                        index: LogIndex(1),
                        term: Term(1),
                    });
                }
                match self.mode {
                    ScriptedWriteMode::AppendThenIdle => {}
                    ScriptedWriteMode::AppendThenCycle => {
                        outputs.push(self_message());
                    }
                    ScriptedWriteMode::AppendThenMissingNode
                    | ScriptedWriteMode::PreAppendNoLifecycleMessage => {
                        outputs.push(missing_node_message());
                    }
                    ScriptedWriteMode::PreAppendRuntimeError
                    | ScriptedWriteMode::RejectNotLeader => {
                        unreachable!("these modes return before outputs are produced")
                    }
                }
                Ok(outputs)
            }
            RaftInput::Message { .. } if self.mode == ScriptedWriteMode::AppendThenCycle => {
                Ok(vec![self_message()])
            }
            _ => Ok(Vec::new()),
        }
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step_batch(proposal_inputs_from_client(proposals))
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs)
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= self.last_log_index()).then_some(Term(1))
    }
}

fn proposal_inputs_from_client(proposals: Vec<ClientProposalInput>) -> Vec<RaftInput> {
    proposals
        .into_iter()
        .map(|proposal| match proposal.proposal_id {
            Some(proposal_id) => RaftInput::TrackedClientProposal {
                proposal_id,
                payload: proposal.payload,
            },
            None => RaftInput::ClientProposal {
                payload: proposal.payload,
            },
        })
        .collect()
}

fn self_message() -> RaftOutput {
    RaftOutput::Send {
        to: NodeId(1),
        message: vote_from(NodeId(1)),
    }
}

fn missing_node_message() -> RaftOutput {
    RaftOutput::Send {
        to: NodeId(2),
        message: vote_from(NodeId(1)),
    }
}

fn vote_from(candidate_id: NodeId) -> Message {
    Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id,
        last_log_index: LogIndex::ZERO,
        last_log_term: Term(0),
    })
}

/// Polls a future exactly once.
///
/// A transport driver resolves a client future from a later `tick` or
/// `deliver`, so a test needs to observe "still pending" without blocking on a
/// completion that only another call can produce.
pub(crate) fn poll_once<F: Future + Unpin>(future: &mut F) -> Option<F::Output> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match std::pin::Pin::new(future).poll(&mut context) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// The committed membership a recovered record calls live.
///
/// A record's current state is one value — a position and the membership
/// observed there — so reading the membership out is one hop rather than a
/// field. Shared because four suites ask the same question, and because writing
/// it inline in each invites a fifth to read the position and the membership
/// from different records.
pub(crate) fn live_of<G>(
    checkpoint: &rafter_service::PeerControlPlaneCheckpoint<G>,
) -> std::collections::BTreeSet<NodeId> {
    checkpoint
        .current_committed
        .as_ref()
        .map(|current| current.membership.clone())
        .unwrap_or_default()
}
