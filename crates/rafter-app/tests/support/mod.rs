#![allow(dead_code, unused_imports)]

use std::{
    cmp::{max, min},
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
};

pub(crate) use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, ClientProposalInput, Input as RaftInput,
    LeadershipTransferRejection, LocalProposalId, LogIndex, MembershipConfig, MembershipSet,
    Message, Node as RaftNode, NodeConfig, NodeId, Output as RaftOutput, PreVoteResponse,
    PromotionBarrier, ProposalRejection, RaftSnapshot, RaftSnapshotMetadata, ReadId,
    ReadIndexCancelReason, ReadIndexRejection, ReplicationProgress, RequestVoteResponse, Role,
    SharedPayload, SnapshotChunkSend, SnapshotGroupId, StagedSnapshotChunk, Term,
};
pub(crate) use rafter_app::error::{ErrorCause, GroupError, StateMachineOperation};
pub(crate) use rafter_app::group::{
    GroupFatalState, GroupInput, GroupStepReport, LeadershipTransferEvent, PoisonedWaiters,
    RaftGroup, ReadReport, StepReportOptions,
};
pub(crate) use rafter_app::membership::{MembershipChange, MembershipEvent, NodeInfo};
pub(crate) use rafter_app::proposal::{
    ClientRequestId, Proposal, ProposalBegin, ProposalEvent, ProposalUnknownOutcomeReason,
};
pub(crate) use rafter_app::read::{
    ReadBarrierRequest, ReadConsistency, ReadEvent, ReadOutcome, ReadProof, ReadProofOutcome,
    ReadRequest,
};
pub(crate) use rafter_app::snapshot::SnapshotEvent;
pub(crate) use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};
pub(crate) use rafter_app::transport::PeerEnvelope;
pub(crate) use rafter_runtime_api::PersistedRaftRuntime;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum ApplyMode {
    #[default]
    Normal,
    Fail,
    DropLastResult,
    WrongIndex,
    WrongTerm,
    WrongLocalProposalId,
}

/// The fault this fake was told to inject.
///
/// A typed error rather than a `String` because the trait now requires one, and
/// because a test that wants to know *which* callback failed should read a
/// variant rather than match a message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordingStateMachineError {
    Encode,
    Decode,
    Apply,
    InstallSnapshot,
}

impl fmt::Display for RecordingStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Encode => "encode failed",
            Self::Decode => "decode failed",
            Self::Apply => "apply failed",
            Self::InstallSnapshot => "install snapshot failed",
        })
    }
}

impl Error for RecordingStateMachineError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RecordingStateMachine {
    pub(crate) applied_index: LogIndex,
    pub(crate) applied: Vec<Vec<u8>>,
    pub(crate) batches: Vec<Vec<LogIndex>>,
    pub(crate) apply_mode: ApplyMode,
    pub(crate) fail_encode: bool,
    pub(crate) fail_decode: bool,
    pub(crate) fail_install_snapshot: bool,
    pub(crate) reported_applied_index: Option<LogIndex>,
    pub(crate) installed_snapshots: Vec<ApplicationSnapshot>,
}

impl ReplicatedStateMachine for RecordingStateMachine {
    type Command = Vec<u8>;
    type CommandResult = Vec<u8>;
    type Query = Vec<u8>;
    type QueryResult = Option<Vec<u8>>;
    type Error = RecordingStateMachineError;

    /// Declared `Supported` because this fake is driven through real installs
    /// and its `fail_install_snapshot` switch is the coverage that must survive.
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.reported_applied_index.unwrap_or(self.applied_index))
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        if self.fail_encode {
            return Err(RecordingStateMachineError::Encode);
        }
        Ok(command.clone())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        if self.fail_decode {
            return Err(RecordingStateMachineError::Decode);
        }
        Ok(payload.to_vec())
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        if self.apply_mode == ApplyMode::Fail {
            return Err(RecordingStateMachineError::Apply);
        }

        self.batches
            .push(batch.entries.iter().map(|entry| entry.index).collect());
        let mut results = Vec::new();
        for entry in batch.entries {
            self.applied_index = entry.index;
            self.applied.push(entry.command.clone());
            let mut result = ApplyResult {
                index: entry.index,
                term: entry.term,
                result: entry.command,
                local_proposal_id: entry.local_proposal_id,
            };
            match self.apply_mode {
                ApplyMode::WrongIndex => {
                    result.index = result.index.next();
                }
                ApplyMode::WrongTerm => {
                    result.term = result.term.next();
                }
                ApplyMode::WrongLocalProposalId => {
                    result.local_proposal_id = Some(LocalProposalId(999));
                }
                ApplyMode::Normal | ApplyMode::Fail | ApplyMode::DropLastResult => {}
            }
            results.push(result);
        }
        if self.apply_mode == ApplyMode::DropLastResult {
            results.pop();
        }
        Ok(results)
    }

    /// An empty query reads served state instead of echoing itself, so a test
    /// can tell whether a read answered from state that predates a write. Every
    /// other query is echoed, which is all a test of barrier plumbing needs.
    fn read(
        &self,
        query: Self::Query,
        _barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        if query.is_empty() {
            return Ok(self.applied.last().cloned());
        }
        Ok(Some(query))
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: Vec::new(),
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        if self.fail_install_snapshot {
            return Err(RecordingStateMachineError::InstallSnapshot.into());
        }
        self.applied_index = snapshot.applied_index;
        self.installed_snapshots.push(snapshot);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TestRuntimeError {
    Forced,
}

impl fmt::Display for TestRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forced => formatter.write_str("forced runtime error"),
        }
    }
}

impl Error for TestRuntimeError {}

#[derive(Clone, Debug)]
pub(crate) struct KernelRuntime {
    node: RaftNode,
}

impl KernelRuntime {
    pub(crate) fn new(id: u64, peers: &[u64]) -> Self {
        Self {
            node: RaftNode::new(
                NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 1)
                    .expect("test node config is valid"),
            ),
        }
    }
}

impl PersistedRaftRuntime for KernelRuntime {
    type Error = TestRuntimeError;

    fn id(&self) -> NodeId {
        self.node.id()
    }

    fn leader_hint(&self) -> Option<NodeId> {
        self.node.leader_hint()
    }

    fn role(&self) -> Role {
        self.node.role()
    }

    fn current_term(&self) -> Term {
        self.node.current_term()
    }

    fn commit_index(&self) -> LogIndex {
        self.node.commit_index()
    }

    fn last_log_index(&self) -> LogIndex {
        self.node.last_log_index()
    }

    fn snapshot_index(&self) -> LogIndex {
        self.node.snapshot_index()
    }

    /// This fake owns a real kernel, so it answers from the kernel's own log:
    /// the highest committed entry carrying an application payload at or below
    /// the bound, falling back to the snapshot boundary capped at the bound.
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        let snapshot_index = self.node.snapshot_index();
        let bound = index.min(self.node.commit_index());
        let first_retained = snapshot_index.next();
        self.node
            .log_entries_slice_from(first_retained)
            .iter()
            .enumerate()
            .rev()
            .map(|(offset, entry)| (LogIndex(first_retained.0 + offset as u64), entry))
            .find(|(entry_index, entry)| {
                *entry_index <= bound && entry.application_payload().is_some()
            })
            .map_or_else(|| snapshot_index.min(index), |(entry_index, _)| entry_index)
    }

    fn membership(&self) -> MembershipConfig {
        self.node.effective_membership()
    }

    fn committed_membership(&self) -> MembershipConfig {
        self.node.committed_membership()
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        self.node.leader_replication_progress()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(self.node.step(input))
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(self.node.step_proposal_batch(proposals))
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(self.node.step_batch(inputs))
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        self.node.term_at_index(index)
    }
}

/// A modeled log shape this fake adopts at the start of a step.
///
/// Reshaping mid-flight is how a test commits and compacts behind a barrier
/// whose read index was already granted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScriptedLogShape {
    pub(crate) application_entries: Option<BTreeSet<LogIndex>>,
    pub(crate) commit_index: LogIndex,
    pub(crate) snapshot_index: LogIndex,
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedRuntime {
    pub(crate) node_id: NodeId,
    pub(crate) leader_hint: Option<NodeId>,
    pub(crate) role: Role,
    pub(crate) current_term: Term,
    pub(crate) commit_index: LogIndex,
    pub(crate) last_log_index: LogIndex,
    pub(crate) snapshot_index: LogIndex,
    /// Indexes whose log entry carries an application payload, since this fake
    /// models no log. `None` — the default — models a log in which every index
    /// is an application entry, so a floor is exactly its own bound; that is
    /// what every fixture written before the read barrier gained an application
    /// floor assumed. A mixed log lists its application entries explicitly.
    pub(crate) application_entries: Option<BTreeSet<LogIndex>>,
    pub(crate) membership: MembershipConfig,
    pub(crate) committed_membership: MembershipConfig,
    pub(crate) replication: Vec<ReplicationProgress>,
    pub(crate) terms: BTreeMap<LogIndex, Term>,
    /// Log shapes adopted at the start of a step, so a test can commit and
    /// compact behind a barrier that has already been granted.
    pub(crate) step_log_shapes: VecDeque<ScriptedLogShape>,
    pub(crate) step_memberships: VecDeque<(MembershipConfig, MembershipConfig)>,
    pub(crate) step_inputs: Vec<RaftInput>,
    pub(crate) step_batches: Vec<Vec<RaftInput>>,
    pub(crate) proposal_batches: Vec<Vec<ClientProposalInput>>,
    pub(crate) step_outputs: VecDeque<Vec<RaftOutput>>,
    pub(crate) step_errors: VecDeque<TestRuntimeError>,
}

impl ScriptedRuntime {
    pub(crate) fn with_terms(terms: impl IntoIterator<Item = (LogIndex, Term)>) -> Self {
        Self {
            node_id: NodeId(1),
            leader_hint: Some(NodeId(1)),
            role: Role::Leader,
            current_term: Term(1),
            commit_index: LogIndex::ZERO,
            last_log_index: LogIndex::ZERO,
            snapshot_index: LogIndex::ZERO,
            application_entries: None,
            membership: membership(&[1], &[]),
            committed_membership: membership(&[1], &[]),
            replication: Vec::new(),
            terms: terms.into_iter().collect(),
            step_log_shapes: VecDeque::new(),
            step_memberships: VecDeque::new(),
            step_inputs: Vec::new(),
            step_batches: Vec::new(),
            proposal_batches: Vec::new(),
            step_outputs: VecDeque::new(),
            step_errors: VecDeque::new(),
        }
    }

    pub(crate) fn with_step_outputs(outputs: impl IntoIterator<Item = Vec<RaftOutput>>) -> Self {
        let mut runtime = Self::with_terms([
            (LogIndex(2), Term(1)),
            (LogIndex(3), Term(1)),
            (LogIndex(4), Term(1)),
            (LogIndex(5), Term(1)),
        ]);
        runtime.current_term = Term(2);
        runtime.step_outputs = outputs.into_iter().collect();
        runtime
    }

    pub(crate) fn with_step_errors(errors: impl IntoIterator<Item = TestRuntimeError>) -> Self {
        let mut runtime = Self::with_step_outputs([]);
        runtime.step_errors = errors.into_iter().collect();
        runtime
    }
}

impl PersistedRaftRuntime for ScriptedRuntime {
    type Error = TestRuntimeError;

    fn id(&self) -> NodeId {
        self.node_id
    }

    fn leader_hint(&self) -> Option<NodeId> {
        self.leader_hint
    }

    fn role(&self) -> Role {
        self.role
    }

    fn current_term(&self) -> Term {
        self.current_term
    }

    fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    fn last_log_index(&self) -> LogIndex {
        self.last_log_index
    }

    fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    /// The fake holds no log, so it answers from the application-entry set it
    /// models: the greatest modeled application entry at or below the bound,
    /// falling back to the snapshot boundary capped at the bound.
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        let boundary = min(self.snapshot_index, index);
        let highest = match &self.application_entries {
            None => (index > self.snapshot_index).then_some(index),
            Some(entries) => entries.range(..=index).next_back().copied(),
        };
        highest.map_or(boundary, |entry| max(entry, boundary))
    }

    fn membership(&self) -> MembershipConfig {
        self.membership.clone()
    }

    fn committed_membership(&self) -> MembershipConfig {
        self.committed_membership.clone()
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        self.replication.clone()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        if let RaftInput::AddLearner { learner_id } = &input {
            self.last_log_index = self.last_log_index.next();
            self.terms.insert(self.last_log_index, self.current_term);
            self.membership = membership(&[1], &[learner_id.0]);
        }
        if let Some(shape) = self.step_log_shapes.pop_front() {
            self.application_entries = shape.application_entries;
            self.commit_index = shape.commit_index;
            self.snapshot_index = shape.snapshot_index;
        }
        if let Some((membership, committed_membership)) = self.step_memberships.pop_front() {
            self.membership = membership;
            self.committed_membership = committed_membership;
        }
        self.step_inputs.push(input);
        if let Some(error) = self.step_errors.pop_front() {
            return Err(error);
        }
        Ok(self.step_outputs.pop_front().unwrap_or_default())
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        self.step_inputs
            .extend(proposals.iter().map(proposal_input_from_client));
        self.proposal_batches.push(proposals);
        if let Some(error) = self.step_errors.pop_front() {
            return Err(error);
        }
        Ok(self.step_outputs.pop_front().unwrap_or_default())
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        self.step_batches.push(inputs.clone());
        self.step_inputs.extend(inputs);
        if let Some(error) = self.step_errors.pop_front() {
            return Err(error);
        }
        Ok(self.step_outputs.pop_front().unwrap_or_default())
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        self.terms.get(&index).copied()
    }
}

fn proposal_input_from_client(proposal: &ClientProposalInput) -> RaftInput {
    match proposal.proposal_id {
        Some(proposal_id) => RaftInput::TrackedClientProposal {
            proposal_id,
            payload: proposal.payload.clone(),
        },
        None => RaftInput::ClientProposal {
            payload: proposal.payload.clone(),
        },
    }
}

pub(crate) fn runtime(id: u64, peers: &[u64]) -> KernelRuntime {
    KernelRuntime::new(id, peers)
}

pub(crate) fn group(
    id: u64,
    peers: &[u64],
) -> RaftGroup<u64, RecordingStateMachine, KernelRuntime> {
    RaftGroup::new(
        7,
        NodeId(id),
        runtime(id, peers),
        RecordingStateMachine::default(),
    )
}

/// Drives a real kernel-backed group to leadership by scripting node 2's
/// pre-vote and vote grants, and returns the term it won.
pub(crate) fn elect_group_leader(
    group: &mut RaftGroup<u64, RecordingStateMachine, KernelRuntime>,
) -> Term {
    let report = group.step(GroupInput::Tick).expect("pre-vote starts");
    let pre_vote_term = report
        .peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::PreVote(request) => Some(request.term),
            _ => None,
        })
        .expect("pre-vote request is emitted");
    let report = group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::PreVoteResponse(PreVoteResponse {
                    term: pre_vote_term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
            },
        })
        .expect("pre-vote grant starts the election");
    let vote_term = report
        .peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::RequestVote(request) => Some(request.term),
            _ => None,
        })
        .expect("request vote is emitted");
    let _ = group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::RequestVoteResponse(RequestVoteResponse {
                    term: vote_term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
            },
        })
        .expect("vote grant elects the leader");
    assert_eq!(group.metrics().role, Role::Leader);
    vote_term
}

/// Acknowledges replication up to `match_index` from node 2, which is quorum in
/// a three-node group led by node 1.
pub(crate) fn acknowledge_replication(
    group: &mut RaftGroup<u64, RecordingStateMachine, KernelRuntime>,
    term: Term,
    match_index: LogIndex,
    peer_messages: &[PeerEnvelope<u64>],
) -> GroupStepReport<u64, Vec<u8>> {
    let sequence = peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::AppendEntries(AppendEntries { sequence, .. }) => Some(*sequence),
            _ => None,
        })
        .expect("the leader replicated its append");
    group
        .step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: 7,
                from: NodeId(2),
                to: NodeId(1),
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    term,
                    follower_id: NodeId(2),
                    success: true,
                    match_index,
                    sequence,
                }),
            },
        })
        .expect("quorum acknowledgement commits and applies")
}

pub(crate) fn membership(voters: &[u64], learners: &[u64]) -> MembershipConfig {
    MembershipConfig::stable(membership_set(voters, learners))
}

pub(crate) fn membership_set(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .expect("test membership is valid")
}

pub(crate) fn scripted_group(
    app: RecordingStateMachine,
) -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    scripted_group_with_runtime(
        app,
        ScriptedRuntime::with_terms([
            (LogIndex(2), Term(1)),
            (LogIndex(3), Term(1)),
            (LogIndex(4), Term(1)),
        ]),
    )
}

pub(crate) fn scripted_group_with_runtime(
    app: RecordingStateMachine,
    runtime: ScriptedRuntime,
) -> RaftGroup<u64, RecordingStateMachine, ScriptedRuntime> {
    RaftGroup::new(7, NodeId(1), runtime, app)
}

pub(crate) fn apply_output(
    index: u64,
    payload: &[u8],
    local_proposal_id: Option<LocalProposalId>,
) -> RaftOutput {
    RaftOutput::Apply {
        index: LogIndex(index),
        term: Term(1),
        payload: SharedPayload::from(payload),
        local_proposal_id,
    }
}

pub(crate) fn append_output(proposal_id: LocalProposalId, index: u64) -> RaftOutput {
    RaftOutput::LocalProposalAppended {
        proposal_id,
        index: LogIndex(index),
        term: Term(1),
    }
}

pub(crate) fn begin_pending_proposal(
    group: &mut RaftGroup<u64, RecordingStateMachine, ScriptedRuntime>,
    local_proposal_id: LocalProposalId,
    client_request_id: Option<ClientRequestId>,
    index: u64,
) {
    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id,
            client_request_id,
            command: format!("proposal-{}", local_proposal_id.0).into_bytes(),
        })
        .expect("test proposal starts");
    assert!(matches!(
        begin,
        ProposalBegin::Appended {
            local_proposal_id: actual_id,
            index: actual_index,
            ..
        } if actual_id == local_proposal_id && actual_index == LogIndex(index)
    ));
}

pub(crate) fn begin_pending_read_barrier(
    group: &mut RaftGroup<u64, RecordingStateMachine, ScriptedRuntime>,
    read_id: ReadId,
    min_applied_index: Option<LogIndex>,
) {
    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, min_applied_index))
        .expect("test read barrier starts");
    assert!(matches!(
        outcome,
        ReadProofOutcome::Pending { read_id: actual_id, .. }
            | ReadProofOutcome::FreshnessUnavailable {
                read_id: actual_id,
                ..
            } if actual_id == read_id
    ));
}

pub(crate) fn assert_read_metrics(
    group: &RaftGroup<u64, RecordingStateMachine, ScriptedRuntime>,
    pending_read_barriers: usize,
    pending_query_reads: usize,
    completed_query_reads: usize,
    reserved_reads: usize,
) {
    let metrics = group.metrics();
    assert_eq!(metrics.pending_reads, pending_read_barriers);
    assert_eq!(metrics.pending_read_barriers, pending_read_barriers);
    assert_eq!(metrics.pending_query_reads, pending_query_reads);
    assert_eq!(metrics.completed_query_reads, completed_query_reads);
    assert_eq!(metrics.reserved_reads, reserved_reads);
}

pub(crate) fn assert_non_monotonic_read_id(
    error: &GroupError<RecordingStateMachineError, TestRuntimeError>,
    read_id: ReadId,
    last_seen_read_id: ReadId,
) {
    assert!(matches!(
        error,
        GroupError::NonMonotonicReadId {
            read_id: actual,
            last_seen_read_id: actual_last_seen,
        } if *actual == read_id && *actual_last_seen == last_seen_read_id
    ));
}

pub(crate) fn test_snapshot(index: u64) -> RaftSnapshot {
    let application = ApplicationSnapshotMetadata::new(
        ApplicationSnapshotKind::new("kv").expect("snapshot kind is valid"),
        ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
    );
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("group-7").expect("snapshot group id is valid"),
        NodeId(1),
        LogIndex(index),
        Term(2),
        Term(2),
        application,
    )
    .expect("snapshot metadata is valid");
    RaftSnapshot::from_payload(metadata, b"snapshot")
}

pub(crate) fn staged_snapshot_chunk(snapshot: &RaftSnapshot) -> StagedSnapshotChunk {
    StagedSnapshotChunk {
        leader_id: NodeId(1),
        transfer_id: snapshot.transfer_id(),
        metadata: snapshot.metadata.clone(),
        total_payload_len: snapshot.application_payload_len,
        application_payload_crc32: snapshot.application_payload_crc32,
        offset: 0,
        bytes: b"snapshot".to_vec(),
        done: true,
    }
}

pub(crate) fn snapshot_chunk_send(snapshot: &RaftSnapshot) -> SnapshotChunkSend {
    SnapshotChunkSend {
        term: Term(2),
        leader_id: NodeId(1),
        transfer_id: snapshot.transfer_id(),
        metadata: snapshot.metadata.clone(),
        total_payload_len: snapshot.application_payload_len,
        application_payload_crc32: snapshot.application_payload_crc32,
        offset: 0,
        len: u32::try_from(snapshot.application_payload_len)
            .expect("test snapshot payload length fits in one chunk"),
        done: true,
    }
}

/// A linearizable request whose query reads served state rather than echoing
/// itself. Use this when the point of the test is *what* a read answered.
pub(crate) fn state_read_request(
    read_id: ReadId,
    min_applied_index: Option<LogIndex>,
) -> ReadRequest<u64, Vec<u8>> {
    ReadRequest::Linearizable {
        group_id: 7,
        read_id,
        query: Vec::new(),
        min_applied_index,
        context: Vec::new(),
    }
}

pub(crate) fn read_request(
    read_id: ReadId,
    min_applied_index: Option<LogIndex>,
) -> ReadBarrierRequest<u64> {
    ReadBarrierRequest {
        group_id: 7,
        read_id,
        min_applied_index,
        context: Vec::new(),
    }
}

pub(crate) fn read_helper_request(
    read_id: ReadId,
    consistency: ReadConsistency,
    min_applied_index: Option<LogIndex>,
) -> ReadRequest<u64, Vec<u8>> {
    match consistency {
        ReadConsistency::Local => ReadRequest::Local {
            group_id: 7,
            query: b"query".to_vec(),
            min_applied_index,
        },
        ReadConsistency::Linearizable => ReadRequest::Linearizable {
            group_id: 7,
            read_id,
            query: b"query".to_vec(),
            min_applied_index,
            context: Vec::new(),
        },
        ReadConsistency::LeaseRead => ReadRequest::Lease {
            group_id: 7,
            query: b"query".to_vec(),
            min_applied_index,
        },
        _ => unreachable!("test helper only handles known read consistency variants"),
    }
}
