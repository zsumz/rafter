use std::fmt;

use crate::{
    ConfigurationEntry, ConfigurationPhase, LocalProposalId, LogIndex, MembershipSet,
    MembershipValidationError, Message, NodeId, PromotionBarrier, RaftSnapshot, ReadId,
    SharedPayload, SnapshotChunkSend, StagedSnapshotChunk, Term,
};

/// Current role of a Raft node.
///
/// This enum is exhaustive because the Raft role state machine has a closed
/// set of roles.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Role {
    Follower,
    /// Probing electability with a pre-vote round (thesis 9.6): the node has
    /// timed out but has not incremented its term or voted for itself.
    PreCandidate,
    Candidate,
    Leader,
}

impl fmt::Display for Role {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Follower => "follower",
            Self::PreCandidate => "pre-candidate",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        })
    }
}

/// Input event accepted by the pure Raft node.
///
/// This enum is exhaustive because the kernel accepts this closed set of
/// protocol, clock, client, and configuration events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Input {
    Tick,
    Message {
        from: NodeId,
        message: Message,
    },
    ClientProposal {
        payload: Vec<u8>,
    },
    /// Proposes an application payload while attaching local-only volatile
    /// correlation metadata for upper layers.
    ///
    /// The proposal ID must not affect Raft protocol behavior and is not
    /// replicated or persisted.
    TrackedClientProposal {
        proposal_id: LocalProposalId,
        payload: Vec<u8>,
    },
    /// Adds a non-voting replica to the stable membership.
    AddLearner {
        learner_id: NodeId,
    },
    /// Promotes an existing learner through a derived joint configuration.
    PromoteLearner {
        learner_id: NodeId,
        promotion_barrier: PromotionBarrier,
    },
    /// Removes a voter through a derived joint configuration.
    RemoveVoter {
        voter_id: NodeId,
    },
    /// Enters joint consensus with the current stable membership as the old
    /// side and `target` as the new side.
    EnterJoint {
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    /// Leaves the current joint configuration by committing its new side as
    /// the final stable membership.
    LeaveJoint,
    /// Changes toward `target` using the safe Raft membership path: stable
    /// learner-only edits commit directly, voter changes enter joint
    /// consensus, and a current joint configuration can only leave to its
    /// recorded new side.
    ChangeMembership {
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    /// Raw configuration escape hatch for tests, repair tools, and protocol
    /// experiments that intentionally bypass the safe transition builders.
    ///
    /// Normal integrations should use [`Input::AddLearner`],
    /// [`Input::PromoteLearner`], [`Input::RemoveVoter`],
    /// [`Input::EnterJoint`], [`Input::LeaveJoint`], or
    /// [`Input::ChangeMembership`].
    #[doc(hidden)]
    DangerousRawConfigurationProposal {
        configuration: ConfigurationEntry,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    /// Asks a leader to hand leadership to `target` (thesis 3.10).
    TransferLeadership {
        target: NodeId,
    },
    /// Requests a linearizable read barrier (thesis 6.4). A granted barrier
    /// means: once the application has applied through the returned
    /// `read_index`, a read observes every write acknowledged before this
    /// request was made. The kernel guarantees the index; the caller waits
    /// for its own apply progress to reach it.
    ReadIndex {
        read_id: ReadId,
    },
}

impl Input {
    /// Builds an add-learner membership input.
    #[must_use]
    pub const fn add_learner(learner_id: NodeId) -> Self {
        Self::AddLearner { learner_id }
    }

    /// Builds a promote-learner membership input.
    #[must_use]
    pub const fn promote_learner(learner_id: NodeId, promotion_barrier: PromotionBarrier) -> Self {
        Self::PromoteLearner {
            learner_id,
            promotion_barrier,
        }
    }

    /// Builds a remove-voter membership input.
    #[must_use]
    pub const fn remove_voter(voter_id: NodeId) -> Self {
        Self::RemoveVoter { voter_id }
    }

    /// Builds an enter-joint membership input.
    #[must_use]
    pub fn enter_joint(target: MembershipSet, promotion_barriers: Vec<PromotionBarrier>) -> Self {
        Self::EnterJoint {
            target,
            promotion_barriers,
        }
    }

    /// Builds a leave-joint membership input.
    #[must_use]
    pub const fn leave_joint() -> Self {
        Self::LeaveJoint
    }

    /// Builds a safe membership-change input.
    #[must_use]
    pub fn change_membership(
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    ) -> Self {
        Self::ChangeMembership {
            target,
            promotion_barriers,
        }
    }

    /// Builds the raw configuration escape hatch. Prefer the safe membership
    /// operations unless the caller is deliberately constructing protocol
    /// state outside the normal transition discipline.
    #[doc(hidden)]
    #[must_use]
    pub fn dangerous_raw_configuration_proposal(
        configuration: ConfigurationEntry,
        promotion_barriers: Vec<PromotionBarrier>,
    ) -> Self {
        Self::DangerousRawConfigurationProposal {
            configuration,
            promotion_barriers,
        }
    }
}

/// Ordered side effects emitted by one [`Node`](crate::Node) step.
///
/// This is the raw kernel API. The order of a returned `Vec<Output>` is
/// load-bearing and must be preserved by direct embedders. Before releasing
/// externally visible effects such as [`Output::Send`],
/// [`Output::ReadIndexGranted`], [`Output::Apply`], or
/// [`Output::ApplySnapshot`], crash-safe embedders must durably persist the
/// corresponding node state and any staged snapshot data required by earlier
/// outputs in the same step. In particular, [`Output::StageSnapshotChunk`] can
/// be paired with an acknowledgement message from the same step; stage the
/// chunk durably before sending that acknowledgement.
///
/// Most applications should use `rafter-runtime` or `rafter-app`, which encode
/// the persist-before-output and app-apply ordering for common embeddings.
///
/// This enum is exhaustive because node steps emit this closed set of side
/// effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Output {
    /// A tracked local proposal was appended by this node while it was leader.
    ///
    /// This is local-only correlation metadata, not client-facing write
    /// success. The entry may still fail to commit or apply. A managed write
    /// API must wait for the later committed application output before
    /// reporting success.
    LocalProposalAppended {
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
    },
    /// Volatile local tracking for a proposal was cleared before the proposal
    /// applied on this node.
    ///
    /// This is local-only correlation metadata. It is not replicated,
    /// persisted, sent on the wire, stored in snapshots, or part of Raft's
    /// protocol state. The proposal may still commit elsewhere; upper layers
    /// should treat this as an unknown-outcome boundary for local waiters.
    LocalProposalDropped {
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        reason: LocalProposalDropReason,
    },
    /// The committed entry at `index` is ready for the state machine.
    ///
    /// `local_proposal_id` is present only when this process still has
    /// volatile local tracking for a tracked proposal at the same index and
    /// term. The payload shares the log's allocation; holding it is cheap.
    Apply {
        index: LogIndex,
        term: Term,
        payload: SharedPayload,
        local_proposal_id: Option<LocalProposalId>,
    },
    /// A snapshot at `snapshot.metadata.last_included_index` replaces the
    /// state machine. The kernel holds no payload bytes: the content is the
    /// staged transfer identified by `snapshot.transfer_id()`, completed by
    /// the [`Output::StageSnapshotChunk`] emitted in the same step (or, for
    /// an application-installed snapshot, already in the application's
    /// store). Promote the staged content before acting on this output.
    ApplySnapshot { snapshot: RaftSnapshot },
    /// Streams one snapshot chunk toward `to`. The transport resolves the
    /// directive against its [`SnapshotChunkSource`](crate::SnapshotChunkSource)
    /// via [`SnapshotChunkSend::resolve`] and sends the resulting
    /// [`InstallSnapshotChunk`](crate::InstallSnapshotChunk) message. An
    /// unresolvable directive is dropped like a lost message.
    SendSnapshotChunk {
        to: NodeId,
        chunk: SnapshotChunkSend,
    },
    /// A validated inbound snapshot chunk for the receiver's snapshot store.
    /// Stage it durably before releasing the acknowledgement emitted in the
    /// same step — the persist-before-output contract; a crash between the
    /// two must never leave the leader ahead of the staged prefix.
    StageSnapshotChunk { chunk: StagedSnapshotChunk },
    /// A client proposal was rejected without being appended.
    RejectProposal {
        proposal_id: Option<LocalProposalId>,
        reason: ProposalRejection,
    },
    /// A leadership-transfer request was rejected.
    LeadershipTransferRejected {
        target: NodeId,
        reason: LeadershipTransferRejection,
    },
    /// The read barrier `read_id` is confirmed at `read_index`: a quorum
    /// acknowledged this node's leadership after the barrier was registered.
    ReadIndexGranted {
        read_id: ReadId,
        read_index: LogIndex,
    },
    /// A read-index request was rejected without being registered.
    ReadIndexRejected {
        read_id: ReadId,
        reason: ReadIndexRejection,
    },
    /// A previously pending local read-index request was cleared before it
    /// could be granted.
    ///
    /// This is local-only correlation metadata for upper-layer waiters. It is
    /// not replicated, persisted, sent on the wire, or part of Raft protocol
    /// state. Callers may retry the read by issuing a new barrier to the
    /// current leader.
    ReadIndexCanceled {
        read_id: ReadId,
        reason: ReadIndexCancelReason,
    },
    /// Sends one Raft protocol message to `to`.
    Send { to: NodeId, message: Message },
}

/// Why a tracked local proposal was dropped before local apply.
///
/// This enum is exhaustive because proposal tracking has a closed set of
/// local loss boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalProposalDropReason {
    LogOverwritten,
    SnapshotCovered,
    LeadershipLost,
}

/// Why a pending read-index request was canceled before grant.
///
/// This enum is exhaustive because read tracking has a closed set of
/// cancellation boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIndexCancelReason {
    LeadershipLost,
    LeaderStateReset,
}

/// Why a read-index request was refused.
///
/// This enum is exhaustive because read-index rejection is closed over these
/// safety and capacity reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIndexRejection {
    NotLeader {
        role: Role,
        term: Term,
    },
    /// The leader has not yet committed an entry in its current term, so its
    /// commit index may trail another node's (thesis 6.4). Propose an
    /// application-level no-op and retry once it commits.
    NoCommitInCurrentTerm,
    LeadershipTransferInProgress {
        target: NodeId,
    },
    /// The leader already holds the maximum number of unconfirmed barriers;
    /// retry after in-flight barriers confirm or the leader steps down.
    TooManyPendingReads,
}

impl fmt::Display for ReadIndexRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { role, term } => write!(
                formatter,
                "read barrier rejected: node is a {role} in term {term}, not the leader"
            ),
            Self::NoCommitInCurrentTerm => formatter.write_str(
                "read barrier rejected: the leader has not committed an entry in its current term",
            ),
            Self::LeadershipTransferInProgress { target } => write!(
                formatter,
                "read barrier rejected: a leadership transfer to {target} is in progress"
            ),
            Self::TooManyPendingReads => formatter.write_str(
                "read barrier rejected: too many barriers are awaiting quorum confirmation",
            ),
        }
    }
}

/// Why a leadership-transfer request was refused.
///
/// This enum is exhaustive because transfer rejection is closed over these
/// role, target, and in-flight-state reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadershipTransferRejection {
    NotLeader,
    TargetIsSelf,
    TargetNotVoter,
    TransferAlreadyInProgress,
}

impl fmt::Display for LeadershipTransferRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotLeader => "this node is not the leader",
            Self::TargetIsSelf => "the transfer target is the leader itself",
            Self::TargetNotVoter => "the transfer target is not an effective voter",
            Self::TransferAlreadyInProgress => "a leadership transfer is already in progress",
        })
    }
}

/// Why a client proposal was refused.
///
/// This enum is exhaustive because proposal rejection is closed over these
/// leadership, sizing, transfer, and membership reasons.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProposalRejection {
    NotLeader {
        role: Role,
        term: Term,
        payload_len: usize,
    },
    /// Proposals are refused while a leadership transfer is pending so the
    /// target can finish catching up (thesis 3.10).
    LeadershipTransferInProgress {
        target: NodeId,
    },
    PayloadTooLarge {
        payload_len: usize,
        max_payload_len: usize,
    },
    Configuration(ConfigurationProposalRejection),
}

/// Why a configuration proposal was refused.
///
/// This enum is exhaustive because membership-change validation is closed over
/// these transition-discipline errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationProposalRejection {
    StableConfigurationRequired {
        phase: ConfigurationPhase,
    },
    JointConfigurationRequired {
        phase: ConfigurationPhase,
    },
    UncommittedConfiguration {
        index: LogIndex,
    },
    NodeAlreadyMember {
        node_id: NodeId,
    },
    VoterNotFound {
        node_id: NodeId,
    },
    CannotRemoveLastVoter {
        node_id: NodeId,
    },
    InvalidMembership {
        error: MembershipValidationError,
    },
    TargetMembershipUnchanged,
    TargetMembershipDoesNotMatchJointNewSide,
    PromotionTargetNotLearner {
        node_id: NodeId,
    },
    DuplicatePromotionBarrier {
        learner_id: NodeId,
    },
    UnusedPromotionBarrier {
        learner_id: NodeId,
    },
    MissingPromotionBarrier {
        learner_id: NodeId,
    },
    StalePromotionBarrier {
        learner_id: NodeId,
        required_match_index: LogIndex,
        supplied_match_index: LogIndex,
    },
    PromotionBarrierNotReached {
        learner_id: NodeId,
        required_match_index: LogIndex,
        actual_match_index: LogIndex,
    },
}

// Proposal rejections are protocol outputs a leader reports back to callers,
// not process errors, so they render as messages but do not implement
// `std::error::Error`.
impl fmt::Display for ProposalRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader {
                role,
                term,
                payload_len,
            } => write!(
                formatter,
                "proposal of {payload_len} bytes rejected: node is a {role} in term {term}, not the leader"
            ),
            Self::PayloadTooLarge {
                payload_len,
                max_payload_len,
            } => write!(
                formatter,
                "proposal payload of {payload_len} bytes exceeds the {max_payload_len} byte maximum"
            ),
            Self::LeadershipTransferInProgress { target } => write!(
                formatter,
                "proposal rejected: a leadership transfer to {target} is in progress"
            ),
            Self::Configuration(rejection) => write!(formatter, "{rejection}"),
        }
    }
}

impl fmt::Display for ConfigurationProposalRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StableConfigurationRequired { phase } => write!(
                formatter,
                "configuration proposal rejected: current configuration is {phase}, but this operation requires a stable configuration"
            ),
            Self::JointConfigurationRequired { phase } => write!(
                formatter,
                "configuration proposal rejected: current configuration is {phase}, but this operation requires a joint configuration"
            ),
            Self::UncommittedConfiguration { index } => write!(
                formatter,
                "configuration proposal rejected: configuration entry at index {index} is still uncommitted"
            ),
            Self::NodeAlreadyMember { node_id } => write!(
                formatter,
                "configuration proposal rejected: node {node_id} is already a member"
            ),
            Self::VoterNotFound { node_id } => write!(
                formatter,
                "configuration proposal rejected: voter {node_id} is not in the current membership"
            ),
            Self::CannotRemoveLastVoter { node_id } => write!(
                formatter,
                "configuration proposal rejected: removing voter {node_id} would leave the membership without voters"
            ),
            Self::InvalidMembership { error } => write!(
                formatter,
                "configuration proposal rejected: derived membership is invalid: {error}"
            ),
            Self::TargetMembershipUnchanged => formatter.write_str(
                "configuration proposal rejected: target membership matches the current membership",
            ),
            Self::TargetMembershipDoesNotMatchJointNewSide => formatter.write_str(
                "configuration proposal rejected: target membership does not match the joint configuration's new side",
            ),
            Self::PromotionTargetNotLearner { node_id } => write!(
                formatter,
                "configuration proposal rejected: promotion target {node_id} is not a learner"
            ),
            Self::DuplicatePromotionBarrier { learner_id } => write!(
                formatter,
                "configuration proposal rejected: duplicate promotion barrier supplied for learner {learner_id}"
            ),
            Self::UnusedPromotionBarrier { learner_id } => write!(
                formatter,
                "configuration proposal rejected: promotion barrier supplied for learner {learner_id}, but that learner is not being promoted"
            ),
            Self::MissingPromotionBarrier { learner_id } => write!(
                formatter,
                "configuration proposal rejected: no promotion barrier supplied for learner {learner_id}"
            ),
            Self::StalePromotionBarrier {
                learner_id,
                required_match_index,
                supplied_match_index,
            } => write!(
                formatter,
                "configuration proposal rejected: promotion barrier for learner {learner_id} supplies match index {supplied_match_index} but index {required_match_index} is required"
            ),
            Self::PromotionBarrierNotReached {
                learner_id,
                required_match_index,
                actual_match_index,
            } => write!(
                formatter,
                "configuration proposal rejected: learner {learner_id} matched index {actual_match_index}, below the required promotion barrier {required_match_index}"
            ),
        }
    }
}
