//! Typed reasons that local protocol operations do not proceed.
//!
//! These values are protocol outputs for callers, not process failures. Their
//! human-readable rendering lives beside the vocabulary without turning the
//! outcomes into `std::error::Error` values.

mod display;

use crate::{ConfigurationPhase, LogIndex, MembershipValidationError, NodeId, Term};

use super::Role;

/// Why a tracked local proposal was dropped before local apply.
///
/// This enum is exhaustive because a tracked proposal leaves the kernel without
/// apply at exactly two boundaries: its log entry is overwritten, or its leader
/// state is torn down. A new boundary must break every caller that classifies
/// write fate rather than falling through a wildcard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalProposalDropReason {
    /// Another leader's log replaced the tracked entry.
    LogOverwritten,
    /// Local leader state ended before the entry applied locally.
    LeadershipLost,
}

/// Why a pending read-index request was canceled before grant.
///
/// This enum is exhaustive because read tracking has a closed set of
/// cancellation boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIndexCancelReason {
    /// The local node ceased being leader.
    LeadershipLost,
    /// Leader initialization reset volatile read tracking.
    LeaderStateReset,
    /// A leadership transfer began and cleared pending barriers.
    LeadershipTransfer {
        /// Voter receiving the leadership transfer.
        target: NodeId,
    },
}

/// Why a read-index request was refused.
///
/// This enum is exhaustive because read-index rejection is closed over these
/// safety and capacity reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIndexRejection {
    /// The local node is not currently leader.
    NotLeader {
        /// Current local role.
        role: Role,
        /// Current local term.
        term: Term,
    },
    /// The leader has not yet committed an entry in its current term, so its
    /// commit index may trail another node's (thesis 6.4). Propose an
    /// application-level no-op and retry once it commits.
    NoCommitInCurrentTerm,
    /// A leadership transfer is in progress.
    LeadershipTransferInProgress {
        /// Voter receiving the leadership transfer.
        target: NodeId,
    },
    /// The leader already holds the maximum number of unconfirmed barriers;
    /// retry after in-flight barriers confirm or the leader steps down.
    TooManyPendingReads,
}

/// Why a leadership-transfer request was refused.
///
/// This enum is exhaustive because transfer rejection is closed over these
/// role, target, and in-flight-state reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadershipTransferRejection {
    /// Only a leader can initiate a transfer.
    NotLeader,
    /// Leadership is already local, so self-transfer has no effect.
    TargetIsSelf,
    /// The requested target is not a current voter.
    TargetNotVoter,
    /// Another leadership transfer is already active.
    TransferAlreadyInProgress,
}

/// Why a client proposal was refused.
///
/// This enum is exhaustive because proposal rejection is closed over these
/// leadership, sizing, transfer, and membership reasons.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProposalRejection {
    /// The local node is not currently leader.
    NotLeader {
        /// Current local role.
        role: Role,
        /// Current local term.
        term: Term,
        /// Rejected payload length in bytes.
        payload_len: usize,
    },
    /// Proposals are refused while a leadership transfer is pending so the
    /// target can finish catching up (thesis 3.10).
    LeadershipTransferInProgress {
        /// Voter receiving the leadership transfer.
        target: NodeId,
    },
    /// The application payload exceeds the configured replication limit.
    PayloadTooLarge {
        /// Rejected payload length in bytes.
        payload_len: usize,
        /// Maximum accepted payload length in bytes.
        max_payload_len: usize,
    },
    /// A membership proposal violated transition discipline.
    Configuration(ConfigurationProposalRejection),
}

/// Why a configuration proposal was refused.
///
/// This enum is exhaustive because membership-change validation is closed over
/// these transition-discipline errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationProposalRejection {
    /// The operation can start only from stable membership.
    StableConfigurationRequired {
        /// Current membership phase.
        phase: ConfigurationPhase,
    },
    /// The operation can start only from joint membership.
    JointConfigurationRequired {
        /// Current membership phase.
        phase: ConfigurationPhase,
    },
    /// A prior configuration entry has not committed yet.
    UncommittedConfiguration {
        /// Log index of the outstanding configuration entry.
        index: LogIndex,
    },
    /// The proposed learner identity already belongs to the membership.
    NodeAlreadyMember {
        /// Existing replica identity.
        node_id: NodeId,
    },
    /// The requested removal target is not a voter.
    VoterNotFound {
        /// Missing voter identity.
        node_id: NodeId,
    },
    /// Removing the requested voter would leave no voter.
    CannotRemoveLastVoter {
        /// Sole remaining voter.
        node_id: NodeId,
    },
    /// The proposed membership is structurally invalid.
    InvalidMembership {
        /// Exact membership validation failure.
        error: MembershipValidationError,
    },
    /// The proposed target is identical to the current membership.
    TargetMembershipUnchanged,
    /// Leaving joint consensus targeted a set other than its recorded new side.
    TargetMembershipDoesNotMatchJointNewSide,
    /// A promotion named a replica that is not currently a learner.
    PromotionTargetNotLearner {
        /// Invalid promotion target.
        node_id: NodeId,
    },
    /// More than one catch-up barrier was supplied for one learner.
    DuplicatePromotionBarrier {
        /// Learner with duplicate evidence.
        learner_id: NodeId,
    },
    /// Catch-up evidence was supplied for a learner not being promoted.
    UnusedPromotionBarrier {
        /// Learner named by unused evidence.
        learner_id: NodeId,
    },
    /// A learner promotion has no catch-up evidence.
    MissingPromotionBarrier {
        /// Learner lacking evidence.
        learner_id: NodeId,
    },
    /// Supplied catch-up evidence was below the required barrier.
    StalePromotionBarrier {
        /// Learner being promoted.
        learner_id: NodeId,
        /// Match index required by the derived barrier.
        required_match_index: LogIndex,
        /// Match index carried by the supplied barrier.
        supplied_match_index: LogIndex,
    },
    /// The learner has not replicated through its promotion barrier.
    PromotionBarrierNotReached {
        /// Learner being promoted.
        learner_id: NodeId,
        /// Match index required before promotion.
        required_match_index: LogIndex,
        /// Learner's current replicated match index.
        actual_match_index: LogIndex,
    },
}
