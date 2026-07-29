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
    LogOverwritten,
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
    LeadershipTransfer { target: NodeId },
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
