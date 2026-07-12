//! Events accepted by the deterministic Raft node.
//!
//! Inputs describe protocol, clock, client, read, membership, and leadership
//! transfer events without embedding storage, transport, or scheduling policy.

use crate::{
    ConfigurationEntry, LocalProposalId, MembershipSet, Message, NodeId, PromotionBarrier, ReadId,
};

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

/// One client proposal inside an explicit kernel proposal batch.
///
/// The optional proposal ID is local-only correlation metadata. It is not part
/// of the replicated log entry and must not affect protocol behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientProposalInput {
    /// Optional local-only correlation ID for this proposal.
    pub proposal_id: Option<LocalProposalId>,
    /// Opaque application bytes to append if the local leader accepts the
    /// proposal.
    pub payload: Vec<u8>,
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
