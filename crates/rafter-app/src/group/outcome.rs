//! Full-fidelity results: an immediate outcome paired with its report.
//!
//! An operation's convenience value and the durable effects the same step
//! released are different things, and a caller that drops the second drops
//! protocol progress — so each of these carries both.

use super::{
    GroupStepReport, LocalProposalId, ProposalBegin, ProposalEvent, ReadOutcome, ReadProofOutcome,
};

/// Full-fidelity result of beginning a local proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposalBeginReport<G, R> {
    /// Immediate proposal lifecycle result.
    pub begin: ProposalBegin<G, R>,
    /// Every durable side effect released by the same runtime step.
    pub report: GroupStepReport<G, R>,
}

/// Full-fidelity result of beginning a local proposal batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposalBatchBeginReport<G, R> {
    /// Immediate result for each proposal, in caller order.
    pub begins: Vec<ProposalBegin<G, R>>,
    /// Every durable side effect released by the batch step.
    pub report: GroupStepReport<G, R>,
}

/// Full-fidelity result of beginning a read-index barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBarrierBeginReport<G, R> {
    /// Immediate state of the requested read proof.
    pub outcome: ReadProofOutcome<G>,
    /// Every durable side effect released by the same runtime step.
    pub report: GroupStepReport<G, R>,
}

/// Full-fidelity result of a state-machine read.
///
/// This carries three type parameters where its siblings carry two, because a
/// query read is the only group operation whose outcome type differs from the
/// report's result type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadReport<G, Q, R> {
    /// Immediate query or freshness outcome.
    pub outcome: ReadOutcome<G, Q>,
    /// Every durable side effect released by the same runtime step.
    pub report: GroupStepReport<G, R>,
}

pub(super) fn report_has_proposal_lifecycle<G, R>(
    local_proposal_id: LocalProposalId,
    report: &GroupStepReport<G, R>,
) -> bool {
    report.proposal_events.iter().any(|event| {
        matches!(
            event,
            ProposalEvent::Appended {
                local_proposal_id: id,
                ..
            } | ProposalEvent::Applied {
                local_proposal_id: id,
                ..
            } | ProposalEvent::Rejected {
                local_proposal_id: id,
                ..
            } | ProposalEvent::UnknownOutcome {
                local_proposal_id: id,
                ..
            } if *id == local_proposal_id
        )
    })
}
