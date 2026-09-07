//! What one proposal's outcome says to a client.
//!
//! Both shipped drivers project the app layer's proposal vocabulary through
//! here, so a rejection and a lost outcome read the same whichever driver
//! observed them. An unknown-outcome reason this build does not recognize is
//! reported as a dropped proposal, which is the safe direction: it keeps the
//! write unresolved rather than telling a caller its request identity is unused.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::*;

#[cfg(test)]
pub(super) fn observe_write_report<G, R, E, RE>(
    local_proposal_id: LocalProposalId,
    options: WriteOptions,
    report: &GroupStepReport<G, R>,
) -> Option<Result<WriteReceipt<R>, ManagedOperationError<E, RE>>>
where
    R: Clone,
    E: Debug,
    RE: Debug,
{
    report.proposal_events.iter().find_map(|event| match event {
        ProposalEvent::Applied {
            local_proposal_id: id,
            index,
            term,
            result,
        } if *id == local_proposal_id => Some(Ok(WriteReceipt {
            index: *index,
            term: *term,
            result: result.clone(),
        })),
        ProposalEvent::Rejected {
            local_proposal_id: id,
            reason,
            leader_hint,
        } if *id == local_proposal_id => Some(Err(ManagedOperationError::Write(
            write_error_from_rejection(reason.clone(), *leader_hint),
        ))),
        ProposalEvent::UnknownOutcome {
            local_proposal_id: id,
            client_request_id,
            reason,
        } if *id == local_proposal_id => Some(Err(ManagedOperationError::Write(
            WriteError::UnknownOutcome {
                local_proposal_id,
                client_request_id: client_request_id.or(options.client_request_id),
                reason: managed_unknown_reason_from_app(reason),
            },
        ))),
        _ => None,
    })
}

pub(super) fn managed_unknown_reason_from_app(
    reason: &ProposalUnknownOutcomeReason,
) -> UnknownOutcomeReason {
    match reason {
        ProposalUnknownOutcomeReason::GroupPoisoned => UnknownOutcomeReason::GroupPoisoned,
        ProposalUnknownOutcomeReason::LocalProposalDropped { .. }
        | ProposalUnknownOutcomeReason::LifecycleUnreported => {
            UnknownOutcomeReason::RuntimeDroppedProposal
        }
        _ => unknown_future_app_reason(),
    }
}

/// The reason reported for an app-layer unknown-outcome variant this build does
/// not recognize.
///
/// `ProposalUnknownOutcomeReason` is `#[non_exhaustive]`, so a newer app layer
/// can name a cause this driver has never heard of. Reporting it as a dropped
/// proposal is the safe direction and the honest one: the app or runtime layer
/// declared the outcome lost, which is exactly what this variant says, and it
/// keeps the write [`WriteFate::Unresolved`] so a caller does not conclude its
/// request identity is still unused.
pub(super) fn unknown_future_app_reason() -> UnknownOutcomeReason {
    UnknownOutcomeReason::RuntimeDroppedProposal
}

pub(super) fn write_unknown_outcome(
    local_proposal_id: LocalProposalId,
    options: WriteOptions,
    reason: UnknownOutcomeReason,
) -> WriteError {
    WriteError::UnknownOutcome {
        local_proposal_id,
        client_request_id: options.client_request_id,
        reason,
    }
}

pub(super) fn write_error_from_rejection(
    reason: ProposalRejection,
    leader_hint: Option<NodeId>,
) -> WriteError {
    match reason {
        ProposalRejection::NotLeader { term, .. } => WriteError::NotLeader { leader_hint, term },
        ProposalRejection::PayloadTooLarge {
            payload_len,
            max_payload_len,
        } => WriteError::PayloadTooLarge {
            max: max_payload_len,
            actual: payload_len,
        },
        reason => WriteError::Rejected { reason },
    }
}
