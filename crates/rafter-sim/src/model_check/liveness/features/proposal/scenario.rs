//! Scenario setup, outcome mapping, and failure shapes for LV-02.
//!
//! Isolating the leader is a scheduling step the detector owes before it may
//! claim authority loss, and the two failure shapes fix whether an exhausted
//! budget reads as missing coverage or as a broken bound. Deciding which bound
//! applies stays with the detector.

use std::collections::BTreeSet;

use rafter::NodeId;

use super::super::super::driver::{
    soak_liveness_coverage_failure, soak_liveness_invariant_failure, soak_transition_failure,
    ProposalTerminalOutcome,
};
use super::super::OperationTerminalOutcome;
use crate::model_check::{
    catalog,
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::{try_apply_soak_action, ExplorationState},
    ProposalId,
};

pub(super) const fn operation_outcome_from_proposal(
    outcome: ProposalTerminalOutcome,
) -> OperationTerminalOutcome {
    match outcome {
        ProposalTerminalOutcome::Committed => OperationTerminalOutcome::Committed,
        ProposalTerminalOutcome::Rejected => OperationTerminalOutcome::Rejected,
        ProposalTerminalOutcome::Unknown => OperationTerminalOutcome::Unknown,
    }
}

pub(super) const fn proposal_outcome_from_operation(
    outcome: OperationTerminalOutcome,
) -> Option<ProposalTerminalOutcome> {
    match outcome {
        OperationTerminalOutcome::Committed => Some(ProposalTerminalOutcome::Committed),
        OperationTerminalOutcome::Rejected => Some(ProposalTerminalOutcome::Rejected),
        OperationTerminalOutcome::Unknown => Some(ProposalTerminalOutcome::Unknown),
        OperationTerminalOutcome::Completed
        | OperationTerminalOutcome::Canceled
        | OperationTerminalOutcome::Installed => None,
    }
}

pub(super) fn isolate_node_one(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<(), SoakFailure> {
    for peer in [NodeId(2), NodeId(3)] {
        try_apply_soak_action(
            state,
            SoakOperation::Partition {
                a: NodeId(1),
                b: peer,
            },
        )
        .map_err(|failure| soak_transition_failure(config, trace, failure))?;
        trace.push(SoakAction::Partition {
            a: NodeId(1),
            b: peer,
        });
        observed_actions.insert(SoakActionKind::Partition);
    }
    Ok(())
}

pub(super) fn proposal_authority_loss_coverage_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    proposal_id: ProposalId,
    budget: usize,
) -> SoakFailure {
    soak_liveness_coverage_failure(
        state,
        config,
        trace,
        catalog::LV_02_PROPOSAL_PROGRESS,
        format!(
            "accepted proposal {} did not establish authority loss within {budget} bounded-fair rounds",
            proposal_id.0
        ),
    )
}

pub(super) fn proposal_termination_bound_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    proposal_id: ProposalId,
    budget: usize,
) -> SoakFailure {
    soak_liveness_invariant_failure(
        state,
        config,
        trace,
        catalog::LV_02_PROPOSAL_PROGRESS,
        format!(
            "accepted proposal {} did not reach an explicit terminal state within {budget} authority-loss rounds",
            proposal_id.0
        ),
    )
}
