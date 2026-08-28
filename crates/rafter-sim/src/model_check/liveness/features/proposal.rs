//! The two proposal liveness detectors the registry pins by name.
//!
//! LV-02 owes progress under a leader that never changes, and termination once
//! authority is lost and the fault is healed; each holds its premise for the
//! whole bounded window or reports uncovered rather than passing. Report
//! rendering and the scenario's failure shapes live in the submodules.

use std::collections::BTreeSet;

use rafter::NodeId;

use super::{
    super::driver::{
        check_soak_safety, drive_liveness_rounds_until_observed, issue_liveness_proposal,
        liveness_proposal_completed, liveness_proposal_terminal_outcome, single_leader,
        soak_liveness_coverage_failure, soak_liveness_invariant_failure, soak_transition_failure,
        LivenessRoundBudget, ProposalTerminalOutcome, StableLeaderGuard,
    },
    production_monitor_state, LivenessFeatureReport, OperationTerminalOutcome,
    TerminalEvidenceRecorder, TerminalRecorderMode,
};
use crate::model_check::{
    catalog,
    helpers::{deliver_all_in_state, elect_node_one_in_state},
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::try_apply_soak_action,
};

mod reports;
mod scenario;

use reports::{proposal_progress_report, proposal_termination_report};
use scenario::{
    isolate_node_one, operation_outcome_from_proposal, proposal_authority_loss_coverage_failure,
    proposal_outcome_from_operation, proposal_termination_bound_failure,
};

pub(super) fn run_proposal_progress_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_proposal_progress_liveness_detector(config, budget, TerminalRecorderMode::Production)
}

pub(super) fn run_proposal_progress_liveness_detector(
    config: SoakConfig,
    budget: usize,
    recorder_mode: TerminalRecorderMode,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)?;
    let round_budget = LivenessRoundBudget::capture(&state, config, 1);
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);

    let leader = NodeId(1);
    if single_leader(&state) != Some(leader) {
        return Err(soak_liveness_coverage_failure(
            &state,
            config,
            &[],
            catalog::LV_02_PROPOSAL_PROGRESS,
            "stable-proposal monitor did not establish its expected leader".to_owned(),
        ));
    }

    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, leader, &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_coverage_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            "stable-proposal monitor could not establish an accepted proposal".to_owned(),
        ));
    };
    check_soak_safety(&state, config, &trace)?;

    let mut terminal_recorder =
        TerminalEvidenceRecorder::new(format!("proposal:{}", proposal_id.0), recorder_mode);
    let mut guard = StableLeaderGuard::new(leader, budget);
    let completion = drive_liveness_rounds_until_observed(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
        |state| {
            terminal_recorder.observe(
                liveness_proposal_completed(state, proposal_id)
                    .then_some(OperationTerminalOutcome::Committed),
            )
        },
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    let outcome = terminal_recorder
        .evidence()
        .map(|evidence| evidence.outcome)
        .and_then(proposal_outcome_from_operation);
    if !completion.observer_held || single_leader(&state) != Some(leader) {
        return Err(soak_liveness_coverage_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            format!(
                "stable-leader premise for proposal {} was lost during the bounded progress window",
                proposal_id.0
            ),
        ));
    }
    if !completion.completed || outcome != Some(ProposalTerminalOutcome::Committed) {
        return Err(soak_liveness_invariant_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            format!(
                "accepted proposal {} did not commit under the same stable leader within {budget} bounded-fair rounds",
                proposal_id.0
            ),
        ));
    }

    Ok(proposal_progress_report(
        &state,
        leader,
        proposal_id,
        round_budget,
        budget,
        completion.rounds_used,
    ))
}

pub(super) fn run_proposal_termination_liveness_check(
    config: SoakConfig,
    authority_loss_budget: usize,
    termination_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_proposal_termination_liveness_detector(
        config,
        authority_loss_budget,
        termination_budget,
        TerminalRecorderMode::Production,
    )
}

pub(super) fn run_proposal_termination_liveness_detector(
    config: SoakConfig,
    authority_loss_budget: usize,
    termination_budget: usize,
    recorder_mode: TerminalRecorderMode,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)?;
    let round_budget = LivenessRoundBudget::capture(&state, config, 2);
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);

    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    isolate_node_one(&mut state, config, &mut trace, &mut observed_actions)?;

    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, NodeId(1), &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_coverage_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            "proposal-termination monitor could not establish an accepted proposal".to_owned(),
        ));
    };
    let stable_leader_at_acceptance = single_leader(&state) == Some(NodeId(1));
    check_soak_safety(&state, config, &trace)?;

    let competing_leader = drive_liveness_rounds_until_observed(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        authority_loss_budget,
        |state| {
            state
                .cluster()
                .leaders()
                .into_iter()
                .any(|leader| leader != NodeId(1))
        },
        |_| true,
    )?;
    if !competing_leader.completed {
        return Err(proposal_authority_loss_coverage_failure(
            &state,
            config,
            &trace,
            proposal_id,
            authority_loss_budget,
        ));
    }

    try_apply_soak_action(&mut state, SoakOperation::Heal)
        .map_err(|failure| soak_transition_failure(config, &trace, failure))?;
    trace.push(SoakAction::Heal);
    observed_actions.insert(SoakActionKind::Heal);
    check_soak_safety(&state, config, &trace)?;

    let mut terminal_recorder =
        TerminalEvidenceRecorder::new(format!("proposal:{}", proposal_id.0), recorder_mode);
    let termination = drive_liveness_rounds_until_observed(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        termination_budget,
        |state| {
            terminal_recorder.observe(
                liveness_proposal_terminal_outcome(state, proposal_id)
                    .map(operation_outcome_from_proposal),
            )
        },
        |_| true,
    )?;
    if termination.completed {
        let Some(outcome) = terminal_recorder
            .evidence()
            .map(|evidence| evidence.outcome)
            .and_then(proposal_outcome_from_operation)
        else {
            return Err(soak_liveness_invariant_failure(
                &state,
                config,
                &trace,
                catalog::LV_02_PROPOSAL_PROGRESS,
                "proposal-termination monitor reported completion without an outcome".to_owned(),
            ));
        };
        return Ok(proposal_termination_report(
            &state,
            proposal_id,
            outcome,
            stable_leader_at_acceptance,
            round_budget,
            authority_loss_budget.saturating_add(termination_budget),
            competing_leader.rounds_used + termination.rounds_used,
        ));
    }

    Err(proposal_termination_bound_failure(
        &state,
        config,
        &trace,
        proposal_id,
        termination_budget,
    ))
}
