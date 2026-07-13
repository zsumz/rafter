use std::collections::BTreeSet;

use rafter::NodeId;

use super::{
    super::driver::{
        check_soak_safety, drive_liveness_rounds_until_observed, issue_liveness_proposal,
        liveness_proposal_completed, liveness_proposal_terminal_outcome, single_leader,
        soak_liveness_coverage_failure, soak_liveness_invariant_failure, soak_transition_failure,
        LivenessRoundBudget, ProposalTerminalOutcome, StableLeaderGuard,
    },
    production_monitor_state, FaultStateRequirement, LivenessFeatureReport,
    LivenessPreconditionProbe, LivenessPreconditions, OperationTerminalOutcome, ProposalEvidence,
    StableLeaderEvidence, TerminalEvidenceRecorder, TerminalRecorderMode,
    LV_02_PROGRESS_CLAUSE_IDS, LV_02_TERMINATION_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    helpers::{deliver_all_in_state, elect_node_one_in_state},
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::{try_apply_soak_action, ExplorationState},
    ProposalId,
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

fn proposal_progress_report(
    state: &ExplorationState,
    leader: NodeId,
    proposal_id: ProposalId,
    round_budget: LivenessRoundBudget,
    round_limit: usize,
    rounds_used: usize,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-02",
        clause_ids: LV_02_PROGRESS_CLAUSE_IDS,
        feature_id: "proposal-progress",
        scenario_id: "stable-leader-reachable-quorum-v1",
        observation_id: "accepted_completed_liveness_proposals",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(leader)),
                accepted_proposal_observed: Some(true),
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit,
        rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader,
            stable_rounds: rounds_used.max(1),
            remained_leader_through_probe: true,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome: ProposalTerminalOutcome::Committed,
        }),
        operation: None,
    }
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

const fn operation_outcome_from_proposal(
    outcome: ProposalTerminalOutcome,
) -> OperationTerminalOutcome {
    match outcome {
        ProposalTerminalOutcome::Committed => OperationTerminalOutcome::Committed,
        ProposalTerminalOutcome::Rejected => OperationTerminalOutcome::Rejected,
        ProposalTerminalOutcome::Unknown => OperationTerminalOutcome::Unknown,
    }
}

const fn proposal_outcome_from_operation(
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

fn isolate_node_one(
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

fn proposal_authority_loss_coverage_failure(
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

fn proposal_termination_bound_failure(
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

fn proposal_termination_report(
    state: &ExplorationState,
    proposal_id: ProposalId,
    outcome: ProposalTerminalOutcome,
    stable_leader_at_acceptance: bool,
    round_budget: LivenessRoundBudget,
    round_limit: usize,
    rounds_used: usize,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-02",
        clause_ids: LV_02_TERMINATION_CLAUSE_IDS,
        feature_id: "proposal-termination",
        scenario_id: "accepted-proposal-authority-loss-v1",
        observation_id: "terminated_liveness_proposals",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: single_leader(state),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(stable_leader_at_acceptance),
                accepted_proposal_observed: Some(true),
                authority_loss_observed: Some(single_leader(state) != Some(NodeId(1))),
            },
        ),
        round_budget,
        round_limit,
        rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader: NodeId(1),
            stable_rounds: 1,
            remained_leader_through_probe: false,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome,
        }),
        operation: None,
    }
}
