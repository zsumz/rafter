//! The read-barrier liveness detector the registry pins by name.
//!
//! LV-03 owes a stable leader that stays leader for the whole operation window
//! and a read-index request that reaches an explicit terminal outcome inside
//! it; a lost premise is reported as uncovered rather than as a violation. Only
//! the recorded client history decides whether a read terminated.

use std::collections::BTreeSet;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_until_stable_leader,
    single_leader, soak_liveness_coverage_failure, soak_liveness_harness_error,
    soak_liveness_invariant_failure, LivenessRoundBudget, StableLeaderGuard,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    OperationTerminalOutcome, StableLeaderEvidence, TerminalEvidenceRecorder, TerminalRecorderMode,
    LV_03_READ_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::{ClientReadOutcome, ExplorationState},
};

pub(super) fn run_read_barrier_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_read_barrier_liveness_detector(
        state,
        config,
        trace,
        observed_actions,
        convergence_budget,
        operation_budget,
        TerminalRecorderMode::Production,
    )
}

pub(super) fn run_read_barrier_liveness_detector(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
    recorder_mode: TerminalRecorderMode,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let round_budget = LivenessRoundBudget::capture(state, config, 2);
    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, convergence_budget)?
    else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {convergence_budget} read-barrier liveness rounds"),
        ));
    };
    let leader = convergence.leader;

    let request_id = state.read_indexes_issued() + 1;
    let read_operation_id = state.cluster().read_registrations().len() as u64;
    apply_to_state(
        state,
        Operation::ReadIndex {
            to: leader,
            request_id,
        },
    );
    trace.push(SoakAction::ReadIndex {
        to: leader,
        request_id,
    });
    observed_actions.insert(SoakActionKind::ReadIndex);
    check_soak_safety(state, config, trace)?;

    let mut terminal_recorder =
        TerminalEvidenceRecorder::new(format!("read:{read_operation_id}"), recorder_mode);
    let mut guard = StableLeaderGuard::new(leader, operation_budget);
    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        operation_budget,
        |state| terminal_recorder.observe(liveness_read_outcome(state, read_operation_id)),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    if !completion.observer_held {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "stable-leader premise for read-index request {request_id} was lost during the bounded operation window"
            ),
        ));
    }
    if completion.completed {
        let Some(operation) = terminal_recorder.evidence() else {
            return Err(soak_liveness_harness_error(
                state,
                config,
                trace,
                "terminal recorder reported read completion without evidence",
            ));
        };
        return Ok(LivenessFeatureReport {
            invariant_id: "LV-03",
            clause_ids: LV_03_READ_CLAUSE_IDS,
            feature_id: "read-barrier",
            scenario_id: "stable-leader-read-barrier-v1",
            observation_id: "terminated_liveness_read_barriers",
            preconditions: LivenessPreconditions::capture(
                state,
                LivenessPreconditionProbe {
                    leader: Some(leader),
                    fault_requirement: FaultStateRequirement::Stopped,
                    stable_leader_observed: Some(true),
                    accepted_proposal_observed: None,
                    authority_loss_observed: None,
                },
            ),
            round_budget,
            round_limit: convergence_budget.saturating_add(operation_budget),
            rounds_used: convergence
                .rounds_used
                .saturating_add(completion.rounds_used),
            fault_cycle: None,
            stable_leader: Some(StableLeaderEvidence {
                leader,
                stable_rounds: convergence.stable_rounds,
                remained_leader_through_probe: true,
            }),
            proposal: None,
            operation: Some(operation),
        });
    }
    Err(soak_liveness_invariant_failure(
        state,
        config,
        trace,
        catalog::LV_03_FEATURE_OPERATION_PROGRESS,
        format!(
            "read-index request {request_id} to leader {leader} did not complete or terminate explicitly within {operation_budget} post-heal rounds"
        ),
    ))
}

fn liveness_read_outcome(
    state: &ExplorationState,
    operation_id: u64,
) -> Option<OperationTerminalOutcome> {
    state
        .client_history()
        .reads
        .get(&operation_id)
        .and_then(|read| match read.outcome {
            ClientReadOutcome::Completed { .. } => Some(OperationTerminalOutcome::Completed),
            ClientReadOutcome::Rejected { .. } => Some(OperationTerminalOutcome::Rejected),
            ClientReadOutcome::Canceled { .. } => Some(OperationTerminalOutcome::Canceled),
            ClientReadOutcome::Pending | ClientReadOutcome::ProofGranted { .. } => None,
        })
}

#[cfg(test)]
#[path = "read_test.rs"]
mod tests;
