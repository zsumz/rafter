use std::collections::BTreeSet;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_until_stable_leader,
    single_leader, soak_liveness_failure, LivenessRoundBudget, StableLeaderGuard,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    StableLeaderEvidence, LV_03_READ_CLAUSE_IDS,
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
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let round_budget = LivenessRoundBudget::capture(state, config, 2);
    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} read-barrier liveness rounds"),
        ));
    };
    let leader = convergence.leader;

    let request_id = state.read_indexes_issued() + 1;
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

    let mut guard = StableLeaderGuard::new(leader, budget);
    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        budget,
        |state| liveness_read_completed(state, request_id),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    if completion.completed && completion.observer_held {
        Ok(LivenessFeatureReport {
            invariant_id: "LV-03",
            clause_ids: LV_03_READ_CLAUSE_IDS,
            feature_id: "read-barrier",
            scenario_id: "stable-leader-read-barrier-v1",
            observation_id: "completed_liveness_read_barriers",
            preconditions: LivenessPreconditions::capture(
                state,
                LivenessPreconditionProbe {
                    leader: Some(leader),
                    fault_requirement: FaultStateRequirement::Stopped,
                    stable_leader_observed: Some(single_leader(state) == Some(leader)),
                    accepted_proposal_observed: None,
                    authority_loss_observed: None,
                },
            ),
            round_budget,
            round_limit: budget.saturating_mul(2),
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
        })
    } else {
        Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "read-index request {request_id} to leader {leader} did not complete within {budget} post-heal rounds"
            ),
        ))
    }
}

fn liveness_read_completed(state: &ExplorationState, request_id: u64) -> bool {
    state
        .client_history()
        .reads
        .get(&request_id)
        .is_some_and(|read| matches!(read.outcome, ClientReadOutcome::Completed { .. }))
}
