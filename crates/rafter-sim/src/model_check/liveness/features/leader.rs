use std::collections::BTreeSet;

use rafter::NodeId;

use super::{
    super::driver::{
        check_soak_safety, drive_liveness_rounds_until_observed, drive_until_stable_leader,
        issue_liveness_proposal, liveness_proposal_completed, liveness_proposal_terminal_outcome,
        single_leader, soak_liveness_failure, BoundedRun, LeaderConvergence, LivenessRoundBudget,
        ProposalTerminalOutcome, StableLeaderGuard,
    },
    production_monitor_state, FaultStateRequirement, LivenessFeatureReport,
    LivenessPreconditionProbe, LivenessPreconditions, ProposalEvidence, StableLeaderEvidence,
    LV_01_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_soak_action,
};

pub(super) fn run_quorum_only_leader_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)?;
    let round_budget = LivenessRoundBudget::capture(&state, config, 2);
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    partition_minority(&mut state, &mut trace, &mut observed_actions);
    check_soak_safety(&state, config, &trace)?;

    let Some(convergence) = drive_until_stable_leader(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
    )?
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!(
                "reachable quorum did not elect a stable leader within {budget} fair-schedule rounds"
            ),
        ));
    };
    let leader = convergence.leader;
    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, leader, &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "quorum-only leader rejected its usability probe".to_owned(),
        ));
    };
    let mut guard = StableLeaderGuard::new(leader, budget);
    let completion = drive_liveness_rounds_until_observed(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
        |state| liveness_proposal_completed(state, proposal_id),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    if !completion.observer_held {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("converged leader {leader} was replaced during the bounded usability window"),
        ));
    }
    if completion.completed && single_leader(&state) == Some(leader) {
        let Some(outcome) = liveness_proposal_terminal_outcome(&state, proposal_id) else {
            return Err(soak_liveness_failure(
                &state,
                config,
                &trace,
                catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
                "completed usability probe had no explicit terminal outcome".to_owned(),
            ));
        };
        if outcome != ProposalTerminalOutcome::Committed {
            return Err(soak_liveness_failure(
                &state,
                config,
                &trace,
                catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
                format!(
                    "leader usability probe terminated as {} instead of committed",
                    outcome.as_str()
                ),
            ));
        }
        return Ok(successful_quorum_only_report(
            &state,
            round_budget,
            budget,
            convergence,
            completion,
            proposal_id,
            outcome,
        ));
    }

    Err(soak_liveness_failure(
        &state,
        config,
        &trace,
        catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
        format!(
            "reachable-quorum leader did not complete its usability probe within {budget} fair-schedule rounds"
        ),
    ))
}

fn partition_minority(
    state: &mut crate::model_check::state::ExplorationState,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) {
    for peer in [NodeId(1), NodeId(2)] {
        apply_soak_action(
            state,
            SoakOperation::Partition {
                a: peer,
                b: NodeId(3),
            },
        );
        trace.push(SoakAction::Partition {
            a: peer,
            b: NodeId(3),
        });
        observed_actions.insert(SoakActionKind::Partition);
    }
}

fn successful_quorum_only_report(
    state: &crate::model_check::state::ExplorationState,
    round_budget: LivenessRoundBudget,
    budget: usize,
    convergence: LeaderConvergence,
    completion: BoundedRun,
    proposal_id: crate::model_check::ProposalId,
    outcome: ProposalTerminalOutcome,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_CLAUSE_IDS,
        feature_id: "quorum-only-leader-convergence",
        scenario_id: "minority-unavailable-stable-quorum-v1",
        observation_id: "quorum_only_post_fault_leaders",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(convergence.leader),
                fault_requirement: FaultStateRequirement::ActivePartition,
                stable_leader_observed: Some(single_leader(state) == Some(convergence.leader)),
                accepted_proposal_observed: Some(true),
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
            leader: convergence.leader,
            stable_rounds: convergence.stable_rounds,
            remained_leader_through_probe: true,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome,
        }),
    }
}
