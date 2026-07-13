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
    LV_01_CONVERGENCE_CLAUSE_IDS, LV_01_USABILITY_CLAUSE_IDS,
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
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    Ok(vec![
        run_quorum_only_leader_convergence_check(config, budget)?,
        run_quorum_only_leader_usability_check(config, budget, budget)?,
    ])
}

pub(super) fn run_quorum_only_leader_convergence_check(
    config: SoakConfig,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let context = converge_quorum_only_leader(config, budget)?;
    Ok(successful_quorum_only_convergence_report(
        &context.state,
        context.round_budget,
        context.convergence,
    ))
}

pub(super) fn run_quorum_only_leader_usability_check(
    config: SoakConfig,
    convergence_budget: usize,
    usability_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut context = converge_quorum_only_leader(config, convergence_budget)?;
    let leader = context.convergence.leader;
    let (completion, proposal_id, outcome) = check_stable_leader_usability(
        &mut context.state,
        config,
        &mut context.trace,
        &mut context.observed_actions,
        leader,
        usability_budget,
    )?;
    Ok(successful_quorum_only_usability_report(
        &context.state,
        context.round_budget,
        context.convergence,
        completion,
        proposal_id,
        outcome,
    ))
}

struct QuorumOnlyLeaderContext {
    state: crate::model_check::state::ExplorationState,
    trace: Vec<SoakAction>,
    observed_actions: BTreeSet<SoakActionKind>,
    round_budget: LivenessRoundBudget,
    convergence: LeaderConvergence,
}

fn converge_quorum_only_leader(
    config: SoakConfig,
    budget: usize,
) -> Result<QuorumOnlyLeaderContext, SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)?;
    let round_budget = LivenessRoundBudget::capture(&state, config, 1);
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
    Ok(QuorumOnlyLeaderContext {
        state,
        trace,
        observed_actions,
        round_budget,
        convergence,
    })
}

fn check_stable_leader_usability(
    state: &mut crate::model_check::state::ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    leader: NodeId,
    budget: usize,
) -> Result<
    (
        BoundedRun,
        crate::model_check::ProposalId,
        ProposalTerminalOutcome,
    ),
    SoakFailure,
> {
    let Some(proposal_id) = issue_liveness_proposal(state, leader, trace, observed_actions) else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "quorum-only leader rejected its usability probe".to_owned(),
        ));
    };
    let mut guard = StableLeaderGuard::new(leader, budget);
    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        budget,
        |state| liveness_proposal_completed(state, proposal_id),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    if !completion.observer_held {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("converged leader {leader} was replaced during the bounded usability window"),
        ));
    }
    if completion.completed && single_leader(state) == Some(leader) {
        let Some(outcome) = liveness_proposal_terminal_outcome(state, proposal_id) else {
            return Err(soak_liveness_failure(
                state,
                config,
                trace,
                catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
                "completed usability probe had no explicit terminal outcome".to_owned(),
            ));
        };
        if outcome != ProposalTerminalOutcome::Committed {
            return Err(soak_liveness_failure(
                state,
                config,
                trace,
                catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
                format!(
                    "leader usability probe terminated as {} instead of committed",
                    outcome.as_str()
                ),
            ));
        }
        return Ok((completion, proposal_id, outcome));
    }

    Err(soak_liveness_failure(
        state,
        config,
        trace,
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

fn successful_quorum_only_convergence_report(
    state: &crate::model_check::state::ExplorationState,
    round_budget: LivenessRoundBudget,
    convergence: LeaderConvergence,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_CONVERGENCE_CLAUSE_IDS,
        feature_id: "quorum-only-leader-convergence",
        scenario_id: "minority-unavailable-stable-quorum-v1",
        observation_id: "quorum_only_post_fault_leaders",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(convergence.leader),
                fault_requirement: FaultStateRequirement::ActivePartition,
                stable_leader_observed: Some(single_leader(state) == Some(convergence.leader)),
                accepted_proposal_observed: None,
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit: round_budget.round_limit(),
        rounds_used: convergence.rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader: convergence.leader,
            stable_rounds: convergence.stable_rounds,
            remained_leader_through_probe: true,
        }),
        proposal: None,
    }
}

fn successful_quorum_only_usability_report(
    state: &crate::model_check::state::ExplorationState,
    round_budget: LivenessRoundBudget,
    convergence: LeaderConvergence,
    completion: BoundedRun,
    proposal_id: crate::model_check::ProposalId,
    outcome: ProposalTerminalOutcome,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_USABILITY_CLAUSE_IDS,
        feature_id: "quorum-only-leader-usability",
        scenario_id: "minority-unavailable-stable-quorum-v1",
        observation_id: "quorum_only_stable_leader_usability_windows",
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
        round_limit: round_budget.round_limit(),
        rounds_used: completion.rounds_used,
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
