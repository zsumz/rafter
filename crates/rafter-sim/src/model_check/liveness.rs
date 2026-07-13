use std::collections::BTreeSet;

use super::{
    catalog,
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::try_apply_soak_action,
    state::ExplorationState,
};

pub(in crate::model_check::liveness) mod driver;
mod features;

use driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_soak_liveness_round,
    drive_until_stable_leader, has_partition, issue_liveness_proposal, liveness_proposal_accepted,
    liveness_proposal_completed, liveness_proposal_terminal_outcome, single_leader,
    soak_liveness_coverage_failure, soak_liveness_invariant_failure, soak_transition_failure,
    FairRoundDriver, LivenessRoundBudget, ProposalTerminalOutcome, StableLeaderGuard,
};
pub(in crate::model_check) use features::LivenessFeatureReport;
use features::{
    run_feature_liveness_checks, EvidenceStatus, FaultCycleEvidence, FaultStateRequirement,
    LivenessPreconditionProbe, LivenessPreconditions, ProposalEvidence, StableLeaderEvidence,
    LV_01_CONVERGENCE_CLAUSE_IDS, LV_01_USABILITY_CLAUSE_IDS,
};

#[cfg(test)]
pub(super) use features::run_snapshot_catchup_liveness_check;

const MIN_SOAK_LIVENESS_ROUNDS: usize = 128;
const POST_HEAL_FAULT_EXERCISE_ROUNDS: usize = 1;

pub(super) fn run_soak_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    run_soak_liveness_check_with_budget_overrides(
        state,
        config,
        trace,
        observed_actions,
        None,
        None,
    )
}

pub(in crate::model_check) fn run_soak_liveness_check_with_budget_overrides(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget_override: Option<usize>,
    usability_budget_override: Option<usize>,
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    let fault_cycle = create_and_heal_post_heal_fault(state, config, trace, observed_actions)?;
    let convergence_round_budget = LivenessRoundBudget::capture(state, config, 1)
        .with_fixed_rounds(fault_cycle.partitioned_rounds);
    let usability_round_budget = LivenessRoundBudget::capture(state, config, 1);
    let convergence_budget =
        convergence_budget_override.unwrap_or(convergence_round_budget.base_rounds);
    let usability_budget = usability_budget_override.unwrap_or(usability_round_budget.base_rounds);

    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, convergence_budget)?
    else {
        return Err(soak_liveness_invariant_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {convergence_budget} post-heal convergence rounds"),
        ));
    };
    let leader = convergence.leader;
    let convergence_report = successful_post_heal_convergence_report(
        state,
        convergence_round_budget,
        convergence,
        fault_cycle,
    );

    let Some(proposal_id) = issue_liveness_proposal(state, leader, trace, observed_actions) else {
        return Err(soak_liveness_invariant_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal stable leader rejected the usability proposal".to_owned(),
        ));
    };
    let accepted_proposal = liveness_proposal_accepted(state, proposal_id);
    check_soak_safety(state, config, trace)?;
    let mut guard = StableLeaderGuard::new(leader, usability_budget);
    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        usability_budget,
        |state| liveness_proposal_completed(state, proposal_id),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    let outcome = liveness_proposal_terminal_outcome(state, proposal_id);
    if !completion.completed
        || !completion.observer_held
        || outcome != Some(ProposalTerminalOutcome::Committed)
        || single_leader(state) != Some(leader)
    {
        return Err(soak_liveness_invariant_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!(
                "post-heal leader {leader} did not commit proposal {} within {usability_budget} bounded-fair usability rounds",
                proposal_id.0
            ),
        ));
    }

    let usability_report = successful_post_heal_usability_report(
        state,
        usability_round_budget,
        convergence,
        completion,
        proposal_id,
        accepted_proposal,
    );
    let mut reports = vec![convergence_report, usability_report];
    reports.extend(run_feature_liveness_checks(config, observed_actions)?);
    Ok(reports)
}

fn successful_post_heal_convergence_report(
    state: &ExplorationState,
    round_budget: LivenessRoundBudget,
    convergence: driver::LeaderConvergence,
    fault_cycle: FaultCycleEvidence,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_CONVERGENCE_CLAUSE_IDS,
        feature_id: "leader-convergence",
        scenario_id: "post-heal-stable-quorum-v1",
        observation_id: "post_heal_quiescent_leaders",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(convergence.leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(convergence.leader)),
                accepted_proposal_observed: None,
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit: round_budget.round_limit(),
        rounds_used: convergence
            .rounds_used
            .saturating_add(fault_cycle.partitioned_rounds),
        fault_cycle: Some(fault_cycle),
        stable_leader: Some(StableLeaderEvidence {
            leader: convergence.leader,
            stable_rounds: convergence.stable_rounds,
            remained_leader_through_probe: true,
        }),
        proposal: None,
        operation: None,
    }
}

fn successful_post_heal_usability_report(
    state: &ExplorationState,
    round_budget: LivenessRoundBudget,
    convergence: driver::LeaderConvergence,
    completion: driver::BoundedRun,
    proposal_id: crate::model_check::ProposalId,
    accepted_proposal: bool,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_USABILITY_CLAUSE_IDS,
        feature_id: "leader-usability",
        scenario_id: "post-heal-stable-quorum-v1",
        observation_id: "post_heal_stable_leader_usability_windows",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(convergence.leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(convergence.leader)),
                accepted_proposal_observed: Some(accepted_proposal),
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
            outcome: ProposalTerminalOutcome::Committed,
        }),
        operation: None,
    }
}

fn create_and_heal_post_heal_fault(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<FaultCycleEvidence, SoakFailure> {
    let (partition_a, partition_b) =
        create_post_heal_partition(state, config, trace, observed_actions)?;
    let exercise = exercise_post_heal_partition(
        state,
        config,
        trace,
        observed_actions,
        partition_a,
        partition_b,
    )?;
    heal_post_heal_partition(
        state,
        config,
        trace,
        observed_actions,
        partition_a,
        partition_b,
    )?;
    Ok(FaultCycleEvidence {
        partition_a,
        partition_b,
        partition_observed: EvidenceStatus::Satisfied,
        partitioned_rounds: POST_HEAL_FAULT_EXERCISE_ROUNDS,
        nodes_exercised: exercise.nodes_exercised,
        ticks_executed: exercise.ticks_executed,
        deliveries_executed: exercise.deliveries_executed,
        drops_executed: exercise.drops_executed,
        protocol_state_changed: exercise.protocol_state_changed,
        partition_active_after_exercise: EvidenceStatus::Satisfied,
        heal_observed: EvidenceStatus::Satisfied,
    })
}

fn create_post_heal_partition(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<(rafter::NodeId, rafter::NodeId), SoakFailure> {
    if has_partition(state.cluster()) {
        try_apply_soak_action(state, SoakOperation::Heal)
            .map_err(|failure| soak_transition_failure(config, trace, failure))?;
        trace.push(SoakAction::Heal);
        observed_actions.insert(SoakActionKind::Heal);
        check_soak_safety(state, config, trace)?;
    }
    let mut nodes = state.cluster().nodes.keys().copied();
    let Some(partition_a) = nodes.next() else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario requires two nodes to create a real fault".to_owned(),
        ));
    };
    let Some(partition_b) = nodes.next() else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario requires two nodes to create a real fault".to_owned(),
        ));
    };

    try_apply_soak_action(
        state,
        SoakOperation::Partition {
            a: partition_a,
            b: partition_b,
        },
    )
    .map_err(|failure| soak_transition_failure(config, trace, failure))?;
    trace.push(SoakAction::Partition {
        a: partition_a,
        b: partition_b,
    });
    observed_actions.insert(SoakActionKind::Partition);
    let partition_observed =
        state.cluster().partitioned(partition_a, partition_b) && has_partition(state.cluster());
    if !partition_observed {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario did not observe its injected partition".to_owned(),
        ));
    }
    check_soak_safety(state, config, trace)?;
    Ok((partition_a, partition_b))
}

struct PartitionExercise {
    nodes_exercised: usize,
    ticks_executed: usize,
    deliveries_executed: usize,
    drops_executed: usize,
    protocol_state_changed: bool,
}

fn exercise_post_heal_partition(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    partition_a: rafter::NodeId,
    partition_b: rafter::NodeId,
) -> Result<PartitionExercise, SoakFailure> {
    let nodes_exercised = state.cluster().nodes.len();
    let protocol_before = super::explorers::protocol_state_fingerprint(state);
    let exercise_trace_start = trace.len();
    let mut fair_rounds = FairRoundDriver::new();
    for round in 0..POST_HEAL_FAULT_EXERCISE_ROUNDS {
        drive_soak_liveness_round(
            &mut fair_rounds,
            state,
            config,
            trace,
            observed_actions,
            round,
        )?;
        check_soak_safety(state, config, trace)?;
        if !state.cluster().partitioned(partition_a, partition_b) {
            return Err(soak_liveness_coverage_failure(
                state,
                config,
                trace,
                catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
                "post-heal scenario partition disappeared during protocol exercise".to_owned(),
            ));
        }
    }
    let exercise_actions = &trace[exercise_trace_start..];
    let ticks_executed = exercise_actions
        .iter()
        .filter(|action| action.kind() == SoakActionKind::Tick)
        .count();
    let deliveries_executed = exercise_actions
        .iter()
        .filter(|action| action.kind() == SoakActionKind::Deliver)
        .count();
    let drops_executed = exercise_actions
        .iter()
        .filter(|action| action.kind() == SoakActionKind::Drop)
        .count();
    let protocol_after = super::explorers::protocol_state_fingerprint(state);
    let partition_active_after_exercise =
        state.cluster().partitioned(partition_a, partition_b) && has_partition(state.cluster());
    if !partition_active_after_exercise {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario partition was not active after protocol exercise".to_owned(),
        ));
    }
    Ok(PartitionExercise {
        nodes_exercised,
        ticks_executed,
        deliveries_executed,
        drops_executed,
        protocol_state_changed: protocol_before != protocol_after,
    })
}

fn heal_post_heal_partition(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    partition_a: rafter::NodeId,
    partition_b: rafter::NodeId,
) -> Result<(), SoakFailure> {
    try_apply_soak_action(state, SoakOperation::Heal)
        .map_err(|failure| soak_transition_failure(config, trace, failure))?;
    trace.push(SoakAction::Heal);
    observed_actions.insert(SoakActionKind::Heal);
    let heal_observed =
        !has_partition(state.cluster()) && !state.cluster().partitioned(partition_a, partition_b);
    if !heal_observed {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario did not observe its fault being healed".to_owned(),
        ));
    }
    check_soak_safety(state, config, trace)?;
    Ok(())
}
