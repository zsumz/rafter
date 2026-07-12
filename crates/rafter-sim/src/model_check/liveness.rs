use std::collections::BTreeSet;

use super::{
    catalog,
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_soak_action,
    state::ExplorationState,
};

pub(in crate::model_check::liveness) mod driver;
mod features;

use driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_soak_liveness_round,
    drive_until_stable_leader, has_partition, issue_liveness_proposal, liveness_proposal_accepted,
    liveness_proposal_completed, liveness_proposal_terminal_outcome, single_leader,
    soak_liveness_failure, LivenessRoundBudget, ProposalTerminalOutcome, StableLeaderGuard,
};
pub(in crate::model_check) use features::LivenessFeatureReport;
use features::{
    run_feature_liveness_checks, EvidenceStatus, FaultCycleEvidence, FaultStateRequirement,
    LivenessPreconditionProbe, LivenessPreconditions, ProposalEvidence, StableLeaderEvidence,
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
    let fault_cycle = create_and_heal_post_heal_fault(state, config, trace, observed_actions)?;
    let round_budget = LivenessRoundBudget::capture(state, config, 2)
        .with_fixed_rounds(fault_cycle.partitioned_rounds);
    let budget = round_budget.base_rounds;

    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} post-heal convergence rounds"),
        ));
    };
    let leader = convergence.leader;

    let Some(proposal_id) = issue_liveness_proposal(state, leader, trace, observed_actions) else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            "post-heal stable leader did not accept the usability proposal".to_owned(),
        ));
    };
    let accepted_proposal = liveness_proposal_accepted(state, proposal_id);
    check_soak_safety(state, config, trace)?;
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
    let outcome = liveness_proposal_terminal_outcome(state, proposal_id);
    if !completion.completed
        || !completion.observer_held
        || outcome != Some(ProposalTerminalOutcome::Committed)
        || single_leader(state) != Some(leader)
    {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            format!(
                "post-heal leader {leader} did not commit proposal {} within {budget} bounded-fair rounds",
                proposal_id.0
            ),
        ));
    }

    let post_heal_report = LivenessFeatureReport {
        invariant_id: "LV-01",
        feature_id: "leader-convergence",
        scenario_id: "post-heal-stable-quorum-v1",
        observation_id: "post_heal_quiescent_leaders",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(leader)),
                accepted_proposal_observed: Some(accepted_proposal),
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit: round_budget.round_limit(),
        rounds_used: convergence
            .rounds_used
            .saturating_add(completion.rounds_used)
            .saturating_add(fault_cycle.partitioned_rounds),
        fault_cycle: Some(fault_cycle),
        stable_leader: Some(StableLeaderEvidence {
            leader,
            stable_rounds: convergence.stable_rounds,
            remained_leader_through_probe: true,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome: ProposalTerminalOutcome::Committed,
        }),
    };
    let mut reports = vec![post_heal_report];
    reports.extend(run_feature_liveness_checks(config, observed_actions)?);
    Ok(reports)
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
        apply_soak_action(state, SoakOperation::Heal);
        trace.push(SoakAction::Heal);
        observed_actions.insert(SoakActionKind::Heal);
        check_soak_safety(state, config, trace)?;
    }
    let mut nodes = state.cluster().nodes.keys().copied();
    let Some(partition_a) = nodes.next() else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario requires two nodes to create a real fault".to_owned(),
        ));
    };
    let Some(partition_b) = nodes.next() else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario requires two nodes to create a real fault".to_owned(),
        ));
    };

    apply_soak_action(
        state,
        SoakOperation::Partition {
            a: partition_a,
            b: partition_b,
        },
    );
    trace.push(SoakAction::Partition {
        a: partition_a,
        b: partition_b,
    });
    observed_actions.insert(SoakActionKind::Partition);
    let partition_observed =
        state.cluster().partitioned(partition_a, partition_b) && has_partition(state.cluster());
    if !partition_observed {
        return Err(soak_liveness_failure(
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
    let protocol_before = state
        .cluster()
        .nodes
        .keys()
        .copied()
        .map(|node_id| {
            (
                node_id,
                state.cluster().current_term(node_id),
                state.cluster().role(node_id),
            )
        })
        .collect::<Vec<_>>();
    let exercise_trace_start = trace.len();
    for round in 0..POST_HEAL_FAULT_EXERCISE_ROUNDS {
        drive_soak_liveness_round(state, config, trace, observed_actions, round)?;
        check_soak_safety(state, config, trace)?;
        if !state.cluster().partitioned(partition_a, partition_b) {
            return Err(soak_liveness_failure(
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
    let protocol_after = state
        .cluster()
        .nodes
        .keys()
        .copied()
        .map(|node_id| {
            (
                node_id,
                state.cluster().current_term(node_id),
                state.cluster().role(node_id),
            )
        })
        .collect::<Vec<_>>();
    let partition_active_after_exercise =
        state.cluster().partitioned(partition_a, partition_b) && has_partition(state.cluster());
    if !partition_active_after_exercise {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            "post-heal scenario partition was not active after protocol exercise".to_owned(),
        ));
    }
    Ok(PartitionExercise {
        nodes_exercised: protocol_before.len(),
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
    apply_soak_action(state, SoakOperation::Heal);
    trace.push(SoakAction::Heal);
    observed_actions.insert(SoakActionKind::Heal);
    let heal_observed =
        !has_partition(state.cluster()) && !state.cluster().partitioned(partition_a, partition_b);
    if !heal_observed {
        return Err(soak_liveness_failure(
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
