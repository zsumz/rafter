//! The injected fault the post-heal scenario owes before it may measure heal.
//!
//! A partition must be created, exercised while it holds, and healed, with each
//! stage observed rather than assumed, so an unobserved stage is reported as
//! missing coverage instead of passing quietly. Measuring convergence after the
//! heal belongs to the parent.

use std::collections::BTreeSet;

use super::driver::{
    check_soak_safety, drive_soak_liveness_round, has_partition, soak_liveness_coverage_failure,
    soak_transition_failure, FairRoundDriver,
};
use super::features::{EvidenceStatus, FaultCycleEvidence};
use super::POST_HEAL_FAULT_EXERCISE_ROUNDS;
use crate::model_check::{
    catalog,
    explorers::protocol_state_fingerprint,
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::try_apply_soak_action,
    state::ExplorationState,
};

pub(super) fn create_and_heal_post_heal_fault(
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
    let protocol_before = protocol_state_fingerprint(state);
    let exercise_trace_start = trace.len();
    let mut fair_rounds = FairRoundDriver::new(config.seed);
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
    let protocol_after = protocol_state_fingerprint(state);
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
