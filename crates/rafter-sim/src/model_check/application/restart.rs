use rafter::{BootstrapState, LogIndex, Node, NodeId, SharedPayload};

use super::super::super::{
    catalog,
    helpers::summarize,
    invariants::{check_applied_floor_recovery, check_exact_durable_restart, AppliedFloorRecovery},
    observations::Observation,
    Action, ExplorationState, Failure, FailureKind,
};

pub(super) fn restart_node_inner(
    state: &mut ExplorationState,
    node_id: NodeId,
    trace: &[Action],
) -> Result<(), Failure> {
    let before_digest = state.cluster.durable_state_digest(node_id);
    let before_applied_floor = state.cluster.durable_applied_floor(node_id);
    let before = state.cluster.bootstrap_state(node_id);
    let before_last_log_index = state.cluster.last_log_index(node_id);
    let expected_replay = expected_replay_from_bootstrap(&before, before_applied_floor);
    let before_pending = state
        .cluster
        .nodes
        .get(&node_id)
        .and_then(Node::pending_snapshot_transfer);
    let before_staged = state.cluster.snapshot_staging.get(&node_id).cloned();
    let before_applied_len = state.cluster.applied.len();

    state
        .cluster
        .0
        .restart_node_from_bootstrap(node_id, before.clone())
        .map_err(|error| {
            restart_failure(
                state,
                trace,
                FailureKind::HarnessError,
                catalog::PS_03_EXACT_DURABLE_RESTART,
                format!("{node_id} failed to restart from bootstrap state: {error:?}"),
            )
        })?;

    if let Some(pending) = before_pending.clone() {
        let Some(node) = state.cluster.0.nodes.get_mut(&node_id) else {
            return Err(restart_failure(
                state,
                trace,
                FailureKind::HarnessError,
                catalog::PS_03_EXACT_DURABLE_RESTART,
                format!("{node_id} restart lost the node record"),
            ));
        };
        let resume_result = node.resume_pending_snapshot_transfer(pending);
        if let Err(error) = resume_result {
            return Err(restart_failure(
                state,
                trace,
                FailureKind::HarnessError,
                catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
                format!("{node_id} failed to resume pending snapshot transfer: {error:?}"),
            ));
        }
        // The kernel record resumes only alongside its durably staged byte
        // prefix; a plain restart would have dropped both together.
        if let Some(staged) = before_staged {
            state.cluster.0.snapshot_staging.insert(node_id, staged);
        }
    }

    let recovered_applies = state.cluster.applied[before_applied_len..].to_vec();
    check_applied_floor_recovery(
        &state.cluster,
        AppliedFloorRecovery {
            node_id,
            applied_floor: before_applied_floor,
            commit_index: before.commit_index,
            last_log_index: before_last_log_index,
            expected_replay: &expected_replay,
            recovered_applies: &recovered_applies,
        },
        trace,
    )?;

    let after = state.cluster.bootstrap_state(node_id);
    if after != before {
        return Err(restart_failure(
            state,
            trace,
            FailureKind::InvariantViolation,
            catalog::PS_03_EXACT_DURABLE_RESTART,
            format!("{node_id} restart changed bootstrap state"),
        ));
    }

    let after_pending = state
        .cluster
        .nodes
        .get(&node_id)
        .and_then(Node::pending_snapshot_transfer);
    if after_pending != before_pending {
        return Err(restart_failure(
            state,
            trace,
            FailureKind::InvariantViolation,
            catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            format!("{node_id} restart changed pending snapshot transfer"),
        ));
    }

    let after_digest = state.cluster.durable_state_digest(node_id);
    check_exact_durable_restart(
        &state.cluster,
        node_id,
        &before_digest,
        &after_digest,
        trace,
    )?;
    mark_restart_observations(state, &before, &after, before_applied_floor);

    Ok(())
}

fn mark_restart_observations(
    state: &mut ExplorationState,
    before: &BootstrapState,
    after: &BootstrapState,
    applied_floor: LogIndex,
) {
    state.mark_observation(Observation::DurableRestartComparisons);
    if applied_floor > LogIndex::ZERO {
        state.mark_observation(Observation::RestartRecoveriesWithNonzeroAppliedFloor);
    }
    if before.voted_for.is_some() && after.current_term == before.current_term {
        state.mark_observation(Observation::SameTermVotedRestarts);
    }
}

fn restart_failure(
    state: &ExplorationState,
    trace: &[Action],
    kind: FailureKind,
    invariant: &'static str,
    message: String,
) -> Failure {
    Failure {
        kind,
        invariant,
        message,
        trace: trace.to_vec(),
        state: summarize(&state.cluster),
    }
}

fn expected_replay_from_bootstrap(
    before: &BootstrapState,
    before_applied_floor: LogIndex,
) -> Vec<(LogIndex, SharedPayload)> {
    before
        .log
        .iter()
        .filter(|entry| entry.index > before_applied_floor && entry.index <= before.commit_index)
        .filter_map(|entry| {
            entry
                .kind
                .application_payload()
                .map(|payload| (entry.index, payload.to_vec().into()))
        })
        .collect()
}
