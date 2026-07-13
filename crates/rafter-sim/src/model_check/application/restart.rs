use rafter::{BootstrapState, LogIndex, Node, NodeId, PendingSnapshotTransfer};

use crate::{DurableStateDigest, ExecutedLogEntry, StagedSnapshotTransfer};

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
    let before_digest = state
        .cluster
        .durable_state_digest(node_id)
        .ok_or_else(|| missing_snapshot_payload_failure(state, node_id, trace))?;
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
    let before_execution_len = state.cluster.execution_history().len();

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

    resume_pending_snapshot_transfer(state, node_id, before_pending.clone(), before_staged, trace)?;

    let recovered_execution = state.cluster.execution_history()[before_execution_len..].to_vec();
    check_applied_floor_recovery(
        &state.cluster,
        AppliedFloorRecovery {
            node_id,
            application_epoch: before_digest.application_epoch,
            applied_floor: before_applied_floor,
            commit_index: before.commit_index,
            last_log_index: before_last_log_index,
            expected_replay: &expected_replay,
            recovered_execution: &recovered_execution,
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

    let expected_applied_floor = expected_replay
        .last()
        .map_or(before_applied_floor, |entry| entry.index);
    check_restart_digest(
        state,
        node_id,
        &before_digest,
        expected_applied_floor,
        trace,
    )?;
    mark_restart_observations(
        state,
        node_id,
        &before,
        &after,
        &before_digest,
        before_applied_floor,
        &expected_replay,
    );

    Ok(())
}

fn resume_pending_snapshot_transfer(
    state: &mut ExplorationState,
    node_id: NodeId,
    pending: Option<PendingSnapshotTransfer>,
    staged: Option<StagedSnapshotTransfer>,
    trace: &[Action],
) -> Result<(), Failure> {
    let Some(pending) = pending else {
        return Ok(());
    };
    let Some(node) = state.cluster.0.nodes.get_mut(&node_id) else {
        return Err(restart_failure(
            state,
            trace,
            FailureKind::HarnessError,
            catalog::PS_03_EXACT_DURABLE_RESTART,
            format!("{node_id} restart lost the node record"),
        ));
    };
    if let Err(error) = node.resume_pending_snapshot_transfer(pending) {
        return Err(restart_failure(
            state,
            trace,
            FailureKind::HarnessError,
            catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            format!("{node_id} failed to resume pending snapshot transfer: {error:?}"),
        ));
    }
    // The kernel record resumes only alongside its durably staged byte prefix.
    if let Some(staged) = staged {
        state.cluster.0.snapshot_staging.insert(node_id, staged);
    }
    Ok(())
}

fn check_restart_digest(
    state: &ExplorationState,
    node_id: NodeId,
    before: &DurableStateDigest,
    expected_applied_floor: LogIndex,
    trace: &[Action],
) -> Result<(), Failure> {
    let after = state
        .cluster
        .durable_state_digest(node_id)
        .ok_or_else(|| missing_snapshot_payload_failure(state, node_id, trace))?;
    check_exact_durable_restart(
        &state.cluster,
        node_id,
        before,
        &after,
        expected_applied_floor,
        trace,
    )
}

fn missing_snapshot_payload_failure(
    state: &ExplorationState,
    node_id: NodeId,
    trace: &[Action],
) -> Failure {
    restart_failure(
        state,
        trace,
        FailureKind::HarnessError,
        catalog::PS_03_EXACT_DURABLE_RESTART,
        format!("{node_id} installed snapshot descriptor has no durable payload bytes"),
    )
}

fn mark_restart_observations(
    state: &mut ExplorationState,
    node_id: NodeId,
    before: &BootstrapState,
    after: &BootstrapState,
    before_digest: &DurableStateDigest,
    applied_floor: LogIndex,
    expected_replay: &[ExecutedLogEntry],
) {
    state.mark_observation(Observation::DurableRestartComparisons);
    state.mark_observation(Observation::RestartTermComparisons);
    state.mark_observation(Observation::RestartTermVoteComparisons);
    if !before_digest.log.is_empty() {
        state.mark_observation(Observation::RestartLogComparisons);
    }
    if before_digest.commit_index > LogIndex::ZERO
        || before_digest.committed_configuration.is_some()
    {
        state.mark_observation(Observation::RestartCommitConfigurationComparisons);
    }
    if before_digest.snapshot.is_some() {
        state.mark_observation(Observation::RestartSnapshotComparisons);
    }
    if before_digest
        .log
        .iter()
        .any(|entry| entry.index <= state.cluster.delivered_ack_floor(node_id))
    {
        state.mark_observation(Observation::RestartAcknowledgedEntryComparisons);
    }
    if applied_floor > LogIndex::ZERO {
        state.mark_observation(Observation::RestartRecoveriesWithNonzeroAppliedFloor);
        state.mark_observation(Observation::RestartAppliedFloorBoundComparisons);
    }
    if !expected_replay.is_empty() {
        state.mark_observation(Observation::RestartNonemptyExpectedReplayComparisons);
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
) -> Vec<ExecutedLogEntry> {
    before
        .log
        .iter()
        .filter(|entry| entry.index > before_applied_floor && entry.index <= before.commit_index)
        .map(|entry| ExecutedLogEntry {
            index: entry.index,
            term: entry.term,
            kind: entry.kind.clone(),
        })
        .collect()
}
