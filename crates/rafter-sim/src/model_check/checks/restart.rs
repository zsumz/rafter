use crate::Cluster;

use super::super::{
    catalog,
    explorers::RestartSafetyExplorer,
    helpers::{summarize, three_node_configs},
    state::{ExplorationState, RestartSnapshotState},
    Bounds, Failure, Summary,
};

/// Exhaustively explores restart and snapshot-transfer schedules while the
/// snapshot carries a committed joint membership.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates restart, snapshot, or
/// committed-prefix safety.
pub fn check_raft_joint_membership_restart_and_snapshot_safety(
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let mut bounds = bounds;
    bounds.depth = if bounds.depth > 12 { bounds.depth } else { 12 };
    let state = RestartSnapshotState::joint_snapshot_transfer();
    let mut explorer = RestartSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    if !explorer.observed_restart {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::PS_03_EXACT_DURABLE_RESTART,
            message: "bounded joint-membership exploration did not reach a restart action"
                .to_string(),
            trace: Vec::new(),
            state: summarize(&state.state.cluster),
        });
    }
    if !explorer.observed_pending_snapshot {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            message:
                "bounded joint-membership exploration did not reach a pending snapshot transfer"
                    .to_string(),
            trace: Vec::new(),
            state: summarize(&state.state.cluster),
        });
    }
    if !explorer.observed_installed_snapshot {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
            message: "bounded joint-membership exploration did not reach an installed snapshot"
                .to_string(),
            trace: Vec::new(),
            state: summarize(&state.state.cluster),
        });
    }
    Ok(explorer.summary())
}

/// Exhaustively explores bounded Raft restart and snapshot-transfer schedules.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates restart or snapshot
/// safety.
pub fn check_raft_restart_and_snapshot_safety(bounds: Bounds) -> Result<Summary, Failure> {
    let mut restart_explorer = RestartSafetyExplorer::new(bounds);
    let mut restart_trace = Vec::new();
    let restart_state =
        RestartSnapshotState::new(ExplorationState::new(Cluster::new(three_node_configs())));
    restart_explorer.explore(&restart_state, &mut restart_trace, 0)?;
    if !restart_explorer.observed_restart {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::PS_03_EXACT_DURABLE_RESTART,
            message: "bounded exploration did not reach a restart action".to_string(),
            trace: Vec::new(),
            state: summarize(&restart_state.state.cluster),
        });
    }

    let mut snapshot_bounds = bounds;
    snapshot_bounds.depth = if bounds.depth > 12 { bounds.depth } else { 12 };
    let mut snapshot_explorer = RestartSafetyExplorer::new(snapshot_bounds);
    let mut snapshot_trace = Vec::new();
    let snapshot_state = RestartSnapshotState::snapshot_transfer();
    snapshot_explorer.explore(&snapshot_state, &mut snapshot_trace, 0)?;
    if !snapshot_explorer.observed_pending_snapshot {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            message: "bounded exploration did not reach a pending snapshot transfer".to_string(),
            trace: Vec::new(),
            state: summarize(&snapshot_state.state.cluster),
        });
    }
    if !snapshot_explorer.observed_installed_snapshot {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
            message: "bounded exploration did not reach an installed snapshot".to_string(),
            trace: Vec::new(),
            state: summarize(&snapshot_state.state.cluster),
        });
    }

    Ok(restart_explorer
        .summary()
        .combined(snapshot_explorer.summary()))
}
