use rafter::{LogIndex, NodeId, PendingSnapshotTransfer};

use crate::Cluster;

use super::super::state::{snapshot_payload_binding_issue, ExplorationState};
use super::{
    catalog, check_applied_payload_agreement, check_committed_prefixes,
    check_internal_derived_state, summarize, Action, Failure, RestartSnapshotState,
};

pub(crate) fn check_restart_snapshot_safety(
    state: &RestartSnapshotState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_internal_derived_state(state.state.cluster(), trace)?;
    check_applied_payload_agreement(state.state.cluster(), trace)?;
    check_snapshot_log_geometry(state.state.cluster(), trace)?;
    check_committed_prefixes(state.state.cluster(), trace)?;
    check_snapshot_boundary_monotonicity(&state.state, trace)?;
    check_snapshot_payload_binding(&state.state, trace)?;
    check_snapshot_transfer_identity(&state.state, trace)?;

    let Some(expected) = &state.expected_snapshot else {
        return Ok(());
    };

    for applied in &state.state.cluster().applied {
        if applied.payload == expected.payload {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE,
                message: "snapshot bytes were exposed as an applied log entry".to_string(),
                trace: trace.to_vec(),
                state: summarize(state.state.cluster()),
            });
        }
    }

    for (node_id, node) in &state.state.cluster().nodes {
        check_snapshot_transfer_integrity(
            state.state.cluster(),
            *node_id,
            node.snapshot_index(),
            node.pending_snapshot_transfer().as_ref(),
            trace,
        )?;
        if node.snapshot_index() < expected.snapshot.metadata.last_included_index {
            continue;
        }

        let bootstrap = state.state.cluster().bootstrap_state(*node_id);

        for entry in bootstrap.log {
            if state
                .divergent_payloads
                .iter()
                .any(|payload| entry.kind.application_payload() == Some(payload.as_slice()))
            {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE,
                    message: format!(
                        "{node_id} resurrected divergent suffix at log index {}",
                        entry.index
                    ),
                    trace: trace.to_vec(),
                    state: summarize(state.state.cluster()),
                });
            }
        }
    }

    Ok(())
}

pub(super) fn check_snapshot_transfer_integrity(
    cluster: &Cluster,
    node_id: NodeId,
    installed_snapshot_index: LogIndex,
    pending: Option<&PendingSnapshotTransfer>,
    trace: &[Action],
) -> Result<(), Failure> {
    let Some(pending) = pending else {
        return Ok(());
    };

    if pending.is_complete() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            message: format!("{node_id} retained a complete pending snapshot transfer"),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }
    if pending.received_bytes() > pending.total_payload_len {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            message: format!(
                "{node_id} pending snapshot bytes {} exceed total {}",
                pending.received_bytes(),
                pending.total_payload_len
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }
    if pending.metadata.last_included_index <= installed_snapshot_index {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
            message: format!(
                "{node_id} retained a stale pending snapshot at {} after installing {}",
                pending.metadata.last_included_index, installed_snapshot_index
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    Ok(())
}

pub(super) fn check_snapshot_boundary_monotonicity(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state.snapshot_history().violations().iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
            message: format!(
                "{} snapshot boundary regressed from {} to {}",
                violation.node_id, violation.previous_boundary, violation.current_boundary
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}

pub(super) fn check_snapshot_payload_binding(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .payload_binding_violations()
        .iter()
        .next()
    {
        return Err(snapshot_failure(state, trace, message.clone()));
    }
    for node_id in state.cluster().nodes.keys().copied() {
        if let Some(message) = snapshot_payload_binding_issue(state.cluster(), node_id) {
            return Err(snapshot_failure(state, trace, message));
        }
    }
    Ok(())
}

pub(super) fn check_snapshot_transfer_identity(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .transfer_identity_violations()
        .iter()
        .next()
    {
        return Err(snapshot_failure(state, trace, message.clone()));
    }
    Ok(())
}

fn snapshot_failure(state: &ExplorationState, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    }
}

pub(super) fn check_snapshot_log_geometry(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        let snapshot_index = node.snapshot_index();
        let first_log_index = snapshot_index.next();
        let last_log_index = node.last_log_index();
        let retained_log_len = node.log_entries_from(first_log_index).len();
        check_snapshot_log_geometry_shape(
            cluster,
            *node_id,
            snapshot_index,
            first_log_index,
            last_log_index,
            retained_log_len,
            trace,
        )?;
    }

    Ok(())
}

pub(super) fn check_snapshot_log_geometry_shape(
    cluster: &Cluster,
    node_id: NodeId,
    snapshot_index: LogIndex,
    first_log_index: LogIndex,
    last_log_index: LogIndex,
    retained_log_len: usize,
    trace: &[Action],
) -> Result<(), Failure> {
    if last_log_index < snapshot_index {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY,
            message: format!(
                "{node_id} snapshot index {snapshot_index} is beyond local last log index {last_log_index}"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    let expected_first_log_index = snapshot_index.next();
    if first_log_index != expected_first_log_index {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY,
            message: format!(
                "{node_id} first retained log index {first_log_index} does not equal snapshot_index+1 ({expected_first_log_index})"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    let expected_retained = last_log_index.0 - snapshot_index.0;
    let retained_log_len = retained_log_len as u64;
    if retained_log_len != expected_retained {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY,
            message: format!(
                "{node_id} retained log length {retained_log_len} does not match visible suffix {first_log_index}..={last_log_index} ({expected_retained} entries)"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    Ok(())
}
