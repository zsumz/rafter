use rafter::{LogIndex, NodeId, PendingSnapshotTransfer, RaftSnapshot};

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
    check_snapshot_covered_prefix_hidden(&state.state, trace)?;
    check_snapshot_next_retained_index(&state.state, trace)?;
    check_snapshot_persisted_boundary(&state.state, trace)?;
    check_committed_prefixes(state.state.cluster(), trace)?;
    check_snapshot_boundary_monotonicity(&state.state, trace)?;
    check_snapshot_payload_binding(&state.state, trace)?;
    check_snapshot_semantic_history(&state.state, trace)?;
    check_snapshot_transfer_identity(&state.state, trace)?;
    check_snapshot_chunk_identity_history(&state.state, trace)?;
    check_snapshot_chunk_offsets_history(&state.state, trace)?;
    check_snapshot_install_completeness_history(&state.state, trace)?;
    check_pending_snapshot_lifecycle(&state.state, trace)
}

pub(super) fn check_snapshot_semantic_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state.snapshot_history().semantic_violations().iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE,
            message: message.clone(),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}

pub(super) fn check_snapshot_covered_prefix_hidden(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .covered_prefix_violations()
        .iter()
        .next()
    {
        return Err(ss03_failure(state.cluster(), trace, message.clone()));
    }
    check_snapshot_covered_prefixes_in_cluster(state.cluster(), trace)
}

fn check_snapshot_covered_prefixes_in_cluster(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        let snapshot_index = node.snapshot_index();
        if snapshot_index == LogIndex::ZERO {
            continue;
        }
        let retained = node.log_entries_from(snapshot_index.next());
        let from_first_log_index = node.log_entries_from(LogIndex(1));
        let covered_entries_visible = usize::from(from_first_log_index != retained);
        check_snapshot_covered_prefix_shape(
            cluster,
            *node_id,
            snapshot_index,
            covered_entries_visible,
            trace,
        )?;
    }
    Ok(())
}

pub(super) fn check_snapshot_covered_prefix_shape(
    cluster: &Cluster,
    node_id: NodeId,
    snapshot_index: LogIndex,
    covered_entries_visible: usize,
    trace: &[Action],
) -> Result<(), Failure> {
    if covered_entries_visible > 0 {
        return Err(ss03_failure(
            cluster,
            trace,
            format!(
                "{node_id} exposed {covered_entries_visible} retained-log entries covered through snapshot index {snapshot_index}"
            ),
        ));
    }
    Ok(())
}

pub(super) fn check_snapshot_next_retained_index(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .next_retained_index_violations()
        .iter()
        .next()
    {
        return Err(ss03_failure(state.cluster(), trace, message.clone()));
    }
    check_snapshot_next_retained_indices_in_cluster(state.cluster(), trace)
}

fn check_snapshot_next_retained_indices_in_cluster(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        let snapshot_index = node.snapshot_index();
        if snapshot_index == LogIndex::ZERO {
            continue;
        }
        let bootstrap = cluster.bootstrap_state(*node_id);
        let expected_first = snapshot_index.next();
        let first_retained_index = bootstrap
            .log
            .first()
            .map_or(expected_first, |entry| entry.index);
        check_snapshot_next_retained_index_shape(
            cluster,
            *node_id,
            snapshot_index,
            first_retained_index,
            node.last_log_index(),
            bootstrap.log.len(),
            trace,
        )?;
    }
    Ok(())
}

pub(super) fn check_snapshot_next_retained_index_shape(
    cluster: &Cluster,
    node_id: NodeId,
    snapshot_index: LogIndex,
    first_retained_index: LogIndex,
    last_log_index: LogIndex,
    retained_log_len: usize,
    trace: &[Action],
) -> Result<(), Failure> {
    let expected_first = snapshot_index.next();
    if first_retained_index != expected_first {
        return Err(ss03_failure(
            cluster,
            trace,
            format!(
                "{node_id} first retained log index {first_retained_index} does not equal snapshot_index+1 ({expected_first})"
            ),
        ));
    }
    if last_log_index < snapshot_index {
        return Err(ss03_failure(
            cluster,
            trace,
            format!(
                "{node_id} snapshot index {snapshot_index} is beyond local last log index {last_log_index}"
            ),
        ));
    }
    let expected_retained = last_log_index.0 - snapshot_index.0;
    if retained_log_len as u64 != expected_retained {
        return Err(ss03_failure(
            cluster,
            trace,
            format!(
                "{node_id} retained log length {retained_log_len} does not match visible suffix {first_retained_index}..={last_log_index} ({expected_retained} entries)"
            ),
        ));
    }
    Ok(())
}

pub(super) fn check_snapshot_persisted_boundary(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .persisted_boundary_violations()
        .iter()
        .next()
    {
        return Err(ss03_failure(state.cluster(), trace, message.clone()));
    }
    check_snapshot_persisted_boundaries_in_cluster(state.cluster(), trace)
}

fn check_snapshot_persisted_boundaries_in_cluster(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        let snapshot_index = node.snapshot_index();
        if snapshot_index == LogIndex::ZERO {
            continue;
        }
        let persisted_at_or_behind = cluster
            .bootstrap_state(*node_id)
            .log
            .into_iter()
            .find(|entry| entry.index <= snapshot_index)
            .map(|entry| entry.index);
        check_snapshot_persisted_boundary_shape(
            cluster,
            *node_id,
            snapshot_index,
            persisted_at_or_behind,
            trace,
        )?;
    }
    Ok(())
}

pub(super) fn check_snapshot_persisted_boundary_shape(
    cluster: &Cluster,
    node_id: NodeId,
    snapshot_index: LogIndex,
    persisted_at_or_behind: Option<LogIndex>,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(entry_index) = persisted_at_or_behind {
        return Err(ss03_failure(
            cluster,
            trace,
            format!(
                "{node_id} retained persisted entry {entry_index} at or behind snapshot index {snapshot_index}"
            ),
        ));
    }
    Ok(())
}

pub(super) fn check_snapshot_chunk_identity_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .chunk_identity_violations()
        .iter()
        .next()
    {
        return Err(ss04_failure(state.cluster(), trace, message.clone()));
    }

    for (node_id, node) in &state.cluster().nodes {
        let Some(pending) = node.pending_snapshot_transfer() else {
            continue;
        };
        let advertised = RaftSnapshot::new(
            pending.metadata.clone(),
            pending.total_payload_len,
            pending.application_payload_crc32,
        );
        if advertised.transfer_id() != pending.transfer_id {
            return Err(ss04_failure(
                state.cluster(),
                trace,
                format!(
                    "{node_id} pending transfer id {} does not match descriptor identity {}",
                    pending.transfer_id,
                    advertised.transfer_id()
                ),
            ));
        }
        if let Some(staged) = state.cluster().snapshot_staging.get(node_id) {
            let staged_snapshot = RaftSnapshot::new(
                staged.metadata.clone(),
                staged.total_payload_len,
                staged.application_payload_crc32,
            );
            if staged.leader_id != pending.leader_id
                || staged.transfer_id != pending.transfer_id
                || staged_snapshot != advertised
            {
                return Err(ss04_failure(
                    state.cluster(),
                    trace,
                    format!(
                        "{node_id} pending and staged transfer {} use different snapshot identities",
                        pending.transfer_id
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn check_snapshot_chunk_offsets_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .chunk_offset_violations()
        .iter()
        .next()
    {
        return Err(ss04_failure(state.cluster(), trace, message.clone()));
    }

    for (node_id, node) in &state.cluster().nodes {
        let Some(pending) = node.pending_snapshot_transfer() else {
            continue;
        };
        check_snapshot_pending_byte_bounds_shape(
            state.cluster(),
            *node_id,
            pending.received_bytes(),
            pending.total_payload_len,
            trace,
        )?;
        let Some(staged) = state.cluster().snapshot_staging.get(node_id) else {
            if pending.received_bytes() > 0 {
                return Err(ss04_failure(
                    state.cluster(),
                    trace,
                    format!(
                        "{node_id} pending transfer records {} bytes without a staged prefix",
                        pending.received_bytes()
                    ),
                ));
            }
            continue;
        };
        if staged.bytes.len() as u64 != pending.received_bytes() {
            return Err(ss04_failure(
                state.cluster(),
                trace,
                format!(
                    "{node_id} pending transfer records {} bytes but staging contains {}",
                    pending.received_bytes(),
                    staged.bytes.len()
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn check_snapshot_pending_byte_bounds_shape(
    cluster: &Cluster,
    node_id: NodeId,
    received_bytes: u64,
    total_payload_len: u64,
    trace: &[Action],
) -> Result<(), Failure> {
    if received_bytes > total_payload_len {
        return Err(ss04_failure(
            cluster,
            trace,
            format!(
                "{node_id} pending snapshot bytes {received_bytes} exceed total {total_payload_len}"
            ),
        ));
    }
    Ok(())
}

pub(super) fn check_snapshot_install_completeness_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .install_completeness_violations()
        .iter()
        .next()
    {
        return Err(ss04_failure(state.cluster(), trace, message.clone()));
    }
    Ok(())
}

pub(super) fn check_pending_snapshot_lifecycle(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(message) = state
        .snapshot_history()
        .pending_lifecycle_violations()
        .iter()
        .next()
    {
        return Err(ss04_failure(state.cluster(), trace, message.clone()));
    }
    for (node_id, node) in &state.cluster().nodes {
        check_pending_snapshot_lifecycle_shape(
            state.cluster(),
            *node_id,
            node.snapshot_index(),
            node.pending_snapshot_transfer().as_ref(),
            trace,
        )?;
    }
    Ok(())
}

pub(super) fn check_pending_snapshot_lifecycle_shape(
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
        return Err(ss04_failure(
            cluster,
            trace,
            format!("{node_id} retained a complete pending snapshot transfer"),
        ));
    }
    if pending.metadata.last_included_index <= installed_snapshot_index {
        return Err(ss04_failure(
            cluster,
            trace,
            format!(
                "{node_id} retained a stale pending snapshot at {} after installing {}",
                pending.metadata.last_included_index, installed_snapshot_index
            ),
        ));
    }
    Ok(())
}

// Compatibility wrapper retained while registry records move to clause-specific detectors.
#[cfg(test)]
pub(super) fn check_snapshot_transfer_integrity(
    cluster: &Cluster,
    node_id: NodeId,
    installed_snapshot_index: LogIndex,
    pending: Option<&PendingSnapshotTransfer>,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(pending) = pending {
        check_snapshot_pending_byte_bounds_shape(
            cluster,
            node_id,
            pending.received_bytes(),
            pending.total_payload_len,
            trace,
        )?;
    }
    check_pending_snapshot_lifecycle_shape(
        cluster,
        node_id,
        installed_snapshot_index,
        pending,
        trace,
    )
}

pub(super) fn check_snapshot_boundary_monotonicity(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state.snapshot_history().boundary_violations().iter().next() {
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
        .payload_binding_coverage_gaps()
        .iter()
        .next()
    {
        return Err(snapshot_coverage_failure(state, trace, message.clone()));
    }
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
        .transfer_identity_instrumentation_errors()
        .iter()
        .next()
    {
        return Err(snapshot_harness_failure(state, trace, message.clone()));
    }
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

fn snapshot_harness_failure(
    state: &ExplorationState,
    trace: &[Action],
    message: String,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::HarnessError,
        invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    }
}

fn snapshot_coverage_failure(
    state: &ExplorationState,
    trace: &[Action],
    message: String,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::CoverageNotReached,
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
    check_snapshot_covered_prefixes_in_cluster(cluster, trace)?;
    check_snapshot_next_retained_indices_in_cluster(cluster, trace)?;
    check_snapshot_persisted_boundaries_in_cluster(cluster, trace)
}

// Compatibility wrapper retained for the original detector fixture identity.
#[cfg(test)]
pub(super) fn check_snapshot_log_geometry_shape(
    cluster: &Cluster,
    node_id: NodeId,
    snapshot_index: LogIndex,
    first_log_index: LogIndex,
    last_log_index: LogIndex,
    retained_log_len: usize,
    trace: &[Action],
) -> Result<(), Failure> {
    check_snapshot_next_retained_index_shape(
        cluster,
        node_id,
        snapshot_index,
        first_log_index,
        last_log_index,
        retained_log_len,
        trace,
    )
}

fn ss03_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}

fn ss04_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}
