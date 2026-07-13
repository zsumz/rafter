use rafter::{LogIndex, NodeId, SharedPayload};

use crate::{Applied, Cluster, DurableStateDigest};

use super::{catalog, summarize, Action, Failure};

pub(crate) fn check_exact_durable_restart(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    check_restart_term_and_vote(cluster, node_id, before, after, trace)?;
    check_restart_log(cluster, node_id, before, after, trace)?;
    check_restart_commit_and_configuration(cluster, node_id, before, after, trace)?;
    check_restart_snapshot(cluster, node_id, before, after, trace)?;
    check_restart_acknowledged_entries(cluster, node_id, before, after, trace)?;
    if before.application_epoch != after.application_epoch
        || before.applied_through != after.applied_through
    {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart changed durable application recovery metadata",
        ));
    }
    Ok(())
}

pub(crate) fn check_restart_term_and_vote(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    if before.current_term != after.current_term || before.voted_for != after.voted_for {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart changed durable term or vote",
        ));
    }
    Ok(())
}

pub(crate) fn check_restart_log(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    if before.log != after.log {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart changed the durable retained log",
        ));
    }
    Ok(())
}

pub(crate) fn check_restart_commit_and_configuration(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    if before.commit_index != after.commit_index
        || before.committed_configuration != after.committed_configuration
    {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart changed durable commit or configuration state",
        ));
    }
    Ok(())
}

pub(crate) fn check_restart_snapshot(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    if before.snapshot != after.snapshot {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart changed the durable snapshot",
        ));
    }
    Ok(())
}

pub(crate) fn check_restart_acknowledged_entries(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    let lost_or_changed = before
        .log
        .iter()
        .filter(|entry| entry.index <= before.commit_index)
        .any(|entry| !after.log.iter().any(|recovered| recovered == entry));
    if lost_or_changed {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart lost or reindexed an acknowledged entry",
        ));
    }
    Ok(())
}

fn ps03_failure(cluster: &Cluster, node_id: NodeId, trace: &[Action], message: &str) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::PS_03_EXACT_DURABLE_RESTART,
        message: format!("{node_id} {message}"),
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AppliedFloorRecovery<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) applied_floor: LogIndex,
    pub(crate) commit_index: LogIndex,
    pub(crate) last_log_index: LogIndex,
    pub(crate) expected_replay: &'a [(LogIndex, SharedPayload)],
    pub(crate) recovered_applies: &'a [Applied],
}

pub(crate) fn check_applied_floor_recovery(
    cluster: &Cluster,
    recovery: AppliedFloorRecovery<'_>,
    trace: &[Action],
) -> Result<(), Failure> {
    check_recovery_applied_floor_bounds(cluster, recovery, trace)?;
    check_recovery_applied_floor_exclusion(cluster, recovery, trace)?;
    check_recovery_exact_committed_suffix(cluster, recovery, trace)
}

pub(crate) fn check_recovery_applied_floor_bounds(
    cluster: &Cluster,
    recovery: AppliedFloorRecovery<'_>,
    trace: &[Action],
) -> Result<(), Failure> {
    let AppliedFloorRecovery {
        node_id,
        applied_floor,
        commit_index,
        last_log_index,
        ..
    } = recovery;

    if applied_floor > commit_index {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} durable applied floor {applied_floor} exceeds commit index {commit_index}"
            ),
        ));
    }
    if applied_floor > last_log_index {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} durable applied floor {applied_floor} exceeds local last log index {last_log_index}"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn check_recovery_applied_floor_exclusion(
    cluster: &Cluster,
    recovery: AppliedFloorRecovery<'_>,
    trace: &[Action],
) -> Result<(), Failure> {
    let AppliedFloorRecovery {
        node_id,
        applied_floor,
        recovered_applies,
        ..
    } = recovery;
    let actual_replay = recovered_applies
        .iter()
        .filter(|applied| applied.node_id == node_id)
        .map(|applied| (applied.index, applied.payload.clone()))
        .collect::<Vec<_>>();
    if let Some((index, _)) = actual_replay
        .iter()
        .find(|(index, _)| *index <= applied_floor)
    {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} replayed application entry at {index} at or below durable applied floor {applied_floor}"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn check_recovery_exact_committed_suffix(
    cluster: &Cluster,
    recovery: AppliedFloorRecovery<'_>,
    trace: &[Action],
) -> Result<(), Failure> {
    let AppliedFloorRecovery {
        node_id,
        applied_floor,
        commit_index,
        expected_replay,
        recovered_applies,
        ..
    } = recovery;
    let actual_replay = recovered_applies
        .iter()
        .filter(|applied| applied.node_id == node_id)
        .map(|applied| (applied.index, applied.payload.clone()))
        .collect::<Vec<_>>();
    if let Some((index, _)) = actual_replay
        .iter()
        .find(|(index, _)| *index > commit_index)
    {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} replayed application entry at {index} above commit index {commit_index}"
            ),
        ));
    }
    if actual_replay != expected_replay {
        let actual_indexes = actual_replay
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let expected_indexes = expected_replay
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} replayed application indexes {actual_indexes:?}; expected {expected_indexes:?} above durable applied floor {applied_floor}"
            ),
        ));
    }
    Ok(())
}

fn ps04_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}
