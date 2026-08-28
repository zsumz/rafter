//! The durable-restart and applied-floor detectors the registry pins by name.
//!
//! Each clause the catalog cites is defined here and compares exactly one
//! facet of the image a restart must return, so a registry row always resolves
//! to a detector in this file. The composition order and the failure shapes
//! live in the private submodules; nothing there decides what a clause means.

use rafter::{LogIndex, NodeId};

use crate::{Cluster, DurableStateDigest};

use super::{catalog, summarize, Action, Failure};

mod recovery;
mod restart;

use recovery::ps04_failure;
pub(crate) use recovery::{check_applied_floor_recovery, AppliedFloorRecovery};
pub(crate) use restart::check_exact_durable_restart;
use restart::ps03_failure;

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
    let acknowledged_floor = cluster.delivered_ack_floor(node_id);
    let covered_through = before.log.last().map_or_else(
        || {
            before
                .snapshot
                .as_ref()
                .map_or(LogIndex::ZERO, |snapshot| snapshot.last_included_index)
        },
        |entry| entry.index,
    );
    if acknowledged_floor > covered_through {
        return Err(ps03_failure(
            cluster,
            node_id,
            trace,
            "restart image does not cover every acknowledged entry",
        ));
    }
    let lost_or_changed = before
        .log
        .iter()
        .filter(|entry| entry.index <= acknowledged_floor)
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
        recovered_execution,
        ..
    } = recovery;
    let actual_replay = recovered_execution
        .iter()
        .filter(|witness| witness.node_id == node_id)
        .map(|witness| &witness.entry)
        .collect::<Vec<_>>();
    if let Some(entry) = actual_replay
        .iter()
        .find(|entry| entry.index <= applied_floor)
    {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} replayed logical entry at {} at or below durable applied floor {applied_floor}",
                entry.index
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
        application_epoch,
        applied_floor,
        commit_index,
        expected_replay,
        recovered_execution,
        ..
    } = recovery;
    if let Some(witness) = recovered_execution.iter().find(|witness| {
        witness.node_id != node_id
            || witness.application_epoch != application_epoch
            || witness.commit_index_at_emit < witness.entry.index
            || witness.commit_index_at_emit > commit_index
    }) {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} recovered malformed execution witness for {} at epoch {} with commit floor {}",
                witness.entry.index, witness.application_epoch, witness.commit_index_at_emit
            ),
        ));
    }
    let actual_replay = recovered_execution
        .iter()
        .map(|witness| witness.entry.clone())
        .collect::<Vec<_>>();
    if let Some(entry) = actual_replay
        .iter()
        .find(|entry| entry.index > commit_index)
    {
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} replayed logical entry at {} above commit index {commit_index}",
                entry.index
            ),
        ));
    }
    if actual_replay != expected_replay {
        let actual_indexes = actual_replay
            .iter()
            .map(|entry| (entry.index, entry.term, &entry.kind))
            .collect::<Vec<_>>();
        let expected_indexes = expected_replay
            .iter()
            .map(|entry| (entry.index, entry.term, &entry.kind))
            .collect::<Vec<_>>();
        return Err(ps04_failure(
            cluster,
            trace,
            format!(
                "{node_id} replayed logical entries {actual_indexes:?}; expected {expected_indexes:?} above durable applied floor {applied_floor}"
            ),
        ));
    }
    Ok(())
}
