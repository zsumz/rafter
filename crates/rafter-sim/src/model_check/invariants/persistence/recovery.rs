//! The applied-floor recovery input and the composition its clauses owe.
//!
//! Every recovery detector reads the same captured floor, commit, and replay
//! witnesses, so gathering them once keeps the clauses comparing one image
//! rather than three. The clause detectors stay in the parent; this module only
//! carries their input and renders their PS-04 failure.

use rafter::{LogIndex, NodeId};

use crate::{Cluster, ExecutedLogEntry, ExecutionWitness};

use super::{
    catalog, check_recovery_applied_floor_bounds, check_recovery_applied_floor_exclusion,
    check_recovery_exact_committed_suffix, summarize, Action, Failure,
};

#[derive(Clone, Copy)]
pub(crate) struct AppliedFloorRecovery<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) application_epoch: u64,
    pub(crate) applied_floor: LogIndex,
    pub(crate) commit_index: LogIndex,
    pub(crate) last_log_index: LogIndex,
    pub(crate) expected_replay: &'a [ExecutedLogEntry],
    pub(crate) recovered_execution: &'a [ExecutionWitness],
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

pub(super) fn ps04_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}
