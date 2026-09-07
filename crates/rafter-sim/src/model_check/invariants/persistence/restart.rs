//! Composition and failure shape for the exact-durable-restart obligation.
//!
//! A restart owes every clause detector in a fixed order plus the recovery
//! metadata comparison none of them cover, so the composed check passing means
//! the image round-tripped whole. The clause detectors themselves stay in the
//! parent; this module only orders them and renders their PS-03 failure.

use rafter::{LogIndex, NodeId};

use crate::{Cluster, DurableStateDigest};

use super::{
    catalog, check_restart_acknowledged_entries, check_restart_commit_and_configuration,
    check_restart_log, check_restart_snapshot, check_restart_term_and_vote, summarize, Action,
    Failure,
};

pub(crate) fn check_exact_durable_restart(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    expected_applied_through: LogIndex,
    trace: &[Action],
) -> Result<(), Failure> {
    check_restart_term_and_vote(cluster, node_id, before, after, trace)?;
    check_restart_log(cluster, node_id, before, after, trace)?;
    check_restart_commit_and_configuration(cluster, node_id, before, after, trace)?;
    check_restart_snapshot(cluster, node_id, before, after, trace)?;
    check_restart_acknowledged_entries(cluster, node_id, before, after, trace)?;
    if before.application_epoch != after.application_epoch
        || after.applied_through != expected_applied_through
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

pub(super) fn ps03_failure(
    cluster: &Cluster,
    node_id: NodeId,
    trace: &[Action],
    message: &str,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::PS_03_EXACT_DURABLE_RESTART,
        message: format!("{node_id} {message}"),
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}
