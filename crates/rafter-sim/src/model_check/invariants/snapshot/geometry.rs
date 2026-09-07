//! The whole-cluster SS-03 geometry sweep, composed from the parent's checks.
//!
//! The commit suite owes snapshot/log index geometry for every node, which is
//! exactly the three per-clause sweeps run in order; ordering them here keeps
//! the caller from picking a subset. The clause detectors and their per-node
//! shapes stay in the parent.

#[cfg(test)]
use rafter::{LogIndex, NodeId};

use crate::Cluster;

use super::{Action, Failure};

pub(in crate::model_check::invariants) fn check_snapshot_log_geometry(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    super::check_snapshot_covered_prefixes_in_cluster(cluster, trace)?;
    super::check_snapshot_next_retained_indices_in_cluster(cluster, trace)?;
    super::check_snapshot_persisted_boundaries_in_cluster(cluster, trace)
}

// Compatibility wrapper retained for the original detector fixture identity.
#[cfg(test)]
pub(in crate::model_check::invariants) fn check_snapshot_log_geometry_shape(
    cluster: &Cluster,
    node_id: NodeId,
    snapshot_index: LogIndex,
    first_log_index: LogIndex,
    last_log_index: LogIndex,
    retained_log_len: usize,
    trace: &[Action],
) -> Result<(), Failure> {
    super::check_snapshot_next_retained_index_shape(
        cluster,
        node_id,
        snapshot_index,
        first_log_index,
        last_log_index,
        retained_log_len,
        trace,
    )
}
