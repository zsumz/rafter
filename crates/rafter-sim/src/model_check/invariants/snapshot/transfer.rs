//! SS-04 transfer shapes the registry does not bind to a clause of its own.
//!
//! A pending transfer owes byte bounds within its advertised total, and the
//! retained wrapper keeps the original fixture identity addressable while the
//! registry moves to clause-specific detectors. The bound clause detectors stay
//! in the parent.

use rafter::NodeId;
#[cfg(test)]
use rafter::{LogIndex, PendingSnapshotTransfer};

use crate::Cluster;

use super::failures::ss04_failure;
use super::{Action, Failure};

pub(in crate::model_check::invariants) fn check_snapshot_pending_byte_bounds_shape(
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

// Compatibility wrapper retained while registry records move to clause-specific detectors.
#[cfg(test)]
pub(in crate::model_check::invariants) fn check_snapshot_transfer_integrity(
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
    super::check_pending_snapshot_lifecycle_shape(
        cluster,
        node_id,
        installed_snapshot_index,
        pending,
        trace,
    )
}
