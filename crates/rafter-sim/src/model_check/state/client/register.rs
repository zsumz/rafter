//! Reference-register reads for the linearizability oracle.
//!
//! A read's visible value is the newest of the applied stream, the recorded
//! snapshot installs, and the currently installed snapshot, all restricted to
//! the same application epoch so a state-loss restart cannot leak old values.

use rafter::{LogIndex, NodeId, SharedPayload};

use crate::Cluster;

pub(in crate::model_check::state) fn register_value_at(
    cluster: &Cluster,
    node_id: NodeId,
    application_epoch: u64,
    read_index: LogIndex,
) -> Option<SharedPayload> {
    let applied = cluster
        .applied()
        .iter()
        .filter(|applied| {
            applied.node_id == node_id
                && applied.application_epoch == application_epoch
                && applied.index <= read_index
        })
        .max_by_key(|applied| applied.index)
        .map(|applied| (applied.index, applied.payload.clone()));
    let installed = cluster
        .snapshot_installs()
        .iter()
        .filter(|snapshot| {
            snapshot.node_id == node_id
                && snapshot.application_epoch == application_epoch
                && snapshot.last_included_index <= read_index
        })
        .max_by_key(|snapshot| snapshot.last_included_index)
        .map(|snapshot| {
            (
                snapshot.last_included_index,
                snapshot.payload.clone().into(),
            )
        });
    let current = (cluster.application_epoch(node_id) == application_epoch)
        .then(|| cluster.node(node_id).snapshot())
        .flatten()
        .filter(|snapshot| snapshot.metadata.last_included_index <= read_index)
        .and_then(|snapshot| {
            cluster.snapshot_payload(node_id, snapshot).map(|payload| {
                (
                    snapshot.metadata.last_included_index,
                    payload.to_vec().into(),
                )
            })
        });
    [applied, installed, current]
        .into_iter()
        .flatten()
        .max_by_key(|(index, _)| *index)
        .map(|(_, value)| value)
}

pub(in crate::model_check::state) fn initial_register_value(
    cluster: &Cluster,
) -> Option<SharedPayload> {
    cluster
        .nodes
        .keys()
        .map(|node_id| {
            let epoch = cluster.application_epoch(*node_id);
            let applied = cluster.local_applied_index(*node_id);
            (
                applied,
                register_value_at(cluster, *node_id, epoch, applied),
            )
        })
        .filter_map(|(index, value)| value.map(|value| (index, value)))
        .max_by_key(|(index, _)| *index)
        .map(|(_, value)| value)
}
