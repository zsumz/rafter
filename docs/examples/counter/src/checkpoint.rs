//! Preserve the service's membership record across restarts.
use crate::{disk, GROUP};
use rafter::{LogIndex, NodeId};
use rafter_service::{CurrentCommittedState, PeerControlPlaneCheckpoint};
use serde::{Deserialize, Serialize};
use std::{io, path::Path};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    node: u64,
    group: u64,
    high_water: Option<u64>,
    current: Option<(u64, Vec<u64>)>,
    contradicted: Option<u64>,
}

pub fn save(path: &Path, node: u64, checkpoint: PeerControlPlaneCheckpoint<u64>) -> io::Result<()> {
    disk::save(
        path,
        &Record {
            node,
            group: checkpoint.group,
            high_water: checkpoint.committed_id_high_water.map(|id| id.0),
            current: checkpoint.current_committed.map(|state| {
                (
                    state.through.0,
                    state.membership.into_iter().map(|id| id.0).collect(),
                )
            }),
            contradicted: checkpoint.contradicted_at.map(|index| index.0),
        },
    )
}

pub fn load(path: &Path, node: u64) -> io::Result<PeerControlPlaneCheckpoint<u64>> {
    let record: Record = disk::load(path)?;
    if record.node != node || record.group != GROUP {
        return Err(disk::invalid(
            "this checkpoint belongs to another node or group",
        ));
    }
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = record.high_water.map(NodeId);
    checkpoint.current_committed = record.current.map(|(through, members)| {
        CurrentCommittedState::new(LogIndex(through), members.into_iter().map(NodeId).collect())
    });
    checkpoint.contradicted_at = record.contradicted.map(LogIndex);
    Ok(checkpoint)
}
