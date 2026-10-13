//! Known durable values; no internal hooks or version-specific implementation APIs.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    MembershipConfig, MembershipSet, NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId,
    StagedSnapshotChunk, Term,
};
use rafter_storage::{PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState};

pub(super) const PAYLOAD: &[u8] = b"published application snapshot with a durable resume boundary";
pub(super) const PREFIX: usize = 17;

pub(super) fn hard_state(commit: u64) -> RaftHardState {
    RaftHardState {
        current_term: Term(8),
        voted_for: Some(NodeId(2)),
        commit_index: LogIndex(commit),
        committed_configuration: None,
    }
}

pub(super) fn entries(start: u64, end: u64) -> Vec<PersistedRaftLogEntry> {
    (start..=end)
        .map(|index| {
            PersistedRaftLogEntry::application(
                LogIndex(index),
                Term(if index < 5 { 7 } else { 8 }),
                format!("durable command {index}").into_bytes(),
            )
        })
        .collect()
}

pub(super) fn snapshot(index: u64) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("upgrade-storage").unwrap(),
            NodeId(2),
            LogIndex(index),
            Term(if index < 5 { 7 } else { 8 }),
            Term(8),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("upgrade-fixture").unwrap(),
                ApplicationSnapshotVersion::new(1).unwrap(),
            ),
        )
        .unwrap()
        .with_committed_membership(MembershipConfig::Stable(
            MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)]).unwrap(),
        )),
        application_payload: PAYLOAD.to_vec(),
    }
}

pub(super) fn descriptor(index: u64) -> RaftSnapshot {
    RaftSnapshot::from_payload(snapshot(index).metadata, PAYLOAD)
}

pub(super) fn chunk(start: usize, end: usize) -> StagedSnapshotChunk {
    let descriptor = descriptor(5);
    StagedSnapshotChunk {
        leader_id: NodeId(2),
        transfer_id: descriptor.transfer_id(),
        metadata: descriptor.metadata,
        total_payload_len: PAYLOAD.len() as u64,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset: start as u64,
        bytes: PAYLOAD[start..end].to_vec(),
        done: end == PAYLOAD.len(),
    }
}
