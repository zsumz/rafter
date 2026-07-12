//! Shared durable-state and snapshot fixtures for bootstrap scenarios.

pub(super) use super::*;

pub(super) fn config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("test Raft node config is valid")
}

pub(super) fn snapshot_metadata(
    index: u64,
    term: u64,
    hard_state_term: u64,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("metadata").expect("snapshot group id is valid"),
        NodeId(1),
        LogIndex(index),
        Term(term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("metadata_catalog").expect("snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid")
}

pub(super) fn snapshot_descriptor(index: u64, term: u64, hard_state_term: u64) -> RaftSnapshot {
    RaftSnapshot::from_payload(snapshot_metadata(index, term, hard_state_term), b"")
}
