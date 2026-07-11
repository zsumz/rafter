use super::*;

mod errors;
mod local;
mod retained_log;

fn snapshot_metadata_for_writer(
    writer_id: u64,
    index: u64,
    term: u64,
    hard_state_term: u64,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("dynamic-membership").expect("snapshot group id is valid"),
        RaftNodeId(writer_id),
        LogIndex(index),
        Term(term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv").expect("snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid")
}
