use super::*;

pub(crate) fn persisted_entry(index: u64, term: u64, payload: &[u8]) -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::application(LogIndex(index), Term(term), payload.to_vec())
}

pub(crate) fn snapshot_metadata(
    index: u64,
    term: u64,
    hard_state_term: u64,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("data-group-10").expect("valid group id"),
        RaftNodeId(2),
        LogIndex(index),
        Term(term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("snapshot metadata is valid")
}

pub(crate) fn compacted_leader_snapshot() -> PersistedRaftSnapshot {
    let create_payload = b"create stream payload".to_vec();
    let append_payload = b"append stream payload".to_vec();
    let snapshot = raft_snapshot_for_writer(3, 1, 1, 2, b"opaque application snapshot");
    let mut leader = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[]),
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("leader hydrates");

    assert!(leader
        .step(RaftInput::Tick)
        .expect("leader elects")
        .is_empty());
    assert_committed(&mut leader, create_payload, LogIndex(2), "create commits");
    assert_committed(&mut leader, append_payload, LogIndex(3), "append commits");
    assert_eq!(leader.commit_index(), LogIndex(3));

    leader
        .compact_log_with_snapshot(snapshot.clone())
        .expect("leader has durable snapshot before compacting prefix");
    assert_eq!(leader.snapshot_index(), LogIndex(3));
    assert_eq!(leader.log_segment.replay_entries(), Vec::new());
    leader
        .snapshot_store
        .current()
        .cloned()
        .expect("runtime writes a normalized local snapshot")
}

pub(crate) fn assert_committed(
    leader: &mut DurableRaftNode,
    payload: Vec<u8>,
    index: LogIndex,
    context: &'static str,
) {
    assert!(leader
        .step(RaftInput::ClientProposal { payload })
        .expect(context)
        .iter()
        .any(
            |output| matches!(output, RaftOutput::Apply { index: applied, .. } if *applied == index)
        ));
}

pub(crate) fn raft_snapshot(
    index: u64,
    term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: snapshot_metadata(index, term, hard_state_term),
        application_payload: payload.to_vec(),
    }
}

pub(crate) fn raft_snapshot_for_writer(
    index: u64,
    term: u64,
    hard_state_term: u64,
    writer_id: u64,
    payload: &[u8],
) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("data-group-10").expect("valid group id"),
            RaftNodeId(writer_id),
            LogIndex(index),
            Term(term),
            Term(hard_state_term),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect("snapshot metadata is valid"),
        application_payload: payload.to_vec(),
    }
}
