use super::*;
use rafter::InMemorySnapshotChunkSource;

#[test]
fn compact_log_with_streamed_snapshot_persists_compacts_and_installs_descriptor() {
    let snapshot = raft_snapshot_for_writer(3, 1, 1, 2, b"streamed application snapshot");
    let descriptor =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
    let mut source = InMemorySnapshotChunkSource::new();
    source
        .insert(&descriptor, snapshot.application_payload.clone())
        .expect("source holds the streamed payload");
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
    assert_committed(
        &mut leader,
        b"create stream payload".to_vec(),
        LogIndex(2),
        "create commits",
    );
    assert_committed(
        &mut leader,
        b"append stream payload".to_vec(),
        LogIndex(3),
        "append commits",
    );

    leader
        .compact_log_with_streamed_snapshot(descriptor.clone(), &source)
        .expect("leader compacts through the streamed snapshot");

    assert_eq!(leader.snapshot_index(), LogIndex(3));
    assert_eq!(leader.log_segment.replay_entries(), Vec::new());
    let installed = leader.snapshot().expect("snapshot descriptor installs");
    assert_eq!(
        installed.metadata.committed_membership(),
        Some(&MembershipConfig::stable(membership_set(&[2])))
    );
    assert_eq!(
        leader.snapshot_store.current(),
        Some(&PersistedRaftSnapshot {
            metadata: installed.metadata.clone(),
            application_payload: snapshot.application_payload,
        })
    );
}
