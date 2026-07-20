use super::*;

pub(crate) fn stale_snapshot_follower() -> DurableRaftNode {
    let mut follower_log = InMemoryRaftLogSegment::new();
    follower_log
        .append_entries(&[persisted_entry(1, 1, b"old prefix")])
        .expect("behind follower has stale prefix");
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(3, &[2]),
        hard_state_store(1, None),
        follower_log,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("behind follower restarts")
}

pub(crate) fn restart_snapshot_follower(follower: &DurableRaftNode) -> DurableRaftNode {
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(3, &[2]),
        follower.hard_state_store.clone(),
        follower.log_segment.clone(),
        follower.snapshot_store.clone(),
    )
    .expect("follower restarts from durable stores")
}

pub(crate) fn install_snapshot_chunk(
    follower: &mut DurableRaftNode,
    snapshot: &PersistedRaftSnapshot,
    transfer_id: rafter::SnapshotTransferId,
    offset: usize,
    end: usize,
) -> Vec<RaftOutput> {
    install_snapshot_chunk_at_term(follower, snapshot, transfer_id, Term(1), offset, end)
}

pub(crate) fn install_snapshot_chunk_at_term<H, L, S>(
    follower: &mut DurableRaftNode<H, L, S>,
    snapshot: &PersistedRaftSnapshot,
    transfer_id: rafter::SnapshotTransferId,
    term: Term,
    offset: usize,
    end: usize,
) -> Vec<RaftOutput>
where
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
{
    follower
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::InstallSnapshotChunk(rafter::InstallSnapshotChunk {
                term,
                leader_id: RaftNodeId(2),
                transfer_id,
                metadata: snapshot.metadata.clone(),
                total_payload_len: snapshot.application_payload.len() as u64,
                application_payload_crc32: rafter_storage::crc32(&snapshot.application_payload),
                offset: offset as u64,
                chunk: snapshot.application_payload[offset..end].to_vec(),
                done: end == snapshot.application_payload.len(),
            }),
        })
        .expect("snapshot chunk persists before response")
}

pub(crate) fn assert_partial_snapshot_transfer(
    follower: &DurableRaftNode,
    outputs: &[RaftOutput],
    received_bytes: u64,
) {
    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, RaftOutput::ApplySnapshot { .. })),
        "follower must not apply or claim caught up after a partial snapshot"
    );
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert_eq!(follower.snapshot_store.current_snapshot(), None);
    assert_eq!(
        follower
            .snapshot_transfer_status()
            .follower
            .expect("partial transfer is visible")
            .received_bytes,
        received_bytes
    );
}

pub(crate) fn applied_snapshot_from(outputs: &[RaftOutput]) -> &RaftSnapshot {
    outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::ApplySnapshot { snapshot } => Some(snapshot),
            _ => None,
        })
        .expect("final chunk applies snapshot")
}

pub(crate) fn snapshot_transfer_id(
    metadata: &RaftSnapshotMetadata,
    total_payload_len: u64,
) -> rafter::SnapshotTransferId {
    RaftSnapshot::new(metadata.clone(), total_payload_len, 0).transfer_id()
}
