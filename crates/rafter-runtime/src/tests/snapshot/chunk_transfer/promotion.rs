use super::*;
use rafter::StagedSnapshotChunk;

/// Stages the whole `payload` for `metadata`'s transfer in two chunks and
/// stops there — the durable shape a crash leaves when the process dies
/// after staging the final chunk but before promoting the staging.
fn complete_staged_transfer_store(
    metadata: &RaftSnapshotMetadata,
    payload: &[u8],
) -> InMemoryRaftSnapshotStore {
    let descriptor = RaftSnapshot::from_payload(metadata.clone(), payload);
    let transfer_id = descriptor.transfer_id();
    let mut snapshot_store = InMemoryRaftSnapshotStore::new();
    snapshot_store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: RaftNodeId(1),
            transfer_id,
            metadata: metadata.clone(),
            total_payload_len: payload.len() as u64,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 0,
            bytes: payload[..4].to_vec(),
            done: false,
        })
        .expect("first chunk stages");
    snapshot_store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: RaftNodeId(1),
            transfer_id,
            metadata: metadata.clone(),
            total_payload_len: payload.len() as u64,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 4,
            bytes: payload[4..].to_vec(),
            done: true,
        })
        .expect("final chunk stages; the crash strikes before promotion");
    snapshot_store
}

#[test]
fn runtime_promotes_complete_staged_transfer_left_by_crash_before_promotion() {
    // Crash window: the final chunk of an inbound transfer was staged
    // durably but the process died before the promote. The kernel refuses
    // to resume complete transfers, so reopen must finish the interrupted
    // installation — promote the staging and compact through its boundary —
    // instead of failing on every boot.
    let payload = b"abcdefghi".to_vec();
    let metadata = snapshot_metadata(3, 4, 5);
    let snapshot_store = complete_staged_transfer_store(&metadata, &payload);

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        snapshot_store,
    )
    .expect("reopen finishes the interrupted installation");

    assert_eq!(runtime.snapshot_index(), LogIndex(3));
    let current = runtime
        .snapshot_store
        .current()
        .expect("promoted snapshot is current");
    assert_eq!(current.metadata, metadata);
    assert_eq!(current.application_payload, payload);
    assert_eq!(
        runtime.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
    assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
}

#[test]
fn runtime_promotes_complete_staged_transfer_and_compacts_covered_log() {
    // The same crash window on a follower that still holds log entries
    // below the transfer boundary: finishing the installation also
    // completes the compaction, so the segment's next appendable index is
    // the kernel's first appendable index.
    let payload = b"abcdefghi".to_vec();
    let metadata = snapshot_metadata(3, 4, 5);
    let snapshot_store = complete_staged_transfer_store(&metadata, &payload);
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
        ])
        .expect("stale prefix persists");

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        log_segment,
        snapshot_store,
    )
    .expect("reopen finishes the interrupted installation and compaction");

    assert_eq!(runtime.snapshot_index(), LogIndex(3));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
    assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
    assert_eq!(runtime.log_segment.replay_entries(), Vec::new());
}

#[test]
fn runtime_restarted_follower_catches_up_from_compacted_leader_snapshot() {
    let snapshot = compacted_leader_snapshot();
    let mut follower = stale_snapshot_follower();
    let transfer_id =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload)
            .transfer_id();
    let split_at = 4;

    let first_outputs = install_snapshot_chunk(&mut follower, &snapshot, transfer_id, 0, split_at);
    assert_partial_snapshot_transfer(&follower, &first_outputs, split_at as u64);

    let mut restarted_follower = restart_snapshot_follower(&follower);
    let final_outputs = install_snapshot_chunk(
        &mut restarted_follower,
        &snapshot,
        transfer_id,
        split_at,
        snapshot.application_payload.len(),
    );
    let applied_snapshot = applied_snapshot_from(&final_outputs);

    assert_eq!(
        applied_snapshot,
        &RaftSnapshot::new(
            snapshot.metadata.clone(),
            snapshot.application_payload.len() as u64,
            rafter_storage::crc32(&snapshot.application_payload),
        )
    );
    assert_eq!(restarted_follower.snapshot_index(), LogIndex(3));
    assert_eq!(restarted_follower.commit_index(), LogIndex(3));
    assert_eq!(restarted_follower.snapshot_store.current(), Some(&snapshot));

    let hydrated_follower = restart_snapshot_follower(&restarted_follower);
    assert_eq!(hydrated_follower.snapshot_index(), LogIndex(3));
    assert_eq!(
        hydrated_follower
            .snapshot_store
            .current()
            .expect("promoted snapshot survives restart")
            .application_payload
            .as_slice(),
        b"opaque application snapshot".as_slice()
    );
}
