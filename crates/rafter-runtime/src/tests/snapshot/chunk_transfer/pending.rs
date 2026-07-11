use super::*;
use rafter::StagedSnapshotChunk;

#[test]
fn runtime_persists_pending_snapshot_chunk_and_resumes_after_restart() {
    let payload = b"abcdefghi".to_vec();
    let metadata = snapshot_metadata(3, 4, 5);
    let descriptor = RaftSnapshot::from_payload(metadata.clone(), &payload);
    let transfer_id = descriptor.transfer_id();
    let first_chunk = rafter::InstallSnapshotChunk {
        term: Term(5),
        leader_id: RaftNodeId(1),
        transfer_id,
        metadata: metadata.clone(),
        total_payload_len: payload.len() as u64,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset: 0,
        chunk: payload[..4].to_vec(),
        done: false,
    };
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("runtime hydrates");

    let first_outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshotChunk(first_chunk.clone()),
        })
        .expect("first snapshot chunk persists");

    assert!(matches!(
        first_outputs.as_slice(),
        [
            RaftOutput::StageSnapshotChunk { chunk },
            RaftOutput::Send {
                message: Message::InstallSnapshotResponse(response),
                ..
            }
        ] if !chunk.done
            && chunk.offset == 0
            && chunk.bytes.as_slice() == &payload[..4]
            && response.success
            && response.next_offset == 4
    ));
    assert_eq!(
        runtime
            .snapshot_store
            .current_pending_snapshot_transfer()
            .expect("pending transfer persisted")
            .received_len,
        4
    );

    let mut restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("runtime resumes pending transfer");
    let follower_status = restarted
        .snapshot_transfer_status()
        .follower
        .expect("pending transfer is visible after restart");
    assert_eq!(follower_status.received_bytes, 4);

    let final_chunk = rafter::InstallSnapshotChunk {
        term: Term(5),
        leader_id: RaftNodeId(1),
        transfer_id,
        metadata: metadata.clone(),
        total_payload_len: payload.len() as u64,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset: 4,
        chunk: payload[4..].to_vec(),
        done: true,
    };
    let final_outputs = restarted
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshotChunk(final_chunk),
        })
        .expect("final snapshot chunk installs");

    assert!(final_outputs.iter().any(|output| matches!(
        output,
        RaftOutput::ApplySnapshot { snapshot }
            if snapshot.metadata == metadata
                && snapshot.application_payload_len == payload.len() as u64
    )));
    assert_eq!(restarted.snapshot_index(), LogIndex(3));
    let current_snapshot = restarted
        .snapshot_store
        .current()
        .expect("current snapshot persisted");
    assert_eq!(current_snapshot.metadata, metadata);
    assert_eq!(
        current_snapshot.application_payload.as_slice(),
        payload.as_slice()
    );
    assert_eq!(
        restarted.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
}

#[test]
fn runtime_clears_stale_pending_snapshot_transfer_on_restart() {
    let mut snapshot_store = InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
        metadata: snapshot_metadata(5, 5, 5),
        application_payload: b"current".to_vec(),
    });
    let stale_metadata = snapshot_metadata(3, 4, 5);
    snapshot_store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: RaftNodeId(1),
            transfer_id: snapshot_transfer_id(&stale_metadata, 10),
            metadata: stale_metadata,
            total_payload_len: 10,
            application_payload_crc32: 0,
            offset: 0,
            bytes: b"partial".to_vec(),
            done: false,
        })
        .expect("stale transfer stages");

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        snapshot_store,
    )
    .expect("runtime hydrates and clears stale pending transfer");

    assert_eq!(runtime.snapshot_index(), LogIndex(5));
    assert_eq!(
        runtime.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
}
