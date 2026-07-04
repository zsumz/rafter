use super::*;

#[test]
fn runtime_persists_installed_snapshot_and_compacts_log_past_local_tail() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[persisted_entry(1, 2, b"local-prefix")])
        .expect("local prefix persists");
    let snapshot = raft_snapshot(3, 4, 5, b"installed snapshot");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        log_segment,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("runtime hydrates without a snapshot");

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshot(rafter::InstallSnapshot {
                term: Term(5),
                leader_id: RaftNodeId(1),
                metadata: snapshot.metadata.clone(),
                application_payload: snapshot.application_payload.clone(),
            }),
        })
        .expect("installed snapshot persists before response escapes");

    assert!(matches!(
        outputs.as_slice(),
        [
            RaftOutput::StageSnapshotChunk { chunk },
            RaftOutput::ApplySnapshot { snapshot: applied },
            RaftOutput::Send {
                message: Message::InstallSnapshotResponse(response),
                ..
            }
        ] if chunk.done
            && chunk.offset == 0
            && chunk.bytes == snapshot.application_payload
            && applied.metadata == snapshot.metadata
            && applied.application_payload_len == snapshot.application_payload.len() as u64
            && response.success
            && response.last_included_index == LogIndex(3)
    ));
    assert_eq!(runtime.snapshot_index(), LogIndex(3));
    assert_eq!(runtime.commit_index(), LogIndex(3));
    assert_eq!(runtime.snapshot_store.current(), Some(&snapshot));
    assert_eq!(runtime.log_segment.replay_entries(), Vec::new());
    assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
}

#[test]
fn runtime_snapshot_write_failure_poisons_runtime_until_restart() {
    let snapshot = raft_snapshot(2, 2, 5, b"snapshot write fails");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        FailingSnapshotStore,
    )
    .expect("runtime hydrates with failing snapshot store");

    assert!(matches!(
        runtime.step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshot(rafter::InstallSnapshot {
                term: Term(5),
                leader_id: RaftNodeId(1),
                metadata: snapshot.metadata,
                application_payload: snapshot.application_payload,
            }),
        }),
        Err(RaftRuntimeError::SnapshotWrite(
            RaftSnapshotStoreWriteError::Io {
                operation: "stage test snapshot chunk",
                ..
            }
        ))
    ));
    // Poisoned accessors may run ahead of durability; nothing was staged
    // or promoted in the store, and the runtime refuses everything until a
    // restart rebuilds from that durable state.
    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::SnapshotWrite(_))
    });
}
