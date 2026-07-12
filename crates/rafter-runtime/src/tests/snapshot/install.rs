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
fn runtime_ignores_inbound_snapshot_at_or_below_durable_boundary_across_restart() {
    let current = raft_snapshot(5, 3, 5, b"current snapshot");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::with_snapshot(current.clone()),
    )
    .expect("runtime hydrates from the current snapshot");

    for stale in [
        raft_snapshot(5, 3, 5, b"different bytes at the same boundary"),
        raft_snapshot(3, 2, 5, b"older snapshot"),
    ] {
        let outputs = runtime
            .step(RaftInput::Message {
                from: RaftNodeId(1),
                message: Message::InstallSnapshot(rafter::InstallSnapshot {
                    term: Term(5),
                    leader_id: RaftNodeId(1),
                    metadata: stale.metadata,
                    application_payload: stale.application_payload,
                }),
            })
            .expect("stale snapshot is acknowledged without installation");

        assert!(outputs.iter().all(|output| !matches!(
            output,
            RaftOutput::StageSnapshotChunk { .. } | RaftOutput::ApplySnapshot { .. }
        )));
        assert_eq!(runtime.snapshot_index(), LogIndex(5));
        assert_eq!(runtime.snapshot_store.current(), Some(&current));
    }

    let restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("runtime reopens at the same snapshot boundary");
    assert_eq!(restarted.snapshot_index(), LogIndex(5));
    assert_eq!(restarted.snapshot_store.current(), Some(&current));
}

#[test]
fn runtime_rejects_local_snapshot_behind_installed_boundary_before_writes() {
    let current = raft_snapshot(5, 3, 5, b"current snapshot");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::with_snapshot(current.clone()),
    )
    .expect("runtime hydrates from the current snapshot");
    let before_log = runtime.log_segment.clone();
    let before_store = runtime.snapshot_store.clone();

    assert_eq!(
        runtime.compact_log_with_snapshot(raft_snapshot(3, 2, 5, b"older local snapshot")),
        Err(RaftRuntimeError::SnapshotBoundaryTermMismatch {
            snapshot_index: LogIndex(3),
            snapshot_term: Term(2),
            local_term: None,
        })
    );
    assert_eq!(runtime.snapshot_index(), LogIndex(5));
    assert_eq!(runtime.log_segment, before_log);
    assert_eq!(runtime.snapshot_store, before_store);
    assert_eq!(runtime.snapshot_store.current(), Some(&current));
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

#[test]
fn runtime_snapshot_promote_failure_suppresses_apply_and_success_response() {
    let snapshot = raft_snapshot(3, 4, 5, b"snapshot promote fails");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        FailingPromoteSnapshotStore(InMemoryRaftSnapshotStore::new()),
    )
    .expect("runtime hydrates with promote-failing snapshot store");

    let error = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshot(rafter::InstallSnapshot {
                term: Term(5),
                leader_id: RaftNodeId(1),
                metadata: snapshot.metadata.clone(),
                application_payload: snapshot.application_payload.clone(),
            }),
        })
        .expect_err("snapshot promotion fails before outputs escape");
    assert!(matches!(
        error,
        RaftRuntimeError::SnapshotWrite(RaftSnapshotStoreWriteError::Io {
            operation: "promote test staged snapshot",
            ..
        })
    ));

    assert_eq!(runtime.snapshot_store.current_snapshot(), None);
    assert_eq!(
        runtime
            .snapshot_store
            .current_pending_snapshot_transfer()
            .map(|transfer| transfer.received_len),
        Some(snapshot.application_payload.len() as u64)
    );
    assert_eq!(runtime.log_segment.compacted_through(), LogIndex::ZERO);

    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::SnapshotWrite(_))
    });
}

#[test]
fn runtime_snapshot_compaction_failure_suppresses_apply_and_success_response() {
    let log_segment = FailingCompactRaftLogSegment {
        entries: vec![persisted_entry(1, 2, b"local-prefix")],
    };
    let snapshot = raft_snapshot(3, 4, 5, b"snapshot compact fails");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        log_segment,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("runtime hydrates with compaction-failing log segment");

    let error = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshot(rafter::InstallSnapshot {
                term: Term(5),
                leader_id: RaftNodeId(1),
                metadata: snapshot.metadata.clone(),
                application_payload: snapshot.application_payload.clone(),
            }),
        })
        .expect_err("snapshot compaction fails before outputs escape");
    assert!(matches!(
        error,
        RaftRuntimeError::LogCompact(RaftLogSegmentCompactError::Io {
            operation: "compact test raft log entries",
            ..
        })
    ));

    assert_eq!(runtime.snapshot_store.current(), Some(&snapshot));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![persisted_entry(1, 2, b"local-prefix")]
    );
    assert_eq!(runtime.log_segment.compacted_through(), LogIndex::ZERO);

    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::LogCompact(_))
    });
}
