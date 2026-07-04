use super::*;

#[test]
fn runtime_hydrates_snapshot_with_retained_full_log_without_compacting_storage() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ])
        .expect("retained full log persists");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot_metadata(2, 2, 3)),
    )
    .expect("runtime hydrates from snapshot and retained full log");

    assert_eq!(runtime.commit_index(), LogIndex(2));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(3), b"retained-suffix".to_vec())]
    );

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(3),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex(3),
                prev_log_term: Term(3),
                entries: vec![LogEntry::application(Term(3), b"new-suffix".to_vec())],
                leader_commit: LogIndex(2),
            }),
        })
        .expect("retained compacted prefix is ignored during persistence repair");

    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(4)
    ));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
            persisted_entry(4, 3, b"new-suffix"),
        ]
    );
}

#[test]
fn runtime_rejects_retained_boundary_entry_that_disagrees_with_snapshot() {
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 3, b"wrong-boundary-term"),
        ])
        .expect("retained full log persists");

    assert!(matches!(
        DurableRaftNode::with_storage_and_snapshot(
            raft_config(2, &[1, 3]),
            hard_state_store(3, None),
            log_segment,
            Some(snapshot_metadata(2, 2, 3)),
        ),
        Err(RaftRuntimeError::Bootstrap(
            BootstrapValidationError::SnapshotBoundaryTermMismatch {
                index: LogIndex(2),
                snapshot_term: Term(2),
                entry_term: Term(3),
            }
        ))
    ));
}

#[test]
fn runtime_compacts_log_through_committed_snapshot_boundary() {
    let snapshot = snapshot_metadata(2, 2, 3);
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ])
        .expect("full log persists before compaction");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot.clone()),
    )
    .expect("runtime hydrates from durable snapshot");

    runtime
        .compact_log_through_snapshot(&snapshot)
        .expect("committed durable snapshot can compact local log");

    assert_eq!(runtime.commit_index(), LogIndex(2));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![persisted_entry(3, 3, b"retained-suffix")]
    );
    runtime
        .log_segment
        .append_entries(&[persisted_entry(4, 3, b"post-compaction")])
        .expect("post-compaction append uses the retained suffix tail");

    let restarted = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        runtime.log_segment.clone(),
        Some(snapshot),
    )
    .expect("runtime restarts from snapshot plus compacted suffix");

    assert_eq!(restarted.commit_index(), LogIndex(2));
    assert_eq!(restarted.last_log_index(), LogIndex(4));
    assert_eq!(
        restarted.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::application(Term(3), b"retained-suffix".to_vec()),
            LogEntry::application(Term(3), b"post-compaction".to_vec()),
        ]
    );
}

#[test]
fn runtime_local_compaction_fills_committed_dynamic_membership_metadata() {
    let (mut runtime, _, new_membership) = super::super::dynamic_membership_recovery_fixture();
    let expected = MembershipConfig::stable(new_membership);

    runtime
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot_metadata_for_writer(1, 16, 8, 8),
            application_payload: b"dynamic state through stable config".to_vec(),
        })
        .expect("runtime fills Raft-owned membership metadata before compaction");

    assert_eq!(
        runtime.snapshot_committed_membership(),
        Some(expected.clone())
    );
    assert_eq!(runtime.committed_membership(), expected.clone());
    assert_eq!(runtime.effective_membership(), expected.clone());
    assert_eq!(runtime.log_segment.replay_entries(), Vec::new());

    let restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[2, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("node restarts from snapshot membership alone");

    assert_eq!(restarted.committed_membership(), expected.clone());
    assert_eq!(restarted.effective_membership(), expected);
    assert_eq!(restarted.log_entries_from(LogIndex(1)), Vec::new());
}

#[test]
fn runtime_local_compaction_rejects_wrong_committed_membership_before_writes() {
    let (mut runtime, _, new_membership) = super::super::dynamic_membership_recovery_fixture();
    let before_log = runtime.log_segment.replay_entries();
    let before_snapshot = runtime.snapshot_store.current().cloned();
    let wrong = MembershipConfig::stable(membership_set(&[1, 2, 3]));
    let expected = MembershipConfig::stable(new_membership);
    let metadata =
        snapshot_metadata_for_writer(1, 16, 8, 8).with_committed_membership(wrong.clone());

    assert_eq!(
        runtime.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: b"wrong dynamic state".to_vec(),
        }),
        Err(RaftRuntimeError::SnapshotMembershipMismatch {
            snapshot_index: LogIndex(16),
            expected: Box::new(expected),
            actual: Box::new(wrong),
        })
    );
    assert_eq!(runtime.log_segment.replay_entries(), before_log);
    assert_eq!(runtime.snapshot_store.current().cloned(), before_snapshot);
}

#[test]
fn runtime_local_compaction_rejects_wrong_committed_configuration_identity_before_writes() {
    let (mut runtime, _, new_membership) = super::super::dynamic_membership_recovery_fixture();
    let before_log = runtime.log_segment.replay_entries();
    let before_snapshot = runtime.snapshot_store.current().cloned();
    let expected = runtime.committed_configuration_state();
    let actual = Some(CommittedConfiguration {
        index: LogIndex(15),
        config_id: ConfigurationId(10),
    });
    let metadata = snapshot_metadata_for_writer(1, 16, 8, 8).with_committed_configuration(
        rafter::SnapshotCommittedConfiguration::new(
            actual,
            MembershipConfig::stable(new_membership),
        ),
    );

    assert_eq!(
        runtime.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: b"wrong dynamic configuration identity".to_vec(),
        }),
        Err(RaftRuntimeError::SnapshotCommittedConfigurationMismatch {
            snapshot_index: LogIndex(16),
            expected,
            actual,
        })
    );
    assert_eq!(runtime.log_segment.replay_entries(), before_log);
    assert_eq!(runtime.snapshot_store.current().cloned(), before_snapshot);
}

#[test]
fn runtime_local_compaction_uses_membership_at_snapshot_boundary() {
    let (mut runtime, _, new_membership) = super::super::dynamic_membership_recovery_fixture();
    let old_membership = MembershipConfig::stable(membership_set(&[1, 2, 3]));

    runtime
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot_metadata_for_writer(1, 14, 8, 8),
            application_payload: b"state before config entries".to_vec(),
        })
        .expect("snapshot before the config suffix compacts");

    assert_eq!(
        runtime.snapshot_committed_membership(),
        Some(old_membership)
    );
    assert_eq!(
        runtime.committed_membership(),
        MembershipConfig::stable(new_membership)
    );
    let retained = runtime.log_segment.replay_entries();
    assert_eq!(retained.len(), 2);
    assert_eq!(retained[0].index, LogIndex(15));
    assert_eq!(retained[1].index, LogIndex(16));
}

#[test]
fn runtime_streamed_local_compaction_writes_normalized_membership_metadata() {
    let (mut runtime, _, new_membership) = super::super::dynamic_membership_recovery_fixture();
    let payload = b"streamed dynamic state through stable config".to_vec();
    let original_descriptor =
        RaftSnapshot::from_payload(snapshot_metadata_for_writer(1, 16, 8, 8), &payload);
    let mut source = InMemorySnapshotChunkSource::new();
    source
        .insert(&original_descriptor, payload.clone())
        .expect("source is keyed by the caller's original descriptor");
    let expected = MembershipConfig::stable(new_membership);

    runtime
        .compact_log_with_streamed_snapshot(original_descriptor.clone(), &source)
        .expect(
            "streamed compaction reads from the original source and writes normalized metadata",
        );

    let installed = runtime.snapshot().expect("snapshot is installed");
    assert_eq!(installed.metadata.committed_membership(), Some(&expected));
    assert_eq!(
        installed.metadata.committed_configuration_state(),
        runtime.committed_configuration_state()
    );
    assert_ne!(
        installed.transfer_id(),
        original_descriptor.transfer_id(),
        "committed configuration metadata participates in the normalized transfer identity"
    );
    assert_eq!(
        runtime.snapshot_store.current().map(|snapshot| {
            (
                snapshot.metadata.committed_configuration.clone(),
                snapshot.application_payload.clone(),
            )
        }),
        Some((installed.metadata.committed_configuration.clone(), payload))
    );
}

#[test]
fn runtime_rejects_log_compaction_ahead_of_local_commit() {
    let snapshot = snapshot_metadata(2, 2, 3);
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ])
        .expect("full log persists before compaction");
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot),
    )
    .expect("runtime hydrates from durable snapshot");

    assert_eq!(
        runtime.compact_log_through_snapshot(&snapshot_metadata(4, 4, 4)),
        Err(RaftRuntimeError::SnapshotAheadOfCommit {
            snapshot_index: LogIndex(4),
            commit_index: LogIndex(2),
        })
    );
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ]
    );
}

#[test]
fn runtime_compaction_failure_poisons_runtime_until_restart() {
    let snapshot = snapshot_metadata(2, 2, 3);
    let log_segment = FailingCompactRaftLogSegment {
        entries: vec![
            persisted_entry(1, 1, b"compacted-one"),
            persisted_entry(2, 2, b"compacted-two"),
            persisted_entry(3, 3, b"retained-suffix"),
        ],
    };
    let mut runtime = DurableRaftNode::with_storage_and_snapshot(
        raft_config(2, &[1, 3]),
        hard_state_store(3, None),
        log_segment,
        Some(snapshot.clone()),
    )
    .expect("runtime hydrates from durable snapshot");

    assert!(matches!(
        runtime.compact_log_through_snapshot(&snapshot),
        Err(RaftRuntimeError::LogCompact(
            RaftLogSegmentCompactError::Io {
                operation: "compact test raft log entries",
                ..
            }
        ))
    ));
    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::LogCompact(_))
    });
}

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
