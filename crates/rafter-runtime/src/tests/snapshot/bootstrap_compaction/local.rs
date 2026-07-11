use super::*;

#[test]
fn runtime_local_compaction_fills_committed_dynamic_membership_metadata() {
    let (mut runtime, _, new_membership) =
        super::super::super::dynamic_membership_recovery_fixture();
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
    let (mut runtime, _, new_membership) =
        super::super::super::dynamic_membership_recovery_fixture();
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
    let (mut runtime, _, new_membership) =
        super::super::super::dynamic_membership_recovery_fixture();
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
    let (mut runtime, _, new_membership) =
        super::super::super::dynamic_membership_recovery_fixture();
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
    let (mut runtime, _, new_membership) =
        super::super::super::dynamic_membership_recovery_fixture();
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
