use super::*;
use rafter_invariant_test::oracle_assert_eq;

/// The recovery fixture, with its committed prefix drained to the caller.
///
/// The fixture restarts at commit 16 over a snapshot boundary at 5, so its
/// applied index starts at 5: the entries between are committed but have never
/// been handed to a state machine in this process. Draining them is the
/// precondition for compacting through 14 or 16 at all — an application cannot
/// have built a snapshot at a boundary it was never given the entries for —
/// and the kernel refuses the compaction until it happens. See
/// `runtime_local_compaction_above_the_applied_index_is_refused` below.
fn drained_recovery_fixture() -> (
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>,
    ConfigurationEntry,
    MembershipSet,
) {
    let (mut runtime, stable, new) = super::super::super::dynamic_membership_recovery_fixture();
    assert_eq!(runtime.applied_index(), LogIndex(5));
    let _ = runtime.drain_committed_outputs();
    assert_eq!(runtime.applied_index(), LogIndex(16));
    (runtime, stable, new)
}

/// A boundary the recovered node has not applied through is refused before any
/// write, even though it is committed: the entries in the gap would be skipped
/// forever, and this runtime never emitted them.
#[test]
fn runtime_local_compaction_above_the_applied_index_is_refused() {
    let (mut runtime, _, _) = super::super::super::dynamic_membership_recovery_fixture();
    let before_log = runtime.log_segment.replay_entries();
    let before_snapshot = runtime.snapshot_store.current().cloned();

    oracle_assert_eq!(
        runtime.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot_metadata_for_writer(1, 16, 8, 8),
            application_payload: b"never applied".to_vec(),
        }),
        Err(RaftRuntimeError::SnapshotAheadOfApplied {
            snapshot_index: LogIndex(16),
            applied_index: LogIndex(5),
        })
    );
    oracle_assert_eq!(runtime.log_segment.replay_entries(), before_log);
    oracle_assert_eq!(runtime.snapshot_store.current().cloned(), before_snapshot);

    // Draining is what makes the same compaction legitimate.
    let _ = runtime.drain_committed_outputs();
    runtime
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot_metadata_for_writer(1, 16, 8, 8),
            application_payload: b"applied through sixteen".to_vec(),
        })
        .expect("the same boundary compacts once the entries have been emitted");
    oracle_assert_eq!(runtime.snapshot_index(), LogIndex(16));
}

#[test]
fn runtime_local_compaction_fills_committed_dynamic_membership_metadata() {
    let (mut runtime, _, new_membership) = drained_recovery_fixture();
    let expected = MembershipConfig::stable(new_membership);

    runtime
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot_metadata_for_writer(1, 16, 8, 8),
            application_payload: b"dynamic state through stable config".to_vec(),
        })
        .expect("runtime fills Raft-owned membership metadata before compaction");

    oracle_assert_eq!(
        runtime.snapshot_committed_membership(),
        Some(expected.clone())
    );
    oracle_assert_eq!(runtime.committed_membership(), expected.clone());
    oracle_assert_eq!(runtime.effective_membership(), expected.clone());
    oracle_assert_eq!(runtime.log_segment.replay_entries(), Vec::new());

    let restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[2, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("node restarts from snapshot membership alone");

    oracle_assert_eq!(restarted.committed_membership(), expected.clone());
    oracle_assert_eq!(restarted.effective_membership(), expected);
    oracle_assert_eq!(restarted.log_entries_from(LogIndex(1)), Vec::new());
}

#[test]
fn runtime_local_compaction_rejects_wrong_boundary_term_before_writes() {
    let (mut runtime, _, _) = drained_recovery_fixture();
    let before_log = runtime.log_segment.replay_entries();
    let before_snapshot = runtime.snapshot_store.current().cloned();

    oracle_assert_eq!(
        runtime.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot_metadata_for_writer(1, 16, 7, 8),
            application_payload: b"wrong boundary term".to_vec(),
        }),
        Err(RaftRuntimeError::SnapshotBoundaryTermMismatch {
            snapshot_index: LogIndex(16),
            snapshot_term: Term(7),
            local_term: Some(Term(8)),
        })
    );
    oracle_assert_eq!(runtime.log_segment.replay_entries(), before_log);
    oracle_assert_eq!(runtime.snapshot_store.current().cloned(), before_snapshot);
}

#[test]
fn runtime_local_compaction_rejects_wrong_committed_membership_before_writes() {
    let (mut runtime, _, new_membership) = drained_recovery_fixture();
    let before_log = runtime.log_segment.replay_entries();
    let before_snapshot = runtime.snapshot_store.current().cloned();
    let wrong = MembershipConfig::stable(membership_set(&[1, 2, 3]));
    let expected = MembershipConfig::stable(new_membership);
    let metadata =
        snapshot_metadata_for_writer(1, 16, 8, 8).with_committed_membership(wrong.clone());

    oracle_assert_eq!(
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
    oracle_assert_eq!(runtime.log_segment.replay_entries(), before_log);
    oracle_assert_eq!(runtime.snapshot_store.current().cloned(), before_snapshot);
}

#[test]
fn runtime_local_compaction_rejects_wrong_committed_configuration_identity_before_writes() {
    let (mut runtime, _, new_membership) = drained_recovery_fixture();
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

    oracle_assert_eq!(
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
    oracle_assert_eq!(runtime.log_segment.replay_entries(), before_log);
    oracle_assert_eq!(runtime.snapshot_store.current().cloned(), before_snapshot);
}

#[test]
fn runtime_local_compaction_uses_membership_at_snapshot_boundary() {
    let (mut runtime, _, new_membership) = drained_recovery_fixture();
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
    let (mut runtime, _, new_membership) = drained_recovery_fixture();
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
