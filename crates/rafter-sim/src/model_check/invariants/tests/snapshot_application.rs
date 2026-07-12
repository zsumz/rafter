use super::*;
use crate::model_check::{
    observations::Observation, scheduling::Operation, state::apply_to_restart_snapshot_state,
};

#[test]
fn applied_order_detects_snapshot_rewinding_applied_entries() {
    let mut cluster = one_node_cluster();
    for index in 1..=3 {
        cluster.applied.push(Applied {
            node_id: NodeId(1),
            application_epoch: 0,
            commit_index_at_emit: LogIndex(index),
            index: LogIndex(index),
            payload: vec![u8::try_from(index).unwrap_or(u8::MAX)].into(),
        });
    }
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(1),
        application_epoch: 0,
        last_included_index: LogIndex(2),
        last_included_term: Term(1),
        committed_membership: None,
        payload: b"rewind".to_vec(),
        applied_records_before_install: 3,
    });

    let failure = check_applied_order(&cluster, &[]).expect_err("rewind must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    assert!(
        failure.message.contains("installed a snapshot at index 2"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_order_detects_apply_at_or_below_snapshot_boundary() {
    let mut cluster = one_node_cluster();
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(1),
        application_epoch: 0,
        last_included_index: LogIndex(5),
        last_included_term: Term(1),
        committed_membership: None,
        payload: b"snapshot".to_vec(),
        applied_records_before_install: 0,
    });
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(3),
        index: LogIndex(3),
        payload: b"stale".to_vec().into(),
    });

    let failure =
        check_applied_order(&cluster, &[]).expect_err("apply below boundary must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    assert!(
        failure
            .message
            .contains("at or below prior applied/snapshot index 5"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_boundary_monotonicity_detects_regression() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let (older, payload) = test_snapshot(1, 1, 1, 2, b"older snapshot");
    state
        .state
        .inject_snapshot_payload(NodeId(1), &older, payload);
    state
        .state
        .inject_bootstrap_state(NodeId(1), bootstrap_with_snapshot(Term(2), older, &[]))
        .expect("older snapshot bootstrap remains structurally valid");
    state.state.refresh_snapshot_history();

    let failure = check_snapshot_boundary_monotonicity(&state.state, &[])
        .expect_err("snapshot rewind must fail SS-01.a");
    assert_eq!(
        failure.invariant(),
        catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE
    );
    assert!(
        failure.message.contains("snapshot boundary regressed"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_boundary_coverage_requires_explored_installation_not_bootstrap_seed() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    for observation in [
        Observation::SnapshotBoundaryAdvances,
        Observation::SnapshotPayloadBindingsChecked,
        Observation::SnapshotTransferIdentitiesChecked,
    ] {
        assert!(
            !state.state.observation_set().contains(observation),
            "bootstrap seeding must not satisfy explored transition coverage"
        );
    }

    for _ in 0..256 {
        if state
            .state
            .observation_set()
            .contains(Observation::SnapshotTransferIdentitiesChecked)
        {
            break;
        }
        if state.state.cluster().pending().next().is_none() {
            break;
        }
        apply_to_restart_snapshot_state(&mut state, Operation::DeliverReadyAt(0), &[])
            .expect("snapshot delivery remains safe");
    }

    for observation in [
        Observation::SnapshotBoundaryAdvances,
        Observation::SnapshotPayloadBindingsChecked,
        Observation::SnapshotTransferIdentitiesChecked,
    ] {
        assert!(
            state.state.observation_set().contains(observation),
            "a real follower installation must satisfy snapshot coverage"
        );
    }
}

#[test]
fn snapshot_payload_binding_detects_metadata_bound_to_different_bytes() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let mut corrupted = expected.payload.as_ref().to_vec();
    corrupted[0] = corrupted[0].wrapping_add(1);
    state
        .state
        .inject_snapshot_payload(NodeId(1), &expected.snapshot, corrupted);

    let failure = check_snapshot_payload_binding(&state.state, &[])
        .expect_err("metadata bound to different bytes must fail SS-01.c");
    assert_eq!(
        failure.invariant(),
        catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE
    );
    assert!(
        failure
            .message
            .contains("does not bind its metadata to the visible payload bytes"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_transfer_identity_detects_install_different_from_delivery() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let before = state.state.cluster().clone();
    let (different, different_payload) = test_snapshot(3, 2, 1, 2, b"different installed snapshot");
    state
        .state
        .inject_snapshot_payload(NodeId(2), &different, different_payload);
    state
        .state
        .inject_bootstrap_state(NodeId(2), bootstrap_with_snapshot(Term(2), different, &[]))
        .expect("different installed snapshot remains structurally valid");
    let delivered = Envelope {
        from: NodeId(1),
        to: NodeId(2),
        message: Message::InstallSnapshotChunk(rafter::InstallSnapshotChunk {
            term: Term(2),
            leader_id: NodeId(1),
            transfer_id: expected.snapshot.transfer_id(),
            metadata: expected.snapshot.metadata.clone(),
            total_payload_len: expected.snapshot.application_payload_len,
            application_payload_crc32: expected.snapshot.application_payload_crc32,
            offset: 0,
            chunk: expected.payload.as_ref().to_vec(),
            done: true,
        }),
    };
    state
        .state
        .record_snapshot_transition(&before, Some(&delivered));

    let failure = check_snapshot_transfer_identity(&state.state, &[])
        .expect_err("installation differing from delivery must fail SS-01.c");
    assert_eq!(
        failure.invariant(),
        catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE
    );
    assert!(
        failure.message.contains("instead of delivered transfer"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_transfer_identity_uses_retained_send_identity_after_sender_advances() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let (newer, newer_payload) = test_snapshot(1, 3, 2, 2, b"newer sender snapshot");
    state
        .state
        .inject_snapshot_payload(NodeId(1), &newer, newer_payload);
    state
        .state
        .inject_bootstrap_state(NodeId(1), bootstrap_with_snapshot(Term(2), newer, &[]))
        .expect("sender advances while retaining old transfer bytes");
    let before = state.state.cluster().clone();

    state.state.inject_snapshot_payload(
        NodeId(2),
        &expected.snapshot,
        expected.payload.as_ref().to_vec(),
    );
    state
        .state
        .inject_bootstrap_state(
            NodeId(2),
            bootstrap_with_snapshot(Term(2), expected.snapshot.clone(), &[]),
        )
        .expect("delayed older transfer installs on lagging follower");
    let delivered = Envelope {
        from: NodeId(1),
        to: NodeId(2),
        message: Message::InstallSnapshotChunk(rafter::InstallSnapshotChunk {
            term: Term(2),
            leader_id: NodeId(1),
            transfer_id: expected.snapshot.transfer_id(),
            metadata: expected.snapshot.metadata.clone(),
            total_payload_len: expected.snapshot.application_payload_len,
            application_payload_crc32: expected.snapshot.application_payload_crc32,
            offset: 0,
            chunk: expected.payload.as_ref().to_vec(),
            done: true,
        }),
    };
    state
        .state
        .record_snapshot_transition(&before, Some(&delivered));

    check_snapshot_transfer_identity(&state.state, &[])
        .expect("delayed transfer remains identified by immutable transfer data");
    assert!(state
        .state
        .observation_set()
        .contains(Observation::SnapshotTransferIdentitiesChecked));
}

#[test]
fn snapshot_transfer_integrity_rejects_complete_pending_transfer() {
    let state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot");
    let pending = PendingSnapshotTransfer {
        leader_id: NodeId(1),
        transfer_id: expected.snapshot.transfer_id(),
        metadata: expected.snapshot.metadata.clone(),
        total_payload_len: expected.snapshot.application_payload_len,
        application_payload_crc32: expected.snapshot.application_payload_crc32,
        received_len: expected.snapshot.application_payload_len,
    };

    let failure = check_snapshot_transfer_integrity(
        state.state.cluster(),
        NodeId(2),
        LogIndex::ZERO,
        Some(&pending),
        &[],
    )
    .expect_err("complete pending transfer must fail SS-04");
    assert_eq!(
        failure.invariant(),
        catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY
    );
    assert!(
        failure
            .message
            .contains("retained a complete pending snapshot transfer"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn restart_snapshot_safety_rejects_snapshot_bytes_as_log_apply() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    state.state.inject_applied_record(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload: expected.payload.clone(),
    });

    let failure = check_restart_snapshot_safety(&state, &[])
        .expect_err("snapshot bytes emitted as a log command must fail SS-05");
    assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    assert!(
        failure
            .message
            .contains("snapshot bytes were exposed as an applied log entry"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_log_geometry_detects_retained_suffix_length_mismatch() {
    let cluster = one_node_cluster();

    let failure = check_snapshot_log_geometry_shape(
        &cluster,
        NodeId(1),
        LogIndex(2),
        LogIndex(3),
        LogIndex(5),
        2,
        &[],
    )
    .expect_err("retained suffix length mismatch must fail SS-03");
    assert_eq!(
        failure.invariant(),
        catalog::SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY
    );
    assert!(
        failure.message.contains("retained log length 2"),
        "unexpected failure message: {}",
        failure.message
    );
}
