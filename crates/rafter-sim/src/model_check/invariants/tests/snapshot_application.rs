use super::*;

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
fn snapshot_metadata_payload_integrity_detects_expected_metadata_with_different_bytes() {
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
        .cluster
        .seed_snapshot_payload(NodeId(1), &expected.snapshot, corrupted);

    let failure = check_snapshot_metadata_payload_integrity(&state, NodeId(1), &expected, &[])
        .expect_err("expected metadata with different bytes must fail SS-01");
    assert_eq!(
        failure.invariant(),
        catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE
    );
    assert!(
        failure
            .message
            .contains("installed expected metadata with different bytes"),
        "unexpected failure message: {}",
        failure.message
    );
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
        &state.state.cluster,
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
    state.state.cluster.applied.push(Applied {
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
