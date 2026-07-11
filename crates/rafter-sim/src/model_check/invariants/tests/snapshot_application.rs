use super::*;

#[test]
fn applied_order_detects_snapshot_rewinding_applied_entries() {
    let mut cluster = one_node_cluster();
    for index in 1..=3 {
        cluster.applied.push(Applied {
            node_id: NodeId(1),
            index: LogIndex(index),
            payload: vec![u8::try_from(index).unwrap_or(u8::MAX)].into(),
        });
    }
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(1),
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
fn application_loss_restart_preserves_immutable_event_history_positions() {
    let mut cluster = one_node_cluster();
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        index: LogIndex(1),
        payload: b"node-one-before-loss".to_vec().into(),
    });
    cluster.applied.push(Applied {
        node_id: NodeId(2),
        index: LogIndex(1),
        payload: b"node-two-before-snapshot".to_vec().into(),
    });
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(2),
        last_included_index: LogIndex(2),
        last_included_term: Term(1),
        committed_membership: None,
        payload: b"node-two-snapshot".to_vec(),
        applied_records_before_install: 2,
    });
    cluster.applied.push(Applied {
        node_id: NodeId(2),
        index: LogIndex(3),
        payload: b"node-two-after-snapshot".to_vec().into(),
    });
    let before_applied = cluster.applied().to_vec();
    let before_installs = cluster.snapshot_installs().to_vec();

    cluster
        .restart_node_from_bootstrap_losing_application_state(
            NodeId(1),
            cluster.bootstrap_state(NodeId(1)),
        )
        .expect("empty application-loss restart is valid");

    assert_eq!(cluster.applied(), before_applied.as_slice());
    assert_eq!(cluster.snapshot_installs(), before_installs.as_slice());
    assert!(
        check_applied_order(&cluster, &[]).is_ok(),
        "unchanged snapshot positions should still describe the immutable event stream"
    );
}

#[test]
fn applied_order_detects_apply_at_or_below_snapshot_boundary() {
    let mut cluster = one_node_cluster();
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(1),
        last_included_index: LogIndex(5),
        last_included_term: Term(1),
        committed_membership: None,
        payload: b"snapshot".to_vec(),
        applied_records_before_install: 0,
    });
    cluster.applied.push(Applied {
        node_id: NodeId(1),
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

    let failure = check_restart_snapshot_safety(&state, &[])
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
