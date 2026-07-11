use super::*;

#[test]
fn applied_agreement_detects_disagreeing_snapshots_at_same_boundary() {
    let mut cluster = one_node_cluster();
    for (node, payload) in [(1, b"state-a".to_vec()), (2, b"state-b".to_vec())] {
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(node),
            application_epoch: 0,
            last_included_index: LogIndex(4),
            last_included_term: Term(1),
            committed_membership: None,
            payload,
            applied_records_before_install: 0,
        });
    }

    let failure = check_applied_payload_agreement(&cluster, &[])
        .expect_err("disagreeing snapshots must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    assert!(
        failure.message.contains("disagreeing snapshots at index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_agreement_detects_snapshot_membership_mismatch_at_same_boundary() {
    let mut cluster = one_node_cluster();
    for (node, membership) in [
        (1, stable_membership(&[1, 2, 3], &[])),
        (2, stable_membership(&[1, 2], &[3])),
    ] {
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(node),
            application_epoch: 0,
            last_included_index: LogIndex(4),
            last_included_term: Term(1),
            committed_membership: Some(membership),
            payload: b"state".to_vec(),
            applied_records_before_install: 0,
        });
    }

    let failure = check_applied_payload_agreement(&cluster, &[])
        .expect_err("same-boundary membership mismatch must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    assert!(
        failure.message.contains("disagreeing snapshots at index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}
