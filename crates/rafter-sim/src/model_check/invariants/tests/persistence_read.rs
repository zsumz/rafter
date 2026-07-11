use super::*;

#[test]
fn exact_durable_restart_detects_digest_change() {
    let cluster = one_node_cluster();
    let before = DurableStateDigest {
        current_term: Term(2),
        voted_for: Some(NodeId(1)),
        commit_index: LogIndex(3),
        committed_configuration: None,
        snapshot: None,
        log: Vec::new(),
        application_epoch: 0,
        applied_through: LogIndex(3),
    };
    let mut after = before.clone();
    after.applied_through = LogIndex(2);

    let failure = check_exact_durable_restart(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("digest mismatch must fail PS-03");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(
        failure
            .message
            .contains("restart changed durable state digest"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_floor_recovery_rejects_replay_at_or_below_floor() {
    let cluster = one_node_cluster();
    let recovered = [Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        index: LogIndex(2),
        payload: b"already-applied".to_vec().into(),
    }];

    let failure = check_applied_floor_recovery(
        &cluster,
        AppliedFloorRecovery {
            node_id: NodeId(1),
            applied_floor: LogIndex(2),
            commit_index: LogIndex(3),
            last_log_index: LogIndex(3),
            expected_replay: &[],
            recovered_applies: &recovered,
        },
        &[],
    )
    .expect_err("replay at durable floor must fail PS-04");
    assert_eq!(failure.invariant(), catalog::PS_04_APPLIED_FLOOR_RECOVERY);
    assert!(
        failure
            .message
            .contains("at or below durable applied floor"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn read_barrier_invariant_detects_grant_below_registration_floor() {
    let mut cluster = one_node_cluster();
    cluster.read_registrations.push(crate::ReadRegistered {
        node_id: NodeId(1),
        request_id: 7,
        committed_floor: LogIndex(5),
    });
    cluster.read_grants.push(crate::ReadGranted {
        node_id: NodeId(1),
        application_epoch: 0,
        request_id: 7,
        read_index: LogIndex(3),
        local_applied_index: LogIndex(3),
    });

    let failure = check_read_barrier_safety(&cluster, &[])
        .expect_err("a grant below the committed floor must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR
    );
    assert!(
        failure.message.contains("below the committed floor 5"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn read_barrier_invariant_detects_unregistered_grant() {
    let mut cluster = one_node_cluster();
    cluster.read_grants.push(crate::ReadGranted {
        node_id: NodeId(1),
        application_epoch: 0,
        request_id: 9,
        read_index: LogIndex(1),
        local_applied_index: LogIndex(1),
    });

    let failure = check_read_barrier_safety(&cluster, &[])
        .expect_err("an unregistered grant must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR
    );
    assert!(failure.message.contains("never registered"));
}
