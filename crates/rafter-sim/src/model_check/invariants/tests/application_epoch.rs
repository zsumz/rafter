use super::*;

#[test]
fn application_loss_restart_preserves_immutable_event_history_positions() {
    let mut cluster = one_node_cluster();
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload: b"node-one-before-loss".to_vec().into(),
    });
    cluster.applied.push(Applied {
        node_id: NodeId(2),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload: b"node-two-before-snapshot".to_vec().into(),
    });
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(2),
        application_epoch: 0,
        last_included_index: LogIndex(2),
        last_included_term: Term(1),
        committed_membership: None,
        payload: b"node-two-snapshot".to_vec(),
        applied_records_before_install: 2,
    });
    cluster.applied.push(Applied {
        node_id: NodeId(2),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(3),
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
fn ordinary_restart_preserves_application_epoch() {
    let mut cluster = one_node_cluster();
    cluster
        .restart_node_from_bootstrap_losing_application_state(
            NodeId(1),
            cluster.bootstrap_state(NodeId(1)),
        )
        .expect("application-loss restart is valid");
    assert_eq!(cluster.application_epoch(NodeId(1)), 1);

    cluster
        .restart_node_from_bootstrap(NodeId(1), cluster.bootstrap_state(NodeId(1)))
        .expect("ordinary restart is valid");

    assert_eq!(
        cluster.application_epoch(NodeId(1)),
        1,
        "ordinary process restart must preserve the application epoch"
    );
}

#[test]
fn applied_order_detects_duplicate_apply_within_one_epoch() {
    let mut cluster = one_node_cluster();
    for payload in [b"first".as_slice(), b"duplicate".as_slice()] {
        cluster.applied.push(Applied {
            node_id: NodeId(1),
            application_epoch: 0,
            commit_index_at_emit: LogIndex(1),
            index: LogIndex(1),
            payload: payload.to_vec().into(),
        });
    }

    let failure = check_applied_order(&cluster, &[])
        .expect_err("same-index apply in one epoch must fail AP-01");
    assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    assert!(
        failure.message.contains("epoch 0 applied index 1"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_order_detects_apply_before_commit() {
    let mut cluster = one_node_cluster();
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(2),
        payload: b"uncommitted".to_vec().into(),
    });

    let failure = check_applied_order(&cluster, &[])
        .expect_err("Apply above the emit-time commit index must fail AP-01");
    assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    assert!(
        failure
            .message
            .contains("applied index 2 when its commit index at emit was 1"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn application_loss_replays_committed_suffix_in_new_epoch() {
    let mut cluster = one_node_cluster();
    let mut bootstrap = bootstrap_state(Term(1), &[(1, Term(1), b"replayed")]);
    bootstrap.commit_index = LogIndex(1);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap.clone())
        .expect("initial committed bootstrap is valid");
    assert_eq!(cluster.application_epoch(NodeId(1)), 0);
    assert_eq!(cluster.applied().len(), 1);

    cluster
        .restart_node_from_bootstrap_losing_application_state(NodeId(1), bootstrap)
        .expect("application-loss restart is valid");

    assert_eq!(cluster.application_epoch(NodeId(1)), 1);
    assert_eq!(
        cluster.applied(),
        &[
            Applied {
                node_id: NodeId(1),
                application_epoch: 0,
                commit_index_at_emit: LogIndex(1),
                index: LogIndex(1),
                payload: b"replayed".to_vec().into(),
            },
            Applied {
                node_id: NodeId(1),
                application_epoch: 1,
                commit_index_at_emit: LogIndex(1),
                index: LogIndex(1),
                payload: b"replayed".to_vec().into(),
            },
        ]
    );
    check_applied_order(&cluster, &[]).expect("same-index replay is legal in a new epoch");
    check_applied_payload_agreement(&cluster, &[])
        .expect("replayed entry must preserve the original command");
}

#[test]
fn read_reconstruction_ignores_values_from_previous_application_epoch() {
    let mut cluster = one_node_cluster();
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload: b"old-epoch-value".to_vec().into(),
    });
    let (snapshot, payload) = test_snapshot(1, 1, 1, 1, b"snapshot at one");
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload);
    cluster
        .restart_node_from_bootstrap_losing_application_state(
            NodeId(1),
            bootstrap_with_snapshot(Term(1), snapshot, &[]),
        )
        .expect("application-loss restart with snapshot is valid");
    assert_eq!(cluster.application_epoch(NodeId(1)), 1);

    let mut state = ExplorationState::new(cluster);
    state.record_client_read(NodeId(1), 7, LogIndex(1));
    state.inject_read_grant(crate::ReadGranted {
        node_id: NodeId(1),
        application_epoch: state.cluster().application_epoch(NodeId(1)),
        request_id: 7,
        read_index: LogIndex(1),
        local_applied_index: LogIndex(1),
    });
    state.refresh_client_history();

    let read = state
        .client_history()
        .reads
        .get(&7)
        .expect("registered read is present");
    let ClientReadOutcome::Completed { result, .. } = &read.outcome else {
        panic!("read should complete once the current epoch has applied through the read index");
    };
    assert_eq!(
        result, &None,
        "read completion must not reconstruct a value solely from the old epoch"
    );
}

#[test]
fn replayed_index_must_match_prior_command_across_epochs() {
    let mut cluster = one_node_cluster();
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload: b"original".to_vec().into(),
    });
    cluster.applied.push(Applied {
        node_id: NodeId(1),
        application_epoch: 1,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload: b"different".to_vec().into(),
    });

    check_applied_order(&cluster, &[]).expect("AP-01 is scoped per application epoch");
    let failure = check_applied_payload_agreement(&cluster, &[])
        .expect_err("different commands at the same log index must still fail");
    assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    assert!(
        failure
            .message
            .contains("different payloads applied at log index 1"),
        "unexpected failure message: {}",
        failure.message
    );
}
