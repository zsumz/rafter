use super::super::applied::{
    check_applied_commit_bound, check_applied_cursor_monotonicity, check_applied_exactly_once,
    check_execution_history_agreement,
};
use super::super::client::check_client_history_read_write_invariants;
use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[test]
fn application_loss_restart_preserves_immutable_event_history_positions() {
    let mut cluster = two_node_cluster();
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
        commit_index_at_emit: LogIndex(2),
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
    cluster.execution_history.push(execution_witness(
        1,
        0,
        1,
        1,
        LogEntryKind::Application(b"node-one-before-loss".to_vec().into()),
        initial_reference_state(),
    ));
    let before_applied = cluster.applied().to_vec();
    let before_execution_history = cluster.execution_history().to_vec();
    let before_installs = cluster.snapshot_installs().to_vec();

    cluster
        .restart_node_from_bootstrap_losing_application_state(
            NodeId(1),
            cluster.bootstrap_state(NodeId(1)),
        )
        .expect("empty application-loss restart is valid");

    assert_eq!(cluster.applied(), before_applied.as_slice());
    assert_eq!(
        cluster.execution_history(),
        before_execution_history.as_slice()
    );
    assert_eq!(cluster.snapshot_installs(), before_installs.as_slice());
    assert!(
        check_applied_order(&cluster, &[]).is_ok(),
        "unchanged snapshot positions should still describe the immutable event stream"
    );
}

#[test]
fn application_loss_epoch_retains_snapshot_applied_floor() {
    let mut state = ExplorationState::new(one_node_cluster());
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"snapshot floor");
    state.inject_snapshot_payload(NodeId(1), &snapshot, payload);
    let bootstrap = bootstrap_with_snapshot(Term(2), snapshot, &[]);
    state
        .inject_bootstrap_state(NodeId(1), bootstrap)
        .expect("snapshot-backed fixture state is valid");
    restart_node_losing_application_state(&mut state, NodeId(1), &[])
        .expect("snapshot-backed application-loss restart is valid");
    oracle_assert_eq!(state.cluster().application_epoch(NodeId(1)), 1);

    state.inject_applied_record(Applied {
        node_id: NodeId(1),
        application_epoch: 1,
        commit_index_at_emit: LogIndex(2),
        index: LogIndex(1),
        payload: b"below snapshot floor".to_vec().into(),
    });

    let failure = oracle_expect_err!(
        check_applied_cursor_monotonicity(state.cluster(), &[]),
        "new application epoch must retain its surviving snapshot floor",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    oracle_assert!(failure
        .message
        .contains("applied index 1 at or below prior applied/snapshot index 2"));
}

#[test]
fn snapshot_bootstrap_seed_initializes_application_epoch_floor() {
    let mut state = ExplorationState::new(one_node_cluster());
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"seeded snapshot floor");
    let bootstrap = bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]);
    apply_snapshot_bootstrap_seeds(
        &mut state,
        vec![SnapshotBootstrapSeed {
            node_id: NodeId(1),
            snapshot,
            payload,
            bootstrap,
        }],
    )
    .expect("snapshot bootstrap seed is valid");

    state.inject_applied_record(Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(2),
        index: LogIndex(1),
        payload: b"below seeded snapshot floor".to_vec().into(),
    });

    let failure = oracle_expect_err!(
        check_applied_cursor_monotonicity(state.cluster(), &[]),
        "snapshot-backed initial epoch must begin at the seeded boundary",
    );
    oracle_assert!(failure
        .message
        .contains("applied index 1 at or below prior applied/snapshot index 2"));
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
fn applied_order_detects_duplicate_execution_within_one_epoch() {
    let mut cluster = one_node_cluster();
    let mut bootstrap = bootstrap_state(Term(1), &[(1, Term(1), b"applied-once")]);
    bootstrap.commit_index = LogIndex(1);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .expect("committed bootstrap is valid");
    assert_eq!(cluster.execution_history().len(), 1);
    let mut state = ExplorationState::new(cluster);

    rewind_execution_cursor_for_fixture(&mut state, NodeId(1));
    apply_to_state(&mut state, Operation::Tick(NodeId(1)));

    let failure = oracle_expect_err!(
        check_applied_exactly_once(state.cluster(), &[]),
        "same-index execution in one epoch must fail AP-01",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    oracle_assert!(
        failure.message.contains("epoch 0 executed logical index 1"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_exactly_once_includes_configuration_entries() {
    let mut cluster = one_node_cluster();
    let configuration = ConfigurationEntry::stable(
        ConfigurationId(9),
        MembershipSet::new(ids(&[1, 2, 3]), Vec::new()).expect("fixture membership is valid"),
    );
    let witness = execution_witness(
        1,
        0,
        1,
        1,
        LogEntryKind::Configuration(configuration),
        initial_reference_state(),
    );
    cluster.execution_history.push(witness.clone());
    cluster.execution_history.push(witness);

    let failure = check_applied_exactly_once(&cluster, &[])
        .expect_err("duplicate configuration execution must fail AP-01");
    assert!(failure.message.contains("epoch 0 executed logical index 1"));
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

    let failure = oracle_expect_err!(
        check_applied_commit_bound(&cluster, &[]),
        "Apply above the emit-time commit index must fail AP-01",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    oracle_assert!(
        failure
            .message
            .contains("applied index 2 when its commit index at emit was 1"),
        "unexpected failure message: {}",
        failure.message
    );

    for kind in [
        LogEntryKind::Noop,
        LogEntryKind::Configuration(ConfigurationEntry::stable(
            ConfigurationId(10),
            MembershipSet::new(ids(&[1, 2, 3]), Vec::new()).expect("fixture membership is valid"),
        )),
    ] {
        let mut cluster = one_node_cluster();
        let mut witness = execution_witness(1, 0, 2, 1, kind, initial_reference_state());
        witness.commit_index_at_emit = LogIndex(1);
        cluster.execution_history.push(witness);

        let failure = oracle_expect_err!(
            check_applied_commit_bound(&cluster, &[]),
            "non-application execution above the emit-time commit index must fail AP-01",
        );
        oracle_assert!(failure
            .message
            .contains("executed index 2 when its commit index at emit was 1"));
    }

    let mut cluster = one_node_cluster();
    cluster.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        last_included_index: LogIndex(2),
        last_included_term: Term(1),
        committed_membership: None,
        payload: b"uncommitted snapshot".to_vec(),
        applied_records_before_install: 0,
    });
    let failure = oracle_expect_err!(
        check_applied_commit_bound(&cluster, &[]),
        "snapshot installation above the emit-time commit index must fail AP-01",
    );
    oracle_assert!(failure
        .message
        .contains("installed snapshot through 2 when its commit index at emit was 1"));
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
    let state = ExplorationState::new(cluster);
    check_execution_history_agreement(&state, &[])
        .expect("replayed entry must preserve the original command");
}

#[test]
fn full_prefix_application_replay_matches_snapshot_anchored_replay() {
    let mut cluster = one_node_cluster();
    let mut full_prefix = bootstrap_state(
        Term(1),
        &[
            (1, Term(1), b"snapshot-state"),
            (2, Term(1), b"post-snapshot-command"),
        ],
    );
    full_prefix.commit_index = LogIndex(2);
    cluster
        .restart_node_from_bootstrap(NodeId(1), full_prefix)
        .expect("the full committed prefix is valid");

    let (snapshot, snapshot_payload) = test_snapshot(1, 1, 1, 1, b"snapshot-state");
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, snapshot_payload.clone());
    let mut bootstrap =
        bootstrap_with_snapshot(Term(1), snapshot, &[(2, Term(1), b"post-snapshot-command")]);
    bootstrap.commit_index = LogIndex(2);

    cluster
        .restart_node_from_bootstrap_losing_application_state(NodeId(1), bootstrap)
        .expect("snapshot plus committed suffix is valid");

    let witnesses = cluster
        .execution_history()
        .iter()
        .filter(|witness| witness.entry.index == LogIndex(2))
        .collect::<Vec<_>>();
    oracle_assert_eq!(witnesses.len(), 2);
    oracle_assert_eq!(witnesses[0].application_epoch, 0);
    oracle_assert_eq!(witnesses[1].application_epoch, 1);
    oracle_assert_eq!(witnesses[0].prior_state, witnesses[1].prior_state);
    oracle_assert_eq!(witnesses[0].resulting_state, witnesses[1].resulting_state);
    oracle_assert_eq!(
        witnesses[1].prior_state.application_value.as_ref(),
        snapshot_payload.as_slice()
    );
    oracle_assert_eq!(
        witnesses[1].resulting_state.application_value.as_ref(),
        b"post-snapshot-command"
    );

    let state = ExplorationState::new(cluster);
    check_execution_history_agreement(&state, &[])
        .expect("equivalent full-prefix and snapshot replay states must agree");
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
    let operation_id = 0;
    state.record_client_read(&crate::ReadRegistered {
        node_id: NodeId(1),
        operation_id,
        request_id: 7,
        committed_floor: LogIndex(1),
    });
    state.inject_read_grant(crate::ReadGranted {
        node_id: NodeId(1),
        operation_id: Some(operation_id),
        application_epoch: state.cluster().application_epoch(NodeId(1)),
        request_id: 7,
        read_index: LogIndex(1),
        local_applied_index: LogIndex(1),
    });
    state.refresh_client_history();

    let read = state
        .client_history()
        .reads
        .get(&operation_id)
        .expect("registered read is present");
    let ClientReadOutcome::Completed { result, .. } = &read.outcome else {
        panic!("read should complete once the current epoch has applied through the read index");
    };
    assert_eq!(
        result,
        &Some(b"snapshot at one".to_vec().into()),
        "read completion must reconstruct the current epoch snapshot, not an old-epoch apply"
    );
}

#[test]
fn read_grant_from_a_previous_application_epoch_fails_closed() {
    let mut state = ExplorationState::new(one_node_cluster());
    state.record_client_read(&crate::ReadRegistered {
        node_id: NodeId(1),
        operation_id: 0,
        request_id: 7,
        committed_floor: LogIndex::ZERO,
    });
    crate::model_check::state::restart_node_losing_application_state(&mut state, NodeId(1), &[])
        .expect("application-loss transition is valid");
    state.inject_read_grant(crate::ReadGranted {
        node_id: NodeId(1),
        operation_id: Some(0),
        application_epoch: 0,
        request_id: 7,
        read_index: LogIndex::ZERO,
        local_applied_index: LogIndex::ZERO,
    });
    state.refresh_client_history();

    let failure = check_client_history_read_write_invariants(&state, &[])
        .expect_err("a stale-epoch grant cannot complete a current read");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::HarnessError
    );
    assert!(failure.message().contains("retained grant epoch 0"));
}

#[test]
fn replayed_index_must_match_prior_command_across_epochs() {
    let mut state = state_with_committed_application_witness(b"original");
    crate::model_check::state::restart_node_losing_application_state(&mut state, NodeId(1), &[])
        .expect("application-loss transition must replay the committed witness");
    assert_eq!(state.cluster().application_epoch(NodeId(1)), 1);
    crate::model_check::state::record_execution_corruption(
        &mut state,
        crate::model_check::state::ExecutionRecorderCorruption::EntryKind(
            LogEntryKind::Application(b"different".to_vec().into()),
        ),
    )
    .expect("real witness is available to the recorder corruption fixture");

    let failure = oracle_expect_err!(
        check_execution_history_agreement(&state, &[]),
        "different commands at the same log index must still fail",
    );
    oracle_assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    oracle_assert!(
        failure
            .message
            .contains("for application log index 1 with payload"),
        "unexpected failure message: {}",
        failure.message
    );
}
