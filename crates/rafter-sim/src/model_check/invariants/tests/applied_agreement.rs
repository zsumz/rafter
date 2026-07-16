use super::super::applied::{check_applied_payload_agreement, check_execution_history_agreement};
use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[test]
fn real_transition_persists_recorder_failure_as_a_harness_error() {
    let mut state = ExplorationState::new(one_node_cluster());
    state.remove_execution_cursor(NodeId(1));

    crate::model_check::state::apply_to_state(
        &mut state,
        crate::model_check::scheduling::Operation::Tick(NodeId(1)),
    );
    let cloned = state.clone();

    for state in [&state, &cloned] {
        let failure = check_commit_safety(state, &[])
            .expect_err("a recorder failure must make commit safety red");
        assert_eq!(
            failure.kind(),
            crate::model_check::FailureKind::HarnessError
        );
        assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
        assert!(
            failure.message.contains("no execution-history cursor"),
            "unexpected failure message: {}",
            failure.message
        );
    }
}

#[rafter_invariant_test::detector_test]
fn execution_agreement_detects_mismatched_configuration_application() {
    let original = ConfigurationEntry::stable(
        ConfigurationId(7),
        MembershipSet::new(ids(&[1, 2]), Vec::new()).expect("fixture membership is valid"),
    );
    let mut state = state_with_committed_configuration_witness(original);
    let different = ConfigurationEntry::stable(
        ConfigurationId(8),
        MembershipSet::new(ids(&[1, 2, 3]), Vec::new()).expect("fixture membership is valid"),
    );
    crate::model_check::state::record_execution_corruption(
        &mut state,
        crate::model_check::state::ExecutionRecorderCorruption::EntryKind(
            LogEntryKind::Configuration(different),
        ),
    )
    .expect("real configuration witness is available to corrupt");

    let failure = oracle_expect_err!(
        check_execution_history_agreement(&state, &[]),
        "different configurations at one index must fail AP-02",
    );
    oracle_assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    oracle_assert!(
        failure
            .message
            .contains("different term/kind/input identities at log index 1"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[rafter_invariant_test::detector_test]
fn execution_agreement_detects_mismatched_reference_result() {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("fixture node config is valid");
    let bootstrap = BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(1),
        committed_configuration: None,
        snapshot: None,
        log: vec![BootstrapLogEntry::application(
            LogIndex(1),
            Term(1),
            b"expected-command".to_vec(),
        )],
    };
    let node = Node::from_bootstrap_applied_through(config.clone(), bootstrap, LogIndex(1))
        .expect("fixture node bootstrap is valid");
    let mut cluster = Cluster::new(vec![config]);
    cluster.nodes.insert(NodeId(1), node);
    cluster.record_outputs(
        NodeId(1),
        vec![Output::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: b"wrong-command".to_vec().into(),
            local_proposal_id: None,
        }],
    );
    let state = ExplorationState::new(cluster);

    let failure = oracle_expect_err!(
        check_execution_history_agreement(&state, &[]),
        "an Apply payload that disagrees with the committed log must fail AP-02",
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

#[rafter_invariant_test::detector_test]
fn execution_agreement_detects_broken_configuration_result() {
    let configuration = ConfigurationEntry::stable(
        ConfigurationId(7),
        MembershipSet::new(ids(&[1, 2]), Vec::new()).expect("fixture membership is valid"),
    );
    let mut state = state_with_committed_configuration_witness(configuration);
    let mut broken = state
        .cluster()
        .execution_history()
        .last()
        .expect("real configuration witness is recorded")
        .resulting_state
        .clone();
    broken.committed_membership = stable_membership(&[1, 3], &[]);
    crate::model_check::state::record_execution_corruption(
        &mut state,
        crate::model_check::state::ExecutionRecorderCorruption::ResultingState(broken),
    )
    .expect("real configuration witness is available to corrupt");

    let failure = oracle_expect_err!(
        check_execution_history_agreement(&state, &[]),
        "a broken configuration result must fail AP-02",
    );
    oracle_assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    oracle_assert!(
        failure
            .message
            .contains("invalid reference-state result at log index 1"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_agreement_detects_disagreeing_snapshots_at_same_boundary() {
    let mut cluster = one_node_cluster();
    for (node, payload) in [(1, b"state-a".to_vec()), (2, b"state-b".to_vec())] {
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(node),
            application_epoch: 0,
            commit_index_at_emit: LogIndex(4),
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

#[rafter_invariant_test::detector_test]
fn applied_agreement_detects_snapshot_membership_mismatch_at_same_boundary() {
    let mut cluster = one_node_cluster();
    for (node, membership) in [
        (1, stable_membership(&[1, 2, 3], &[])),
        (2, stable_membership(&[1, 2], &[3])),
    ] {
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(node),
            application_epoch: 0,
            commit_index_at_emit: LogIndex(4),
            last_included_index: LogIndex(4),
            last_included_term: Term(1),
            committed_membership: Some(membership),
            payload: b"state".to_vec(),
            applied_records_before_install: 0,
        });
    }

    let failure = oracle_expect_err!(
        check_applied_payload_agreement(&cluster, &[]),
        "same-boundary membership mismatch must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    oracle_assert!(
        failure.message.contains("disagreeing snapshots at index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}
