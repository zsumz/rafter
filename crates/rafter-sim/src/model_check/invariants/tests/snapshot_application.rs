use super::super::snapshot::{
    check_pending_snapshot_lifecycle_shape, check_restart_snapshot_safety,
    check_snapshot_boundary_monotonicity, check_snapshot_chunk_identity_history,
    check_snapshot_chunk_offsets_history, check_snapshot_covered_prefix_shape,
    check_snapshot_install_completeness_history, check_snapshot_next_retained_index_shape,
    check_snapshot_payload_binding, check_snapshot_persisted_boundary_shape,
    check_snapshot_semantic_history, check_snapshot_transfer_identity,
};
use super::*;
use crate::model_check::{
    observations::Observation, scheduling::Operation, state::apply_to_restart_snapshot_state,
};

fn partial_snapshot_transfer_state() -> RestartSnapshotState {
    let mut state = RestartSnapshotState::snapshot_transfer();
    for _ in 0..256 {
        if state
            .state
            .cluster()
            .node(NodeId(2))
            .pending_snapshot_transfer()
            .is_some_and(|pending| pending.received_bytes() > 0)
        {
            return state;
        }
        apply_to_restart_snapshot_state(&mut state, Operation::DeliverReadyAt(0), &[])
            .expect("snapshot transfer setup remains safe");
    }
    panic!("snapshot transfer did not reach a partial staged prefix");
}

fn partial_request(
    pending: &PendingSnapshotTransfer,
    offset: u64,
    crc32: u32,
) -> rafter::InstallSnapshotChunk {
    rafter::InstallSnapshotChunk {
        term: Term(2),
        leader_id: pending.leader_id,
        transfer_id: pending.transfer_id,
        metadata: pending.metadata.clone(),
        total_payload_len: pending.total_payload_len,
        application_payload_crc32: crc32,
        offset,
        chunk: vec![0xA5],
        done: false,
    }
}

fn record_mutated_partial_chunk(request: rafter::InstallSnapshotChunk) -> ExplorationState {
    let transfer = partial_snapshot_transfer_state();
    let before = transfer.state.cluster().clone();
    let mut after = before.clone();
    let mut advanced = after
        .node(NodeId(2))
        .pending_snapshot_transfer()
        .expect("fixture starts with a partial transfer");
    advanced.received_len += request.chunk.len() as u64;
    after
        .snapshot_staging
        .get_mut(&NodeId(2))
        .expect("partial transfer has staged bytes")
        .bytes
        .extend_from_slice(&request.chunk);
    after
        .node_mut(NodeId(2))
        .resume_pending_snapshot_transfer(advanced)
        .expect("mutated post-state remains individually valid");
    let delivered = Envelope {
        from: request.leader_id,
        to: NodeId(2),
        message: Message::InstallSnapshotChunk(request),
    };
    let mut state = ExplorationState::new(after);
    state.record_snapshot_transition(&before, Some(&delivered));
    state
}

fn install_expected_snapshot_on_follower(state: &mut RestartSnapshotState) {
    let expected_index = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .metadata
        .last_included_index;
    for _ in 0..256 {
        if state.state.cluster().node(NodeId(2)).snapshot_index() >= expected_index {
            return;
        }
        apply_to_restart_snapshot_state(state, Operation::DeliverReadyAt(0), &[])
            .expect("snapshot installation remains safe");
    }
    panic!("snapshot transfer did not install on the follower");
}

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

    let failure = super::super::applied::check_applied_cursor_monotonicity(&cluster, &[])
        .expect_err("apply below boundary must be detected");
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
    let mut observed = Cluster::new(three_node_configs());
    observed.seed_snapshot_payload(NodeId(1), &older, payload);
    observed
        .restart_node_from_bootstrap(NodeId(1), bootstrap_with_snapshot(Term(2), older, &[]))
        .expect("older snapshot bootstrap remains structurally valid");
    state.state.observe_snapshot_cluster_for_detector(&observed);

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
fn seeded_snapshot_without_prefix_fails_payload_binding_coverage_closed() {
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"unwitnessed snapshot");
    let mut state = ExplorationState::new(one_node_cluster());
    state.inject_snapshot_payload(NodeId(1), &snapshot, payload);
    state
        .inject_bootstrap_state(NodeId(1), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("snapshot bootstrap is valid");
    state.refresh_snapshot_history();

    let failure = check_snapshot_payload_binding(&state, &[])
        .expect_err("an unwitnessed seeded snapshot cannot pass payload binding");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::CoverageNotReached
    );
    assert!(failure
        .message()
        .contains("no logical-prefix reference witness"));
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
fn snapshot_reference_binding_detects_self_consistent_application_identity_change() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let wrong_payload = b"self-consistent but wrong application state".to_vec();
    let wrong_snapshot =
        rafter::RaftSnapshot::from_payload(expected.snapshot.metadata.clone(), &wrong_payload);
    install_mutated_source_snapshot(&mut state, wrong_snapshot, wrong_payload);

    let failure = check_snapshot_payload_binding(&state.state, &[])
        .expect_err("snapshot bytes differing from the witnessed reference state must fail");
    assert_eq!(
        failure.invariant(),
        catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE
    );
    assert!(failure.message.contains("witnessed reference state"));
}

#[test]
fn snapshot_reference_binding_detects_self_consistent_boundary_term_change() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let mut metadata = expected.snapshot.metadata.clone();
    metadata.last_included_term = Term(2);
    let wrong_snapshot = rafter::RaftSnapshot::from_payload(metadata, &expected.payload);
    install_mutated_source_snapshot(
        &mut state,
        wrong_snapshot,
        expected.payload.as_ref().to_vec(),
    );

    let failure = check_snapshot_payload_binding(&state.state, &[])
        .expect_err("snapshot term differing from the witnessed logical prefix must fail");
    assert!(failure.message.contains("witnessed logical-prefix term"));
}

#[test]
fn snapshot_reference_binding_detects_self_consistent_membership_change() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let metadata = expected
        .snapshot
        .metadata
        .clone()
        .with_committed_membership(stable_membership(&[1, 3], &[]));
    let wrong_snapshot = rafter::RaftSnapshot::from_payload(metadata, &expected.payload);
    install_mutated_source_snapshot(
        &mut state,
        wrong_snapshot,
        expected.payload.as_ref().to_vec(),
    );

    let failure = check_snapshot_payload_binding(&state.state, &[])
        .expect_err("snapshot membership differing from the witnessed reference state must fail");
    assert!(failure.message.contains("committed membership differs"));
}

#[test]
fn snapshot_reference_binding_detects_self_consistent_configuration_identity_change() {
    let mut state = RestartSnapshotState::joint_snapshot_transfer();
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let membership = expected
        .snapshot
        .metadata
        .committed_membership()
        .expect("joint fixture carries membership")
        .clone();
    let metadata = expected
        .snapshot
        .metadata
        .clone()
        .with_committed_configuration(rafter::SnapshotCommittedConfiguration::new(
            Some(CommittedConfiguration {
                index: LogIndex(1),
                config_id: ConfigurationId(8),
            }),
            membership,
        ));
    let wrong_snapshot = rafter::RaftSnapshot::from_payload(metadata, &expected.payload);
    install_mutated_source_snapshot(
        &mut state,
        wrong_snapshot,
        expected.payload.as_ref().to_vec(),
    );

    let failure = check_snapshot_payload_binding(&state.state, &[]).expect_err(
        "snapshot configuration identity differing from the witnessed reference state must fail",
    );
    assert!(failure
        .message
        .contains("committed configuration identity differs"));
}

fn install_mutated_source_snapshot(
    state: &mut RestartSnapshotState,
    snapshot: rafter::RaftSnapshot,
    payload: Vec<u8>,
) {
    state
        .state
        .inject_snapshot_payload(NodeId(1), &snapshot, payload);
    state
        .state
        .inject_bootstrap_state(NodeId(1), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("mutated source snapshot remains structurally valid");
    state.state.refresh_snapshot_history();
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
    let mut committed_prefix = bootstrap_state(
        Term(2),
        &[
            (1, Term(1), b"old prefix"),
            (2, Term(1), expected.payload.as_ref()),
        ],
    );
    committed_prefix.commit_index = LogIndex(2);
    state
        .state
        .inject_bootstrap_state(NodeId(2), committed_prefix)
        .expect("target prefix remains structurally valid");
    state.state.refresh_snapshot_history();
    let before = state.state.cluster().clone();
    state
        .state
        .inject_snapshot_payload(NodeId(2), &expected.snapshot, expected.payload.to_vec());
    state
        .state
        .inject_bootstrap_state(
            NodeId(2),
            bootstrap_with_snapshot(Term(2), expected.snapshot.clone(), &[]),
        )
        .expect("mutated snapshot installation is structurally valid");
    state.state.inject_applied_record(Applied {
        node_id: NodeId(2),
        application_epoch: 0,
        commit_index_at_emit: expected.snapshot.metadata.last_included_index,
        index: expected.snapshot.metadata.last_included_index,
        payload: expected.payload.clone(),
    });
    state.state.record_snapshot_transition(&before, None);

    let failure = check_restart_snapshot_safety(&state, &[])
        .expect_err("a snapshot transition without an ApplySnapshot record must fail SS-05");
    assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    assert!(
        failure
            .message
            .contains("without a matching ApplySnapshot output record"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_semantics_rejects_log_apply_alongside_snapshot_output() {
    let fixture = RestartSnapshotState::snapshot_transfer();
    let expected = fixture
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let before = fixture.state.cluster().clone();
    let mut after = before.clone();
    after.seed_snapshot_payload(
        NodeId(2),
        &expected.snapshot,
        expected.payload.as_ref().to_vec(),
    );
    after
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_with_snapshot(Term(2), expected.snapshot.clone(), &[]),
        )
        .expect("snapshot installation is structurally valid");
    after.snapshot_installs.push(SnapshotInstalled {
        node_id: NodeId(2),
        application_epoch: after.application_epoch(NodeId(2)),
        last_included_index: expected.snapshot.metadata.last_included_index,
        last_included_term: expected.snapshot.metadata.last_included_term,
        committed_membership: expected.snapshot.metadata.committed_membership().cloned(),
        payload: expected.payload.as_ref().to_vec(),
        applied_records_before_install: before.applied().len(),
    });
    after.applied.push(Applied {
        node_id: NodeId(2),
        application_epoch: after.application_epoch(NodeId(2)),
        commit_index_at_emit: expected.snapshot.metadata.last_included_index,
        index: expected.snapshot.metadata.last_included_index,
        payload: expected.payload,
    });
    let mut state = ExplorationState::new(after);
    state.record_snapshot_transition(&before, None);

    let failure = check_snapshot_semantic_history(&state, &[])
        .expect_err("an ApplySnapshot must not also surface as a log Apply");
    assert!(
        failure
            .message
            .contains("emitted log Apply for index 2 while installing snapshot boundary 2"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_semantics_allows_snapshot_bytes_equal_to_a_prior_command() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let payload = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .payload
        .clone();
    state.state.inject_applied_record(Applied {
        node_id: NodeId(2),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        index: LogIndex(1),
        payload,
    });

    install_expected_snapshot_on_follower(&mut state);

    check_snapshot_semantic_history(&state.state, &[])
        .expect("equal bytes do not change an Apply record into an ApplySnapshot record");
}

#[test]
fn snapshot_semantics_rejects_exact_overwritten_entry_resurrection() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    install_expected_snapshot_on_follower(&mut state);
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .clone();
    state
        .state
        .inject_bootstrap_state(
            NodeId(2),
            bootstrap_with_snapshot(Term(2), expected, &[(3, Term(2), b"divergent suffix")]),
        )
        .expect("resurrected suffix is structurally valid in isolation");
    state.state.refresh_snapshot_history();

    let failure = check_restart_snapshot_safety(&state, &[])
        .expect_err("the exact overwritten entry identity must remain absent");
    assert!(
        failure
            .message
            .contains("resurrected overwritten log entry 3 term 2"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn snapshot_semantics_allows_later_entry_to_reuse_overwritten_bytes() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    install_expected_snapshot_on_follower(&mut state);
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .clone();
    state
        .state
        .inject_bootstrap_state(
            NodeId(2),
            bootstrap_with_snapshot(Term(3), expected, &[(3, Term(3), b"divergent suffix")]),
        )
        .expect("later same-byte command is structurally valid");
    state.state.refresh_snapshot_history();

    check_snapshot_semantic_history(&state.state, &[])
        .expect("same bytes at a distinct term are a distinct logical entry");
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

#[test]
fn snapshot_covered_prefix_detector_rejects_visible_covered_entry() {
    let cluster = one_node_cluster();
    let failure = check_snapshot_covered_prefix_shape(&cluster, NodeId(1), LogIndex(2), 1, &[])
        .expect_err("SS-03.a must reject a visible covered entry");
    assert!(failure.message.contains("covered through snapshot index 2"));
}

#[test]
fn snapshot_next_retained_index_detector_rejects_gap() {
    let cluster = one_node_cluster();
    let failure = check_snapshot_next_retained_index_shape(
        &cluster,
        NodeId(1),
        LogIndex(2),
        LogIndex(4),
        LogIndex(4),
        1,
        &[],
    )
    .expect_err("SS-03.b must reject a retained-index gap");
    assert!(failure.message.contains("does not equal snapshot_index+1"));
}

#[test]
fn snapshot_persisted_boundary_detector_rejects_entry_behind_snapshot() {
    let cluster = one_node_cluster();
    let failure = check_snapshot_persisted_boundary_shape(
        &cluster,
        NodeId(1),
        LogIndex(3),
        Some(LogIndex(2)),
        &[],
    )
    .expect_err("SS-03.c must reject persisted data behind the boundary");
    assert!(failure.message.contains("persisted entry 2"));
}

#[test]
fn snapshot_chunk_identity_history_rejects_descriptor_change() {
    let transfer = partial_snapshot_transfer_state();
    let pending = transfer
        .state
        .cluster()
        .node(NodeId(2))
        .pending_snapshot_transfer()
        .expect("fixture starts with a partial transfer");
    let request = partial_request(
        &pending,
        pending.received_bytes(),
        pending.application_payload_crc32.wrapping_add(1),
    );
    let state = record_mutated_partial_chunk(request);
    let failure = check_snapshot_chunk_identity_history(&state, &[])
        .expect_err("SS-04.a must retain and compare chunk descriptors");
    assert!(failure.message.contains("descriptor identity"));
}

#[test]
fn snapshot_chunk_offset_history_rejects_out_of_order_progress() {
    let transfer = partial_snapshot_transfer_state();
    let pending = transfer
        .state
        .cluster()
        .node(NodeId(2))
        .pending_snapshot_transfer()
        .expect("fixture starts with a partial transfer");
    let request = partial_request(
        &pending,
        pending.received_bytes() + 1,
        pending.application_payload_crc32,
    );
    let state = record_mutated_partial_chunk(request);
    let failure = check_snapshot_chunk_offsets_history(&state, &[])
        .expect_err("SS-04.b must compare accepted offset with prior staged length");
    assert!(failure.message.contains("staged prefix ended"));
}

#[test]
fn snapshot_install_completeness_history_rejects_incomplete_install() {
    let transfer = partial_snapshot_transfer_state();
    let expected = transfer
        .expected_snapshot
        .as_ref()
        .expect("fixture has expected snapshot")
        .clone();
    let before = transfer.state.cluster().clone();
    let pending = before
        .node(NodeId(2))
        .pending_snapshot_transfer()
        .expect("fixture starts with partial transfer");
    let remaining = pending.total_payload_len - pending.received_bytes();
    assert!(remaining > 1);
    let request = rafter::InstallSnapshotChunk {
        term: Term(2),
        leader_id: pending.leader_id,
        transfer_id: pending.transfer_id,
        metadata: pending.metadata.clone(),
        total_payload_len: pending.total_payload_len,
        application_payload_crc32: pending.application_payload_crc32,
        offset: pending.received_bytes(),
        chunk: vec![0xA5; usize::try_from(remaining - 1).expect("fixture payload fits usize")],
        done: true,
    };
    let mut after = before.clone();
    after.seed_snapshot_payload(
        NodeId(2),
        &expected.snapshot,
        expected.payload.as_ref().to_vec(),
    );
    after
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_with_snapshot(Term(2), expected.snapshot, &[]),
        )
        .expect("mutated installed post-state is valid in isolation");
    let delivered = Envelope {
        from: request.leader_id,
        to: NodeId(2),
        message: Message::InstallSnapshotChunk(request),
    };
    let mut state = ExplorationState::new(after);
    state.record_snapshot_transition(&before, Some(&delivered));
    let failure = check_snapshot_install_completeness_history(&state, &[])
        .expect_err("SS-04.c must reject installation before all bytes arrive");
    assert!(failure.message.contains("before the complete byte range"));
}

#[test]
fn pending_snapshot_lifecycle_detector_rejects_stale_transfer() {
    let transfer = RestartSnapshotState::snapshot_transfer();
    let expected = transfer
        .expected_snapshot
        .as_ref()
        .expect("expected snapshot");
    let pending = PendingSnapshotTransfer {
        leader_id: NodeId(1),
        transfer_id: expected.snapshot.transfer_id(),
        metadata: expected.snapshot.metadata.clone(),
        total_payload_len: expected.snapshot.application_payload_len,
        application_payload_crc32: expected.snapshot.application_payload_crc32,
        received_len: 1,
    };
    let failure = check_pending_snapshot_lifecycle_shape(
        transfer.state.cluster(),
        NodeId(2),
        expected.snapshot.metadata.last_included_index,
        Some(&pending),
        &[],
    )
    .expect_err("SS-04.d must reject stale pending state");
    assert!(failure.message.contains("stale pending snapshot"));
}
