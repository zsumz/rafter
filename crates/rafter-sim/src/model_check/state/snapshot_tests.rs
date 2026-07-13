use super::*;
use crate::model_check::{
    helpers::{bootstrap_with_snapshot, test_snapshot},
    state::RestartSnapshotState,
};

fn final_chunk(from: NodeId, to: NodeId, snapshot: &RaftSnapshot, payload: &[u8]) -> Envelope {
    Envelope {
        from,
        to,
        message: Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: snapshot.metadata.hard_state_term,
            leader_id: from,
            transfer_id: snapshot.transfer_id(),
            metadata: snapshot.metadata.clone(),
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
            offset: 0,
            chunk: payload.to_vec(),
            done: true,
        }),
    }
}

fn record_installation(
    state: &mut RestartSnapshotState,
    from: NodeId,
    to: NodeId,
    snapshot: &RaftSnapshot,
    payload: &[u8],
) -> ObservationSet {
    let before = state.state.cluster().clone();
    state
        .state
        .inject_snapshot_payload(to, snapshot, payload.to_vec());
    state
        .state
        .inject_bootstrap_state(
            to,
            bootstrap_with_snapshot(snapshot.metadata.hard_state_term, snapshot.clone(), &[]),
        )
        .expect("detector fixture snapshot bootstrap is valid");
    let delivered = final_chunk(from, to, snapshot, payload);
    let after = state.state.cluster().clone();
    let observations =
        state
            .state
            .snapshot_history
            .record_transition(&before, &after, Some(&delivered));
    // Bootstrap injection bypasses the runtime ApplySnapshot output path.
    state.state.snapshot_history.semantic_violations.clear();
    observations
}

fn record_verified_installation(state: &mut RestartSnapshotState) {
    let expected = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .clone();
    let observations = record_installation(
        state,
        NodeId(1),
        NodeId(2),
        &expected.snapshot,
        expected.payload.as_ref(),
    );

    assert!(observations.contains(Observation::SnapshotPayloadBindingsChecked));
    assert!(observations.contains(Observation::SnapshotTransferIdentitiesChecked));
    assert!(state
        .state
        .snapshot_history
        .payload_binding_violations()
        .is_empty());
    assert!(state
        .state
        .snapshot_history
        .transfer_identity_violations()
        .is_empty());
}

fn assert_detector_fails_closed(
    state: &RestartSnapshotState,
    expected_kind: crate::model_check::FailureKind,
    expected_message: &str,
) {
    let failure = crate::model_check::invariants::check_restart_snapshot_safety(state, &[])
        .expect_err("unchecked snapshot installation must fail closed");
    assert_eq!(failure.kind(), expected_kind);
    assert!(
        failure.message().contains(expected_message),
        "unexpected detector failure: {failure}"
    );
}

fn assert_detector_reports_violation(state: &RestartSnapshotState, expected_message: &str) {
    let failure = crate::model_check::invariants::check_restart_snapshot_safety(state, &[])
        .expect_err("corrupted snapshot installation must fail");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::InvariantViolation
    );
    assert!(
        failure.message().contains(expected_message),
        "unexpected detector failure: {failure}"
    );
}

#[test]
fn detector_retains_missing_reference_after_verified_installation() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    record_verified_installation(&mut state);

    let (unwitnessed, payload) = test_snapshot(1, 10_000, 2, 2, b"unwitnessed application state");
    state
        .state
        .inject_snapshot_payload(NodeId(1), &unwitnessed, payload.clone());
    state
        .state
        .inject_bootstrap_state(
            NodeId(1),
            bootstrap_with_snapshot(Term(2), unwitnessed.clone(), &[]),
        )
        .expect("unwitnessed sender snapshot is structurally valid");

    let observations =
        record_installation(&mut state, NodeId(1), NodeId(2), &unwitnessed, &payload);

    assert!(!observations.contains(Observation::SnapshotPayloadBindingsChecked));
    assert!(observations.contains(Observation::SnapshotTransferIdentitiesChecked));
    let issues = state.state.snapshot_history.payload_binding_coverage_gaps();
    assert!(
        issues
            .iter()
            .any(|issue| issue.contains("has no logical-prefix reference witness")),
        "unexpected payload-binding issues: {issues:?}"
    );
    assert!(state
        .state
        .snapshot_history
        .transfer_identity_violations()
        .is_empty());
    assert_detector_fails_closed(
        &state,
        crate::model_check::FailureKind::CoverageNotReached,
        "has no logical-prefix reference witness",
    );
}

#[test]
fn detector_retains_missing_sender_payload_after_verified_installation() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    record_verified_installation(&mut state);

    let prior = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .clone();
    let payload = b"witnessed next application state";
    let mut committed_bootstrap = bootstrap_with_snapshot(Term(2), prior, &[(3, Term(2), payload)]);
    committed_bootstrap.commit_index = LogIndex(3);
    state
        .state
        .inject_bootstrap_state(NodeId(1), committed_bootstrap)
        .expect("sender prefix for the next snapshot is valid");
    state.state.refresh_snapshot_history();

    let (next, next_payload) = test_snapshot(1, 3, 2, 2, payload);
    assert!(state
        .state
        .cluster()
        .snapshot_payload(NodeId(1), &next)
        .is_none());
    let observations = record_installation(&mut state, NodeId(1), NodeId(2), &next, &next_payload);

    assert!(observations.contains(Observation::SnapshotPayloadBindingsChecked));
    assert!(!observations.contains(Observation::SnapshotTransferIdentitiesChecked));
    assert!(state
        .state
        .snapshot_history
        .payload_binding_violations()
        .is_empty());
    assert!(state
        .state
        .snapshot_history
        .transfer_identity_instrumentation_errors()
        .iter()
        .any(|issue| issue.contains("without sender payload bytes available")));
    assert_detector_fails_closed(
        &state,
        crate::model_check::FailureKind::HarnessError,
        "without sender payload bytes available",
    );
}

#[test]
fn detector_does_not_witness_an_uncommitted_snapshot_prefix() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    record_verified_installation(&mut state);

    let prior = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .clone();
    let payload = b"uncommitted application state";
    state
        .state
        .inject_bootstrap_state(
            NodeId(1),
            bootstrap_with_snapshot(Term(2), prior, &[(3, Term(2), payload)]),
        )
        .expect("uncommitted sender suffix is structurally valid");
    state.state.refresh_snapshot_history();

    assert!(!state
        .state
        .snapshot_history
        .reference_witnesses_by_log_prefix
        .contains_key(&(NodeId(1), LogIndex(3))));

    let (next, next_payload) = test_snapshot(1, 3, 2, 2, payload);
    state
        .state
        .inject_snapshot_payload(NodeId(1), &next, next_payload.clone());
    let observations = record_installation(&mut state, NodeId(1), NodeId(2), &next, &next_payload);

    assert!(!observations.contains(Observation::SnapshotPayloadBindingsChecked));
    assert!(state
        .state
        .snapshot_history
        .payload_binding_coverage_gaps()
        .iter()
        .any(|issue| issue.contains("has no logical-prefix reference witness")));
    assert_detector_fails_closed(
        &state,
        crate::model_check::FailureKind::CoverageNotReached,
        "has no logical-prefix reference witness",
    );
}

#[test]
fn detector_checks_changed_identity_at_the_same_snapshot_boundary() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    record_verified_installation(&mut state);

    let current = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .clone();
    let boundary = current.metadata.last_included_index.0;
    let (replacement, payload) = test_snapshot(
        1,
        boundary,
        current.metadata.last_included_term.0,
        current.metadata.hard_state_term.0,
        b"different same-boundary application state",
    );
    state
        .state
        .inject_snapshot_payload(NodeId(1), &replacement, payload.clone());
    state
        .state
        .inject_bootstrap_state(
            NodeId(1),
            bootstrap_with_snapshot(current.metadata.hard_state_term, replacement.clone(), &[]),
        )
        .expect("replacement sender snapshot is structurally valid");

    let observations =
        record_installation(&mut state, NodeId(1), NodeId(2), &replacement, &payload);

    assert!(!observations.contains(Observation::SnapshotBoundaryAdvances));
    assert!(observations.contains(Observation::SnapshotChunkIdentitiesChecked));
    assert!(observations.contains(Observation::SnapshotChunkOffsetsChecked));
    assert!(observations.contains(Observation::SnapshotInstallCompletenessChecked));
    assert!(!observations.contains(Observation::SnapshotPayloadBindingsChecked));
    assert!(state
        .state
        .snapshot_history
        .payload_binding_violations()
        .iter()
        .any(|issue| issue.contains("application payload differs from witnessed reference state")));
    assert_detector_reports_violation(
        &state,
        "application payload differs from witnessed reference state",
    );
}

#[test]
fn detector_checks_exact_payload_identity_when_same_boundary_descriptors_collide() {
    let mut state = RestartSnapshotState::snapshot_transfer();
    let first_payload = [0x29, 0x2c, 0x99, 0xbf, 0xb5, 0xb8, 0x20, 0xb7];
    let second_payload = [0x11, 0x98, 0x3d, 0x82, 0xcb, 0x0b, 0xeb, 0xd2];
    let (first, _) = test_snapshot(1, 2, 1, 1, &first_payload);
    let (replacement, _) = test_snapshot(1, 2, 1, 1, &second_payload);
    assert_eq!(
        first, replacement,
        "fixture payloads must collide in CRC32 identity"
    );

    record_installation(&mut state, NodeId(1), NodeId(2), &first, &first_payload);
    state
        .state
        .inject_snapshot_payload(NodeId(1), &replacement, second_payload.to_vec());
    state
        .state
        .inject_bootstrap_state(
            NodeId(1),
            bootstrap_with_snapshot(Term(1), replacement.clone(), &[]),
        )
        .expect("replacement sender snapshot is structurally valid");

    let observations = record_installation(
        &mut state,
        NodeId(1),
        NodeId(2),
        &replacement,
        &second_payload,
    );

    assert!(observations.contains(Observation::SnapshotChunkIdentitiesChecked));
    assert!(observations.contains(Observation::SnapshotChunkOffsetsChecked));
    assert!(observations.contains(Observation::SnapshotInstallCompletenessChecked));
    assert!(state
        .state
        .snapshot_history
        .payload_binding_violations()
        .iter()
        .any(|issue| issue.contains("application payload differs from witnessed reference state")));
}
