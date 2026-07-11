use super::*;

#[test]
fn tracked_rejection_preserves_id_without_log_append() {
    let mut runtime = durable_node_with_log(
        2,
        &[1, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );

    let proposal_id = LocalProposalId(9);
    let outputs = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id,
            payload: b"not-leader".to_vec(),
        })
        .expect("rejection does not require log persistence");

    assert!(runtime.log_segment.replay_entries().is_empty());
    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::RejectProposal {
            proposal_id: Some(id),
            reason: rafter::ProposalRejection::NotLeader { .. },
        }] if *id == proposal_id
    ));
}

#[test]
fn tracked_local_append_is_suppressed_when_log_append_fails() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader election noop persists")
        .is_empty());

    let error = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id: LocalProposalId(10),
            payload: b"must-not-escape".to_vec(),
        })
        .expect_err("tracked proposal append fails");

    assert!(matches!(error, RaftRuntimeError::LogAppend(_)));
    assert!(matches!(
        runtime
            .step(RaftInput::Tick)
            .expect_err("failed append poisons the runtime"),
        RaftRuntimeError::Poisoned { .. }
    ));
}

#[test]
fn tracked_rejection_does_not_touch_failing_log_segment() {
    let mut runtime = durable_node_with_log(
        2,
        &[1, 3],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment {
            inner: InMemoryRaftLogSegment::new(),
            allowed: 0,
        },
    );

    let proposal_id = LocalProposalId(11);
    let outputs = runtime
        .step(RaftInput::TrackedClientProposal {
            proposal_id,
            payload: b"rejected-before-append".to_vec(),
        })
        .expect("not-leader rejection does not append");

    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::RejectProposal {
            proposal_id: Some(id),
            reason: rafter::ProposalRejection::NotLeader { .. },
        }] if *id == proposal_id
    ));
}
