#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn in_memory_driver_rejects_waiters_after_shutdown() {
    let driver = elected_driver();
    let handle = driver.handle();

    block_on(handle.shutdown()).expect("shutdown succeeds");

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::ShuttingDown)
    );
    assert_eq!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::ShuttingDown)
    );
    assert_eq!(
        block_on(handle.transfer_leadership(NodeId(2))),
        Err(TransferLeadershipError::ShuttingDown)
    );
}

#[test]
fn in_memory_driver_resolves_waiters_when_apply_poisons_group() {
    let driver = KvDriver::new_elected(
        NodeId(1),
        vec![group_with_app(
            1,
            &[],
            3,
            KvStateMachine {
                fail_apply: true,
                ..KvStateMachine::default()
            },
        )],
    )
    .expect("single-node primary elects");
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::GroupPoisoned,
        })
    );
    assert!(matches!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Poisoned { .. })
    ));
}

#[test]
fn in_memory_driver_reports_not_leader_before_election() {
    let driver = KvDriver::new(NodeId(1), groups()).expect("driver builds");
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("follower rejects writes");

    assert!(matches!(
        error,
        WriteError::NotLeader {
            leader_hint: None,
            ..
        }
    ));
}

#[test]
fn in_memory_driver_reports_proposal_id_exhaustion_after_max() {
    let mut adopted = group(1, &[], 3);
    adopted
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(u64::MAX - 1),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal consumes the penultimate local proposal id");
    let driver = KvDriver::new_elected(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let handle = driver.handle();

    block_on(handle.write(("last".to_owned(), "ok".to_owned())))
        .expect("last proposal ID may be used once");
    assert_eq!(
        block_on(handle.write(("again".to_owned(), "no".to_owned()))),
        Err(WriteError::LocalProposalIdExhausted)
    );
}

#[test]
fn in_memory_driver_bounds_pending_writes_and_publishes_metrics() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenCycle);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::DriveBoundReached,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "bounded unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_empty_network_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenIdle);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::EmptyNetwork,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "empty-network unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_maps_post_append_dispatch_error_to_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenMissingNode);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::PostAppendDriverError,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "post-append unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_preserves_pre_append_runtime_error() {
    let driver = scripted_write_driver(ScriptedWriteMode::PreAppendRuntimeError);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::Storage {
            message: "persisted Raft log diverges from committed state at index 1".to_owned(),
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        0,
        "pre-append runtime error does not leak pending proposal state"
    );
}

#[test]
fn in_memory_driver_maps_no_lifecycle_proposal_output_to_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::PreAppendNoLifecycleMessage);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::RuntimeDroppedProposal,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        0,
        "malformed no-lifecycle output is cleaned up before returning"
    );
}
