#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn in_memory_driver_reports_unsupported_lease_reads_explicitly() {
    let driver = elected_driver();
    let handle = driver.handle();

    let error = block_on(handle.read("alpha".to_owned(), ReadConsistency::LeaseRead))
        .expect_err("the driver does not serve lease reads");

    assert!(
        matches!(
            error,
            ReadError::UnsupportedConsistency {
                consistency: ReadConsistency::LeaseRead,
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_reports_read_id_exhaustion_after_max() {
    let mut adopted = scripted_read_group(ScriptedReadMode::Reject);
    adopted
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: (),
            read_id: ReadId(u64::MAX - 1),
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect("manual read consumes the penultimate read id");
    let driver = ScriptedReadDriver::new(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let handle = driver.handle();

    assert!(matches!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Rejected {
            read_id: Some(ReadId(u64::MAX)),
            ..
        })
    ));
    let error = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the read id space is spent");

    assert!(matches!(error, ReadError::ReadIdExhausted), "got {error:?}");
}

#[test]
fn in_memory_driver_local_reads_do_not_consume_read_ids() {
    let driver = scripted_read_driver(ScriptedReadMode::Reject);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Local))
            .expect("local read succeeds without read id")
            .result,
        None
    );
    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Local))
            .expect("repeated local read succeeds without read id")
            .result,
        None
    );
    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the scripted group refuses the barrier");

    assert!(
        matches!(
            error,
            ReadError::Rejected {
                read_id: Some(ReadId(1)),
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(1),
                },
                leader_hint: Some(NodeId(1)),
            }
        ),
        "the first linearizable read consumed read id 1, so no local read did; got {error:?}"
    );
}

#[test]
fn in_memory_driver_cancels_freshness_unavailable_linearizable_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Grant(LogIndex(5)));
    let handle = driver.handle();

    for expected_read_id in [ReadId(1), ReadId(2)] {
        let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
            .expect_err("the state machine is behind the granted read index");

        assert!(
            matches!(
                error,
                ReadError::FreshnessUnavailable {
                    read_id: Some(read_id),
                    required_applied_index: LogIndex(5),
                    local_applied_index: LogIndex::ZERO,
                } if read_id == expected_read_id
            ),
            "got {error:?}"
        );
        assert_eq!(
            handle.metrics().expect("metrics").current().pending_reads,
            0,
            "abandoned freshness-unavailable read must not leak pending app state"
        );
    }
}

/// The managed read path routes its step report like every other driver path.
///
/// This barrier step grants a read index the state machine has not reached and
/// emits a peer message in the same step. The outcome is
/// `LinearizableFreshnessUnavailable`, which carries no peer messages, so the
/// old signature dropped that message and the driver abandoned the read as
/// though nothing were in flight. Now the message reaches the network and the
/// driver keeps driving, which the unroutable destination makes visible.
#[test]
fn managed_read_routes_every_effect_the_barrier_step_emitted() {
    let driver = scripted_read_driver(ScriptedReadMode::GrantWithPeerTraffic(LogIndex(5)));
    let handle = driver.handle();

    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the routed peer message has no destination in this fixture");

    // The replacement for a substring match on a rendered message: the driver's
    // own routing failure is preserved as a typed cause, so the assertion is
    // about a type rather than about text a caller cannot parse.
    let ReadError::Transport { cause } = &error else {
        panic!("the read step's peer message must reach the network, got {error:?}");
    };
    assert_eq!(
        error.kind(),
        ReadErrorKind::Transport,
        "an unroutable step is a delivery failure"
    );
    assert!(
        cause.to_string().contains("is missing"),
        "the preserved cause names the node the driver could not reach, got {cause}"
    );
}

/// The production regression. After an election the new leader's only entry in
/// its own term is a `Noop`, and the barrier grants there. The managed driver
/// passes `min_applied_index: None`, so a caller cannot work around a floor it
/// does not control — and the network drains promptly when nothing else is
/// happening, so the driver reached `handle_linearizable_freshness_gap` and
/// returned `FreshnessUnavailable` for every linearizable read until an
/// unrelated write committed. It now answers.
#[test]
fn managed_read_answers_after_an_election_without_an_intervening_write() {
    let driver = scripted_read_driver(ScriptedReadMode::GrantAtNonApplicationIndex(LogIndex(1)));
    let handle = driver.handle();

    let receipt = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect("a post-election linearizable read answers with no write in between");

    assert_eq!(receipt.result, None);
    let proof = receipt
        .proof
        .expect("a linearizable read carries its proof");
    assert_eq!(proof.read_index, LogIndex(1));
    assert_eq!(
        proof.required_applied_index,
        LogIndex::ZERO,
        "the leadership noop at the read index requires nothing of the state machine"
    );
    assert_eq!(proof.local_applied_index, LogIndex::ZERO);
}

/// Nobody refused anything: the driver's bounded loop ran out, so this is the
/// driver's own decision and it says so. It used to borrow a transport failure
/// and write the reason into the message, which a caller reasonably reads as
/// "the network broke" and retries against the same replica.
#[test]
fn a_read_that_exhausts_the_drive_bound_is_abandoned() {
    let driver = scripted_read_driver(ScriptedReadMode::Pending);
    let handle = driver.handle();

    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("a barrier that never resolves exhausts the drive bound");

    assert!(
        matches!(
            error,
            ReadError::Abandoned {
                read_id: ReadId(1),
                reason: ReadAbandonReason::DriveBoundReached,
            }
        ),
        "got {error:?}"
    );
    assert_eq!(error.kind(), ReadErrorKind::Abandoned);
}

/// The cancellation half of the contract, pinned separately from the error
/// half: `Abandoned` is returned only after `cancel_read` cleared the group's
/// waiter, so `reserved_reads` is back where it started.
#[test]
fn an_abandoned_read_leaves_no_reserved_read() {
    let driver = scripted_read_driver(ScriptedReadMode::Pending);
    let handle = driver.handle();
    let reserved_before = handle.metrics().expect("metrics").current().reserved_reads;

    let _ = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("a barrier that never resolves exhausts the drive bound");

    let metrics = handle.metrics().expect("metrics").current();
    assert_eq!(metrics.reserved_reads, reserved_before);
    assert_eq!(
        metrics.pending_reads, 0,
        "abandoned stalled read must not leak pending app state"
    );
}

/// Negative: a freshness gap is a statement about this replica's state, and it
/// carries the two indexes that explain it. Folding it into `Abandoned` would
/// discard them.
#[test]
fn a_freshness_gap_is_not_reported_as_abandonment() {
    let driver = scripted_read_driver(ScriptedReadMode::Grant(LogIndex(5)));
    let handle = driver.handle();

    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the state machine is behind the granted read index");

    assert!(
        matches!(
            error,
            ReadError::FreshnessUnavailable {
                required_applied_index: LogIndex(5),
                local_applied_index: LogIndex::ZERO,
                ..
            }
        ),
        "got {error:?}"
    );
    assert_ne!(error.kind(), ReadErrorKind::Abandoned);
}

/// Negative: the cluster's refusal must not be reported as the driver's
/// decision. `Abandoned` says nothing about the cluster by construction.
#[test]
fn a_rejected_barrier_is_not_reported_as_abandonment() {
    let driver = scripted_read_driver(ScriptedReadMode::Reject);
    let handle = driver.handle();

    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the scripted group refuses the barrier");

    assert_eq!(error.kind(), ReadErrorKind::Rejected);
    assert_ne!(error.kind(), ReadErrorKind::Abandoned);
}

/// Negative: the doc comment claims the `ReadId` is spent. Reissuing it through
/// the group makes that claim executable rather than rhetorical.
#[test]
fn an_abandoned_read_id_is_not_reusable() {
    let driver = scripted_read_driver(ScriptedReadMode::Pending);
    let handle = driver.handle();

    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("a barrier that never resolves exhausts the drive bound");
    let ReadError::Abandoned { read_id, .. } = error else {
        panic!("expected an abandoned read, got {error:?}");
    };

    let mut group = scripted_read_group(ScriptedReadMode::Pending);
    let _ = group
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: (),
            read_id,
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect("the first use of a read id is accepted");
    let reused = group
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: (),
            read_id,
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect_err("a spent read id cannot be reissued");

    assert!(
        matches!(
            reused,
            GroupError::DuplicateReadId { read_id: actual } if actual == read_id
        ) || matches!(
            reused,
            GroupError::NonMonotonicReadId { read_id: actual, .. } if actual == read_id
        ),
        "got {reused:?}"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_rejected_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Reject);
    let handle = driver.handle();

    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex::ZERO
    );
    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the scripted group refuses the barrier");

    assert!(
        matches!(
            error,
            ReadError::Rejected {
                read_id: Some(ReadId(1)),
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(1),
                },
                leader_hint: Some(NodeId(1)),
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex(1),
        "rejected read publishes the scripted metrics transition"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_canceled_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Cancel);
    let handle = driver.handle();

    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex::ZERO
    );
    let error = block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the cluster invalidated the barrier");

    assert!(
        matches!(
            error,
            ReadError::Canceled {
                read_id: ReadId(1),
                reason: ReadIndexCancelReason::LeaderStateReset,
                leader_hint: Some(NodeId(1)),
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex(1),
        "canceled read publishes the scripted metrics transition"
    );
}
