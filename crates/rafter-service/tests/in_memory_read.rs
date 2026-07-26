#![allow(clippy::wildcard_imports)]

mod support;

use std::{cell::Cell, collections::VecDeque};

use rafter_runtime::RaftRuntimeError;
use rafter_runtime_api::PersistedRaftRuntime;

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

// ---------------------------------------------------------------------------
// Barriers the group ends during routing, rather than in the read call.
//
// Adopted from the gen-7 reproduction: `InMemoryRaftState::route_report` used
// to extend the network with the report's peer messages and drop the rest, read
// events included. The test above covers the other half — a cancellation the
// read call itself observes and returns as `ReadOutcome::Canceled`.
// ---------------------------------------------------------------------------

/// How a [`LateEndRuntime`] ends the barrier it started.
///
/// Both ways exist because they are separate arms of the driver's reading of a
/// routed event, and a barrier can end either way while a frame is in flight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LateEnd {
    Cancel,
    Reject,
}

/// A leader that starts a quorum round for a read barrier and then loses
/// leadership when the round's own frame comes back.
#[derive(Debug)]
struct LateEndRuntime {
    end: LateEnd,
    read_id: Cell<Option<ReadId>>,
}

impl PersistedRaftRuntime for LateEndRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }

    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }

    fn role(&self) -> Role {
        Role::Leader
    }

    fn current_term(&self) -> Term {
        Term(1)
    }

    fn commit_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    fn committed_application_index_through(&self, _index: LogIndex) -> LogIndex {
        LogIndex::ZERO
    }

    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("scripted membership is valid"),
        )
    }

    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        match input {
            // Start the round: one frame goes out, no grant yet.
            RaftInput::ReadIndex { read_id } => {
                self.read_id.set(Some(read_id));
                Ok(vec![RaftOutput::Send {
                    to: NodeId(1),
                    message: Message::RequestVote(RequestVote {
                        term: Term(1),
                        candidate_id: NodeId(1),
                        last_log_index: LogIndex::ZERO,
                        last_log_term: Term(0),
                    }),
                }])
            }
            // The frame comes back to a node that is no longer leader, so the
            // kernel ends every barrier it was holding.
            RaftInput::Message { .. } => Ok(self
                .read_id
                .get()
                .map(|read_id| match self.end {
                    LateEnd::Cancel => vec![RaftOutput::ReadIndexCanceled {
                        read_id,
                        reason: ReadIndexCancelReason::LeaderStateReset,
                    }],
                    LateEnd::Reject => vec![RaftOutput::ReadIndexRejected {
                        read_id,
                        reason: ReadIndexRejection::NotLeader {
                            role: Role::Follower,
                            term: Term(1),
                        },
                    }],
                })
                .unwrap_or_default()),
            _ => Ok(Vec::new()),
        }
    }

    fn step_proposal_batch(
        &mut self,
        _proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(Vec::new())
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut outputs = VecDeque::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs.into())
    }

    fn term_at_index(&self, _index: LogIndex) -> Option<Term> {
        Some(Term(1))
    }
}

fn late_end_driver(end: LateEnd) -> InMemoryRaftDriver<(), KvStateMachine, LateEndRuntime> {
    InMemoryRaftDriver::new(
        NodeId(1),
        vec![RaftGroup::new(
            (),
            NodeId(1),
            LateEndRuntime {
                end,
                read_id: Cell::new(None),
            },
            KvStateMachine::default(),
        )],
    )
    .expect("a quiescent group is adoptable")
}

/// A barrier the group cancelled during routing must reach the client as the
/// cancellation it was, not as a driver invariant violation.
#[test]
fn a_barrier_cancelled_during_routing_is_reported_as_a_cancellation() {
    let driver = late_end_driver(LateEnd::Cancel);
    let handle = driver.handle();

    let error = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the barrier was cancelled");

    assert!(
        matches!(
            error,
            ReadError::Canceled {
                reason: ReadIndexCancelReason::LeaderStateReset,
                ..
            }
        ),
        "a leadership loss during routing reached the client as {error:?}"
    );
    assert_ne!(
        error.kind(),
        ReadErrorKind::ManagedInvariantViolation,
        "an ordinary cluster event was reported as a driver invariant violation"
    );
}

/// The other terminal event, on the same routing path. Rejection and
/// cancellation are separate arms of the driver's reading of a routed event, so
/// a rejection observed during routing gets its own test rather than riding on
/// the cancellation one.
#[test]
fn a_barrier_rejected_during_routing_is_reported_as_a_rejection() {
    let driver = late_end_driver(LateEnd::Reject);
    let handle = driver.handle();

    let error = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the barrier was rejected");

    assert!(
        matches!(
            error,
            ReadError::Rejected {
                read_id: Some(ReadId(1)),
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(1),
                },
                ..
            }
        ),
        "a rejection during routing reached the client as {error:?}"
    );
}

/// A routed answer answers the barrier it names and no other.
///
/// `ScriptedReadMode::Cancel` ends the barrier inside the read call's own step,
/// and `RaftGroup` derives that outcome *from* the report's read events — so
/// the call returns its answer with the same event still sitting in the
/// driver's routed slot. The next read must run its own round rather than take
/// that leftover, which is only true because the slot is matched by read ID.
#[test]
fn a_routed_answer_is_not_reused_by_a_later_barrier() {
    let driver = scripted_read_driver(ScriptedReadMode::Cancel);
    let handle = driver.handle();

    let first = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the first barrier was cancelled");
    let second = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the second barrier is cancelled on its own account");

    assert!(
        matches!(
            first,
            ReadError::Canceled {
                read_id: ReadId(1),
                ..
            }
        ),
        "got {first:?}"
    );
    assert!(
        matches!(
            second,
            ReadError::Canceled {
                read_id: ReadId(2),
                ..
            }
        ),
        "the second read answered with the first read's barrier: {second:?}"
    );
}
