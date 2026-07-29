//! Adversarial controls for the fenced-lock history checkers.
//!
//! These histories are intentionally handwritten. Cluster tests prove that the
//! recorders produce accepted histories; this suite proves the checkers reject
//! specific corruptions instead of passing only because real runs are healthy.

#[allow(dead_code, reason = "checker tests use a subset of command builders")]
mod support;

use rafter_reference_fenced_lock::{
    check_guarded_history, check_linearizable, check_linearizable_with_budget, ApplyDisposition,
    ApplyOutcome, BlockedReason, CheckError, FencingToken, GuardedCheckError, GuardedHistoryDefect,
    GuardedHistoryEvent, GuardedRejection, GuardedWrite, HistoryDefect, HistoryEvent,
    LockHolderView, LockQuery, LockQueryResult, LockResponse, LogicalTime, OperationId,
    OperationResult, RecordingGuardedResource, ReferenceLockService, ResourceStatus, SearchBudget,
    MAX_HISTORY_OPERATIONS,
};
use support::{
    acquire, config, epoch, expire_through, open_session, release, resource, submit, time, token,
};

const LOCK: &str = "orders";

#[test]
fn sequential_commands_and_query_match_the_specification() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(submit(0, 1, 1, acquire(LOCK, 10)), acquired(1, 10));
    history.read(
        LockQuery::GetLock {
            resource: resource(LOCK),
        },
        held_status(LOCK, 0, 1, 10),
    );

    let report = check_linearizable(config(2, 2), history.events()).expect("the history is legal");
    assert_eq!(report.searched_operations(), 3);
    assert_eq!(report.discharged_operations(), 0);
}

#[test]
fn a_concurrent_unknown_and_query_require_backtracking() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    let lost = history.invoke(submit(0, 1, 1, acquire(LOCK, 10)));
    let query = history.invoke_query(LockQuery::GetLock {
        resource: resource(LOCK),
    });
    history.unknown(lost);
    history.complete_query(query, empty_status(LOCK, None, 0));

    let report =
        check_linearizable(config(1, 1), history.events()).expect("the absent branch is legal");
    assert!(
        report.configurations() > report.searched_operations(),
        "the applied branch must be explored and rejected before the absent branch succeeds"
    );
}

#[test]
fn two_distinct_acquisitions_cannot_receive_the_same_token() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(submit(0, 1, 1, acquire(LOCK, 10)), acquired(1, 10));
    history.ran(submit(0, 1, 2, release(LOCK, 1)), released());
    history.ran(submit(0, 1, 3, acquire(LOCK, 10)), acquired(1, 10));

    assert!(matches!(
        check_linearizable(config(1, 1), history.events()),
        Err(CheckError::NotLinearizable(_))
    ));
}

#[test]
fn impossible_and_real_time_stale_queries_are_rejected() {
    let mut impossible = History::default();
    impossible.read(
        LockQuery::GetLock {
            resource: resource(LOCK),
        },
        held_status(LOCK, 0, 9, 10),
    );
    let Err(CheckError::NotLinearizable(violation)) =
        check_linearizable(config(1, 1), impossible.events())
    else {
        panic!("an invented holder must be rejected");
    };
    assert!(matches!(
        violation.blocked()[0].reason,
        BlockedReason::QueryMismatch { .. }
    ));

    let mut stale = History::default();
    stale.ran(open_session(0, 1), session_opened(1));
    stale.ran(submit(0, 1, 1, acquire(LOCK, 10)), acquired(1, 10));
    stale.read(
        LockQuery::GetLock {
            resource: resource(LOCK),
        },
        empty_status(LOCK, None, 0),
    );
    assert!(matches!(
        check_linearizable(config(1, 1), stale.events()),
        Err(CheckError::NotLinearizable(_))
    ));
}

#[test]
fn not_committed_then_replayed_is_a_contradiction() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    let command = submit(0, 1, 1, acquire(LOCK, 10));
    let refused = history.invoke(command);
    history.not_committed(refused);
    history.ran(
        command,
        ApplyOutcome {
            disposition: ApplyDisposition::Replayed,
            response: acquired(1, 10).response,
        },
    );

    assert!(matches!(
        check_linearizable(config(1, 1), history.events()),
        Err(CheckError::NotLinearizable(_))
    ));
}

#[test]
fn an_unknown_mutation_admits_both_legal_fates() {
    let mut absent = History::default();
    absent.ran(open_session(0, 1), session_opened(1));
    let lost = absent.invoke(submit(0, 1, 1, acquire(LOCK, 10)));
    absent.unknown(lost);
    absent.read(
        LockQuery::GetLock {
            resource: resource(LOCK),
        },
        empty_status(LOCK, None, 0),
    );
    check_linearizable(config(1, 1), absent.events()).expect("an unknown mutation may be absent");

    let mut applied = History::default();
    applied.ran(open_session(0, 1), session_opened(1));
    let lost = applied.invoke(submit(0, 1, 1, acquire(LOCK, 10)));
    applied.unknown(lost);
    applied.read(
        LockQuery::GetLock {
            resource: resource(LOCK),
        },
        held_status(LOCK, 0, 1, 10),
    );
    check_linearizable(config(1, 1), applied.events())
        .expect("an unknown mutation may have taken effect");
}

#[test]
fn provable_refusals_and_abandoned_queries_are_discharged() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    let refused = history.invoke(submit(0, 1, 1, acquire(LOCK, 10)));
    history.not_committed(refused);
    let query = history.invoke_query(LockQuery::GetLock {
        resource: resource(LOCK),
    });
    history.abandon_query(query);

    let report =
        check_linearizable(config(1, 1), history.events()).expect("both non-answers are absent");
    assert_eq!(report.searched_operations(), 1);
    assert_eq!(report.discharged_operations(), 2);
}

#[test]
fn malformed_lock_histories_are_never_searched() {
    let operation_id = OperationId::new(7);
    let command = open_session(0, 1);
    let invocation = HistoryEvent::Invoked {
        operation_id,
        command,
    };
    let completion = HistoryEvent::Completed {
        operation_id,
        outcome: session_opened(1),
    };

    assert_malformed(
        &[invocation, invocation, completion],
        HistoryDefect::RepeatedInvocation { operation_id },
    );
    assert_malformed(
        &[invocation, completion, completion],
        HistoryDefect::RepeatedTerminal { operation_id },
    );
    assert_malformed(
        &[HistoryEvent::Unknown { operation_id }],
        HistoryDefect::TerminalWithoutInvocation { operation_id },
    );
    assert_malformed(
        &[invocation],
        HistoryDefect::UnterminatedOperation { operation_id },
    );
    assert_malformed(
        &[
            HistoryEvent::QueryInvoked {
                operation_id,
                query: LockQuery::GetLock {
                    resource: resource(LOCK),
                },
            },
            completion,
        ],
        HistoryDefect::MismatchedTerminal { operation_id },
    );
}

#[test]
fn replicated_logical_time_cannot_regress() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(submit(0, 1, 1, expire_through(5)), expired(0, 5));
    history.ran(submit(0, 1, 2, expire_through(3)), expired(0, 3));

    assert!(matches!(
        check_linearizable(config(1, 1), history.events()),
        Err(CheckError::NotLinearizable(_))
    ));
}

#[test]
fn guarded_history_keeps_resources_separate_and_accepts_equal_retries() {
    let mut guard = RecordingGuardedResource::new(resource("alpha"));
    assert_eq!(guard.apply(write("alpha", 5, 10)), Ok(10));
    assert_eq!(guard.apply(write("alpha", 5, 11)), Ok(11));
    assert_eq!(guard.apply(write("alpha", 7, 12)), Ok(12));
    assert_eq!(
        guard.apply(write("alpha", 5, 13)),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: token(7),
        })
    );
    let report = check_guarded_history(guard.history()).expect("the recorder is exact");
    assert_eq!(report.checked_operations(), 4);

    let scoped = guarded_operations(&[
        ("alpha", "alpha", 9, Ok(1)),
        ("beta", "beta", 1, Ok(2)),
        ("alpha", "beta", 10, Err(GuardedRejection::WrongResource)),
    ]);
    let report = check_guarded_history(&scoped).expect("resource token floors are independent");
    assert_eq!(report.resources(), 2);

    let refusal_only =
        guarded_operations(&[("alpha", "beta", 1, Err(GuardedRejection::WrongResource))]);
    let report = check_guarded_history(&refusal_only).expect("the refused guard was still checked");
    assert_eq!(report.resources(), 1);
}

#[test]
fn accepted_stale_guarded_write_and_missing_terminal_are_rejected() {
    let stale = guarded_operations(&[("alpha", "alpha", 7, Ok(1)), ("alpha", "alpha", 5, Ok(2))]);
    assert!(matches!(
        check_guarded_history(&stale),
        Err(GuardedCheckError::Violation(_))
    ));

    let operation_id = OperationId::new(1);
    assert_eq!(
        check_guarded_history(&[GuardedHistoryEvent::Invoked {
            operation_id,
            guarded_resource: resource("alpha"),
            write: write("alpha", 1, 1),
        }]),
        Err(GuardedCheckError::Malformed(
            GuardedHistoryDefect::UnterminatedOperation { operation_id }
        ))
    );
}

#[test]
fn explicit_bounds_are_undecided_never_green() {
    let mut one = History::default();
    one.ran(open_session(0, 1), session_opened(1));
    one.ran(open_session(0, 2), session_replaced(2));
    assert!(matches!(
        check_linearizable_with_budget(
            config(1, 1),
            one.events(),
            SearchBudget::new(1).expect("one is nonzero"),
        ),
        Err(CheckError::BudgetExhausted { bound: 1, .. })
    ));

    let bounds = config(1, 1);
    let mut oracle = ReferenceLockService::new(bounds);
    let mut long = History::default();
    for session_epoch in 1..=MAX_HISTORY_OPERATIONS as u64 + 1 {
        let command = open_session(0, session_epoch);
        long.ran(command, oracle.apply(command));
    }
    assert_eq!(
        check_linearizable(bounds, long.events()),
        Err(CheckError::HistoryTooLong {
            operations: MAX_HISTORY_OPERATIONS + 1,
            bound: MAX_HISTORY_OPERATIONS,
        })
    );
}

fn assert_malformed(history: &[HistoryEvent], expected: HistoryDefect) {
    assert_eq!(
        check_linearizable(config(1, 1), history),
        Err(CheckError::Malformed(expected))
    );
}

fn session_opened(session_epoch: u64) -> ApplyOutcome {
    ApplyOutcome {
        disposition: ApplyDisposition::SessionOpened,
        response: LockResponse::SessionOpened {
            session_epoch: epoch(session_epoch),
        },
    }
}

fn session_replaced(session_epoch: u64) -> ApplyOutcome {
    ApplyOutcome {
        disposition: ApplyDisposition::SessionReplaced,
        response: LockResponse::SessionOpened {
            session_epoch: epoch(session_epoch),
        },
    }
}

fn acquired(fencing_token: u64, expiry: u64) -> ApplyOutcome {
    ApplyOutcome {
        disposition: ApplyDisposition::Applied,
        response: LockResponse::Operation(OperationResult::Acquired {
            token: token(fencing_token),
            expiry: time(expiry),
        }),
    }
}

fn released() -> ApplyOutcome {
    ApplyOutcome {
        disposition: ApplyDisposition::Applied,
        response: LockResponse::Operation(OperationResult::Released),
    }
}

fn expired(released_locks: u32, logical_time: u64) -> ApplyOutcome {
    ApplyOutcome {
        disposition: ApplyDisposition::Applied,
        response: LockResponse::Operation(OperationResult::Expired {
            released_locks,
            logical_time: time(logical_time),
        }),
    }
}

fn held_status(name: &str, owner: u32, fencing_token: u64, expiry: u64) -> LockQueryResult {
    LockQueryResult::Lock(ResourceStatus {
        resource: resource(name),
        holder: Some(LockHolderView {
            owner: support::client(owner),
            token: token(fencing_token),
            expiry: time(expiry),
        }),
        token_floor: Some(token(fencing_token)),
        logical_time: LogicalTime::ZERO,
    })
}

fn empty_status(
    name: &str,
    token_floor: Option<FencingToken>,
    logical_time: u64,
) -> LockQueryResult {
    LockQueryResult::Lock(ResourceStatus {
        resource: resource(name),
        holder: None,
        token_floor,
        logical_time: time(logical_time),
    })
}

fn write(name: &str, fencing_token: u64, value: u64) -> GuardedWrite {
    GuardedWrite {
        resource: resource(name),
        token: token(fencing_token),
        value,
    }
}

fn guarded_operations(
    operations: &[(&str, &str, u64, Result<u64, GuardedRejection>)],
) -> Vec<GuardedHistoryEvent> {
    let mut history = Vec::new();
    for (index, (guarded, claimed, fencing_token, result)) in operations.iter().enumerate() {
        let operation_id = OperationId::new(index as u64 + 1);
        history.push(GuardedHistoryEvent::Invoked {
            operation_id,
            guarded_resource: resource(guarded),
            write: write(claimed, *fencing_token, index as u64 + 1),
        });
        history.push(GuardedHistoryEvent::Completed {
            operation_id,
            result: *result,
        });
    }
    history
}

#[derive(Default)]
struct History {
    events: Vec<HistoryEvent>,
    next_operation_id: u64,
}

impl History {
    fn invoke(&mut self, command: rafter_reference_fenced_lock::Command) -> OperationId {
        let operation_id = self.allocate();
        self.events.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        operation_id
    }

    fn invoke_query(&mut self, query: LockQuery) -> OperationId {
        let operation_id = self.allocate();
        self.events.push(HistoryEvent::QueryInvoked {
            operation_id,
            query,
        });
        operation_id
    }

    fn ran(&mut self, command: rafter_reference_fenced_lock::Command, outcome: ApplyOutcome) {
        let operation_id = self.invoke(command);
        self.complete(operation_id, outcome);
    }

    fn read(&mut self, query: LockQuery, result: LockQueryResult) {
        let operation_id = self.invoke_query(query);
        self.complete_query(operation_id, result);
    }

    fn complete(&mut self, operation_id: OperationId, outcome: ApplyOutcome) {
        self.events.push(HistoryEvent::Completed {
            operation_id,
            outcome,
        });
    }

    fn complete_query(&mut self, operation_id: OperationId, result: LockQueryResult) {
        self.events.push(HistoryEvent::QueryCompleted {
            operation_id,
            result,
        });
    }

    fn unknown(&mut self, operation_id: OperationId) {
        self.events.push(HistoryEvent::Unknown { operation_id });
    }

    fn not_committed(&mut self, operation_id: OperationId) {
        self.events
            .push(HistoryEvent::NotCommitted { operation_id });
    }

    fn abandon_query(&mut self, operation_id: OperationId) {
        self.events
            .push(HistoryEvent::QueryAbandoned { operation_id });
    }

    fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    fn allocate(&mut self) -> OperationId {
        self.next_operation_id += 1;
        OperationId::new(self.next_operation_id)
    }
}
