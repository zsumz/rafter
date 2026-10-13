//! Explicit larger histories retain the same oracle and fail-closed budgets.
#[allow(
    dead_code,
    reason = "only command builders needed by these histories are used"
)]
mod support;

use rafter_reference_fenced_lock::{
    check_linearizable, check_linearizable_with_budget, ApplyDisposition, ApplyOutcome, CheckError,
    HistoryEvent, LockResponse, OperationId, OperationResult, SearchBudget,
    MAX_SEARCH_CONFIGURATIONS,
};
use support::{acquire, config, epoch, open_session, submit, time, token};

fn budget() -> SearchBudget {
    SearchBudget::new(MAX_SEARCH_CONFIGURATIONS)
        .unwrap()
        .with_operation_limit(32)
        .unwrap()
}

fn sessions(count: u64) -> Vec<HistoryEvent> {
    (1..=count)
        .flat_map(|number| {
            let operation_id = OperationId::new(number);
            [
                HistoryEvent::Invoked {
                    operation_id,
                    command: open_session(0, number),
                },
                HistoryEvent::Completed {
                    operation_id,
                    outcome: ApplyOutcome {
                        disposition: if number == 1 {
                            ApplyDisposition::SessionOpened
                        } else {
                            ApplyDisposition::SessionReplaced
                        },
                        response: LockResponse::SessionOpened {
                            session_epoch: epoch(number),
                        },
                    },
                },
            ]
        })
        .collect()
}

#[test]
fn explicit_history_limit_is_bounded_and_does_not_change_the_default() {
    let history = sessions(32);
    assert_eq!(
        check_linearizable(config(1, 1), &history),
        Err(CheckError::HistoryTooLong {
            operations: 32,
            bound: 24,
        })
    );
    let report = check_linearizable_with_budget(config(1, 1), &history, budget()).unwrap();
    assert_eq!(report.searched_operations(), 32);
    assert_eq!(
        check_linearizable_with_budget(config(1, 1), &sessions(33), budget()),
        Err(CheckError::HistoryTooLong {
            operations: 33,
            bound: 32,
        })
    );
    assert!(budget().with_operation_limit(33).is_none());
    assert!(budget().with_operation_limit(usize::MAX).is_none());
}

#[test]
fn a_larger_history_still_rejects_impossible_results_and_configuration_exhaustion() {
    let mut history = sessions(32);
    let HistoryEvent::Completed { outcome, .. } = history.last_mut().unwrap() else {
        unreachable!()
    };
    outcome.response = LockResponse::SessionOpened {
        session_epoch: epoch(99),
    };
    assert!(matches!(
        check_linearizable_with_budget(config(1, 1), &history, budget()),
        Err(CheckError::NotLinearizable(_))
    ));
    let limited = SearchBudget::new(1)
        .unwrap()
        .with_operation_limit(32)
        .unwrap();
    assert!(matches!(
        check_linearizable_with_budget(config(1, 1), &sessions(32), limited),
        Err(CheckError::BudgetExhausted { bound: 1, .. })
    ));
}

#[test]
fn an_uncertain_attempt_must_remain_in_the_history_to_explain_a_cached_retry() {
    let mut history = sessions(24);
    let command = submit(0, 24, 1, acquire("vault", 100));
    history.extend([
        HistoryEvent::Invoked {
            operation_id: OperationId::new(25),
            command,
        },
        HistoryEvent::Unknown {
            operation_id: OperationId::new(25),
        },
        HistoryEvent::Invoked {
            operation_id: OperationId::new(26),
            command,
        },
        HistoryEvent::Completed {
            operation_id: OperationId::new(26),
            outcome: ApplyOutcome {
                disposition: ApplyDisposition::Replayed,
                response: LockResponse::Operation(OperationResult::Acquired {
                    token: token(1),
                    expiry: time(100),
                }),
            },
        },
    ]);
    let report = check_linearizable_with_budget(config(1, 1), &history, budget()).unwrap();
    assert_eq!(report.searched_operations(), 26);
    history.drain(48..50);
    assert!(matches!(
        check_linearizable_with_budget(config(1, 1), &history, budget()),
        Err(CheckError::NotLinearizable(_))
    ));
}
