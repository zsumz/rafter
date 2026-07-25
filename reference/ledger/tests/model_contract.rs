mod support;

use rafter_reference_ledger::{
    AccountId, ApplyDisposition, BusinessRejection, Command, HistoryEvent, Ledger, LedgerQuery,
    LedgerQueryResult, LedgerResponse, Mutation, MutationResult, OperationId, RequestRejection,
};
use support::{amount, client, config, epoch, execute, open_session, sequence};

#[test]
fn exact_retry_returns_the_cached_result_without_a_second_effect() {
    let mut ledger = Ledger::new(config(2, 4));
    ledger.apply(open_session(0, 1));
    let command = execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(7),
        },
    );

    let first = ledger.apply(command.clone());
    let state_after_first = ledger.view();
    let retry = ledger.apply(command);

    assert_eq!(first.response, retry.response);
    assert_eq!(retry.disposition, ApplyDisposition::Replayed);
    assert_eq!(ledger.view(), state_after_first);
}

#[test]
fn conflicting_retry_is_rejected_without_changing_state() {
    let mut ledger = Ledger::new(config(1, 4));
    ledger.apply(open_session(0, 1));
    ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(1),
        },
    ));
    let before = ledger.view();

    let conflict = ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(2),
        },
    ));

    assert_eq!(
        conflict.response,
        LedgerResponse::Rejected(RequestRejection::ConflictingRetry)
    );
    assert_eq!(conflict.disposition, ApplyDisposition::Rejected);
    assert_eq!(ledger.view(), before);
}

#[test]
fn deterministic_business_rejection_consumes_and_caches_its_sequence() {
    let mut ledger = Ledger::new(config(2, 4));
    ledger.apply(open_session(0, 1));
    ledger.apply(open_session(1, 1));
    let rejected_deposit = execute(
        0,
        1,
        1,
        Mutation::Deposit {
            account_id: AccountId::new(9),
            amount: amount(5),
        },
    );

    let first = ledger.apply(rejected_deposit.clone());
    assert_eq!(
        first.response,
        LedgerResponse::Mutation(MutationResult::Rejected(BusinessRejection::AccountNotFound))
    );
    ledger.apply(execute(
        1,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(9),
        },
    ));

    let retry = ledger.apply(rejected_deposit);
    assert_eq!(retry.response, first.response);
    assert_eq!(retry.disposition, ApplyDisposition::Replayed);
    assert_eq!(ledger.account_balance(AccountId::new(9)), Some(0));
}

#[test]
fn greater_session_epoch_fences_old_commands_and_preserves_current_retries() {
    let mut ledger = Ledger::new(config(1, 4));
    ledger.apply(open_session(0, 1));
    ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(1),
        },
    ));

    assert_eq!(
        ledger.apply(open_session(0, 2)).disposition,
        ApplyDisposition::SessionReplaced
    );
    assert_eq!(
        ledger
            .apply(execute(
                0,
                1,
                2,
                Mutation::Deposit {
                    account_id: AccountId::new(1),
                    amount: amount(3),
                },
            ))
            .response,
        LedgerResponse::Rejected(RequestRejection::StaleSession { current: epoch(2) })
    );

    let deposit = execute(
        0,
        2,
        1,
        Mutation::Deposit {
            account_id: AccountId::new(1),
            amount: amount(3),
        },
    );
    let first = ledger.apply(deposit.clone());
    assert_eq!(
        ledger.apply(open_session(0, 2)).disposition,
        ApplyDisposition::SessionAlreadyOpen
    );
    let retry = ledger.apply(deposit);
    assert_eq!(retry.response, first.response);
    assert_eq!(retry.disposition, ApplyDisposition::Replayed);
}

#[test]
fn sequence_gaps_and_stale_sequences_fail_closed() {
    let mut ledger = Ledger::new(config(1, 4));
    ledger.apply(open_session(0, 1));
    let account = AccountId::new(1);

    assert_eq!(
        ledger
            .apply(execute(
                0,
                1,
                2,
                Mutation::OpenAccount {
                    account_id: account
                }
            ))
            .response,
        LedgerResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(1)
        })
    );
    ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: account,
        },
    ));
    ledger.apply(execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: account,
            amount: amount(1),
        },
    ));

    assert_eq!(
        ledger
            .apply(execute(
                0,
                1,
                1,
                Mutation::OpenAccount {
                    account_id: account
                }
            ))
            .response,
        LedgerResponse::Rejected(RequestRejection::StaleSequence {
            highest: sequence(2)
        })
    );
}

#[test]
fn transfer_preserves_supply_and_only_zero_balance_accounts_close() {
    let mut ledger = Ledger::new(config(1, 4));
    ledger.apply(open_session(0, 1));
    let source = AccountId::new(1);
    let destination = AccountId::new(2);

    for (request_sequence, mutation) in [
        (1, Mutation::OpenAccount { account_id: source }),
        (
            2,
            Mutation::Deposit {
                account_id: source,
                amount: amount(10),
            },
        ),
        (
            3,
            Mutation::OpenAccount {
                account_id: destination,
            },
        ),
        (
            4,
            Mutation::Transfer {
                from: source,
                to: destination,
                amount: amount(4),
            },
        ),
    ] {
        ledger.apply(execute(0, 1, request_sequence, mutation));
    }

    assert_eq!(ledger.summary().total_balance, 10);
    assert_eq!(ledger.summary().successful_deposits, 10);
    assert_eq!(
        ledger
            .apply(execute(
                0,
                1,
                5,
                Mutation::CloseAccount { account_id: source }
            ))
            .response,
        LedgerResponse::Mutation(MutationResult::Rejected(BusinessRejection::AccountNotEmpty))
    );
    ledger.apply(execute(
        0,
        1,
        6,
        Mutation::Transfer {
            from: source,
            to: destination,
            amount: amount(6),
        },
    ));
    ledger.apply(execute(
        0,
        1,
        7,
        Mutation::CloseAccount { account_id: source },
    ));

    assert_eq!(ledger.summary().open_accounts, 1);
    assert_eq!(ledger.summary().total_balance, 10);
    assert_eq!(ledger.account_balance(source), None);
}

#[test]
fn snapshot_restore_preserves_cached_replay_and_conflict_detection() {
    let bounds = config(1, 4);
    let mut ledger = Ledger::new(bounds);
    ledger.apply(open_session(0, 1));
    ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(1),
        },
    ));
    let deposit = execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: AccountId::new(1),
            amount: amount(8),
        },
    );
    let original = ledger.apply(deposit.clone());

    let mut restored =
        Ledger::from_snapshot(bounds, ledger.snapshot()).expect("valid snapshot restores");
    assert_eq!(restored.view(), ledger.view());
    assert_eq!(restored.apply(deposit).response, original.response);
    assert_eq!(
        restored
            .apply(execute(
                0,
                1,
                2,
                Mutation::CloseAccount {
                    account_id: AccountId::new(1)
                }
            ))
            .response,
        LedgerResponse::Rejected(RequestRejection::ConflictingRetry)
    );
    assert_eq!(restored.account_balance(AccountId::new(1)), Some(8));
}

#[test]
fn configured_bounds_reject_without_evicting_live_state() {
    let mut ledger = Ledger::new(config(1, 1));
    assert_eq!(
        ledger.apply(open_session(1, 1)).response,
        LedgerResponse::Rejected(RequestRejection::ClientOutOfRange)
    );
    ledger.apply(open_session(0, 1));
    ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(1),
        },
    ));
    assert_eq!(
        ledger
            .apply(execute(
                0,
                1,
                2,
                Mutation::OpenAccount {
                    account_id: AccountId::new(2)
                }
            ))
            .response,
        LedgerResponse::Mutation(MutationResult::Rejected(
            BusinessRejection::AccountCapacityExceeded
        ))
    );
    assert_eq!(ledger.account_balance(AccountId::new(1)), Some(0));
}

#[test]
fn history_vocabulary_covers_every_terminal_outcome_a_client_can_observe() {
    let operation_id = OperationId::new(7);
    let account_id = AccountId::new(3);
    let history = [
        HistoryEvent::Invoked {
            operation_id,
            command: open_session(0, 1),
        },
        // A deterministic rejection is an ordinary completion: the client got
        // its answer, and the answer was "no".
        HistoryEvent::Completed {
            operation_id,
            response: LedgerResponse::Rejected(RequestRejection::SessionNotOpen),
        },
        HistoryEvent::Unknown { operation_id },
        HistoryEvent::NotCommitted { operation_id },
        HistoryEvent::QueryInvoked {
            operation_id,
            query: LedgerQuery::GetAccount { account_id },
        },
        HistoryEvent::QueryCompleted {
            operation_id,
            result: LedgerQueryResult::Account {
                account_id,
                balance: Some(0),
            },
        },
        HistoryEvent::QueryAbandoned { operation_id },
    ];

    assert_eq!(operation_id.get(), 7);
    assert_eq!(client(0).get(), 0);
    for event in &history {
        assert_eq!(
            event.operation_id(),
            operation_id,
            "every event names the operation it belongs to"
        );
    }
    // The two lost-outcome events are distinct: one says the command may have
    // committed, the other says it provably did not.
    assert_ne!(history[2], history[3]);
}

#[test]
fn zero_is_unrepresentable_for_epoch_sequence_and_amount() {
    assert_eq!(rafter_reference_ledger::SessionEpoch::new(0), None);
    assert_eq!(rafter_reference_ledger::Sequence::new(0), None);
    assert_eq!(rafter_reference_ledger::Amount::new(0), None);
}

#[test]
fn execute_against_unopened_session_is_rejected() {
    let mut ledger = Ledger::new(config(1, 1));
    let outcome = ledger.apply(Command::Execute {
        request: rafter_reference_ledger::RequestIdentity {
            client_id: client(0),
            session_epoch: epoch(1),
            sequence: sequence(1),
        },
        mutation: Mutation::OpenAccount {
            account_id: AccountId::new(1),
        },
    });

    assert_eq!(
        outcome.response,
        LedgerResponse::Rejected(RequestRejection::SessionNotOpen)
    );
}
