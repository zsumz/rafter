//! Focused tests for the history checker itself.
//!
//! These histories are written by hand rather than recorded from a cluster, so
//! each one isolates exactly one thing the checker must do. A checker that only
//! ever ran over real histories would be trusted on the strength of never
//! having failed; these tests make it fail on demand.

mod support;

use rafter_reference_ledger::{
    check_linearizable, AccountId, BlockedReason, CheckError, Command, HistoryDefect, HistoryEvent,
    LedgerQuery, LedgerQueryResult, LedgerResponse, Mutation, MutationResult, OperationId,
    MAX_HISTORY_OPERATIONS,
};
use support::{amount, config, epoch, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);
const BETA: AccountId = AccountId::new(12);

#[test]
fn sequential_traffic_that_matches_the_specification_is_linearizable() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    history.ran(deposit(0, 2, ALPHA, 5), mutation(deposited(5)));
    history.read(balance_of(ALPHA), balance(ALPHA, Some(5)));

    let report =
        check_linearizable(config(2, 4), history.events()).expect("this history is linearizable");
    assert_eq!(report.checked_operations(), 4);
    assert_eq!(report.discharged_operations(), 0);
}

#[test]
fn concurrent_operations_may_be_reordered_against_their_invocation_order() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(open_session(1, 1), session_opened(1));
    history.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    history.ran(deposit(0, 2, ALPHA, 10), mutation(deposited(10)));

    // The transfer is invoked first and returns first, but it can only succeed
    // after the account it targets exists. Their intervals overlap, so real
    // time permits either order and only one of the two is legal.
    let transfer = history.invoke(execute(
        0,
        1,
        3,
        Mutation::Transfer {
            from: ALPHA,
            to: BETA,
            amount: amount(4),
        },
    ));
    let open_beta = history.invoke(open_account(1, 1, BETA));
    history.complete(
        transfer,
        mutation(MutationResult::Transferred {
            from_balance: 6,
            to_balance: 4,
        }),
    );
    history.complete(open_beta, mutation(MutationResult::AccountOpened));

    let report =
        check_linearizable(config(2, 4), history.events()).expect("this history is linearizable");
    assert_eq!(report.checked_operations(), 6);
}

#[test]
fn a_read_that_no_ordering_can_produce_is_rejected_with_evidence() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    history.ran(deposit(0, 2, ALPHA, 5), mutation(deposited(5)));
    // Nothing in this history deposits ninety more.
    history.read(balance_of(ALPHA), balance(ALPHA, Some(95)));

    let Err(CheckError::NotLinearizable(violation)) =
        check_linearizable(config(2, 4), history.events())
    else {
        panic!("a balance no ordering produces must be rejected");
    };

    assert_eq!(
        violation.placed().len(),
        3,
        "the three writes are explainable"
    );
    assert_eq!(violation.blocked().len(), 1);
    assert_eq!(violation.blocked()[0].operation_id, OperationId::new(4));
    assert_eq!(
        violation.blocked()[0].reason,
        BlockedReason::QueryMismatch {
            expected: balance(ALPHA, Some(5)),
            observed: balance(ALPHA, Some(95)),
        }
    );

    // The failure has to be replayable from what it prints, so the rendered
    // evidence carries the whole history, not just a verdict.
    let rendered = CheckError::NotLinearizable(violation).to_string();
    assert!(rendered.contains("no real-time ordering explains this history"));
    assert!(rendered.contains("QueryCompleted"));
    assert!(rendered.contains("Deposited"));
}

#[test]
fn an_unknown_outcome_is_read_both_ways() {
    // Read as never-run: a later query proves the deposit did not take effect.
    let mut never_ran = History::default();
    never_ran.ran(open_session(0, 1), session_opened(1));
    never_ran.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    let lost = never_ran.invoke(deposit(0, 2, ALPHA, 25));
    never_ran.unknown(lost);
    never_ran.read(balance_of(ALPHA), balance(ALPHA, Some(0)));
    check_linearizable(config(2, 4), never_ran.events())
        .expect("an unknown outcome may be read as never having run");

    // Read as taken-effect: the same history, except the query proves it did.
    // Trying one reading first is an implementation choice, so exactly one of
    // these two histories forces the search to backtrack into the other branch.
    let mut took_effect = History::default();
    took_effect.ran(open_session(0, 1), session_opened(1));
    took_effect.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    let lost = took_effect.invoke(deposit(0, 2, ALPHA, 25));
    took_effect.unknown(lost);
    took_effect.read(balance_of(ALPHA), balance(ALPHA, Some(25)));
    check_linearizable(config(2, 4), took_effect.events())
        .expect("an unknown outcome may be read as having taken effect");
}

#[test]
fn a_not_committed_operation_that_took_effect_is_caught() {
    // The recorder claimed this deposit provably never replicated, and then a
    // later read observed its effect. Only the stronger terminal event makes
    // that a contradiction.
    let mut refused = History::default();
    refused.ran(open_session(0, 1), session_opened(1));
    refused.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    let never_replicated = refused.invoke(deposit(0, 2, ALPHA, 25));
    refused.not_committed(never_replicated);
    refused.read(balance_of(ALPHA), balance(ALPHA, Some(25)));

    let Err(CheckError::NotLinearizable(violation)) =
        check_linearizable(config(2, 4), refused.events())
    else {
        panic!("a refused command whose effect is visible must be rejected");
    };
    assert_eq!(
        violation.blocked()[0].reason,
        BlockedReason::QueryMismatch {
            expected: balance(ALPHA, Some(0)),
            observed: balance(ALPHA, Some(25)),
        },
        "the specification never ran the refused deposit"
    );

    // The identical history under the weaker terminal event is accepted, which
    // is exactly the strength the new outcome adds: `Unknown` would have let
    // this defect through.
    let mut unresolved = History::default();
    unresolved.ran(open_session(0, 1), session_opened(1));
    unresolved.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    let lost = unresolved.invoke(deposit(0, 2, ALPHA, 25));
    unresolved.unknown(lost);
    unresolved.read(balance_of(ALPHA), balance(ALPHA, Some(25)));
    check_linearizable(config(2, 4), unresolved.events())
        .expect("the same history is explainable when the outcome is merely unknown");
}

#[test]
fn discharged_operations_constrain_no_ordering() {
    let mut history = History::default();
    history.ran(open_session(0, 1), session_opened(1));
    history.ran(
        open_account(0, 1, ALPHA),
        mutation(MutationResult::AccountOpened),
    );
    let refused = history.invoke(deposit(0, 2, ALPHA, 25));
    history.not_committed(refused);
    let unanswered = history.invoke_query(balance_of(ALPHA));
    history.abandon(unanswered);
    history.read(balance_of(ALPHA), balance(ALPHA, Some(0)));

    let report =
        check_linearizable(config(2, 4), history.events()).expect("this history is linearizable");
    assert_eq!(report.checked_operations(), 3);
    assert_eq!(
        report.discharged_operations(),
        2,
        "a refused command and an unanswered query are settled without searching"
    );
}

#[test]
fn malformed_histories_are_refused_rather_than_checked_weakly() {
    let stray = OperationId::new(9);
    assert_eq!(
        check_linearizable(
            config(1, 1),
            &[HistoryEvent::Unknown {
                operation_id: stray
            }]
        ),
        Err(CheckError::Malformed(
            HistoryDefect::TerminalWithoutInvocation {
                operation_id: stray
            }
        ))
    );

    let mut unterminated = History::default();
    let dangling = unterminated.invoke(open_session(0, 1));
    assert_eq!(
        check_linearizable(config(1, 1), unterminated.events()),
        Err(CheckError::Malformed(
            HistoryDefect::UnterminatedOperation {
                operation_id: dangling
            }
        )),
        "a lost completion must fail loudly instead of weakening the check"
    );

    let mut repeated = History::default();
    let once = repeated.invoke(open_session(0, 1));
    repeated.complete(once, session_opened(1));
    repeated.complete(once, session_opened(1));
    assert_eq!(
        check_linearizable(config(1, 1), repeated.events()),
        Err(CheckError::Malformed(HistoryDefect::RepeatedTerminal {
            operation_id: once
        }))
    );

    let mut crossed = History::default();
    let query = crossed.invoke_query(balance_of(ALPHA));
    crossed.complete(query, session_opened(1));
    assert_eq!(
        check_linearizable(config(1, 1), crossed.events()),
        Err(CheckError::Malformed(HistoryDefect::MismatchedTerminal {
            operation_id: query
        }))
    );
}

#[test]
fn a_history_above_the_bound_is_refused_rather_than_truncated() {
    let mut history = History::default();
    for _ in 0..=MAX_HISTORY_OPERATIONS {
        history.ran(open_session(0, 1), session_opened(1));
    }

    assert_eq!(
        check_linearizable(config(1, 1), history.events()),
        Err(CheckError::HistoryTooLong {
            operations: MAX_HISTORY_OPERATIONS + 1,
            bound: MAX_HISTORY_OPERATIONS,
        }),
        "an oversized history is undecided, never quietly shortened"
    );
}

/// Records events the way the cluster driver does: one invocation and one
/// terminal event per operation, in the order the client observed them.
#[derive(Default)]
struct History {
    events: Vec<HistoryEvent>,
    next_operation_id: u64,
}

impl History {
    fn invoke(&mut self, command: Command) -> OperationId {
        let operation_id = self.allocate();
        self.events.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        operation_id
    }

    fn invoke_query(&mut self, query: LedgerQuery) -> OperationId {
        let operation_id = self.allocate();
        self.events.push(HistoryEvent::QueryInvoked {
            operation_id,
            query,
        });
        operation_id
    }

    fn complete(&mut self, operation_id: OperationId, response: LedgerResponse) {
        self.events.push(HistoryEvent::Completed {
            operation_id,
            response,
        });
    }

    fn unknown(&mut self, operation_id: OperationId) {
        self.events.push(HistoryEvent::Unknown { operation_id });
    }

    fn not_committed(&mut self, operation_id: OperationId) {
        self.events
            .push(HistoryEvent::NotCommitted { operation_id });
    }

    fn answer(&mut self, operation_id: OperationId, result: LedgerQueryResult) {
        self.events.push(HistoryEvent::QueryCompleted {
            operation_id,
            result,
        });
    }

    fn abandon(&mut self, operation_id: OperationId) {
        self.events
            .push(HistoryEvent::QueryAbandoned { operation_id });
    }

    /// Records a command that overlapped nothing: invoked, then returned.
    fn ran(&mut self, command: Command, response: LedgerResponse) {
        let operation_id = self.invoke(command);
        self.complete(operation_id, response);
    }

    /// Records a query that overlapped nothing.
    fn read(&mut self, query: LedgerQuery, result: LedgerQueryResult) {
        let operation_id = self.invoke_query(query);
        self.answer(operation_id, result);
    }

    fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    fn allocate(&mut self) -> OperationId {
        self.next_operation_id += 1;
        OperationId::new(self.next_operation_id)
    }
}

fn open_account(client_id: u32, request_sequence: u64, account_id: AccountId) -> Command {
    execute(
        client_id,
        1,
        request_sequence,
        Mutation::OpenAccount { account_id },
    )
}

fn deposit(client_id: u32, request_sequence: u64, account_id: AccountId, value: u64) -> Command {
    execute(
        client_id,
        1,
        request_sequence,
        Mutation::Deposit {
            account_id,
            amount: amount(value),
        },
    )
}

fn session_opened(session_epoch: u64) -> LedgerResponse {
    LedgerResponse::SessionOpened {
        session_epoch: epoch(session_epoch),
    }
}

const fn mutation(result: MutationResult) -> LedgerResponse {
    LedgerResponse::Mutation(result)
}

const fn deposited(value: u64) -> MutationResult {
    MutationResult::Deposited { balance: value }
}

const fn balance_of(account_id: AccountId) -> LedgerQuery {
    LedgerQuery::GetAccount { account_id }
}

const fn balance(account_id: AccountId, value: Option<u64>) -> LedgerQueryResult {
    LedgerQueryResult::Account {
        account_id,
        balance: value,
    }
}
