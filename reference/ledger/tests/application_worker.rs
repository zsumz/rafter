//! Exact-package proof for the reusable durable-application fast path.
//!
//! This crate is outside Rafter's root workspace and its package lane rejects
//! checkout paths and unpublished hooks. The test therefore represents what
//! an external application can compose from the public package archives: its
//! own transactional store, the runtime's bounded worker, durable completion
//! before reply release, and recovery from the application's applied floor.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use rafter::{LocalProposalId, LogIndex, Term};
use rafter_app::state_machine::ApplyEntry;
use rafter_reference_ledger::{
    store::LedgerStore, AccountId, DurableLedgerStateMachine, LedgerApplicationEntry, Mutation,
};
use rafter_runtime::application::{
    ApplicationEntry, ApplicationEvent, ApplicationSubmitRejection, ApplicationWorker,
    ApplicationWorkerOptions,
};

use scratch::ScratchDir;
use support::{config, execute, open_session};

const ACCOUNT: AccountId = AccountId::new(19);

fn entry(index: u64, command: rafter_reference_ledger::Command) -> LedgerApplicationEntry {
    LedgerApplicationEntry::new(ApplyEntry {
        index: LogIndex(index),
        term: Term(3),
        command,
        local_proposal_id: Some(LocalProposalId(index + 40)),
    })
}

#[test]
fn bounded_worker_returns_only_durable_results_and_reopens_at_the_consumed_floor() {
    let scratch = ScratchDir::new("application-worker-package-consumer");
    let first = entry(1, open_session(0, 1));
    let second = entry(
        2,
        execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: ACCOUNT,
            },
        ),
    );
    let byte_credit = first.retained_bytes().max(second.retained_bytes());
    let store = LedgerStore::open(scratch.path(), config(1, 2)).expect("ledger store opens");
    let application = DurableLedgerStateMachine::new(store, scratch.path().join("raft/snapshots"));
    let mut worker = ApplicationWorker::start(
        application,
        ApplicationWorkerOptions::new().with_limits(1, byte_credit, 1, byte_credit),
        || {},
    )
    .expect("application worker starts");

    worker
        .try_submit(vec![first])
        .expect("the first committed entry is accepted");
    let refusal = worker
        .try_submit(vec![second])
        .expect_err("unconsumed completion retains the single entry credit");
    assert!(matches!(
        refusal.rejection(),
        ApplicationSubmitRejection::Full {
            available_entries: 0,
            ..
        }
    ));
    let mut refused = refusal.into_entries();
    let second = refused.pop().expect("the refused entry is returned");
    assert!(refused.is_empty());

    let first_completion = worker.complete().expect("the first result arrives");
    let ApplicationEvent::Applied(first_completion) = first_completion else {
        panic!("the durable ledger must not fail");
    };
    let (first_entries, first_outcomes) = first_completion.into_parts();
    assert_eq!(first_entries[0].as_apply_entry().index, LogIndex(1));
    assert_eq!(first_outcomes[0].index, LogIndex(1));
    assert_eq!(first_outcomes[0].term, Term(3));
    assert_eq!(
        first_outcomes[0].local_proposal_id,
        Some(LocalProposalId(41))
    );
    assert_eq!(worker.durable_through(), LogIndex(1));
    assert_eq!(worker.available(), (1, byte_credit));

    worker
        .try_submit(vec![second])
        .expect("consuming the first durable result releases its credit");
    let second_completion = worker.complete().expect("the second result arrives");
    let ApplicationEvent::Applied(second_completion) = second_completion else {
        panic!("the durable ledger must not fail");
    };
    assert_eq!(second_completion.entries()[0].log_index(), LogIndex(2));
    assert_eq!(second_completion.outcomes()[0].index, LogIndex(2));
    assert_eq!(worker.durable_through(), LogIndex(2));
    let application = worker
        .shutdown_into_store()
        .expect("the idle worker returns the external application's store");
    assert_eq!(application.store().applied_index(), LogIndex(2));
    drop(application);

    let reopened =
        LedgerStore::open(scratch.path(), config(1, 2)).expect("durable application reopens");
    assert_eq!(reopened.applied_index(), LogIndex(2));
    assert_eq!(reopened.ledger().view().accounts, vec![(ACCOUNT, 0)]);
}
