//! Streamed replacement preserves exact byte cuts, atomic floors, and v1 recovery.

#[path = "support/scratch.rs"]
mod scratch;
#[allow(dead_code)]
#[path = "support/snapshot_writer.rs"]
mod setup;
#[allow(dead_code)]
mod support;

use rafter::LogIndex;
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_ledger::{
    store::{raw_journal, FaultPlan, LedgerStore, LedgerStoreError, WriteFault, HEADER_LEN},
    AccountId, Ledger, LedgerStateMachine, Mutation,
};
use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

fn replacement(accounts: usize) -> LedgerStateMachine {
    let mut app = setup::large(accounts);
    let at = app.applied_index().unwrap().0 + 1;
    // Keep a cached mutation/result in the replacement, not just accounts.
    app.apply_batch(setup::batch(
        at,
        [execute(
            0,
            2,
            1,
            Mutation::Deposit {
                account_id: AccountId::new(0),
                amount: amount(3),
            },
        )],
    ))
    .unwrap();
    app
}

fn prior(accounts: usize) -> Ledger {
    let mut ledger = Ledger::new(config(1, accounts));
    ledger.apply(open_session(0, 1));
    ledger.apply(execute(
        0,
        1,
        1,
        Mutation::OpenAccount {
            account_id: AccountId::new(0),
        },
    ));
    ledger.apply(execute(
        0,
        1,
        2,
        Mutation::Deposit {
            account_id: AccountId::new(0),
            amount: amount(1),
        },
    ));
    ledger
}

fn expected(accounts: usize, app: &LedgerStateMachine) -> Vec<u8> {
    let scratch = ScratchDir::new("journal-stream-expected");
    let mut store = LedgerStore::open(scratch.path(), config(1, accounts)).unwrap();
    store
        .replace(app.ledger(), app.applied_index().unwrap())
        .unwrap();
    assert_eq!(
        store.journal_len(),
        HEADER_LEN as u64
            + LedgerStore::planned_frame_len(app.ledger(), app.applied_index().unwrap()).unwrap()
    );
    raw_journal::read(scratch.path()).unwrap()
}

fn cut(accounts: usize, app: &LedgerStateMachine, bytes: &[u8], fault: WriteFault) {
    let scratch = ScratchDir::new("journal-stream-cut");
    let mut store =
        LedgerStore::open_with_faults(scratch.path(), config(1, accounts), FaultPlan::at(2, fault))
            .unwrap();
    let old = prior(accounts);
    store.commit(&old, LogIndex(3)).unwrap();
    let before = raw_journal::read(scratch.path()).unwrap();
    let error = store
        .replace(app.ledger(), app.applied_index().unwrap())
        .unwrap_err();
    assert!(
        matches!(error, LedgerStoreError::InjectedFault { fault: found, plan: 2 } if found == fault)
    );
    assert_eq!(store.fired_fault(), Some(fault));
    assert!(store.requires_reopen());
    assert_eq!(store.applied_index(), LogIndex(3));
    assert_eq!(store.ledger(), &old);
    assert_eq!(store.journal_len(), before.len() as u64);
    assert!(matches!(
        store.replace(app.ledger(), app.applied_index().unwrap()),
        Err(LedgerStoreError::StoreRequiresReopen)
    ));
    let staged = scratch.path().join("ledger.journal.tmp");
    match fault {
        WriteFault::AfterBytes(stop) => assert_eq!(
            std::fs::read(&staged).unwrap(),
            bytes[..usize::try_from(stop).unwrap()]
        ),
        WriteFault::AtFileSync | WriteFault::BeforeRename => {
            assert_eq!(std::fs::read(&staged).unwrap(), bytes);
        }
        WriteFault::BeforeFirstByte | WriteFault::AfterRename => assert!(!staged.exists()),
        other => panic!("not a rewrite fault: {other:?}"),
    }
    let published = fault == WriteFault::AfterRename;
    assert_eq!(
        raw_journal::read(scratch.path()).unwrap(),
        if published { bytes } else { &before }
    );
    drop(store);
    let reopened = LedgerStore::open(scratch.path(), config(1, accounts)).unwrap();
    assert!(!reopened.requires_reopen());
    assert_eq!(
        reopened.ledger(),
        if published { app.ledger() } else { &old }
    );
    assert_eq!(
        reopened.applied_index(),
        if published {
            app.applied_index().unwrap()
        } else {
            LogIndex(3)
        }
    );
    assert!(!staged.exists());
    assert_eq!(reopened.recovery().torn_tail(), None);
}

#[test]
fn every_byte_cut_of_a_replacement_keeps_the_old_image_until_rename() {
    let app = replacement(2);
    let bytes = expected(2, &app);
    for stop in 0..bytes.len() as u64 {
        cut(2, &app, &bytes, WriteFault::AfterBytes(stop));
    }
    for fault in [
        WriteFault::BeforeFirstByte,
        WriteFault::AtFileSync,
        WriteFault::BeforeRename,
        WriteFault::AfterRename,
    ] {
        cut(2, &app, &bytes, fault);
    }
}

#[test]
fn byte_fault_offsets_span_multiple_encoder_buffers_and_the_final_commit_record() {
    let app = replacement(4096);
    let bytes = expected(4096, &app);
    for stop in [
        16_383,
        16_384,
        16_385,
        32_767,
        32_768,
        32_769,
        65_535,
        65_536,
        65_537,
        bytes.len() as u64 - 13,
        bytes.len() as u64 - 1,
    ] {
        cut(4096, &app, &bytes, WriteFault::AfterBytes(stop));
    }
}

#[test]
fn a_byte_limit_at_or_after_completion_does_not_fire_and_next_append_uses_the_new_end() {
    let app = replacement(2048);
    let at = app.applied_index().unwrap();
    let bytes = expected(2048, &app);
    for stop in [bytes.len() as u64, bytes.len() as u64 + 1] {
        let scratch = ScratchDir::new("journal-stream-complete");
        let mut store = LedgerStore::open_with_faults(
            scratch.path(),
            config(1, 2048),
            FaultPlan::at(1, WriteFault::AfterBytes(stop)),
        )
        .unwrap();
        store.replace(app.ledger(), at).unwrap();
        assert_eq!(store.fired_fault(), None);
        assert!(!store.requires_reopen());
        assert_eq!(raw_journal::read(scratch.path()).unwrap(), bytes);
        assert_eq!(store.journal_len(), bytes.len() as u64);
        store.commit(app.ledger(), LogIndex(at.0 + 1)).unwrap();
        let length = store.journal_len();
        drop(store);
        let reopened = LedgerStore::open(scratch.path(), config(1, 2048)).unwrap();
        assert_eq!(reopened.applied_index(), LogIndex(at.0 + 1));
        assert_eq!(reopened.ledger(), app.ledger());
        assert_eq!(reopened.journal_len(), length);
        assert_eq!(reopened.recovery().committed_frames(), 2);
    }
}
