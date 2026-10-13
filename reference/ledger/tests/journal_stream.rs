//! Streamed publication preserves exact byte cuts, atomic floors, and v1 recovery.

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
    store::{
        raw_journal, FaultPlan, LedgerStore, LedgerStoreError, TornTail, WriteFault, HEADER_LEN,
    },
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

fn append_cut(app: &LedgerStateMachine, frame: &[u8], fault: WriteFault) {
    let accounts = app.ledger().view().accounts.len();
    let scratch = ScratchDir::new("journal-stream-append-cut");
    let mut store =
        LedgerStore::open_with_faults(scratch.path(), config(1, accounts), FaultPlan::at(2, fault))
            .unwrap();
    let old = prior(accounts);
    store.commit(&old, LogIndex(3)).unwrap();
    let before = raw_journal::read(scratch.path()).unwrap();
    let at = app.applied_index().unwrap();
    assert!(matches!(store.commit(app.ledger(), at),
        Err(LedgerStoreError::InjectedFault { fault: found, plan: 2 }) if found == fault));
    assert_eq!(store.fired_fault(), Some(fault));
    assert!(store.requires_reopen());
    assert_eq!(store.applied_index(), LogIndex(3));
    assert_eq!(store.ledger(), &old);
    assert_eq!(store.journal_len(), before.len() as u64);
    assert!(matches!(
        store.commit(app.ledger(), at),
        Err(LedgerStoreError::StoreRequiresReopen)
    ));
    let length = match fault {
        WriteFault::BeforeFirstByte => 0,
        WriteFault::AfterBytes(stop) => usize::try_from(stop).unwrap(),
        WriteFault::AtFileSync | WriteFault::BeforeSeal | WriteFault::AtSealSync => frame.len(),
        other => panic!("not an append fault: {other:?}"),
    };
    let mut tail = frame[..length].to_vec();
    if length != 0 && fault != WriteFault::AtSealSync {
        tail[0] = 0;
    }
    let mut bytes = before.clone();
    bytes.extend_from_slice(&tail);
    assert_eq!(raw_journal::read(scratch.path()).unwrap(), bytes);
    drop(store);
    let recovered = LedgerStore::open(scratch.path(), config(1, accounts));
    if matches!(fault, WriteFault::AtFileSync | WriteFault::BeforeSeal) {
        assert!(matches!(recovered, Err(LedgerStoreError::UnreadableFrame {
            corruption: TornTail::UnsealedCompleteFrame { len }, ..
        }) if len == frame.len() as u64));
        // A complete unsealed frame is ambiguous; a read cannot repair it.
        assert_eq!(raw_journal::read(scratch.path()).unwrap(), bytes);
    } else {
        let mut recovered = recovered.unwrap();
        let sealed = fault == WriteFault::AtSealSync;
        assert_eq!(recovered.ledger(), if sealed { app.ledger() } else { &old });
        assert_eq!(
            recovered.applied_index(),
            if sealed { at } else { LogIndex(3) }
        );
        assert_eq!(
            recovered.recovery().discarded_bytes(),
            if sealed { 0 } else { length as u64 }
        );
        assert_eq!(
            raw_journal::read(scratch.path()).unwrap(),
            if sealed { bytes } else { before }
        );
        // Subsequent publication uses the recovered end, after any truncation.
        recovered.commit(app.ledger(), LogIndex(at.0 + 1)).unwrap();
        let end = recovered.journal_len();
        drop(recovered);
        let reopened = LedgerStore::open(scratch.path(), config(1, accounts)).unwrap();
        assert_eq!(reopened.ledger(), app.ledger());
        assert_eq!(reopened.applied_index(), LogIndex(at.0 + 1));
        assert_eq!(reopened.journal_len(), end);
    }
}

#[test]
fn append_fault_offsets_span_buffers_and_preserve_the_sync_then_seal_boundary() {
    let app = replacement(4096);
    let bytes = expected(4096, &app);
    let frame = &bytes[HEADER_LEN..];
    for stop in [
        0,
        1,
        2,
        16_383,
        16_384,
        16_385,
        32_767,
        32_768,
        32_769,
        65_535,
        65_536,
        65_537,
        frame.len() as u64 - 13,
        frame.len() as u64 - 1,
    ] {
        append_cut(&app, frame, WriteFault::AfterBytes(stop));
    }
    for fault in [
        WriteFault::BeforeFirstByte,
        WriteFault::AtFileSync,
        WriteFault::BeforeSeal,
        WriteFault::AtSealSync,
    ] {
        append_cut(&app, frame, fault);
    }
}

#[test]
fn a_completed_append_seals_once_and_preserves_exact_frame_bytes_and_offsets() {
    let app = replacement(2048);
    let at = app.applied_index().unwrap();
    let bytes = expected(2048, &app);
    let frame = &bytes[HEADER_LEN..];
    for stop in [frame.len() as u64, frame.len() as u64 + 1] {
        let scratch = ScratchDir::new("journal-stream-append-complete");
        let mut store = LedgerStore::open_with_faults(
            scratch.path(),
            config(1, 2048),
            FaultPlan::at(2, WriteFault::AfterBytes(stop)),
        )
        .unwrap();
        store.commit(&prior(2048), LogIndex(3)).unwrap();
        let mut expected = raw_journal::read(scratch.path()).unwrap();
        expected.extend_from_slice(frame);
        store.commit(app.ledger(), at).unwrap();
        assert_eq!(store.fired_fault(), None);
        assert!(!store.requires_reopen());
        assert_eq!(raw_journal::read(scratch.path()).unwrap(), expected);
        assert_eq!(store.journal_len(), expected.len() as u64);
        drop(store);
        let reopened = LedgerStore::open(scratch.path(), config(1, 2048)).unwrap();
        assert_eq!(reopened.ledger(), app.ledger());
        assert_eq!(reopened.applied_index(), at);
        assert_eq!(reopened.recovery().committed_frames(), 2);
    }
}

#[test]
fn large_file_recovery_preserves_refusal_repair_and_subsequent_append_offsets() {
    let accounts = 131_072;
    let app = replacement(accounts);
    let at = app.applied_index().unwrap();
    let scratch = ScratchDir::new("journal-stream-large-recovery");
    let mut store = LedgerStore::open(scratch.path(), config(1, accounts)).unwrap();
    store.commit(app.ledger(), at).unwrap();
    let first_end = store.journal_len();
    store.commit(app.ledger(), LogIndex(at.0 + 1)).unwrap();
    store.commit(app.ledger(), LogIndex(at.0 + 2)).unwrap();
    let end = store.journal_len();
    assert!(end > 6 * 1024 * 1024);
    drop(store);
    let reopened = LedgerStore::open(scratch.path(), config(1, accounts)).unwrap();
    assert_eq!(reopened.ledger(), app.ledger());
    assert_eq!(reopened.applied_index(), LogIndex(at.0 + 2));
    assert_eq!(reopened.journal_len(), end);
    assert_eq!(reopened.recovery().committed_frames(), 3);
    drop(reopened);

    let mut bytes = raw_journal::read(scratch.path()).unwrap();
    // Corrupt an image in the middle of the file, beyond a read-buffer boundary.
    bytes[usize::try_from(first_end).unwrap() + 32_769] ^= 1;
    raw_journal::write(scratch.path(), &bytes).unwrap();
    assert!(
        matches!(LedgerStore::open(scratch.path(), config(1, accounts)),
        Err(LedgerStoreError::UnreadableFrame {
            offset, corruption: TornTail::ImageCorrupt, committed_frames: 1, unreadable_bytes
        }) if offset == first_end && unreadable_bytes == end - first_end)
    );
    assert_eq!(raw_journal::read(scratch.path()).unwrap(), bytes);

    let mut repaired = LedgerStore::open_and_repair(scratch.path(), config(1, accounts)).unwrap();
    assert_eq!(repaired.applied_index(), at);
    assert_eq!(repaired.ledger(), app.ledger());
    assert_eq!(repaired.journal_len(), first_end);
    let repair = repaired.recovery().repair().unwrap();
    assert_eq!(repair.discarded_bytes(), end - first_end);
    assert_eq!(
        raw_journal::read(scratch.path()).unwrap(),
        bytes[..usize::try_from(first_end).unwrap()]
    );
    repaired.commit(app.ledger(), LogIndex(at.0 + 3)).unwrap();
    drop(repaired);
    let reopened = LedgerStore::open(scratch.path(), config(1, accounts)).unwrap();
    assert_eq!(reopened.ledger(), app.ledger());
    assert_eq!(reopened.applied_index(), LogIndex(at.0 + 3));
    assert_eq!(reopened.recovery().committed_frames(), 2);
}
