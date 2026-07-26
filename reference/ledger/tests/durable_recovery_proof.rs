//! What opening is allowed to discard, asserted from the attacker's side.
//!
//! These began as an adversarial hunt against the store's stated recovery
//! claims and every one of them failed. They are kept because the claims they
//! attack are the ones a shape test can appear to satisfy while being false:
//! each fixture presents a tail that *looks* like an interrupted append and is
//! not one, and asserts that opening says so rather than shortening the file.
//!
//! The single fact under all of them is the frame mark. An append writes a
//! frame's first byte unsealed before any other byte and promotes it only after
//! the rest of the frame is durable, so a tail no commit point covered can be
//! proved rather than inferred from where the scan stopped — and a sealed frame
//! that lost bytes afterwards, which is a strict prefix of something this build
//! wrote and passes every shape test, is not that tail.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::{raw_journal, LedgerStore, LedgerStoreError, TornTail},
    AccountId, ApplyOutcome, Command, DurableLedgerError, DurableLedgerStateMachine, Mutation,
};

use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);
const BETA: AccountId = AccountId::new(12);

fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
        execute(0, 1, 2, Mutation::OpenAccount { account_id: BETA }),
        execute(
            0,
            1,
            3,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(40),
            },
        ),
        execute(
            0,
            1,
            4,
            Mutation::Transfer {
                from: ALPHA,
                to: BETA,
                amount: amount(15),
            },
        ),
    ]
}

fn open(directory: &Path) -> DurableLedgerStateMachine {
    let store = LedgerStore::open(directory, config(2, 4)).expect("a ledger store opens");
    DurableLedgerStateMachine::new(store)
}

fn apply_one(
    app: &mut DurableLedgerStateMachine,
    index: LogIndex,
    command: Command,
) -> Result<ApplyOutcome, DurableLedgerError> {
    app.apply_batch(ApplyBatch {
        entries: vec![ApplyEntry {
            index,
            term: Term(1),
            command,
            local_proposal_id: None,
        }],
    })
    .map(|mut results| results.pop().expect("one entry, one result").result)
}

fn apply_all(app: &mut DurableLedgerStateMachine, commands: &[Command]) {
    for (position, command) in commands.iter().enumerate() {
        apply_one(app, LogIndex(position as u64 + 1), command.clone())
            .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
}

// ---------------------------------------------------------------------------
// The one-byte case, which is the whole argument in miniature.
//
// A journal that loses its last byte ends in a strict prefix of a frame this
// build wrote, carrying this store's magic and version and failing no checksum
// over the bytes present. Every shape test says "interrupted append"; the frame
// it ends in was committed and acknowledged. Losing one byte used to delete a
// whole 157-byte frame from the medium during a read, undo a transfer, and
// report the loss as "bytes no commit point ever covered".
// ---------------------------------------------------------------------------

#[test]
fn a_shortened_journal_must_not_have_its_last_committed_frame_deleted_by_a_read() {
    let scratch = ScratchDir::new("probe-shortened-journal");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let applied = app.store().applied_index();
    let frames = app.store().recovery().committed_frames();
    let view_before = app.ledger().view();
    let len_before = app.store().journal_len();
    drop(app);

    // Drop exactly one byte. This is not an append: the last frame is sealed,
    // acknowledged, and complete on the medium the store handed back `Ok` for.
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    bytes.pop();
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Err(LedgerStoreError::UnreadableFrame { .. }) => {}
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => {
            let report = *store.recovery();
            let after = raw_journal::read(scratch.path()).expect("the journal reads back");
            panic!(
                "`open` deleted a committed transaction from the medium and returned Ok.\n\
                 journal length      {len_before} -> {} -> {} (on disk after open)\n\
                 applied index       {applied} -> {}\n\
                 committed frames    {frames} -> {}\n\
                 torn_tail()         = {:?}   (is_interrupted_append = {:?})\n\
                 discarded_bytes()   = {}  <- documented as \"bytes no commit point ever covered\"\n\
                 repair()            = {:?} <- documented as the only thing that loses commits\n\
                 is_clean()          = {}\n\
                 balances before     = {:?}\n\
                 balances after      = {:?}",
                bytes.len(),
                after.len(),
                store.applied_index(),
                report.committed_frames(),
                report.torn_tail(),
                report.torn_tail().map(TornTail::is_interrupted_append),
                report.discarded_bytes(),
                report.repair(),
                report.is_clean(),
                view_before.accounts,
                store.ledger().view().accounts,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The same, landing in a different record. Which record the loss happens to
// land in is not a fact about whether the frame was committed, so it must not
// change the verdict.
// ---------------------------------------------------------------------------

#[test]
fn a_journal_truncated_inside_a_committed_image_must_not_be_silently_shortened() {
    let scratch = ScratchDir::new("probe-truncated-image");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let applied = app.store().applied_index();
    drop(app);

    // Land inside the last frame's image: 13 (commit record) + a few image
    // bytes off the end.
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let keep = bytes.len() - 20;
    bytes.truncate(keep);
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Err(_) => {}
        Ok(store) => {
            let report = *store.recovery();
            panic!(
                "`open` shortened a journal past a committed frame and returned Ok: \
                 applied index {applied} -> {}, torn_tail = {:?} \
                 (is_interrupted_append = {:?}), discarded_bytes = {}, repair = {:?}",
                store.applied_index(),
                report.torn_tail(),
                report.torn_tail().map(TornTail::is_interrupted_append),
                report.discarded_bytes(),
                report.repair(),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// A zero-filled tail — what a delayed-allocation filesystem leaves after a
// crash that extended a file without persisting its data — is the ordinary
// residue of a crash, and it used to be the one residue this store called
// fatal. Sixty-four zeros refused the store and left a destructive repair as
// the only documented way forward.
// ---------------------------------------------------------------------------

#[test]
fn a_zero_filled_tail_is_an_interrupted_append() {
    let scratch = ScratchDir::new("probe-zero-tail");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    drop(app);

    // An append that reached the medium as a length extension with unwritten
    // (zero) data: 64 zero bytes past the last committed frame.
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    bytes.extend(std::iter::repeat_n(0_u8, 64));
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Ok(store) => {
            let report = *store.recovery();
            // `is_truncatable_residue` rather than `is_interrupted_append`: a
            // zero-filled tail is `TornTail::ZeroFilledToEnd`, which is
            // truncated on the delayed-allocation premise this test is named
            // for and not on the interrupted-append proof. The two are separate
            // predicates precisely so this line has to say which it means.
            assert!(
                report
                    .torn_tail()
                    .is_some_and(TornTail::is_truncatable_residue),
                "a zero-filled tail classified as {:?}",
                report.torn_tail()
            );
        }
        Err(error) => panic!(
            "a 64-byte zero-filled tail — the ordinary residue of a crash on a \
             delayed-allocation filesystem — refuses the store and can only be cleared by the \
             destructive repair path: {error}"
        ),
    }
}

// ---------------------------------------------------------------------------
// The repair path reaches every unreadable region `open` refuses over, so
// "refuses to open" is never a dead end.
// ---------------------------------------------------------------------------

#[test]
fn open_and_repair_can_clear_every_unreadable_region_open_refuses() {
    let scratch = ScratchDir::new("probe-repair-coverage");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    drop(app);

    // Corrupt the FIRST committed frame's image, then re-seal the frame so the
    // framing checksums all verify and the failure lands in `decode_snapshot`
    // / `from_snapshot` rather than in `read_frame`. Simplest reachable form:
    // flip a byte in the first image and leave the framing checksums stale, so
    // `open` reports `ImageCorrupt` — which is an unreadable frame.
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let first_image = 21 + 17; // HEADER_LEN + BEGIN_LEN
    bytes[first_image] ^= 0x01;
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let refused = LedgerStore::open(scratch.path(), config(2, 4));
    assert!(
        matches!(refused, Err(LedgerStoreError::UnreadableFrame { .. })),
        "a corrupt first image must refuse: {refused:?}"
    );

    let repaired =
        LedgerStore::open_and_repair(scratch.path(), config(2, 4)).expect("repair clears it");
    let repair = repaired
        .recovery()
        .repair()
        .expect("the repair is reported");
    assert_eq!(
        repair.offset(),
        21,
        "the repair discards from the first frame"
    );
    // The whole history is gone and the report cannot say how many
    // transactions were in it.
    assert_eq!(repaired.applied_index(), LogIndex::ZERO);
}

// ---------------------------------------------------------------------------
// The staging sweep used to delete anything named `ledger.journal.*` —
// including the copy an operator makes before running the repair the process
// itself tells them to run — and reported the loss as one boolean.
// ---------------------------------------------------------------------------

#[test]
fn the_staging_sweep_must_not_delete_an_operators_copy_of_the_journal() {
    let scratch = ScratchDir::new("probe-sweep-backup");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    drop(app);

    // The obvious thing to do before running `--repair-app-store true`, which
    // the process's own NEEDS_REPAIR message asks an operator to run.
    let journal = scratch.path().join("ledger.journal");
    let backup = scratch.path().join("ledger.journal.backup-2026-07-25");
    std::fs::copy(&journal, &backup).expect("the operator copies the journal aside");
    let backed_up = std::fs::metadata(&backup).expect("the copy exists").len();

    let store = LedgerStore::open(scratch.path(), config(2, 4)).expect("the store opens");
    let report = *store.recovery();

    assert!(
        backup.exists(),
        "opening the store deleted the operator's {backed_up}-byte copy of the journal. \
         The whole report of that loss is removed_staged_file() = {}, is_clean() = {}; no name, \
         no count, no size. `sweep_staged_files` removes every entry whose name starts with \
         \"ledger.journal.\".",
        report.removed_staged_file(),
        report.is_clean(),
    );
}

// ---------------------------------------------------------------------------
// A vanished journal is recreated empty, which is correct and is also exactly
// what a fresh replica looks like. It used to be reported as a clean opening,
// with `created()` read by nothing outside the test suites.
// ---------------------------------------------------------------------------

#[test]
fn a_vanished_journal_is_not_reported_as_a_clean_opening() {
    let scratch = ScratchDir::new("probe-vanished-journal");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let applied = app.store().applied_index();
    drop(app);

    std::fs::remove_file(scratch.path().join("ledger.journal")).expect("the journal is removed");

    let store = LedgerStore::open(scratch.path(), config(2, 4)).expect("the store opens");
    let report = *store.recovery();
    assert!(
        !report.is_clean(),
        "a store whose journal vanished opened empty (applied index {applied} -> {}) and reported \
         is_clean() = true, created() = {}. `is_clean` is what `replica.rs` branches on; nothing \
         in the process reads `created()`.",
        store.applied_index(),
        report.created(),
    );
}
