//! Application crash points over the durable transactional backend.
//!
//! Every test here interrupts a real publication at a named boundary, reopens
//! the store, and asks the same question: is the recovered state exactly the
//! one before the transaction or exactly the one after it? "Exactly" is load
//! bearing. The comparison is over a whole [`DurableState`] — account
//! balances, sessions, cached mutations, cached results, the deposit total,
//! and the applied Raft index together — so a recovery that moved a balance
//! without its cached result, or an applied index without its data, fails here
//! rather than being caught by whichever later assertion happened to look.
//!
//! Injection is deterministic and per-store. Every failure message carries the
//! [`FaultPlan`] that produced it, which is the whole reproduction input.
//!
//! The suite is also required to prove that its own injections bite: a crash
//! test that silently stopped interrupting anything would assert only that an
//! uninterrupted store works. Each scenario asserts that its fault fired, and
//! the byte sweep asserts that it reached every torn-tail shape the format can
//! produce.

mod support;

// The driver is shared with `adapter_cluster.rs`, which is where its read path,
// its unknown-outcome accounting, and its in-memory construction are exercised.
// This suite drives the durable composition and uses a smaller part of it.
#[allow(dead_code)]
#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/durable.rs"]
mod durable;
#[path = "support/scratch.rs"]
mod scratch;
#[path = "support/storage.rs"]
mod storage;

use std::{collections::BTreeSet, path::Path};

use rafter::{LogIndex, NodeId, Term};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyEntry, ReplicatedStateMachine,
};
use rafter_reference_ledger::{
    check_linearizable,
    store::{
        raw_journal, FaultPlan, LedgerStore, LedgerStoreError, TornTail, WriteFault, BEGIN_LEN,
        COMMIT_LEN, HEADER_LEN,
    },
    AccountId, ApplyDisposition, ApplyOutcome, Command, DurableLedgerError,
    DurableLedgerStateMachine, LedgerResponse, LedgerView, Mutation, MutationResult,
    ReferenceLedger,
};

use cluster::LedgerCluster;
use durable::DurableLedgerApps;
use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);
const BETA: AccountId = AccountId::new(12);

/// Everything one transaction is required to move, compared as one value.
///
/// The contract names four things the transaction commits together. Three of
/// them live in the view — account mutations, the session and deduplication
/// mutation, and the cached command result — and the fourth is the applied
/// index beside it. Comparing the pair is how "together" is asserted: there is
/// no way to be equal on one half and not the other.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableState {
    applied_index: LogIndex,
    view: LedgerView,
}

// ---------------------------------------------------------------------------
// Store-level crash points
// ---------------------------------------------------------------------------

#[test]
fn a_crash_at_every_byte_of_a_transaction_recovers_to_exactly_one_side_of_it() {
    let commands = workload();
    let interrupted = commands.len();
    let prefix = &commands[..interrupted - 1];

    // Establish the two legal answers, and the exact frame the interrupted
    // store would have appended, by running the same workload uninterrupted.
    let reference = ScratchDir::new("sweep-reference");
    let mut app = open(reference.path(), FaultPlan::none());
    apply_all(&mut app, prefix);
    let before = durable_state(&app);
    apply_one(
        &mut app,
        index_of(interrupted),
        commands[interrupted - 1].clone(),
    )
    .expect("an uninterrupted transaction commits");
    let after = durable_state(&app);
    let frame_len = LedgerStore::planned_frame_len(app.ledger(), after.applied_index)
        .expect("the frame the sweep interrupts is encodable");
    drop(app);

    assert_ne!(
        before, after,
        "a sweep whose two answers were equal would prove nothing"
    );

    let image_len = frame_len - as_u64(BEGIN_LEN) - as_u64(COMMIT_LEN);
    let mut observed_tails = BTreeSet::new();

    for stop in 0..=frame_len {
        let plan = FaultPlan::at(as_u64(interrupted), WriteFault::AfterBytes(stop));
        let scratch = ScratchDir::new("sweep");
        let mut app = open(scratch.path(), plan.clone());
        apply_all(&mut app, prefix);
        let outcome = apply_one(
            &mut app,
            index_of(interrupted),
            commands[interrupted - 1].clone(),
        );

        let committed = stop == frame_len;
        assert_interrupted_append(&app, outcome, stop, committed, &before, &plan);
        // Dropping the handle is the crash: everything below is what a fresh
        // process can read back.
        drop(app);

        let recovered = open(scratch.path(), FaultPlan::none());
        let expected_tail = expected_tail(stop, frame_len);
        assert_eq!(
            recovered.store().recovery().torn_tail(),
            expected_tail,
            "`{plan}` left a residue the format does not describe"
        );
        assert!(
            expected_tail.is_none_or(TornTail::is_truncatable_residue),
            "an interrupted append must leave a tail a later opener may truncate (`{plan}`)"
        );
        observed_tails.insert(stopped_record(stop, image_len, frame_len));
        assert_eq!(
            durable_state(&recovered),
            if committed {
                after.clone()
            } else {
                before.clone()
            },
            "`{plan}` recovered to neither side of the transaction"
        );
    }

    // The sweep is only evidence if it actually stopped in every record of the
    // frame, including the write-ahead window where the whole image is on the
    // medium and the commit record is not.
    assert_eq!(
        observed_tails,
        BTreeSet::from([
            "before the first byte",
            "inside the begin record",
            "inside the image",
            "a whole image with no commit record",
            "inside the commit record",
            "sealed",
        ]),
        "the sweep missed a record of the frame"
    );
}

#[test]
fn a_crash_before_the_transaction_begins_leaves_the_journal_untouched() {
    let commands = workload();
    let scratch = ScratchDir::new("before-first-byte");
    let plan = FaultPlan::at(1, WriteFault::BeforeFirstByte);
    let mut app = open(scratch.path(), plan.clone());

    apply_one(&mut app, LogIndex(1), commands[0].clone())
        .expect_err("the first transaction never starts");
    assert_eq!(app.store().fired_fault(), Some(WriteFault::BeforeFirstByte));
    assert_eq!(
        app.store().journal_len(),
        as_u64(HEADER_LEN),
        "a transaction that emitted nothing appended nothing (`{plan}`)"
    );
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(recovered.store().recovery().torn_tail(), None);
    assert_eq!(recovered.store().recovery().discarded_bytes(), 0);
    assert_eq!(
        durable_state(&recovered),
        DurableState {
            applied_index: LogIndex::ZERO,
            view: ReferenceLedger::new(config(2, 4)).view(),
        },
        "an empty journal recovers to an empty ledger"
    );
}

#[test]
fn a_written_but_uncommitted_transaction_is_not_a_transaction() {
    let commands = workload();
    let scratch = ScratchDir::new("no-commit-record");
    let (before, image_len) = state_and_image_len(&commands, commands.len());

    // Stop exactly where the image ends: everything the transaction changes is
    // on disk, and the record that says it counts is not. This is the window a
    // write-ahead journal exists to make representable, and the answer has to
    // be that nothing happened.
    let stop = as_u64(BEGIN_LEN) + image_len;
    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(stop));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("the transaction was never committed");
    assert_eq!(
        app.store().fired_fault(),
        Some(WriteFault::AfterBytes(stop))
    );
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        recovered.store().recovery().torn_tail(),
        Some(TornTail::UnsealedAppend { present: stop }),
        "the residue is a written transaction the append never sealed (`{plan}`)"
    );
    assert_eq!(
        recovered.store().recovery().discarded_bytes(),
        stop,
        "every byte of the uncommitted transaction is discarded (`{plan}`)"
    );
    assert_eq!(
        durable_state(&recovered),
        before,
        "an uncommitted transaction changed nothing (`{plan}`)"
    );
}

// ---------------------------------------------------------------------------
// The write-ahead window itself, which the format documents and no test armed.
//
// `WriteFault::BeforeSeal` is the boundary where the whole frame is on the
// medium and the byte that seals it is not. Until the mark carried a
// completeness test beside it, the store answered by truncating, and the answer
// was never asserted — so the byte-for-byte identical case of a committed frame
// whose mark byte rotted was truncated too, silently.
// ---------------------------------------------------------------------------

#[test]
fn a_whole_frame_that_was_never_sealed_is_not_resolved_by_a_read() {
    let commands = workload();
    let scratch = ScratchDir::new("unsealed-whole-frame");
    let (before, image_len) = state_and_image_len(&commands, commands.len());
    let frame_len = as_u64(BEGIN_LEN) + image_len + as_u64(COMMIT_LEN);

    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::BeforeSeal);
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("a frame that was not sealed is not a transaction");
    assert_eq!(app.store().fired_fault(), Some(WriteFault::BeforeSeal));
    drop(app);

    let length_before = raw_journal::read(scratch.path())
        .expect("the journal reads")
        .len();

    // These bytes are a whole frame. The only thing wrong with them is the one
    // byte that says whether they count, and a committed frame whose mark rotted
    // to zero is the same bytes, so `open` refuses instead of choosing between
    // two histories it cannot see.
    let refused = LedgerStore::open(scratch.path(), config(2, 4))
        .expect_err("a whole unsealed frame is not residue `open` may truncate");
    let LedgerStoreError::UnreadableFrame { corruption, .. } = refused else {
        panic!("unexpected refusal under `{plan}`: {refused}");
    };
    assert_eq!(
        corruption,
        TornTail::UnsealedCompleteFrame { len: frame_len },
        "under `{plan}` the tail must be named a whole unsealed frame"
    );
    assert!(
        !corruption.is_interrupted_append(),
        "a whole unsealed frame is not an interrupted append (`{plan}`)"
    );

    // Nothing was shortened by the refusal, which is the property that makes
    // refusing recoverable under both readings and truncating recoverable under
    // only one.
    assert_eq!(
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
        length_before,
        "a refusal must not shorten the journal (`{plan}`)"
    );

    let repaired = LedgerStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("the repair entry point resolves it");
    let repair = repaired
        .recovery()
        .repair()
        .expect("a repair that discarded a frame reports it");
    assert_eq!(repair.discarded_bytes(), frame_len, "under `{plan}`");
    assert_eq!(
        durable_state(&DurableLedgerStateMachine::new(
            repaired,
            scratch.path().join("raft/snapshots")
        )),
        before,
        "the repair resolves the write-ahead window to the pre-transaction state (`{plan}`)"
    );
}

// ---------------------------------------------------------------------------
// The boundary just past the seal. The seal byte is written and its barrier
// fails, so the caller is told the outcome is unknown and either side is legal.
// What is *not* legal is the third answer: shortening the journal.
// ---------------------------------------------------------------------------

#[test]
fn a_failed_seal_barrier_never_shortens_the_journal() {
    let commands = workload();
    let scratch = ScratchDir::new("failed-seal-sync");
    let (before, _) = state_and_image_len(&commands, commands.len());
    let after = uninterrupted_state(&commands);

    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AtSealSync);
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("a failed barrier is a failed transaction");
    assert_eq!(app.store().fired_fault(), Some(WriteFault::AtSealSync));
    drop(app);

    let length_before = raw_journal::read(scratch.path())
        .expect("the journal reads")
        .len();
    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Ok(store) => {
            assert_eq!(
                store.recovery().discarded_bytes(),
                0,
                "a frame whose seal reached the file is not residue (`{plan}`)"
            );
            let state = durable_state(&DurableLedgerStateMachine::new(
                store,
                scratch.path().join("raft/snapshots"),
            ));
            assert!(
                state == before || state == after,
                "a failed seal barrier recovered to neither side (`{plan}`)"
            );
        }
        Err(LedgerStoreError::UnreadableFrame { corruption, .. }) => {
            assert!(
                matches!(corruption, TornTail::UnsealedCompleteFrame { .. }),
                "if the seal did not land, the tail is the whole-frame ambiguity and nothing \
                 else (`{plan}`): {corruption:?}"
            );
        }
        Err(other) => panic!("unexpected refusal under `{plan}`: {other}"),
    }
    assert_eq!(
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
        length_before,
        "neither outcome of a failed seal barrier may shorten the journal (`{plan}`)"
    );
}

#[test]
fn a_torn_commit_record_does_not_commit() {
    let commands = workload();
    let scratch = ScratchDir::new("torn-commit-record");
    let (before, image_len) = state_and_image_len(&commands, commands.len());

    // One byte short of a whole commit record. Every other check in the frame
    // passes; the only thing missing is the seal.
    let stop = as_u64(BEGIN_LEN) + image_len + as_u64(COMMIT_LEN) - 1;
    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(stop));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("a torn commit record is not a commit");
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        recovered.store().recovery().torn_tail(),
        Some(TornTail::UnsealedAppend { present: stop }),
        "under `{plan}`"
    );
    assert_eq!(
        durable_state(&recovered),
        before,
        "a frame missing one byte of its commit record is not committed (`{plan}`)"
    );
}

#[test]
fn a_failed_durability_barrier_is_refused_by_open_and_resolved_only_by_the_repair() {
    let commands = workload();
    let scratch = ScratchDir::new("failed-sync");
    let (before, _) = state_and_image_len(&commands, commands.len());
    let after = uninterrupted_state(&commands);

    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AtFileSync);
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("a failed barrier is a failed transaction");
    assert_eq!(app.store().fired_fault(), Some(WriteFault::AtFileSync));
    assert!(
        app.store().requires_reopen(),
        "a caller cannot infer from `Err` that no bytes changed (`{plan}`)"
    );
    drop(app);

    // `AtFileSync` fires after the whole unsealed frame is emitted, so the
    // journal ends in a frame that verifies in every respect except its mark.
    // That is the one boundary `open` will not resolve, and the reason is that
    // the same bytes are also what a committed frame whose mark byte rotted
    // leaves: truncating is right under the first reading and deletes an
    // acknowledged transaction under the second. This test used to assert that
    // recovery picked a side; it pinned a choice recovery had no grounds to
    // make.
    let refused = LedgerStore::open(scratch.path(), config(2, 4))
        .expect_err("a whole unsealed frame is not something `open` may resolve");
    let LedgerStoreError::UnreadableFrame { corruption, .. } = refused else {
        panic!("unexpected refusal under `{plan}`: {refused}");
    };
    assert!(
        matches!(corruption, TornTail::UnsealedCompleteFrame { .. }),
        "a failed barrier must be named as the whole unsealed frame it is, not as an interrupted \
         append (`{plan}`): {corruption:?}"
    );

    // The contract still promises the answer is one of the two sides. What
    // changed is who says so: a caller that has decided the frame was never
    // committed asks for the repair by name, and gets exactly the
    // pre-transaction state.
    let repaired = LedgerStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("the repair entry point resolves it");
    let repair = repaired
        .recovery()
        .repair()
        .expect("the repair reports what it discarded");
    assert!(
        matches!(repair.corruption(), TornTail::UnsealedCompleteFrame { .. }),
        "the repair names the ambiguity it resolved (`{plan}`): {repair}"
    );
    let recovered = DurableLedgerStateMachine::new(repaired, scratch.path().join("raft/snapshots"));
    let state = durable_state(&recovered);
    assert_eq!(
        state, before,
        "the repair resolves a failed barrier to the pre-transaction state (`{plan}`)"
    );
    assert_ne!(
        state, after,
        "the repair discarded the frame, so the post-transaction state is not what it produces"
    );
}

#[test]
fn recovery_truncates_the_torn_tail_so_the_next_transaction_appends_cleanly() {
    let commands = workload();
    let scratch = ScratchDir::new("truncating-recovery");
    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(40));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    let before = durable_state(&app);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("the last transaction is interrupted");
    drop(app);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    assert!(
        recovered.store().recovery().torn_tail().is_some(),
        "the scenario left no residue to truncate (`{plan}`)"
    );
    assert_eq!(recovered.store().recovery().discarded_bytes(), 40);
    assert_eq!(durable_state(&recovered), before);

    // The point of truncating is that an append cannot follow abandoned bytes.
    apply_one(
        &mut recovered,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect("a recovered journal accepts the retried transaction");
    let retried = durable_state(&recovered);
    drop(recovered);

    let reopened = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        reopened.store().recovery().torn_tail(),
        None,
        "the retried transaction sits on a clean boundary"
    );
    assert_eq!(durable_state(&reopened), retried);
    assert_eq!(
        retried,
        uninterrupted_state(&commands),
        "retrying after recovery reaches the state the uninterrupted run reached"
    );
}

#[test]
fn an_acknowledged_command_is_never_re_executed_after_recovery() {
    let commands = workload();
    let scratch = ScratchDir::new("no-re-execution");
    let acknowledged = commands[3].clone();

    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(40));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    let acknowledged_balance = app.ledger().account_balance(ALPHA);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect_err("the last transaction is interrupted");
    drop(app);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    let recovered_floor = recovered
        .applied_index()
        .expect("a durable ledger reports its applied index");

    // The deposit at sequence 3 was acknowledged before the crash. Its session
    // entry has to have survived in the same transaction that moved the
    // balance, so replaying it returns the cached result rather than depositing
    // again.
    let replayed = apply_one(
        &mut recovered,
        LogIndex(recovered_floor.0 + 1),
        acknowledged,
    )
    .expect("a fresh index applies");
    assert_eq!(
        replayed.disposition,
        ApplyDisposition::Replayed,
        "an acknowledged command must not execute a second time (`{plan}`)"
    );
    assert_eq!(
        replayed.response,
        LedgerResponse::Mutation(MutationResult::Deposited { balance: 40 })
    );
    assert_eq!(
        recovered.ledger().account_balance(ALPHA),
        acknowledged_balance,
        "the replay changed no balance (`{plan}`)"
    );

    // And the entry itself can never come back at or below the floor.
    let error = apply_one(&mut recovered, recovered_floor, commands[0].clone())
        .expect_err("an entry at the applied floor is refused");
    assert!(
        matches!(
            error,
            DurableLedgerError::Adapter(
                rafter_reference_ledger::LedgerAdapterError::AppliedIndexRegression { .. }
            )
        ),
        "unexpected refusal: {error}"
    );
}

#[test]
fn a_crash_after_the_commit_point_but_before_the_reply_is_answered_by_the_cache() {
    let commands = workload();
    let scratch = ScratchDir::new("commit-without-reply");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..commands.len() - 1]);

    // This transaction commits. Its results are then dropped on the floor,
    // which is exactly what a process death between the commit point and the
    // client reply looks like: durable effects, no acknowledgement.
    let unreplied = apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1].clone(),
    )
    .expect("the transaction commits");
    let committed = durable_state(&app);
    drop(app);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        durable_state(&recovered),
        committed,
        "a committed transaction survives a crash that swallowed its reply"
    );

    // The client never heard an answer, so it retries the same request
    // identity. The cache the transaction committed alongside the balance is
    // what makes that safe.
    let retried = apply_one(
        &mut recovered,
        LogIndex(committed.applied_index.0 + 1),
        commands[commands.len() - 1].clone(),
    )
    .expect("a fresh index applies");
    assert_eq!(retried.disposition, ApplyDisposition::Replayed);
    assert_eq!(
        retried.response, unreplied.response,
        "the retry returns the answer the crashed run computed"
    );
    assert_eq!(
        recovered.ledger().view(),
        committed.view,
        "the retry moved no balance"
    );
}

#[test]
fn recovery_then_replay_reconstructs_the_uninterrupted_run() {
    let commands = workload();
    let uninterrupted = uninterrupted_state(&commands);

    // Interrupt each transaction in turn, recover, replay the rest of the log
    // from the recovered floor, and land in the same place every time.
    for interrupted in 1..=commands.len() {
        let plan = FaultPlan::at(as_u64(interrupted), WriteFault::AfterBytes(35));
        let scratch = ScratchDir::new("replay-equivalence");
        let mut app = open(scratch.path(), plan.clone());
        for (position, command) in commands.iter().enumerate().take(interrupted) {
            let outcome = apply_one(&mut app, index_of(position + 1), command.clone());
            if position + 1 == interrupted {
                outcome.expect_err(&format!(
                    "`{plan}` must interrupt transaction {interrupted}"
                ));
            } else {
                outcome.expect("earlier transactions commit");
            }
        }
        drop(app);

        let mut recovered = open(scratch.path(), FaultPlan::none());
        let floor = recovered
            .applied_index()
            .expect("a durable ledger reports its applied index");
        assert!(
            floor.0 < as_u64(interrupted),
            "the interrupted transaction must not have committed (`{plan}`)"
        );

        for (position, command) in commands.iter().enumerate() {
            let index = index_of(position + 1);
            if index > floor {
                apply_one(&mut recovered, index, command.clone())
                    .unwrap_or_else(|error| panic!("replay after `{plan}` failed: {error}"));
            }
        }

        assert_eq!(
            durable_state(&recovered),
            uninterrupted,
            "replay from the recovered floor did not reconstruct the uninterrupted run (`{plan}`)"
        );
        assert_eq!(
            recovered.ledger().view(),
            replay_through_oracle(&commands).view(),
            "the reconstructed ledger disagrees with the independent oracle (`{plan}`)"
        );
    }
}

// ---------------------------------------------------------------------------
// Format integrity
// ---------------------------------------------------------------------------

#[test]
fn a_corrupted_committed_frame_refuses_the_store_rather_than_truncating_it() {
    let commands = workload();
    let (before, image_len) = state_and_image_len(&commands, commands.len());
    let last_frame_len = as_u64(BEGIN_LEN) + image_len + as_u64(COMMIT_LEN);

    // Offsets into the last frame, one in each record, named by the check that
    // is supposed to catch them. None of these is a prefix of any frame this
    // build writes — every byte is present and only its value is wrong — so
    // none of them proves the frame was never committed. Recovery cannot tell
    // whether it is looking at an interrupted append or at acknowledged history
    // that has rotted, so it refuses instead of shortening the file.
    let cases = [
        // Bytes zero through three are the frame's identity. Byte zero is the
        // mark, and flipping it leaves a byte that is neither mark, which is not
        // something an append can produce: an append writes the unsealed mark
        // and promotes it to the sealed one, and there is no third value in
        // between. Bytes one through three are the magic, which an append never
        // writes wrong at all. Both are answered above the seal test, at every
        // length, by `verify_identity`; byte one used to fall through to the
        // begin record's checksum instead, which is the same refusal reached
        // two questions later and only because the tail happened to be long
        // enough to have a checksum.
        (
            0_u64,
            TornTail::NotALedgerFrame {
                magic: [!b'R', b'L', b'B', b'G'],
            },
        ),
        (
            1,
            TornTail::NotALedgerFrame {
                magic: [b'R', !b'L', b'B', b'G'],
            },
        ),
        (as_u64(BEGIN_LEN) + 2, TornTail::ImageCorrupt),
        (
            as_u64(BEGIN_LEN) + image_len + 6,
            TornTail::CommitRecordCorrupt,
        ),
    ];

    for (offset_in_frame, expected) in cases {
        let scratch = ScratchDir::new("corrupt-frame");
        let mut app = open(scratch.path(), FaultPlan::none());
        apply_all(&mut app, &commands);
        drop(app);

        let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads back");
        let whole_len = as_u64(bytes.len());
        let frame_start = whole_len - last_frame_len;
        let target = usize::try_from(frame_start + offset_in_frame).expect("offsets are small");
        bytes[target] ^= 0xFF;
        raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

        let error = LedgerStore::open(scratch.path(), config(2, 4)).expect_err(&format!(
            "flipping byte {offset_in_frame} of the last frame must refuse the store"
        ));
        assert!(
            matches!(
                error,
                LedgerStoreError::UnreadableFrame {
                    offset,
                    corruption,
                    unreadable_bytes,
                    ..
                } if offset == frame_start
                    && corruption == expected
                    && unreadable_bytes == last_frame_len
            ),
            "unexpected refusal for byte {offset_in_frame}: {error}"
        );

        // And the refusal changed nothing on the medium. This is the whole
        // point: a read that shortens the file is not a read.
        assert_eq!(
            journal_len_on_disk(scratch.path()),
            whole_len,
            "a refused open must not have touched the journal"
        );

        // Discarding it is available, by name, and it says what it cost.
        let repaired = LedgerStore::open_and_repair(scratch.path(), config(2, 4))
            .expect("a repair opens what is readable");
        let repair = repaired
            .recovery()
            .repair()
            .expect("the repair discarded a region and must report it");
        assert_eq!(repair.offset(), frame_start);
        assert_eq!(repair.corruption(), expected);
        assert_eq!(repair.discarded_bytes(), last_frame_len);
        assert_eq!(
            DurableState {
                applied_index: repaired.applied_index(),
                view: repaired.ledger().view(),
            },
            before,
            "the repair left the frames before the corruption"
        );
        assert_eq!(
            journal_len_on_disk(scratch.path()),
            whole_len - last_frame_len,
            "the repair shortened the journal by exactly what it reported"
        );
    }
}

#[test]
fn an_early_corrupt_frame_never_deletes_the_frames_after_it() {
    // The cascade the fail-closed rule exists to stop. One flipped bit inside
    // an *early* committed frame makes every later frame unreachable, because a
    // frame's offset is only knowable through the frame before it. Treating
    // that as a torn tail deletes whole, correctly sealed, acknowledged
    // transactions from the medium — during an operation the caller asked for
    // as a read.
    let commands = workload();
    let scratch = ScratchDir::new("corruption-cascade");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let acknowledged = durable_state(&app);
    drop(app);

    let original = raw_journal::read(scratch.path()).expect("the journal reads back");
    let whole_len = as_u64(original.len());

    // Byte 2 of the second frame's image: frame one is untouched, and frames
    // three onward are whole and correctly sealed.
    let first_image_len =
        u32::from_be_bytes(original[26..30].try_into().expect("four bytes")) as usize;
    let second_frame = HEADER_LEN + BEGIN_LEN + first_image_len + COMMIT_LEN;
    let mut corrupt = original.clone();
    corrupt[second_frame + BEGIN_LEN + 2] ^= 0x01;
    raw_journal::write(scratch.path(), &corrupt).expect("the journal rewrites");

    let error = LedgerStore::open(scratch.path(), config(2, 4))
        .expect_err("a corrupt early frame must refuse the store");
    assert!(
        matches!(
            error,
            LedgerStoreError::UnreadableFrame {
                offset,
                corruption: TornTail::ImageCorrupt,
                committed_frames: 1,
                ..
            } if offset == as_u64(second_frame)
        ),
        "unexpected refusal: {error}"
    );
    assert_eq!(
        journal_len_on_disk(scratch.path()),
        whole_len,
        "every acknowledged frame is still on the medium after the refusal"
    );

    // Undoing the bit flip is enough to get the whole history back, which is
    // what proves those frames were never damaged and would have been destroyed
    // for nothing.
    raw_journal::write(scratch.path(), &original).expect("the journal rewrites");
    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        durable_state(&recovered),
        acknowledged,
        "the refusal preserved every transaction the run acknowledged"
    );
    assert_eq!(
        recovered.store().recovery().committed_frames(),
        as_u64(commands.len()),
        "all of the frames were readable all along"
    );
}

#[test]
fn a_commit_record_seals_only_the_frame_it_was_written_for() {
    // Two journals whose last frames differ in content but not in length: the
    // ledger's fields are fixed width, so a different deposit amount encodes to
    // the same number of bytes. Splicing one journal's commit record onto the
    // other's image passes every local check — the magic, the version, the
    // record's own checksum, and the image's checksum are all intact — and is
    // still refused, because the commit record's frame checksum was computed
    // over a frame it no longer follows.
    let mine = ScratchDir::new("splice-target");
    let theirs = ScratchDir::new("splice-source");
    let commands = workload();
    let mut divergent = commands.clone();
    divergent[3] = execute(
        0,
        1,
        3,
        Mutation::Deposit {
            account_id: ALPHA,
            amount: amount(41),
        },
    );

    let (_, image_len) = state_and_image_len(&commands, commands.len());
    let frame_len = as_u64(BEGIN_LEN) + image_len + as_u64(COMMIT_LEN);

    let mut app = open(mine.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    drop(app);
    let mut app = open(theirs.path(), FaultPlan::none());
    apply_all(&mut app, &divergent);
    drop(app);

    let mut target = raw_journal::read(mine.path()).expect("the journal reads back");
    let source = raw_journal::read(theirs.path()).expect("the journal reads back");
    assert_eq!(
        target.len(),
        source.len(),
        "the two journals must be the same shape for this splice to be interesting"
    );
    let commit_start = target.len() - COMMIT_LEN;
    assert_ne!(
        target[commit_start..],
        source[commit_start..],
        "the two commit records must differ, or the splice changes nothing"
    );
    target[commit_start..].copy_from_slice(&source[commit_start..]);
    raw_journal::write(mine.path(), &target).expect("the journal rewrites");

    // A whole commit record that seals nothing is not a prefix of anything an
    // append writes, so it is corruption rather than residue, and the store
    // refuses rather than deciding on the caller's behalf that the frame it
    // cannot verify was never committed.
    let error = LedgerStore::open(mine.path(), config(2, 4))
        .expect_err("a commit record from another frame must not seal this one");
    assert!(
        matches!(
            error,
            LedgerStoreError::UnreadableFrame {
                corruption: TornTail::CommitRecordCorrupt,
                unreadable_bytes,
                ..
            } if unreadable_bytes == frame_len
        ),
        "unexpected refusal: {error}"
    );
}

#[test]
fn creating_the_journal_survives_a_crash_at_every_stage() {
    // Creation stages a header and renames it. Both states an interrupted
    // creation can leave — a partial staging file and a whole one that was
    // never renamed — must open into a working store, because the alternative
    // is a directory that no later open can ever fix.
    let ledger_config = config(2, 4);
    let whole_header = {
        let measure = ScratchDir::new("create-measure");
        let store = LedgerStore::open(measure.path(), ledger_config).expect("a fresh store opens");
        assert!(
            store.recovery().created(),
            "the measurement created a store"
        );
        drop(store);
        raw_journal::read(measure.path()).expect("the journal reads back")
    };
    assert_eq!(
        whole_header.len(),
        HEADER_LEN,
        "a created journal is exactly its header"
    );

    for (label, staged) in [
        ("a partial staged header", &whole_header[..7]),
        (
            "a whole staged header that was never renamed",
            &whole_header[..],
        ),
    ] {
        let scratch = ScratchDir::new("create-crash");
        std::fs::write(scratch.path().join("ledger.journal.tmp"), staged)
            .expect("the staging file writes");

        let recovered = LedgerStore::open(scratch.path(), ledger_config).unwrap_or_else(|error| {
            panic!("an open after {label} must create the journal: {error}")
        });
        assert!(
            recovered.recovery().created(),
            "an interrupted creation left nothing to adopt, so {label} creates ({:?})",
            recovered.recovery()
        );
        assert!(
            recovered.recovery().removed_staged_file(),
            "{label} must be swept"
        );
        assert_eq!(
            journal_len_on_disk(scratch.path()),
            as_u64(HEADER_LEN),
            "{label} left a journal that is exactly its header"
        );
        assert_eq!(
            names_in(scratch.path()),
            vec![String::from("ledger.journal")],
            "{label} left something beside the journal"
        );
    }
}

#[test]
fn the_sweep_removes_this_stores_staging_name_and_nothing_else() {
    // The sweep used to remove anything whose name began with the journal's
    // name and a dot, reasoning that a staging file is always somebody's
    // abandoned work. That had the direction of its own proof backwards: every
    // file this store stages matches the prefix, but matching the prefix does
    // not make a file one. The process tells an operator to run a repair, the
    // obvious first move is to copy the journal aside, and the obvious name for
    // the copy is exactly what the old rule deleted.
    let ledger_config = config(2, 4);
    let scratch = ScratchDir::new("foreign-staging");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &workload()[..2]);
    let before = durable_state(&app);
    drop(app);

    let backup = "ledger.journal.backup-2026-07-25";
    std::fs::copy(
        scratch.path().join("ledger.journal"),
        scratch.path().join(backup),
    )
    .expect("an operator copies the journal aside");
    std::fs::write(scratch.path().join("ledger.journal.tmp"), vec![0_u8; 4096])
        .expect("an abandoned staging file writes");

    let recovered = LedgerStore::open(scratch.path(), ledger_config).expect("the store reopens");
    assert_eq!(
        recovered.recovery().removed_staged_bytes(),
        Some(4096),
        "the abandoned staging file must be removed at open, and its size reported: deleting a \
         file is worth more than a bit in a report"
    );
    assert_eq!(
        names_in(scratch.path()),
        vec![String::from("ledger.journal"), String::from(backup)],
        "the sweep removed a name this store could not have written"
    );
    assert_eq!(
        DurableState {
            applied_index: recovered.applied_index(),
            view: recovered.ledger().view(),
        },
        before,
        "sweeping residue is not a change of state"
    );
}

#[test]
fn a_rewrite_at_an_unchanged_index_may_not_drop_the_deduplication_cache() {
    let ledger_config = config(2, 4);
    let scratch = ScratchDir::new("replace-dedup");
    let commands = workload();

    // Two ledgers at one applied index. The poorer one is the state as it stood
    // before the last mutation completed, republished at the index that
    // mutation moved the store to — which is what a stale snapshot presents
    // when its payload is one commit behind the index it declares.
    let mut poorer = rafter_reference_ledger::Ledger::new(ledger_config);
    for command in &commands[..commands.len() - 1] {
        poorer.apply(command.clone());
    }
    let mut richer = poorer.clone();
    richer.apply(commands[commands.len() - 1].clone());
    let at = index_of(commands.len());

    let mut store = LedgerStore::open(scratch.path(), ledger_config).expect("a fresh store opens");
    store
        .commit(&richer, at)
        .expect("the first transaction commits");

    // The applied index is identical, so the store's only other floor sees
    // nothing wrong. Only the deduplication cache moved backwards.
    assert_eq!(store.applied_index(), at);
    let error = store
        .replace(&poorer, at)
        .expect_err("a rewrite that loses a completed request must be refused");
    assert!(
        matches!(
            error,
            LedgerStoreError::DeduplicationRegression {
                offered: Some(_),
                ..
            }
        ),
        "unexpected refusal: {error}"
    );

    // Republishing the same state is still legal — the check refuses a loss,
    // not a rewrite. Compaction is exactly this call.
    store
        .replace(&richer, at)
        .expect("republishing the durable state at its own index commits");
    store.compact().expect("compaction republishes in place");
}

#[test]
fn the_journal_header_binds_a_store_to_its_format_and_its_bounds() {
    let scratch = ScratchDir::new("header");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &workload());
    drop(app);

    // Different resource bounds decide which images are valid, so they are
    // refused rather than reinterpreted.
    let error = LedgerStore::open(scratch.path(), config(3, 4))
        .expect_err("a journal is bound to the configuration it was created under");
    assert!(
        matches!(error, LedgerStoreError::ConfigMismatch { .. }),
        "unexpected refusal: {error}"
    );

    let original = raw_journal::read(scratch.path()).expect("the journal reads back");

    let mut wrong_magic = original.clone();
    wrong_magic[1] = b'X';
    raw_journal::write(scratch.path(), &wrong_magic).expect("the journal rewrites");
    assert!(
        matches!(
            LedgerStore::open(scratch.path(), config(2, 4)),
            Err(LedgerStoreError::NotALedgerJournal { .. })
        ),
        "a file that is not a ledger journal is not opened as one"
    );

    let mut wrong_version = original.clone();
    wrong_version[4] = 9;
    raw_journal::write(scratch.path(), &wrong_version).expect("the journal rewrites");
    assert!(
        matches!(
            LedgerStore::open(scratch.path(), config(2, 4)),
            Err(LedgerStoreError::UnsupportedFormatVersion { version: 9 })
        ),
        "a format this build cannot read is refused rather than guessed at"
    );

    let mut corrupt_header = original;
    corrupt_header[HEADER_LEN - 1] ^= 0xFF;
    raw_journal::write(scratch.path(), &corrupt_header).expect("the journal rewrites");
    assert!(
        matches!(
            LedgerStore::open(scratch.path(), config(2, 4)),
            Err(LedgerStoreError::HeaderChecksumMismatch { .. })
        ),
        "a corrupt header is caught by its own checksum"
    );
}

#[test]
fn the_store_refuses_a_transaction_that_does_not_advance_the_applied_floor() {
    // The state machine already refuses a replayed entry, so this is the store
    // saying the same thing on its own. It matters that both do: the store is
    // the thing recovery reads, and a journal holding two frames at the same
    // index has no rule for choosing between them.
    let scratch = ScratchDir::new("monotonic");
    let ledger_config = config(2, 4);
    let mut store = LedgerStore::open(scratch.path(), ledger_config).expect("a fresh store opens");
    let mut ledger = rafter_reference_ledger::Ledger::new(ledger_config);
    ledger.apply(open_session(0, 1));

    store
        .commit(&ledger, LogIndex(1))
        .expect("the first transaction advances the floor");

    for repeated in [LogIndex(1), LogIndex::ZERO] {
        let error = store
            .commit(&ledger, repeated)
            .expect_err("a non-advancing append must be refused");
        assert!(
            matches!(error, LedgerStoreError::NonMonotonicAppliedIndex { .. }),
            "unexpected refusal at {repeated}: {error}"
        );
    }

    // A rewrite is the exception, and only at the current index: compacting in
    // place must not require inventing one.
    store
        .replace(&ledger, LogIndex(1))
        .expect("a rewrite may republish the current index");
    let error = store
        .replace(&ledger, LogIndex::ZERO)
        .expect_err("a rewrite must not move the floor backwards");
    assert!(
        matches!(error, LedgerStoreError::NonMonotonicAppliedIndex { .. }),
        "unexpected refusal: {error}"
    );
}

#[test]
fn a_poisoned_store_refuses_every_later_transaction() {
    let commands = workload();
    let scratch = ScratchDir::new("poisoned");
    let plan = FaultPlan::at(1, WriteFault::AtFileSync);
    let mut app = open(scratch.path(), plan.clone());

    apply_one(&mut app, LogIndex(1), commands[0].clone())
        .expect_err("the first transaction fails its barrier");
    assert!(app.store().requires_reopen(), "under `{plan}`");

    let error = apply_one(&mut app, LogIndex(2), commands[1].clone())
        .expect_err("a poisoned store accepts nothing");
    assert!(
        matches!(
            error,
            DurableLedgerError::Store(LedgerStoreError::StoreRequiresReopen)
        ),
        "a poisoned store must say so rather than fail some other way: {error}"
    );
}

// ---------------------------------------------------------------------------
// Snapshots and compaction
// ---------------------------------------------------------------------------

#[test]
fn a_snapshot_round_trip_preserves_everything_the_contract_lists() {
    let commands = workload();
    let source = ScratchDir::new("snapshot-source");
    let destination = ScratchDir::new("snapshot-destination");

    let mut app = open(source.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let expected = durable_state(&app);
    let snapshot = app
        .build_snapshot(expected.applied_index)
        .expect("a durable ledger snapshots its own applied index");
    drop(app);

    let mut installed = open(destination.path(), FaultPlan::none());
    installed
        .install_snapshot(snapshot)
        .expect("a matching snapshot installs");
    assert_eq!(
        durable_state(&installed),
        expected,
        "balances, sessions, cached mutations, cached results, and the applied floor all survive"
    );
    drop(installed);

    // The install has to have been a transaction, not an in-memory adoption.
    let reopened = open(destination.path(), FaultPlan::none());
    assert_eq!(
        durable_state(&reopened),
        expected,
        "an installed snapshot is durable without any further write"
    );
    assert_eq!(
        reopened.store().recovery().committed_frames(),
        1,
        "an install replaces the journal rather than extending it"
    );
}

#[test]
fn a_crash_during_a_snapshot_install_leaves_exactly_one_side_of_it() {
    let commands = workload();
    let source = ScratchDir::new("install-crash-source");
    let mut app = open(source.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let installed_state = durable_state(&app);
    let snapshot = app
        .build_snapshot(installed_state.applied_index)
        .expect("a durable ledger snapshots its own applied index");
    drop(app);

    // The destination already holds a shorter prefix of the same history, so
    // "pre-install" is a real state rather than an empty one.
    let prefix = &commands[..2];
    let mut observed_pre_install = 0;
    let mut observed_post_install = 0;

    for fault in [
        WriteFault::BeforeFirstByte,
        WriteFault::AfterBytes(10),
        WriteFault::AfterBytes(as_u64(HEADER_LEN) + as_u64(BEGIN_LEN) + 4),
        WriteFault::AtFileSync,
        WriteFault::BeforeRename,
        WriteFault::AfterRename,
    ] {
        let destination = ScratchDir::new("install-crash");
        // Two clean transactions, then the install is the third write plan.
        let plan = FaultPlan::at(3, fault);
        let mut app = open(destination.path(), plan.clone());
        apply_all(&mut app, prefix);
        let before = durable_state(&app);

        app.install_snapshot(clone_snapshot(&snapshot))
            .expect_err(&format!("`{plan}` must interrupt the install"));
        assert_eq!(
            app.store().fired_fault(),
            Some(fault),
            "`{plan}` never reached its boundary"
        );
        drop(app);

        let recovered = open(destination.path(), FaultPlan::none());
        let state = durable_state(&recovered);
        if state == before {
            observed_pre_install += 1;
            assert_eq!(
                recovered.store().recovery().torn_tail(),
                None,
                "an interrupted rewrite never damages the journal it did not replace (`{plan}`)"
            );
        } else {
            assert_eq!(
                state, installed_state,
                "`{plan}` recovered to neither side of the install"
            );
            observed_post_install += 1;
        }
    }

    // A rewrite publishes at the rename, so the faults before it must land on
    // one side and the fault after it on the other. A run that saw only one
    // side would mean the rename stopped being the commit point.
    assert_eq!(
        (observed_pre_install, observed_post_install),
        (5, 1),
        "the rename is the install's commit point"
    );
}

#[test]
fn an_abandoned_staging_file_is_removed_rather_than_reused() {
    let commands = workload();
    let scratch = ScratchDir::new("staging");
    let plan = FaultPlan::at(3, WriteFault::BeforeRename);
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..2]);
    let before = durable_state(&app);
    app.compact()
        .expect_err("the compaction is interrupted before it publishes");
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    assert!(
        recovered.store().recovery().removed_staged_file(),
        "an abandoned staging file must be removed at open (`{plan}`)"
    );
    assert_eq!(durable_state(&recovered), before);
}

#[test]
fn compaction_never_makes_an_acknowledged_command_executable_again() {
    let commands = workload();
    let scratch = ScratchDir::new("compaction");
    // The highest completed sequence is the only one a session still answers
    // from cache, so the command to retry is the last one the run acknowledged.
    let acknowledged = commands[commands.len() - 1].clone();

    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let expected = durable_state(&app);
    let before_compaction = app.store().journal_len();
    app.compact().expect("compaction publishes");
    assert!(
        app.store().journal_len() < before_compaction,
        "compaction left the journal at {before_compaction} bytes"
    );
    drop(app);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        recovered.store().recovery().committed_frames(),
        1,
        "a compacted journal holds exactly the current state"
    );
    assert_eq!(
        durable_state(&recovered),
        expected,
        "compaction preserved every fact the transaction moved"
    );

    // The deduplication state is the thing compaction must not drop: without
    // it, this acknowledged deposit would run a second time.
    let replayed = apply_one(
        &mut recovered,
        LogIndex(expected.applied_index.0 + 1),
        acknowledged,
    )
    .expect("a fresh index applies");
    assert_eq!(replayed.disposition, ApplyDisposition::Replayed);
    assert_eq!(
        recovered.ledger().view(),
        expected.view,
        "the replay after compaction moved no balance"
    );
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

/// What `open` truncating a zero-filled tail actually costs a replica, and what
/// gets it back.
///
/// `TornTail::is_truncatable_residue` takes a trade: it truncates a zero tail
/// under `open`, with no flag, knowing the bytes may be committed frames a
/// zeroed region erased. The argument for the trade is that refusing would
/// refuse the store on the most ordinary crash there is. The argument for the
/// trade being *survivable* is separate, and this is it — the application store
/// is a projection of the replicated log, its applied index is the join point,
/// and entries above that index are re-applied.
///
/// So the loss is real and local: this replica's applied floor moves backwards
/// under an ordinary restart. It is also repaired by replication, and that half
/// is checked here rather than asserted in a doc comment, because it is the
/// half that makes the trade defensible and it is not a fact about the store.
///
/// What it does not show, and what nothing here can: that the group still holds
/// those entries. A zeroing event across a quorum loses them outright.
#[test]
fn a_zero_filled_tail_costs_a_replica_its_floor_and_replication_returns_it() {
    let scratch = ScratchDir::new("cluster-zero-tail");
    let ledger_config = config(2, 4);
    let mut apps = DurableLedgerApps::new(scratch.path(), ledger_config);
    // No fault fires; this marks node 3 as one whose residue a scenario put
    // there deliberately, so the factory's "no unexplained residue" assertion
    // stays on for the other two.
    apps.arm(NodeId(3), FaultPlan::none());
    let node_three = apps.directory(NodeId(3));

    let mut cluster = LedgerCluster::with_apps(ledger_config, apps);
    let leader = cluster.elect_leader();
    let commands = workload();
    let (last, rest) = commands.split_last().expect("the workload is not empty");
    for command in rest {
        cluster.submit(leader, command.clone());
    }
    cluster.settle();

    // The boundary the final frame begins at, taken from the store rather than
    // computed, so the zeroing starts exactly where a frame does.
    let boundary = journal_len(&cluster, NodeId(3));
    let floor_before = cluster.applied_index(NodeId(3));

    cluster.submit(leader, last.clone());
    cluster.settle();
    let quorum_view = cluster.state_machine(NodeId(2)).ledger().view();
    let floor_after = cluster.applied_index(NodeId(3));
    let total = journal_len(&cluster, NodeId(3));
    assert!(
        floor_after > floor_before && total > boundary,
        "the final transaction has to have committed on node 3 for its loss to mean anything"
    );

    // The final frame's sector reaches the drive as zeros. Nothing else changes,
    // and this is the whole of the injury.
    let mut bytes = raw_journal::read(&node_three).expect("the journal reads");
    for byte in &mut bytes[boundary..] {
        *byte = 0;
    }
    raw_journal::write(&node_three, &bytes).expect("the journal rewrites");

    cluster.restart(NodeId(3));
    let recovery = *cluster.state_machine(NodeId(3)).store().recovery();
    let zeroed = as_u64(total - boundary);
    assert_eq!(
        recovery.torn_tail(),
        Some(TornTail::ZeroFilledToEnd { present: zeroed })
    );
    assert_eq!(
        recovery.discarded_without_proof(),
        zeroed,
        "an ordinary restart deleted bytes it could not show were uncommitted, and has to say so"
    );
    assert_eq!(
        recovery.repair(),
        None,
        "no flag was reached; this is the plain entry point"
    );
    assert!(
        !recovery.created(),
        "the journal was shortened, not replaced"
    );

    // The catch-up. Nothing is submitted, so anything node 3 regains is the log
    // handing back what the restart deleted.
    cluster.run_rounds(8);
    cluster.settle();
    assert_eq!(
        cluster.applied_index(NodeId(3)),
        floor_after,
        "replication must return the applied floor the truncation took"
    );
    assert_eq!(
        cluster.state_machine(NodeId(3)).ledger().view(),
        quorum_view,
        "and the transaction in those bytes with it"
    );
    assert!(cluster.crashed().is_empty());
}

/// One replica's committed journal length, as an index into its own bytes.
fn journal_len(cluster: &LedgerCluster<DurableLedgerApps>, node_id: NodeId) -> usize {
    usize::try_from(cluster.state_machine(node_id).store().journal_len())
        .expect("a test journal fits this platform's address space")
}

#[test]
fn a_replica_that_crashed_mid_transaction_recovers_and_rejoins() {
    let scratch = ScratchDir::new("cluster-crash");
    let ledger_config = config(2, 4);
    let mut apps = DurableLedgerApps::new(scratch.path(), ledger_config);

    // Node 3 loses power part way through the image of its third durable
    // transaction. The image is never shorter than its fixed prologue, so a
    // stop 13 bytes into it lands inside the image for any ledger this test
    // can build.
    let stop = as_u64(BEGIN_LEN) + 13;
    let plan = FaultPlan::at(3, WriteFault::AfterBytes(stop));
    apps.arm(NodeId(3), plan.clone());

    let mut cluster = LedgerCluster::with_apps(ledger_config, apps);
    let leader = cluster.elect_leader();
    assert_eq!(leader, NodeId(1), "the lowest election timeout wins");

    let commands = workload();
    for command in &commands {
        cluster.submit(leader, command.clone());
    }

    assert_eq!(
        cluster
            .crashed()
            .into_iter()
            .map(|(node_id, _)| node_id)
            .collect::<Vec<_>>(),
        vec![NodeId(3)],
        "the armed replica must be the one that died, and the only one (`{plan}`)"
    );

    // The surviving quorum kept serving while node 3 was down, so the cluster
    // is strictly ahead of anything node 3 can recover on its own.
    cluster.settle();
    let quorum_view = cluster.state_machine(NodeId(2)).ledger().view();

    cluster.restart(NodeId(3));
    let recovered = cluster.state_machine(NodeId(3));
    assert_eq!(
        recovered.store().recovery().torn_tail(),
        Some(TornTail::UnsealedAppend { present: stop }),
        "the restarted replica recovered across the transaction it died in (`{plan}`)"
    );
    assert!(
        !recovered.store().recovery().created(),
        "a restart reads the journal it left behind rather than making a new one"
    );
    assert_ne!(
        recovered.ledger().view(),
        quorum_view,
        "a replica that came back already caught up would make the catch-up vacuous"
    );
    assert!(
        cluster.crashed().is_empty(),
        "the restarted replica is alive again"
    );

    // Catching up is the leader's work, not recovery's: a restarted replica
    // knows only what its own durable state said, and the entries committed
    // while it was gone reach it through ordinary replication.
    cluster.submit(
        leader,
        execute(
            0,
            1,
            5,
            Mutation::Deposit {
                account_id: BETA,
                amount: amount(7),
            },
        ),
    );
    cluster.run_rounds(8);
    cluster.settle();

    let converged = cluster.state_machine(leader).ledger().view();
    assert_ne!(
        converged, quorum_view,
        "the post-restart command must have moved the cluster on"
    );
    for node_id in cluster.node_ids() {
        assert_eq!(
            cluster.state_machine(node_id).ledger().view(),
            converged,
            "replica {} did not converge with the rest (`{plan}`)",
            node_id.0
        );
    }
    assert!(
        cluster.crashed().is_empty(),
        "no replica died during the catch-up (`{plan}`)"
    );
    check_linearizable(cluster.config(), cluster.history())
        .unwrap_or_else(|error| panic!("under `{plan}`: {error}"));
}

// ---------------------------------------------------------------------------
// Scenario support
// ---------------------------------------------------------------------------

/// The command sequence these scenarios replicate, at indexes 1 upward.
///
/// It opens a session, opens two accounts, deposits, and transfers, so the
/// committed state exercises every field the transaction has to carry: two
/// balances, an active session, a cached mutation, a cached result, and the
/// deposit total.
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

/// Opens a durable ledger over `directory` under `faults`.
fn open(directory: &Path, faults: FaultPlan) -> DurableLedgerStateMachine {
    let armed = faults.to_string();
    let store =
        LedgerStore::open_with_faults(directory, config(2, 4), faults).unwrap_or_else(|error| {
            panic!(
                "a ledger store opens at {} under `{armed}`: {error}",
                directory.display()
            )
        });
    DurableLedgerStateMachine::new(store, directory.join("raft/snapshots"))
}

/// Applies one command at `index`, returning its outcome.
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
    .map(|mut results| {
        results
            .pop()
            .expect("a one-entry batch returns one result")
            .result
    })
}

/// Applies `commands` at consecutive indexes from one, expecting each to commit.
fn apply_all(app: &mut DurableLedgerStateMachine, commands: &[Command]) {
    for (position, command) in commands.iter().enumerate() {
        apply_one(app, index_of(position + 1), command.clone())
            .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
}

/// Returns everything a transaction moves, as one comparable value.
fn durable_state(app: &DurableLedgerStateMachine) -> DurableState {
    DurableState {
        applied_index: app
            .applied_index()
            .expect("a durable ledger reports its applied index"),
        view: app.ledger().view(),
    }
}

/// Returns the state before the `interrupted`-th command, and that command's
/// image length.
fn state_and_image_len(commands: &[Command], interrupted: usize) -> (DurableState, u64) {
    let scratch = ScratchDir::new("measure");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..interrupted - 1]);
    let before = durable_state(&app);
    apply_one(
        &mut app,
        index_of(interrupted),
        commands[interrupted - 1].clone(),
    )
    .expect("an uninterrupted transaction commits");
    let frame_len = LedgerStore::planned_frame_len(
        app.ledger(),
        app.applied_index()
            .expect("a durable ledger reports its applied index"),
    )
    .expect("the measured frame is encodable");
    (before, frame_len - as_u64(BEGIN_LEN) - as_u64(COMMIT_LEN))
}

/// Returns the state an uninterrupted run of `commands` reaches.
fn uninterrupted_state(commands: &[Command]) -> DurableState {
    let scratch = ScratchDir::new("uninterrupted");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, commands);
    durable_state(&app)
}

/// Replays `commands` through the structurally independent oracle.
fn replay_through_oracle(commands: &[Command]) -> ReferenceLedger {
    let mut oracle = ReferenceLedger::new(config(2, 4));
    for command in commands {
        oracle.apply(command.clone());
    }
    oracle
}

/// Returns the journal's length on the medium, which is what a destructive
/// recovery would have changed.
fn journal_len_on_disk(directory: &Path) -> u64 {
    std::fs::metadata(directory.join("ledger.journal"))
        .expect("the journal exists")
        .len()
}

/// Returns every file name in the store's directory, sorted.
fn names_in(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("the store directory reads")
        .map(|entry| {
            entry
                .expect("a directory entry reads")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// Asserts what one boundary of the sweep did to the live handle, before the
/// handle is dropped and a fresh opener decides what reached the medium.
fn assert_interrupted_append(
    app: &DurableLedgerStateMachine,
    outcome: Result<ApplyOutcome, DurableLedgerError>,
    stop: u64,
    committed: bool,
    before: &DurableState,
    plan: &FaultPlan,
) {
    if committed {
        outcome.unwrap_or_else(|error| panic!("a whole frame commits under `{plan}`: {error}"));
        assert_eq!(
            app.store().fired_fault(),
            None,
            "a fault that stops after the last byte stops nothing (`{plan}`)"
        );
        return;
    }

    let error = outcome.expect_err(&format!("`{plan}` must interrupt the transaction"));
    assert!(
        matches!(
            error,
            DurableLedgerError::Store(LedgerStoreError::InjectedFault { .. })
        ),
        "`{plan}` failed for the wrong reason: {error}"
    );
    assert_eq!(
        app.store().fired_fault(),
        Some(WriteFault::AfterBytes(stop)),
        "`{plan}` never reached its boundary"
    );
    assert!(
        app.store().requires_reopen(),
        "a store that failed mid-publication cannot say where its file ends (`{plan}`)"
    );
    // The machine reports a *durable* applied index, so a transaction that did
    // not commit must not have moved it — nor the ledger beside it. A machine
    // that adopted state before publishing it would report a floor above what
    // recovery can reach, and an in-process restart from that floor would skip
    // an entry that never committed.
    assert_eq!(
        durable_state(app),
        *before,
        "a failed transaction moved the reported state (`{plan}`)"
    );
}

/// Returns the torn tail a stop after `stop` bytes of a frame must leave.
///
/// An interrupted append leaves one tail whatever byte it stopped on, because
/// the answer recovery needs is not "which record is missing" but "did this
/// frame's append ever seal it". An append extends the file, so the byte count
/// here really is the write's frontier, and pinning it is sharper than pinning
/// a record shape.
fn expected_tail(stop: u64, frame_len: u64) -> Option<TornTail> {
    if stop == 0 || stop == frame_len {
        None
    } else if stop == 1 {
        // One byte written is the unsealed mark and nothing else, so the tail is
        // zeros to the end of the file. It is truncated on the delayed-allocation
        // premise rather than on the interrupted-append proof, and the store
        // names which — see `TornTail::is_truncatable_residue`. The sweep asserts
        // the boundary here rather than letting one variant cover both.
        Some(TornTail::ZeroFilledToEnd { present: 1 })
    } else {
        Some(TornTail::UnsealedAppend { present: stop })
    }
}

/// Names the record a stop landed in, so a sweep can assert it crossed every
/// one of them.
///
/// The store no longer classifies a tail by record — the frame mark decides —
/// so this is the sweep's own bookkeeping rather than a projection of the
/// store's answer, and it is what keeps the sweep honest about having visited
/// the begin record, the image, the write-ahead window, and the commit record.
fn stopped_record(stop: u64, image_len: u64, frame_len: u64) -> &'static str {
    let begin = as_u64(BEGIN_LEN);
    if stop == 0 {
        "before the first byte"
    } else if stop == frame_len {
        "sealed"
    } else if stop < begin {
        "inside the begin record"
    } else if stop < begin + image_len {
        "inside the image"
    } else if stop == begin + image_len {
        "a whole image with no commit record"
    } else {
        "inside the commit record"
    }
}

/// Copies a snapshot so one built value can be installed repeatedly.
fn clone_snapshot(snapshot: &ApplicationSnapshot) -> ApplicationSnapshot {
    ApplicationSnapshot {
        applied_index: snapshot.applied_index,
        payload: snapshot.payload.clone(),
        raft_snapshot: None,
    }
}

/// The one-based log index of the `position`-th command.
fn index_of(position: usize) -> LogIndex {
    LogIndex(as_u64(position))
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).expect("test sizes fit a u64")
}
