//! Regression: two scope statements about `LedgerStore::open` that the code did
//! not keep.
//!
//! **One.** `open_and_repair` called itself "the destructive half of
//! [`LedgerStore::open`]" and said "Opening a store is a read"; `open_inner`
//! said the two entry points "differ in exactly one branch … whether a region a
//! commit point may have covered is allowed to disappear"; and the consumer
//! binary told its operator that "the only thing that shortens this journal is
//! the repair path, and reaching it takes an explicit flag rather than a
//! restart, because the transactions it discards may be ones this replica
//! already acknowledged to a client."
//!
//! `is_truncatable_residue`'s own rule two said the opposite on the same page: a
//! `ZeroFilledToEnd` tail "cannot" be shown to be uncommitted, "a committed
//! final frame that a zeroed sector erased leaves exactly these bytes", and it
//! is truncated. By `open`.
//!
//! The truncation stands — the argument for it is on `is_truncatable_residue`,
//! and gating it would refuse the store on an ordinary power cut and send every
//! operator to a flag that discards strictly more. What changed is that the
//! prose stopped denying it and the report started naming it:
//! `RecoveryReport::discarded_without_proof` is the count, and the process
//! announces it as `possibly_committed=`.
//!
//! **Two.** `read_frame` justified refusing every zero run that is not
//! zeros-to-end with: "any run with a single non-zero byte anywhere after it,
//! **which is every run that has a committed frame behind it**". The two halves
//! of that sentence are not the same set. An interrupted append whose leading
//! bytes did not reach the medium is a run with non-zero bytes after it and no
//! committed frame anywhere behind it, and it fell in neither rule's scope nor
//! either rule's stated out-of-scope list.
//!
//! It is `TornTail::NotALedgerFrame` and it refuses, which is fail-closed and
//! correct: the same bytes are what a committed final frame leaves when a zeroed
//! region takes its identity and stops before its end. The classification did
//! not move. It is now enumerated under rule one's out-of-scope list, the
//! variant's own doc no longer claims an append cannot produce it, and the
//! "which is" that joined two different sets is gone.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::{raw_journal, LedgerStore, LedgerStoreError, TornTail},
    AccountId, Command, DurableLedgerStateMachine, Mutation,
};

use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);

fn open(directory: &Path) -> DurableLedgerStateMachine {
    let store = LedgerStore::open(directory, config(2, 4)).expect("a ledger store opens");
    DurableLedgerStateMachine::new(store, directory.join("raft/snapshots"))
}

fn commands(frames: usize) -> Vec<Command> {
    let mut commands = vec![
        open_session(0, 1),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
    ];
    for sequence in 2..frames as u64 {
        commands.push(execute(
            0,
            1,
            sequence,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(2),
            },
        ));
    }
    commands
}

/// Commits `frames` transactions and returns the byte offset each one ended at.
fn commit_frames(directory: &Path, frames: usize) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut app = open(directory);
    for (position, command) in commands(frames).iter().enumerate() {
        app.apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(position as u64 + 1),
                term: Term(1),
                command: command.clone(),
                local_proposal_id: None,
            }],
        })
        .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
        offsets.push(
            raw_journal::read(directory)
                .expect("the journal reads")
                .len(),
        );
    }
    offsets
}

// ---------------------------------------------------------------------------
// One. The read-only entry point deletes an acknowledged transaction, and
// reports no repair for having done it.
// ---------------------------------------------------------------------------

/// The whole claim, on one store: acknowledged, crashed, restarted with no
/// flag, gone.
///
/// The store is asked what it holds *before* the damage, so the loss is stated
/// in the terms a client would have seen rather than in bytes.
#[test]
fn gen5_open_without_the_repair_flag_discards_an_acknowledged_transaction() {
    let scratch = ScratchDir::new("gen5-open-is-not-a-read");
    let offsets = commit_frames(scratch.path(), 4);
    let last_start = *offsets
        .get(offsets.len() - 2)
        .expect("four frames committed");

    let before = LedgerStore::open(scratch.path(), config(2, 4)).expect("the store reopens clean");
    let acknowledged_index = before.applied_index();
    let acknowledged_frames = before.recovery().committed_frames();
    drop(before);

    // The last frame's sector reaches the drive as zeros. Nothing else changes.
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let total = bytes.len();
    for byte in &mut bytes[last_start..] {
        *byte = 0;
    }
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    // No flag. No repair entry point. The plain read.
    let after = LedgerStore::open(scratch.path(), config(2, 4))
        .expect("`open` accepts a zero-filled tail rather than refusing it");

    println!(
        "before: applied_index={acknowledged_index:?} committed_frames={acknowledged_frames}\n\
         after `LedgerStore::open`: applied_index={:?} committed_frames={} \
         torn_tail={:?} discarded_bytes={} repair={:?}\n\
         journal shortened {total} -> {}",
        after.applied_index(),
        after.recovery().committed_frames(),
        after.recovery().torn_tail(),
        after.recovery().discarded_bytes(),
        after.recovery().repair(),
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
    );

    assert_eq!(
        after.recovery().torn_tail(),
        Some(TornTail::ZeroFilledToEnd {
            present: (total - last_start) as u64
        }),
    );
    assert!(
        after.applied_index() < acknowledged_index,
        "the applied floor must not have moved backwards under a read"
    );
    assert_eq!(
        after.recovery().repair(),
        None,
        "and the report offers the caller no repair to look at"
    );
    assert_eq!(
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
        last_start,
        "`open` shortened the file"
    );

    // What the fix owes: the loss is not denied and is not left to a caller who
    // knows to match on the variant. The count is on the report, and it is the
    // count of bytes this opening deleted without being able to show no commit
    // point covered them.
    assert_eq!(
        after.recovery().discarded_without_proof(),
        (total - last_start) as u64,
        "every byte `open` deleted here was deleted on the weaker premise"
    );
    assert!(
        !after.recovery().is_clean(),
        "and an opening that may have deleted an acknowledged transaction is not a clean one"
    );
}

// ---------------------------------------------------------------------------
// Two. One interrupted append, two sector orders, opposite verdicts.
//
// The verdicts are right and unchanged. What was wrong is that the refusing one
// was justified by a sentence that did not describe it, and that the case
// appeared in neither truncation rule's out-of-scope list. It is in rule one's
// now, and this is the boundary that list names.
// ---------------------------------------------------------------------------

/// Leaves a partial frame four behind, with `unpersisted` leading bytes zero.
///
/// This is one physical event throughout: an append of frame four that never
/// reached its seal. `unpersisted == 0` is the front-to-back writeback the
/// truncation rule was written for; anything larger is the same append with its
/// first bytes still in a cache the crash took.
fn interrupted_append(label: &str, unpersisted: usize) -> (ScratchDir, usize) {
    let scratch = ScratchDir::new(label);
    let offsets = commit_frames(scratch.path(), 4);
    let last_start = offsets[offsets.len() - 2];

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let total = bytes.len();
    // Half a frame present, which is what a prefix is.
    bytes.truncate(last_start + (total - last_start) / 2);
    // An append writes its first byte unsealed and seals it last.
    bytes[last_start] = 0x00;
    for byte in &mut bytes[last_start..last_start + unpersisted] {
        *byte = 0;
    }
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");
    (scratch, last_start)
}

/// The flip. Same residue, same guarantee available about it, opposite answers.
#[test]
fn gen5_an_interrupted_append_is_truncated_or_refused_by_sector_order_alone() {
    // Front to back: the mark is there, the identity is there, the frame is a
    // prefix. Rule one, and it truncates.
    let (in_order, committed_len) = interrupted_append("gen5-append-in-order", 0);
    let store = LedgerStore::open(in_order.path(), config(2, 4))
        .expect("an interrupted append is residue `open` truncates");
    println!(
        "leading bytes persisted:     tail={:?} -> journal {} bytes, opens",
        store.recovery().torn_tail(),
        raw_journal::read(in_order.path())
            .expect("the journal reads back")
            .len(),
    );
    assert!(matches!(
        store.recovery().torn_tail(),
        Some(TornTail::UnsealedAppend { .. })
    ));
    assert_eq!(
        raw_journal::read(in_order.path())
            .expect("the journal reads back")
            .len(),
        committed_len,
    );
    drop(store);

    // The same append, with one 16-bit word of its front still in the cache the
    // crash took. No committed frame is behind it — there is nothing behind it
    // at all — and the store refuses.
    for unpersisted in [2_usize, 3, 16, 32] {
        let (out_of_order, _) =
            interrupted_append(&format!("gen5-append-gap-{unpersisted}"), unpersisted);
        let refused = LedgerStore::open(out_of_order.path(), config(2, 4))
            .expect_err("the same interrupted append refuses when its leading bytes did not land");
        println!("{unpersisted} leading bytes lost:  {refused}");
        assert!(
            matches!(
                refused,
                LedgerStoreError::UnreadableFrame {
                    corruption: TornTail::NotALedgerFrame { .. },
                    ..
                }
            ),
            "{unpersisted}: {refused}"
        );

        // And the way out is the destructive entry point, which reports these
        // bytes as a `Repair`. That is not a mistake in the report: a `Repair`
        // is an upper bound on the loss, computed from where the scan stopped,
        // and here the bound happens to be an upper bound on nothing. Nothing
        // reading those bytes could have said so, which is why the entry point
        // is a decision rather than a verdict.
        let repaired = LedgerStore::open_and_repair(out_of_order.path(), config(2, 4))
            .expect("the repair entry point clears it");
        println!(
            "{unpersisted} leading bytes lost:  repaired, repair={:?}",
            repaired.recovery().repair()
        );
        assert!(
            repaired.recovery().repair().is_some(),
            "{unpersisted}: an uncommitted append is reported as a repair"
        );
        assert_eq!(
            repaired.recovery().discarded_without_proof(),
            0,
            "{unpersisted}: a repair's losses are the repair's to count, not this field's"
        );
    }
}
