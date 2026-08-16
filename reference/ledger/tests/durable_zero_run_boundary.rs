//! Where the verdict on a run of zeros flips, and what falls on each side.
//!
//! Generation three's rule was "truncating requires the unsealed mark *and*
//! positive evidence that the bytes are not a whole frame", proved from a
//! single-fault assumption. The rule was sound over its scope and the prose
//! claimed a scope one step wider: **one** zeroed byte is the single fault it
//! covers, and nothing tested **two**. Two adjacent zero bytes — one 16-bit
//! word, far under a sector, and one physical event rather than two — reached
//! the begin magic, which the store consulted only *below* the mark test. The
//! re-read failed at `BeginRecordCorrupt`, `classify_unsealed` folded that into
//! `UnsealedAppend`, and `open` deleted the frame and every committed frame
//! after it while reporting the loss as "bytes no commit point ever covered".
//!
//! The sibling fenced-lock store refused the identical shape at every length,
//! because its `verify_identity` reads the magic *above* the mark test. This
//! suite is the ledger's half of that agreement, and it sweeps the boundary
//! lengths on both sides of every rule rather than poking one point:
//!
//! - a zero run over a committed frame, at 1, 2, 3, and a sector — refused at
//!   every length, for two different reasons that meet at two bytes;
//! - a zero run to the end of the file, which is the one shape that is still
//!   truncated, and the residual loss that admits;
//! - the boundary between those two, which is one non-zero byte.

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

/// One sector, which is the unit a drive returns zeros for when it cannot read
/// a block and the unit a delayed allocation leaves unwritten.
const SECTOR: usize = 512;

fn open(directory: &Path) -> DurableLedgerStateMachine {
    let store = LedgerStore::open(directory, config(2, 4)).expect("a ledger store opens");
    DurableLedgerStateMachine::new(store, directory.join("raft/snapshots"))
}

/// A workload of `frames` transactions: a session, an account, then deposits.
///
/// The count is a parameter because one of the runs swept below is a whole
/// sector, and a journal has to be long enough for committed frames to sit
/// *behind* a sector of damage. That the shortest journal cannot express the
/// case is itself worth knowing: with four frames a zeroed sector reaches the
/// end of the file, which is the residual-loss shape rather than this one.
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

/// What `open` made of a journal, in the two shapes that matter.
#[derive(Debug)]
enum Verdict {
    Refused(TornTail),
    Truncated {
        from: usize,
        to: usize,
        tail: Option<TornTail>,
    },
}

impl Verdict {
    /// What this verdict cost, in the terms the finding is about.
    fn cost(&self) -> String {
        match self {
            Self::Refused(corruption) => format!("REFUSED   ({corruption})"),
            Self::Truncated { from, to, tail } => format!(
                "TRUNCATED {from} -> {to} bytes, {} deleted, tail {tail:?}",
                from - to
            ),
        }
    }
}

/// Zeroes `run` bytes at the start of the third frame of a journal long enough
/// to keep committed frames behind the damage, and reports what a fresh opener
/// did.
fn verdict_for_zero_run(label: &str, run: usize) -> Verdict {
    let scratch = ScratchDir::new(label);
    // Enough frames that `run` zeros never reach the end of the file: that is
    // the difference between this case and the residual-loss case below, and
    // the assertion checks it rather than trusting the arithmetic.
    let offsets = commit_frames(scratch.path(), 8 + run / 64);
    let start = offsets[1];
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let before = bytes.len();
    assert!(
        start + run < before,
        "the fixture must leave committed frames behind the damage: {run} zeros at {start} \
         of {before}"
    );
    for byte in &mut bytes[start..start + run] {
        *byte = 0;
    }
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Err(LedgerStoreError::UnreadableFrame { corruption, .. }) => {
            assert_eq!(
                raw_journal::read(scratch.path())
                    .expect("the journal reads back")
                    .len(),
                before,
                "a refusal must not shorten the journal"
            );
            Verdict::Refused(corruption)
        }
        Err(other) => panic!("unexpected failure at {run} zero bytes: {other}"),
        Ok(store) => Verdict::Truncated {
            from: before,
            to: raw_journal::read(scratch.path())
                .expect("the journal reads back")
                .len(),
            tail: store.recovery().torn_tail(),
        },
    }
}

// ---------------------------------------------------------------------------
// The finding. A zero run over a committed frame that has another committed
// frame behind it is refused at every run length, and the two adjacent lengths
// that used to disagree now agree.
// ---------------------------------------------------------------------------

/// Boundary lengths 1, 2, 3, and a sector, over the *third* of four frames.
///
/// One byte is the mark alone: the re-read finds a whole frame and refuses with
/// `UnsealedCompleteFrame`. Two bytes reach the identity and refuse with
/// `NotALedgerFrame`. Those are two different rules meeting at the boundary
/// between them, which is why both verdicts appear below and why the assertion
/// is that neither truncates rather than that both name the same variant.
#[test]
fn a_zero_run_over_a_committed_frame_is_refused_at_every_length() {
    let mut transcript = Vec::new();
    for run in [1_usize, 2, 3, SECTOR] {
        let verdict = verdict_for_zero_run(&format!("zero-run-mid-{run}"), run);
        transcript.push(format!(
            "  {run} zero byte(s) over frame three: {}",
            verdict.cost()
        ));
        assert!(
            matches!(verdict, Verdict::Refused(_)),
            "a zero run over a committed frame with committed frames behind it must refuse:\n{}",
            transcript.join("\n")
        );
    }

    // The two rules that meet at two bytes, named, so a change that collapses
    // them into one says which one it kept.
    let one = verdict_for_zero_run("zero-run-boundary-1", 1);
    assert!(
        matches!(
            one,
            Verdict::Refused(TornTail::UnsealedCompleteFrame { .. })
        ),
        "one zeroed byte is the mark alone and must be answered by the re-read: {one:?}"
    );
    let two = verdict_for_zero_run("zero-run-boundary-2", 2);
    assert!(
        matches!(two, Verdict::Refused(TornTail::NotALedgerFrame { .. })),
        "two zeroed bytes reach the identity and must be answered above the mark: {two:?}"
    );
}

/// A zeroed sector over one committed frame, stated as the loss it used to be.
///
/// This is the shape a drive returns for a block it cannot read and the shape a
/// delayed-allocation filesystem leaves for a block whose data never reached the
/// medium: one physical event, many changed bytes. It used to truncate the
/// zeroed frame *and* the whole untouched committed frame after it, return `Ok`,
/// and report the loss as bytes no commit point ever covered.
#[test]
fn a_zeroed_sector_over_a_committed_frame_never_reaches_the_frame_behind_it() {
    let scratch = ScratchDir::new("zeroed-sector");
    let offsets = commit_frames(scratch.path(), 16);
    let third_start = offsets[1];
    let fourth_start = offsets[2];

    let healthy = LedgerStore::open(scratch.path(), config(2, 4)).expect("the healthy store opens");
    let committed_balance = healthy.ledger().view().accounts.clone();
    let committed_applied = healthy.applied_index();
    let committed_frames = healthy.recovery().committed_frames();
    drop(healthy);

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let total = bytes.len();
    assert!(
        third_start + SECTOR < total,
        "the fixture must leave committed frames behind a whole sector of damage"
    );
    for byte in &mut bytes[third_start..third_start + SECTOR] {
        *byte = 0;
    }
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let error = LedgerStore::open(scratch.path(), config(2, 4)).expect_err(
        "a zeroed sector over a committed frame must not be resolved by a read: it takes \
         every committed frame after it, which are whole, correctly sealed and acknowledged",
    );
    let LedgerStoreError::UnreadableFrame {
        corruption, offset, ..
    } = error
    else {
        panic!("a zeroed committed frame refused for the wrong reason: {error}");
    };
    assert!(
        matches!(corruption, TornTail::NotALedgerFrame { .. }),
        "a zeroed sector destroys the begin identity and must be named as that: {corruption:?}"
    );
    assert_eq!(
        offset, third_start as u64,
        "the refusal named the wrong frame"
    );
    assert_eq!(
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
        total,
        "nothing may be shortened, and frame four at byte {fourth_start} least of all"
    );

    // The repair entry point is the documented way forward, and it is the only
    // thing that may shorten the file. What it costs is reported.
    let repaired = LedgerStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("the repair entry point resolves what `open` refuses");
    let repair = repaired
        .recovery()
        .repair()
        .expect("a repair that discarded a region records it");
    assert_eq!(repair.offset(), third_start as u64);
    assert!(
        repair.discarded_bytes() > 0,
        "a repair that shortened the file must count the bytes"
    );
    assert_eq!(
        repaired.recovery().discarded_bytes(),
        0,
        "a repair's losses are not counted as bytes no commit point covered"
    );
    assert!(
        repaired.applied_index() < committed_applied
            && repaired.recovery().committed_frames() < committed_frames
            && repaired.ledger().view().accounts != committed_balance,
        "the repair is the destructive act and must be visible as one"
    );
}

// ---------------------------------------------------------------------------
// The other side. Zeros to the end of the file stay truncatable, because that
// is what an ordinary crash on a delayed-allocation filesystem leaves, and the
// residual loss that admits is bounded and named.
// ---------------------------------------------------------------------------

/// The one shape still truncated, and the one non-zero byte that ends it.
///
/// `durable_zero_tail.rs` sweeps the truncatable side by length. This pins the
/// *boundary*: the rule's scope is "zeros all the way to the end of the file",
/// and a single non-zero byte after them puts the same zeros outside it.
#[test]
fn one_non_zero_byte_after_a_zero_run_moves_it_from_truncated_to_refused() {
    for zeros in [1_usize, 2, 3, SECTOR] {
        let scratch = ScratchDir::new(&format!("zero-tail-boundary-{zeros}"));
        commit_frames(scratch.path(), 4);
        let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
        let committed_len = bytes.len();
        bytes.extend(std::iter::repeat_n(0_u8, zeros));
        raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

        let store = LedgerStore::open(scratch.path(), config(2, 4)).unwrap_or_else(|error| {
            panic!("{zeros} zeros to end of file are ordinary crash residue: {error}")
        });
        assert_eq!(
            store.recovery().torn_tail(),
            Some(TornTail::ZeroFilledToEnd {
                present: zeros as u64
            }),
            "{zeros} zeros to end of file must be named as the residue they are"
        );
        assert_eq!(
            raw_journal::read(scratch.path())
                .expect("the journal reads back")
                .len(),
            committed_len,
            "{zeros} zeros to end of file are truncated"
        );
        drop(store);

        // One non-zero byte after them, and the same zeros are outside the rule.
        bytes.push(0x01);
        raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");
        let error = LedgerStore::open(scratch.path(), config(2, 4)).expect_err(&format!(
            "{zeros} zeros followed by one non-zero byte are not zeros to the end of the file"
        ));
        assert!(
            matches!(
                error,
                LedgerStoreError::UnreadableFrame {
                    corruption: TornTail::NotALedgerFrame { .. },
                    ..
                }
            ),
            "{zeros} zeros plus one byte must refuse: {error}"
        );
    }
}

/// The residual loss, pinned on both sides so it cannot grow quietly.
///
/// A committed frame that is both the *last* frame and entirely zeroed is
/// discarded, and the transactions in it are lost. Nothing separates it from the
/// delayed allocation the rule exists for. What the rule does guarantee is the
/// bound: the loss stops at the zeros. The frame *before* the damage survives,
/// which is the whole difference from the shape this suite was written for,
/// where an untouched frame behind the damage went with it.
#[test]
fn a_zeroed_final_frame_is_lost_and_the_loss_stops_there() {
    let scratch = ScratchDir::new("zeroed-final-frame");
    let offsets = commit_frames(scratch.path(), 4);
    let fourth_start = offsets[2];

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let total = bytes.len();
    for byte in &mut bytes[fourth_start..] {
        *byte = 0;
    }
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let store = LedgerStore::open(scratch.path(), config(2, 4))
        .expect("zeros to the end of the file are the residue `open` may truncate");
    assert_eq!(
        store.recovery().torn_tail(),
        Some(TornTail::ZeroFilledToEnd {
            present: (total - fourth_start) as u64
        }),
    );
    // The loss, stated: one frame, the zeroed one.
    assert_eq!(
        store.recovery().committed_frames(),
        3,
        "the zeroed final frame is lost — this is the residual loss of the zero-fill rule"
    );
    // The bound, stated: it stops at the zeros. Every byte discarded was zero,
    // and the frame before the damage is intact.
    assert_eq!(
        store.applied_index(),
        LogIndex(3),
        "the loss must stop at the damage rather than running back through the journal"
    );
    assert_eq!(
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
        fourth_start,
        "exactly the zeroed region is discarded and nothing before it"
    );
}
