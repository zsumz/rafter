//! A zero-filled tail gets one verdict, at every length.
//!
//! These sweep the *boundary* of `TornTail::is_interrupted_append` rather than
//! poking one point. The classifier used to flip on length alone here: below a
//! begin record any bytes at all were benign residue, and at or above it the
//! same zeros failed the magic test and became corruption an operator had to
//! clear with a destructive repair. Neither verdict was a statement about
//! whether the bytes were committed, which is what the classifier is for.
//!
//! Zeros past the last committed frame are what a crash on a delayed-allocation
//! filesystem leaves — the file's size reached the medium and its data did not
//! — and they are also, exactly, what an unsealed frame mark looks like. The
//! store puts that mark there on purpose, so the ordinary residue of a crash
//! reads as what it is.

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
    ]
}

fn open(directory: &Path) -> DurableLedgerStateMachine {
    let store = LedgerStore::open(directory, config(2, 4)).expect("a ledger store opens");
    DurableLedgerStateMachine::new(store)
}

fn commit_workload(directory: &Path) -> Vec<u8> {
    let mut app = open(directory);
    for (position, command) in workload().iter().enumerate() {
        app.apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(position as u64 + 1),
                term: Term(1),
                command: command.clone(),
                local_proposal_id: None,
            }],
        })
        .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
    drop(app);
    raw_journal::read(directory).expect("the journal reads")
}

/// What `open` made of a journal whose committed prefix is followed by `zeros`
/// zero bytes.
#[derive(Debug, Eq, PartialEq)]
enum Verdict {
    /// Classified as an interrupted append and silently truncated away.
    TruncatedAsBenign(TornTail),
    /// Classified as corruption; the store refuses and needs an operator.
    RefusedAsCorruption(TornTail),
}

fn verdict_for_zero_tail(zeros: usize) -> Verdict {
    let scratch = ScratchDir::new(&format!("zero-tail-{zeros}"));
    let mut bytes = commit_workload(scratch.path());
    let committed_len = bytes.len();
    bytes.extend(std::iter::repeat_n(0_u8, zeros));
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Ok(store) => {
            let tail = store
                .recovery()
                .torn_tail()
                .expect("a zero tail is residue of some kind");
            let after = raw_journal::read(scratch.path()).expect("the journal reads back");
            assert_eq!(
                after.len(),
                committed_len,
                "a benign verdict truncates the tail off the medium"
            );
            Verdict::TruncatedAsBenign(tail)
        }
        Err(LedgerStoreError::UnreadableFrame { corruption, .. }) => {
            Verdict::RefusedAsCorruption(corruption)
        }
        Err(other) => panic!("unexpected failure at {zeros} zero bytes: {other}"),
    }
}

// ---------------------------------------------------------------------------
// One kind of residue, one verdict — and it has to be the benign one.
//
// A zero fill was a fifth shape the four-shape enumeration did not name, and
// the verdict on it flipped purely on how many zeros landed.
// ---------------------------------------------------------------------------

#[test]
fn a_zero_filled_tail_gets_one_verdict_regardless_of_its_length() {
    let mut transcript = Vec::new();
    for zeros in [1_usize, 8, 16, 17, 18, 32, 64, 512] {
        transcript.push((zeros, verdict_for_zero_tail(zeros)));
    }

    let benign: Vec<_> = transcript
        .iter()
        .filter(|(_, verdict)| matches!(verdict, Verdict::TruncatedAsBenign(_)))
        .collect();
    let refused: Vec<_> = transcript
        .iter()
        .filter(|(_, verdict)| matches!(verdict, Verdict::RefusedAsCorruption(_)))
        .collect();

    assert!(
        refused.is_empty(),
        "one kind of residue — zeros past the last committed frame — gets two opposite \
         verdicts depending only on how many bytes landed.\n\
         truncated away as an interrupted append: {benign:?}\n\
         refused as corruption needing an operator: {refused:?}\n\
         The flip used to sit at BEGIN_LEN (17): below it `read_frame` never reached the magic \
         test, so any bytes at all were `PartialBeginRecord`; at or above it the zeros failed \
         the magic test and became `BeginRecordCorrupt`. Neither verdict was a statement about \
         whether the bytes were committed, which is what `is_interrupted_append` is for."
    );
    // "One verdict" is not enough on its own: refusing every length would be
    // consistent too, and would brick a replica on the most ordinary crash
    // there is. The verdict has to be the benign one, because zeros past the
    // last committed frame *are* the unsealed append mark.
    assert_eq!(
        benign.len(),
        transcript.len(),
        "a zero-filled tail is what an interrupted append leaves, at every length"
    );
}

// ---------------------------------------------------------------------------
// The same, stated as the thing the caller sees: a replica that crashed on a
// delayed-allocation filesystem must come back, rather than refusing to serve
// and announcing NEEDS_REPAIR at a supervisor whose only remedy is destructive.
// ---------------------------------------------------------------------------

#[test]
fn an_ordinary_crash_residue_does_not_demand_a_destructive_repair() {
    let verdict = verdict_for_zero_tail(64);
    assert!(
        matches!(verdict, Verdict::TruncatedAsBenign(_)),
        "64 zero bytes past the last committed frame — a crash that extended the file \
         without persisting its data — is {verdict:?}. `open` refuses, `replica.rs` maps \
         `UnreadableFrame` to `ApplicationStoreNeedsRepair`, the readiness gate never opens, \
         and the only documented way forward is `--repair-app-store true`, which discards \
         from the unreadable offset to the end of the file. The store treats the one residue \
         that lost nothing as fatal, and the residue that loses a committed frame \
         (a short file) as benign."
    );
}
