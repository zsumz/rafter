//! A zero-filled tail gets one verdict, at every length.
//!
//! These sweep the *boundary* of `TornTail::is_truncatable_residue` rather than
//! poking one point. The classifier used to flip on length alone here: below a
//! begin record any bytes at all were residue, and at or above it the same zeros
//! failed the magic test and became corruption an operator had to clear with a
//! destructive repair. Neither verdict was a statement about whether the bytes
//! were committed, which is what the classifier is for.
//!
//! Zeros past the last committed frame are what a crash on a delayed-allocation
//! filesystem leaves — the file's size reached the medium and its data did not.
//! They are truncated, and that is a trade rather than a reading: the same bytes
//! are what a zeroed sector leaves over the last committed frames, and
//! `TornTail::is_truncatable_residue` argues why refusing them would be worse
//! and what accepting them costs. These tests pin the uniformity of the verdict.
//! They do not, and cannot, show the bytes were uncommitted — which is why the
//! predicate they assert on is the disjunction and not
//! `TornTail::is_interrupted_append`, whose proof does not reach here.

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
    /// Truncated away by `open`, with no flag and no operator.
    ///
    /// Not "as benign", which is what this arm was called. `open` truncating a
    /// zero tail is not a finding that the bytes were uncommitted; it is a
    /// trade taken in their absence, and the bytes may have been a committed
    /// frame a zeroed sector erased. `RecoveryReport::discarded_without_proof`
    /// is what the store says about that, and the name here should not say
    /// something softer than the store does.
    Truncated(TornTail),
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
                "a store that opened over a zero tail truncated it off the medium"
            );
            assert_eq!(
                store.recovery().discarded_without_proof(),
                zeros as u64,
                "every byte `open` shortened here was shortened without a proof it was \
                 uncommitted, and the report has to say so"
            );
            Verdict::Truncated(tail)
        }
        Err(LedgerStoreError::UnreadableFrame { corruption, .. }) => {
            Verdict::RefusedAsCorruption(corruption)
        }
        Err(other) => panic!("unexpected failure at {zeros} zero bytes: {other}"),
    }
}

// ---------------------------------------------------------------------------
// One kind of residue, one verdict — and it has to be the truncating one.
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

    let truncated: Vec<_> = transcript
        .iter()
        .filter(|(_, verdict)| matches!(verdict, Verdict::Truncated(_)))
        .collect();
    let refused: Vec<_> = transcript
        .iter()
        .filter(|(_, verdict)| matches!(verdict, Verdict::RefusedAsCorruption(_)))
        .collect();

    assert!(
        refused.is_empty(),
        "one kind of residue — zeros past the last committed frame — gets two opposite \
         verdicts depending only on how many bytes landed.\n\
         truncated: {truncated:?}\n\
         refused as corruption needing an operator: {refused:?}\n\
         The flip used to sit at BEGIN_LEN (17): below it `read_frame` never reached the magic \
         test, so any bytes at all were `PartialBeginRecord`; at or above it the zeros failed \
         the magic test and became `BeginRecordCorrupt`. Neither verdict was a statement about \
         whether the bytes were committed, which is what the two truncation rules are for."
    );
    // "One verdict" is not enough on its own: refusing every length would be
    // consistent too, and would brick a replica on the most ordinary crash
    // there is. So the verdict has to be the truncating one — not because these
    // bytes are proved uncommitted, which no test on them can show, but because
    // `TornTail::is_truncatable_residue` takes that trade deliberately and
    // reports what it may have cost.
    assert_eq!(
        truncated.len(),
        transcript.len(),
        "a zero-filled tail is truncated at every length"
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
        matches!(verdict, Verdict::Truncated(_)),
        "64 zero bytes past the last committed frame — a crash that extended the file \
         without persisting its data — is {verdict:?}. `open` refuses, `replica.rs` maps \
         `UnreadableFrame` to `ApplicationStoreNeedsRepair`, the readiness gate never opens, \
         and the only way forward is `--repair-app-store true`, which discards from the \
         unreadable offset to the end of the file — strictly more than truncating the tail."
    );
}

/// And what it costs is reported, so the replica coming back is not the same
/// thing as the crash having lost nothing.
///
/// This is the pair to the test above and exists because that test's argument
/// is a *trade*: refusing would be worse. A trade is only honest if the price
/// is on the receipt, and this is the receipt. The store's own words for the
/// two halves are `discarded_bytes` — everything opening shortened — and
/// `discarded_without_proof`, the part it could not show was uncommitted.
#[test]
fn what_a_truncated_zero_tail_may_have_cost_is_reported_rather_than_implied() {
    let scratch = ScratchDir::new("zero-tail-reported");
    let mut bytes = commit_workload(scratch.path());
    let committed_len = bytes.len();
    bytes.extend(std::iter::repeat_n(0_u8, 96));
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let store = LedgerStore::open(scratch.path(), config(2, 4)).expect("a zero tail opens");
    let recovery = store.recovery();
    assert_eq!(recovery.discarded_bytes(), 96);
    assert_eq!(
        recovery.discarded_without_proof(),
        96,
        "a zero tail is truncated on the weaker premise, and every byte of it counts"
    );
    assert_eq!(
        recovery.repair(),
        None,
        "no flag was involved, which is exactly why the count above has to exist"
    );
    assert!(!recovery.is_clean(), "and the opening is not a clean one");
    assert_eq!(store.journal_len(), committed_len as u64);

    // The other side of the boundary: residue rule one *proved* uncommitted is
    // discarded too, and reports nothing under the second count. A report that
    // said "possibly committed" after every ordinary crash would be a report
    // nobody reads.
    let interrupted = ScratchDir::new("unsealed-tail-reported");
    let mut bytes = commit_workload(interrupted.path());
    bytes.extend_from_slice(&[0x00, b'L', b'B', b'G', 1, 0xAB]);
    raw_journal::write(interrupted.path(), &bytes).expect("the journal rewrites");

    let store =
        LedgerStore::open(interrupted.path(), config(2, 4)).expect("an interrupted append opens");
    assert_eq!(
        store.recovery().torn_tail(),
        Some(TornTail::UnsealedAppend { present: 6 })
    );
    assert_eq!(store.recovery().discarded_bytes(), 6);
    assert_eq!(
        store.recovery().discarded_without_proof(),
        0,
        "rule one proved no commit point covered these, so nothing here is unproven"
    );
}
