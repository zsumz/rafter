//! Third-generation probe: the append mark's converse.
//!
//! `TornTail::is_interrupted_append` documents its own obligation exactly:
//!
//! > This is used in one direction only — a tail may be truncated **because**
//! > it is an interrupted append — so it is the converse that has to hold:
//! > this returning `true` must prove no commit point covered those bytes.
//!
//! The proof it then gives is `interrupted => unsealed`, whose contrapositive
//! is `sealed => not interrupted`. Neither is the converse it says it needs.
//! These probes supply the missing direction's counterexample: a frame that
//! *was* sealed, committed, and acknowledged, whose first byte later reads
//! `0x00`.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::{
        raw_journal, LedgerStore, LedgerStoreError, TornTail, BEGIN_LEN, COMMIT_LEN, HEADER_LEN,
    },
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

/// Byte offsets of every sealed frame in a journal, walked the way the store
/// walks it.
fn frame_offsets(bytes: &[u8]) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut offset = HEADER_LEN;
    while offset + BEGIN_LEN <= bytes.len() {
        offsets.push(offset);
        let image_len = u32::from_be_bytes([
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
            bytes[offset + 8],
        ]) as usize;
        offset += BEGIN_LEN + image_len + COMMIT_LEN;
    }
    offsets
}

// ---------------------------------------------------------------------------
// The converse, at the last committed frame.
//
// One byte of the medium is lost, and it is the mark byte. The frame it names
// was sealed by a completed append, its `sync_data` returned, and the caller
// was told `Ok`. Every other single-byte loss inside that frame refuses the
// store. This one is called "residue".
// ---------------------------------------------------------------------------

#[test]
fn a_committed_frame_whose_mark_byte_was_lost_must_not_be_deleted_by_a_read() {
    let scratch = ScratchDir::new("probe-mark-converse-last");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let applied = app.store().applied_index();
    let frames = app.store().recovery().committed_frames();
    let view_before = app.ledger().view();
    let len_before = app.store().journal_len();
    drop(app);

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let offsets = frame_offsets(&bytes);
    let last = *offsets.last().expect("the workload committed frames");
    assert_eq!(bytes[last], b'R', "the frame under test is sealed");
    // Exactly the corruption every other byte of this frame is protected
    // against by a checksum: the mark byte is the one byte no checksum is
    // consulted for, because the mark test runs first and returns.
    bytes[last] = 0x00;
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Err(LedgerStoreError::UnreadableFrame { .. }) => {}
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => {
            let report = *store.recovery();
            let after = raw_journal::read(scratch.path()).expect("the journal reads back");
            panic!(
                "`open` deleted a committed transaction from the medium and returned Ok.\n\
                 corrupted byte      offset {last} ('R' -> 0x00), one byte, inside a sealed frame\n\
                 journal length      {len_before} -> {} (on disk after open)\n\
                 applied index       {applied} -> {}\n\
                 committed frames    {frames} -> {}\n\
                 torn_tail()         = {:?}   (is_interrupted_append = {:?})\n\
                 discarded_bytes()   = {}  <- documented as \"bytes no commit point ever covered\"\n\
                 repair()            = {:?} <- documented as the only thing that loses commits\n\
                 is_clean()          = {}\n\
                 balances before     = {:?}\n\
                 balances after      = {:?}",
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
// The same corruption in the middle of the journal, which is where the store's
// own prose says the stakes are: "a frame in the middle of a long journal can
// become unreadable, and everything after it — whole, correctly sealed,
// acknowledged transactions — is then unreachable too". For every other
// corruption the store refuses. For this one it deletes them.
// ---------------------------------------------------------------------------

#[test]
fn an_interior_frames_lost_mark_byte_must_not_delete_every_frame_after_it() {
    let scratch = ScratchDir::new("probe-mark-converse-interior");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let applied = app.store().applied_index();
    let frames = app.store().recovery().committed_frames();
    let view_before = app.ledger().view();
    drop(app);

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let offsets = frame_offsets(&bytes);
    assert!(offsets.len() >= 4, "the workload wrote several frames");
    let interior = offsets[1];
    let tail_frames = offsets.len() - 1;
    assert_eq!(bytes[interior], b'R', "the frame under test is sealed");
    bytes[interior] = 0x00;
    let len_before = bytes.len();
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    match LedgerStore::open(scratch.path(), config(2, 4)) {
        Err(LedgerStoreError::UnreadableFrame { .. }) => {}
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => {
            let report = *store.recovery();
            let after = raw_journal::read(scratch.path()).expect("the journal reads back");
            panic!(
                "`open` deleted {tail_frames} committed transactions from the medium and \
                 returned Ok.\n\
                 corrupted byte      offset {interior} ('R' -> 0x00) in frame 2 of {}\n\
                 journal length      {len_before} -> {} (on disk after open)\n\
                 applied index       {applied} -> {}\n\
                 committed frames    {frames} -> {}\n\
                 torn_tail()         = {:?}   (is_interrupted_append = {:?})\n\
                 discarded_bytes()   = {}\n\
                 repair()            = {:?}\n\
                 is_clean()          = {}\n\
                 balances before     = {:?}\n\
                 balances after      = {:?}",
                offsets.len(),
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
// The control. Every neighbouring byte of the same frame refuses the store, so
// the verdict is decided by *which* byte was lost rather than by whether the
// frame was committed — which is the property the mark was introduced to
// remove.
// ---------------------------------------------------------------------------

#[test]
fn the_verdict_depends_only_on_which_byte_of_a_committed_frame_was_lost() {
    let mut verdicts = Vec::new();
    // Bytes 0..5 of the begin record: the mark, the rest of the magic, and the
    // version. Byte 5 onward is a length field whose leading byte is already
    // zero, so zeroing it is not a loss and is left out.
    for byte_in_frame in 0..5_usize {
        let scratch = ScratchDir::new(&format!("probe-mark-neighbour-{byte_in_frame}"));
        let commands = workload();
        let mut app = open(scratch.path());
        apply_all(&mut app, &commands);
        drop(app);

        let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
        let offsets = frame_offsets(&bytes);
        let last = *offsets.last().expect("frames exist");
        let original = bytes[last + byte_in_frame];
        bytes[last + byte_in_frame] = 0x00;
        raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

        let verdict = match LedgerStore::open(scratch.path(), config(2, 4)) {
            Err(error) => format!("REFUSED  ({error})"),
            Ok(store) => format!(
                "OPENED   torn_tail = {:?}, is_interrupted_append = {:?}, \
                 discarded = {}, applied index = {}",
                store.recovery().torn_tail(),
                store
                    .recovery()
                    .torn_tail()
                    .map(TornTail::is_interrupted_append),
                store.recovery().discarded_bytes(),
                store.applied_index(),
            ),
        };
        verdicts.push(format!(
            "  frame byte {byte_in_frame} (0x{original:02x} -> 0x00): {verdict}"
        ));
    }

    let opened = verdicts
        .iter()
        .filter(|line| line.contains("OPENED"))
        .count();
    assert_eq!(
        opened,
        0,
        "a one-byte loss inside a sealed, acknowledged frame produced two different \
         verdicts depending on which byte it hit:\n{}",
        verdicts.join("\n")
    );
}

// ---------------------------------------------------------------------------
// The other store's half of the same shape, for the divergence.
//
// `reference/fenced-lock/tests/probe_mark_divergence.rs` presents an
// interrupted write by a newer build — byte zero unsealed, version byte 2 — and
// the lock store refuses. This store used to truncate it, on the strength of
// byte zero alone, and that is the divergence the third-generation hunt found.
//
// The two now agree, and the ledger is the one that moved. Truncating requires
// the unsealed mark *and* evidence the bytes are not a whole frame, and a
// version this build cannot read is exactly the absence of that evidence: not
// knowing the layout is not knowing whether the bytes are whole. Both stores
// refuse, and neither entry point clears it, because the bytes may equally be a
// newer build's committed work.
// ---------------------------------------------------------------------------

#[test]
fn the_ledger_refuses_the_shape_the_lock_store_refuses() {
    let scratch = ScratchDir::new("probe-mark-divergence-ledger");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    drop(app);

    // An interrupted append by a newer build: the unsealed mark, this store's
    // begin magic, a format version this build cannot read, and enough bytes to
    // carry a whole begin record so the version field is actually reached.
    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let length_before = bytes.len();
    let offset = length_before as u64;
    bytes.extend_from_slice(&[0x00, b'L', b'B', b'G', 2]);
    bytes.extend_from_slice(&[0_u8; BEGIN_LEN - 5]);
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    for (entry_point, opened) in [
        ("open", LedgerStore::open(scratch.path(), config(2, 4))),
        (
            "open_and_repair",
            LedgerStore::open_and_repair(scratch.path(), config(2, 4)),
        ),
    ] {
        match opened {
            Err(LedgerStoreError::UnsupportedFrameVersion {
                version: 2,
                offset: at,
            }) => {
                assert_eq!(at, offset, "`{entry_point}` named the wrong offset");
            }
            Err(other) => panic!("`{entry_point}` refused for the wrong reason: {other}"),
            Ok(store) => panic!(
                "`{entry_point}` resolved a tail declaring a version it cannot read, so a \
                 downgrade meeting a newer build's committed frame becomes a way to delete it: \
                 torn tail {:?}, discarded {} bytes",
                store.recovery().torn_tail(),
                store.recovery().discarded_bytes(),
            ),
        }
        assert_eq!(
            raw_journal::read(scratch.path())
                .expect("the journal reads back")
                .len(),
            bytes.len(),
            "`{entry_point}` must not shorten a journal it refused"
        );
    }
}

// ---------------------------------------------------------------------------
// The other side of that gate: a foreign version byte in a tail too short to
// hold a begin record is still ordinary residue, because the bytes are a strict
// prefix whatever build wrote them. The evidence of incompleteness comes from
// the length, not from a field this build cannot interpret.
// ---------------------------------------------------------------------------

#[test]
fn a_short_tail_is_residue_whatever_version_byte_it_carries() {
    let scratch = ScratchDir::new("probe-mark-divergence-short");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let applied = app.store().applied_index();
    drop(app);

    let mut bytes = raw_journal::read(scratch.path()).expect("the journal reads");
    let committed_len = bytes.len();
    bytes.extend_from_slice(&[0x00, b'L', b'B', b'G', 2, 0, 0, 0, 8]);
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let store = LedgerStore::open(scratch.path(), config(2, 4))
        .expect("nine bytes cannot be a whole frame under any layout");
    assert_eq!(store.applied_index(), applied);
    assert_eq!(
        store.recovery().torn_tail(),
        Some(TornTail::UnsealedAppend { present: 9 })
    );
    assert_eq!(
        raw_journal::read(scratch.path())
            .expect("the journal reads back")
            .len(),
        committed_len,
        "the residue is truncated off the medium"
    );
}
