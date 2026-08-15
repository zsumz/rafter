//! The version gate, at the journal and at the frame.
//!
//! A version this build cannot read is a newer build's work, not damage, so it
//! is refused by *both* entry points. Repairing is the remedy for a frame that
//! was torn; letting it also clear a frame that was merely newer would make the
//! documented answer to "this will not open" a way to delete committed work.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::{raw_journal, LedgerStore, LedgerStoreError},
    AccountId, DurableLedgerStateMachine, Mutation,
};

use scratch::ScratchDir;
use support::{amount, config, execute, open_session};

const ALPHA: AccountId = AccountId::new(11);

fn commit_workload(directory: &Path) -> Vec<u8> {
    let store = LedgerStore::open(directory, config(2, 4)).expect("a ledger store opens");
    let mut app = DurableLedgerStateMachine::new(store, directory.join("raft/snapshots"));
    let commands = [
        open_session(0, 1),
        execute(0, 1, 1, Mutation::OpenAccount { account_id: ALPHA }),
        execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ALPHA,
                amount: amount(40),
            },
        ),
    ];
    for (position, command) in commands.iter().enumerate() {
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

// ---------------------------------------------------------------------------
// A future journal version must be refused, and must stay refused through the
// repair entry point: repairing is for an unreadable *frame*, not for a whole
// file this build does not understand.
// ---------------------------------------------------------------------------

#[test]
fn a_future_journal_version_is_refused_by_both_entry_points() {
    let scratch = ScratchDir::new("version-future-journal");
    let mut bytes = commit_workload(scratch.path());
    bytes[4] = 2; // header version byte
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    let plain = LedgerStore::open(scratch.path(), config(2, 4));
    assert!(
        matches!(
            plain,
            Err(LedgerStoreError::UnsupportedFormatVersion { version: 2 })
        ),
        "a version-2 journal must refuse `open`: {plain:?}"
    );

    let repaired = LedgerStore::open_and_repair(scratch.path(), config(2, 4));
    assert!(
        matches!(
            repaired,
            Err(LedgerStoreError::UnsupportedFormatVersion { version: 2 })
        ),
        "a version-2 journal must refuse `open_and_repair` too, or the repair path becomes a \
         way to delete a newer build's whole history: {repaired:?}"
    );

    let after = raw_journal::read(scratch.path()).expect("the journal reads back");
    assert_eq!(
        after.len(),
        bytes.len(),
        "neither refusal may shorten the file"
    );
}

// ---------------------------------------------------------------------------
// A future FRAME version (a frame appended by a newer build over a header this
// build still reads) is refused by `open` — but `open_and_repair`, the remedy
// the process's own NEEDS_REPAIR message names, deletes it.
// ---------------------------------------------------------------------------

#[test]
fn a_future_frame_version_is_not_deleted_by_the_documented_remedy() {
    let scratch = ScratchDir::new("version-future-frame");
    let mut bytes = commit_workload(scratch.path());
    let before = bytes.len();
    // Byte 4 of the LAST begin record: the frame's version byte. Frames are not
    // equal-sized, so the offset is found by walking the framing the way the
    // store does rather than by arithmetic.
    let mut last_begin = 21_usize;
    let mut offset = 21_usize;
    while offset + 17 <= bytes.len() {
        if bytes[offset..offset + 4] != *b"RLBG" {
            break;
        }
        last_begin = offset;
        let image_len = u32::from_be_bytes(
            bytes[offset + 5..offset + 9]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        offset += 17 + image_len + 13;
    }
    assert_eq!(
        &bytes[last_begin..last_begin + 4],
        b"RLBG",
        "the last begin record was located"
    );
    assert_eq!(
        bytes[last_begin + 4],
        1,
        "the located byte is this build's frame version"
    );
    bytes[last_begin + 4] = 2;
    raw_journal::write(scratch.path(), &bytes).expect("the journal rewrites");

    // A downgrade is not damage, so it gets its own refusal and it survives the
    // remedy for damage. `read_frame` used to fold magic, version, and checksum
    // into one `BeginRecordCorrupt`, which made a newer build's committed work
    // indistinguishable from a torn write — and the operator was told to run
    // the flag that discards torn writes.
    let plain = LedgerStore::open(scratch.path(), config(2, 4));
    assert!(
        matches!(
            plain,
            Err(LedgerStoreError::UnsupportedFrameVersion { version: 2, .. })
        ),
        "a version-2 frame must refuse `open` by name: {plain:?}"
    );

    let repaired = LedgerStore::open_and_repair(scratch.path(), config(2, 4));
    assert!(
        matches!(
            repaired,
            Err(LedgerStoreError::UnsupportedFrameVersion { version: 2, .. })
        ),
        "a version-2 frame must refuse `open_and_repair` too, or the documented remedy for damage \
         becomes a way to delete a newer build's committed work: {repaired:?}"
    );

    let after = raw_journal::read(scratch.path()).expect("the journal reads back");
    assert_eq!(
        after.len(),
        before,
        "neither refusal may shorten the journal"
    );
}
