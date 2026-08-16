//! The version gate, asserted at every length a slot can have.
//!
//! A format version is refused because a slot declaring another version was
//! written whole by another build, which needs no corruption at all. That
//! argument is about the *field*, so it has to hold wherever the field is
//! present — and the gate used to sit behind a full-header slice, which made
//! the same bytes readable at one length and refused at another.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_fenced_lock::{
    store::{raw_slot, LockStore, LockStoreError, SlotDamage},
    Command, DurableLockStateMachine,
};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, submit};

const RESOURCE: &str = "orders/shard-0";

fn commit_workload(directory: &Path) -> DurableLockStateMachine {
    let store = LockStore::open(directory, config(2, 4)).expect("a lock store opens");
    let mut app = DurableLockStateMachine::new(store, directory.join("raft/snapshots"));
    let commands: Vec<Command> = vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
    ];
    for (position, command) in commands.iter().enumerate() {
        app.apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(position as u64 + 1),
                term: Term(1),
                command: *command,
                local_proposal_id: None,
            }],
        })
        .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
    app
}

/// The damage `open` reports for a live slot whose bytes were altered, or the
/// refusal it produced.
fn verdict(directory: &Path) -> Result<Option<SlotDamage>, SlotDamage> {
    match LockStore::open(directory, config(2, 4)) {
        Err(LockStoreError::UnreadableSlot { damage, .. }) => Err(damage),
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => Ok(store.recovery().damaged_slot().map(|(_, damage)| damage)),
    }
}

// ---------------------------------------------------------------------------
// A bitflip in the version byte must never turn a refused slot into a readable
// one. The version byte is inside the header checksum's coverage, so flipping
// a version-2 slot back to version 1 must break that checksum.
// ---------------------------------------------------------------------------

#[test]
fn a_version_bitflip_is_fail_closed_in_both_directions() {
    let scratch = ScratchDir::new("version-bitflip");
    let app = commit_workload(scratch.path());
    let live = app.store().live_slot().expect("the workload committed");
    drop(app);

    let healthy = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    assert_eq!(healthy[4], 1, "byte 4 is this build's version");

    // Readable -> refused. Fail-closed: availability lost, safety kept.
    let mut flipped = healthy.clone();
    flipped[4] = 3;
    raw_slot::write(scratch.path(), live, &flipped).expect("the live slot rewrites");
    assert_eq!(
        verdict(scratch.path()),
        Err(SlotDamage::UnsupportedFormatVersion { version: 3 }),
        "a version bitflip in a healthy slot must refuse the store"
    );

    // A slot correctly sealed for version 2 by some future build, refused here.
    let sealed_v2 = {
        let mut image = healthy.clone();
        image[4] = 2;
        raw_slot::reseal(image)
    };
    raw_slot::write(scratch.path(), live, &sealed_v2).expect("the live slot rewrites");
    assert_eq!(
        verdict(scratch.path()),
        Err(SlotDamage::UnsupportedFormatVersion { version: 2 }),
        "a well-sealed version-2 slot must refuse the store"
    );

    // Refused -> readable is the dangerous direction. Flipping the version byte
    // of that version-2 slot back to 1 must be caught by the header checksum.
    let mut forged = sealed_v2.clone();
    forged[4] = 1;
    raw_slot::write(scratch.path(), live, &forged).expect("the live slot rewrites");
    match verdict(scratch.path()) {
        Err(SlotDamage::HeaderChecksumMismatch { .. }) => {}
        other => panic!(
            "flipping a version-2 slot's version byte back to 1 produced {other:?} rather than a \
             header checksum failure: the version field is not effectively checksum-protected"
        ),
    }
}

// ---------------------------------------------------------------------------
// The version gate's other direction: not "which version is refused" but "at
// what length is the field consulted". A gate that only runs once enough other
// bytes have arrived is a statement about length, not about the field.
// ---------------------------------------------------------------------------

#[test]
fn the_version_byte_is_consulted_at_every_length_a_slot_can_have() {
    let scratch = ScratchDir::new("version-by-length");
    let app = commit_workload(scratch.path());
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    drop(app);

    let healthy = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    let mut v2 = healthy.clone();
    v2[4] = 2;
    let v2 = raw_slot::reseal(v2);

    // The stale slot is the one under test, so the live image stays whole and
    // the store can actually open in the accepting case.
    //
    // Both marks are swept. A sealed fragment is refused for being cut short
    // whatever its version says, so it alone cannot show that the version was
    // read — the unsealed mark is where the question has teeth, because an
    // unsealed slot is otherwise residue a later opener skips without looking.
    // Attributing residue to this build without reading the field that says
    // which build wrote it is the whole defect this file exists over.
    let mut consulted = Vec::new();
    let mut ignored = Vec::new();
    for mark in [b'R', 0x00] {
        for length in [8_usize, 20, 36, 37, 41, v2.len()] {
            let mut fragment = v2[..length].to_vec();
            fragment[0] = mark;
            raw_slot::write(scratch.path(), stale, &fragment).expect("the stale slot rewrites");
            match verdict(scratch.path()) {
                Err(damage) => consulted.push((mark, length, damage)),
                Ok(damage) => ignored.push((mark, length, damage)),
            }
        }
    }

    assert!(
        ignored.is_empty(),
        "bytes carrying format version 2 are refused at some lengths and waved through as this \
         build's own publication residue at others.\n\
         refused (version consulted): {consulted:?}\n\
         accepted (version ignored):  {ignored:?}\n\
         The version test must run on every slot long enough to carry the field, before anything \
         classifies the slot by how many bytes it has."
    );
    assert!(
        consulted.iter().any(|(mark, _, damage)| *mark == 0x00
            && matches!(damage, SlotDamage::UnsupportedFormatVersion { version: 2 })),
        "an unsealed fragment declaring another build's version must be named by its version \
         rather than adopted as this build's residue: {consulted:?}"
    );
}

// ---------------------------------------------------------------------------
// The version refusal is the one the repair entry point must not clear.
//
// The lock store gained `open_and_repair` because an ordinary crash can leave a
// whole image whose mark reads unsealed, and refusing that with no way forward
// would be worse than the refusal. A version this build cannot read is a
// different fact: it needs no corruption at all, so those bytes are a newer
// build's committed work, and the documented remedy for damage must not be a way
// to discard one. The sibling ledger refuses the same shape from both entry
// points for the same reason.
// ---------------------------------------------------------------------------

#[test]
fn a_future_slot_version_is_not_given_up_by_the_documented_remedy() {
    for mark in [b'R', 0x00] {
        let scratch = ScratchDir::new(&format!("version-gate-repair-{mark:#04x}"));
        let app = commit_workload(scratch.path());
        let live = app.store().live_slot().expect("the workload committed");
        drop(app);

        let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
        bytes[0] = mark;
        bytes[4] = 2;
        raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");
        let length_before = bytes.len();

        for (entry_point, opened) in [
            ("open", LockStore::open(scratch.path(), config(2, 4))),
            (
                "open_and_repair",
                LockStore::open_and_repair(scratch.path(), config(2, 4)),
            ),
        ] {
            match opened {
                Err(LockStoreError::UnreadableSlot {
                    slot,
                    damage: SlotDamage::UnsupportedFormatVersion { version: 2 },
                    ..
                }) => assert_eq!(
                    slot, live,
                    "`{entry_point}` named the wrong slot at mark {mark:#04x}"
                ),
                Err(other) => panic!(
                    "`{entry_point}` refused a version-2 slot at mark {mark:#04x} for the wrong \
                     reason: {other}"
                ),
                Ok(store) => panic!(
                    "`{entry_point}` resolved a version-2 slot at mark {mark:#04x}, so the \
                     documented remedy for damage became a way to discard a newer build's \
                     committed work: live slot {:?}, generation {}",
                    store.live_slot(),
                    store.generation()
                ),
            }
            assert_eq!(
                raw_slot::read(scratch.path(), live)
                    .expect("the slot reads back")
                    .len(),
                length_before,
                "`{entry_point}` must not rewrite a slot it refused"
            );
        }
    }
}

/// The third entry point on the same bytes, and why it answers differently.
///
/// The rule the test above pins is about a *remedy for damage*: a caller asking
/// to repair damage did not ask to discard a newer build's committed work, so
/// `open_and_repair` refuses. `discard_and_reseed` is not a remedy for damage
/// and does not read what it deletes — a caller running it asked to delete the
/// store — so the version gate has nothing to protect there, and the slot goes
/// with everything else.
///
/// This is pinned rather than left implicit because the difference is a decision
/// and not an oversight. Re-seeding after a binary downgrade rebuilds this
/// replica in the older build's format from the same committed log, which is
/// what a fresh replica of that build would hold. Whether that build should own
/// this replica at all is a question about the deployment, and no entry point
/// here answers it.
#[test]
fn a_reseed_deletes_a_version_this_build_cannot_read() {
    let scratch = ScratchDir::new("version-gate-reseed");
    let app = commit_workload(scratch.path());
    let live = app.store().live_slot().expect("the workload committed");
    drop(app);

    let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    bytes[4] = 2;
    raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");

    assert!(
        matches!(
            LockStore::open_and_repair(scratch.path(), config(2, 4)),
            Err(LockStoreError::UnreadableSlot {
                damage: SlotDamage::UnsupportedFormatVersion { version: 2 },
                ..
            })
        ),
        "the remedy for damage must still refuse it"
    );

    let reseeded = LockStore::discard_and_reseed(scratch.path(), config(2, 4))
        .expect("the re-seed deletes whatever it finds");
    let reseed = reseeded
        .recovery()
        .reseed()
        .expect("a re-seed reports what it deleted");
    assert_eq!(reseeded.live_slot(), None, "{reseed}");
    assert_eq!(
        reseeded.applied_index(),
        LogIndex::ZERO,
        "a re-seeded store starts at the floor the log has to lift it from"
    );
    assert!(
        reseed.slots().iter().any(|state| matches!(
            state.damage(),
            Some(SlotDamage::UnsupportedFormatVersion { version: 2 })
        )),
        "the report has to name the version it deleted rather than only counting bytes: {reseed}"
    );
}
