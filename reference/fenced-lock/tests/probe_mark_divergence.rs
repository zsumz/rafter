//! Regression: the two stores' marks must answer the same question.
//!
//! This began as a probe of a divergence. The ledger's `read_frame` read the
//! frame mark first and treated *any* unsealed tail as an interrupted append;
//! the lock store's `verify_slot` ordered identity before the seal, so the
//! version gate ran first and the same fact — "a newer build's write into this
//! artifact was interrupted" — got opposite verdicts from two stores whose
//! recovery rules were written as the same rule. The probe asked for the lock
//! store to be brought to the ledger's answer.
//!
//! It is here in the other shape, because the corrected rule decides it the
//! other way and the probe's original expectation cannot survive it. Skipping or
//! truncating requires the unsealed mark **and** positive evidence that the
//! bytes are not a whole artifact. A version this build cannot read supplies no
//! such evidence: not knowing the layout is exactly not knowing whether the
//! bytes are whole. So both stores refuse, both refuse from both entry points,
//! and the ledger moved rather than the lock store. The ledger's half is
//! `reference/ledger/tests/probe_mark_converse.rs`.
//!
//! What the probe was right about stands: the refusal must not be terminal. The
//! lock store had no repair entry point at all, so *every* refusal was the end
//! of the road. It has one now, and the tests below pin both halves — that it
//! clears damage, and that it deliberately does not clear a version.
//!
//! "Not terminal" needed a second entry point to stay true, and for a while it
//! was not. The repair refuses a discard the adopted image cannot dominate, and
//! an ordinary crash mid-acquisition is that shape every time, so a store in
//! that state was refused by both readers with nothing beyond them.
//! `LockStore::discard_and_reseed` is what closes it, and `gen5_repair_deadlock.rs`
//! is that boundary.
//!
//! It does not weaken the version gate below, because it is not a remedy for
//! damage: it deletes whatever it finds and never claims to have read it. The
//! rule the tests here pin is about the *remedy* — a caller asking to repair
//! damage did not ask to discard a newer build's committed work — and a caller
//! asking to delete the store did. `a_reseed_deletes_a_version_this_build_
//! cannot_read` is the third entry point on the same bytes.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_fenced_lock::{
    store::{raw_slot, LockStore, LockStoreError, SlotDamage},
    ApplyOutcome, Command, DurableLockError, DurableLockStateMachine,
};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, submit};

const RESOURCE: &str = "orders/shard-0";

fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
    ]
}

fn open(directory: &Path) -> DurableLockStateMachine {
    let store = LockStore::open(directory, config(2, 4)).expect("a lock store opens");
    DurableLockStateMachine::new(store)
}

fn apply_one(
    app: &mut DurableLockStateMachine,
    index: LogIndex,
    command: Command,
) -> Result<ApplyOutcome, DurableLockError> {
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

fn apply_all(app: &mut DurableLockStateMachine, commands: &[Command]) {
    for (position, command) in commands.iter().enumerate() {
        apply_one(app, LogIndex(position as u64 + 1), *command)
            .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
}

// ---------------------------------------------------------------------------
// A newer build's write into the stale slot, interrupted. Byte zero is
// unsealed, the version byte is one this build cannot read.
//
// The unsealed mark says these bytes were not sealed. It does not say they are
// incomplete, and the version byte is precisely what stops this build from
// finding out: it does not know the layout, so "is this a whole image?" has no
// answer here. Skipping without that answer is how an acknowledged fencing mark
// gets dropped, so the store refuses — and refuses from both entry points,
// because the bytes may equally be a newer build's committed image and the
// remedy for damage must not discard one.
// ---------------------------------------------------------------------------

#[test]
fn an_unsealed_slot_declaring_a_foreign_version_is_refused_by_both_entry_points() {
    let scratch = ScratchDir::new("probe-divergence-newer-build");

    let mut app = open(scratch.path());
    apply_all(&mut app, &workload());
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    let generation = app.store().generation();
    drop(app);

    // The stale slot, as a newer build's interrupted publication would leave it:
    // unsealed byte zero, and a format version this build cannot read.
    let mut bytes = raw_slot::read(scratch.path(), stale).expect("the stale slot reads");
    bytes[0] = 0x00;
    bytes[4] = 2;
    raw_slot::write(scratch.path(), stale, &bytes).expect("the stale slot rewrites");

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
            }) => assert_eq!(slot, stale, "`{entry_point}` named the wrong slot"),
            Err(other) => panic!("`{entry_point}` refused for the wrong reason: {other}"),
            Ok(store) => panic!(
                "`{entry_point}` resolved a slot declaring a version it cannot read, so a \
                 downgrade meeting a newer build's committed image becomes a way to discard it. \
                 Live slot {live} held generation {generation}; the store opened at generation \
                 {} with live slot {:?}.",
                store.generation(),
                store.live_slot(),
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// The half of the probe that stands unchanged: the refusal must not be the end
// of the road. Damage the lock store cannot read used to have no documented way
// forward at all, because the store had no repair entry point — the ledger has
// had one for as long as it has refused anything.
// ---------------------------------------------------------------------------

#[test]
fn a_refusal_the_lock_store_can_repair_is_not_terminal() {
    let scratch = ScratchDir::new("probe-divergence-repairable");

    let mut app = open(scratch.path());
    apply_all(&mut app, &workload());
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    let generation = app.store().generation();
    let applied = app.store().applied_index();
    drop(app);

    // A sealed image with one byte of its trailer lost. Recovery cannot show an
    // interrupted publication left it, so it may have been the live image, and
    // `open` refuses.
    let mut bytes = raw_slot::read(scratch.path(), stale).expect("the stale slot reads");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    raw_slot::write(scratch.path(), stale, &bytes).expect("the stale slot rewrites");

    let refused = LockStore::open(scratch.path(), config(2, 4))
        .expect_err("a corrupted sealed image must refuse `open`");
    assert!(
        matches!(refused, LockStoreError::UnreadableSlot { .. }),
        "unexpected refusal: {refused}"
    );

    let repaired = LockStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("the repair entry point clears what `open` refuses");
    let repair = repaired
        .recovery()
        .repair()
        .expect("a repair that gave up a slot reports it");
    assert_eq!(repair.slot(), stale, "the repair names the slot it gave up");
    assert_eq!(repair.adopted(), live, "and the slot it adopted instead");
    assert_eq!(repair.adopted_generation(), generation);
    assert_eq!(
        repaired.live_slot(),
        Some(live),
        "the live slot is what the repair adopted"
    );
    assert_eq!(
        repaired.applied_index(),
        applied,
        "giving up the stale slot costs nothing the live slot held"
    );
}

// ---------------------------------------------------------------------------
// The A/B the probe was built around, restated for the rule that replaced it.
//
// It used to be that byte zero was unsealed in both cases and the version byte
// alone decided whether the store opened. Both are refusals now, and both are
// refusals for a reason the unsealed mark cannot supply on its own. What the
// version byte still decides is which entry point can clear it, and that
// difference is deliberate: a repair may give up damage, and must not give up a
// newer build's committed work.
//
// The slot under test is the **live** one, because that is where the ambiguity
// actually lives. An unsealed whole image in the stale slot carries a generation
// the live slot outranks, and recovery resolves it with no operator at all —
// which is what the byte sweep in `durable_crash.rs` walks. Here the whole image
// is the newest one, and nothing separates "sealed and then rotted" from "never
// sealed".
// ---------------------------------------------------------------------------

/// A workload whose last publication does *not* raise a fencing mark.
///
/// A release leaves the resource's high-water mark exactly where the preceding
/// acquisition put it, so the live image and the stale one carry the same marks.
/// That is the case where the two readings of `UnsealedCompleteImage` agree on
/// every mark a client could hold, and it is the case a repair may resolve.
fn workload_ending_in_a_release() -> Vec<Command> {
    vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
        submit(0, 1, 4, release(RESOURCE, 2)),
    ]
}

#[test]
fn the_version_byte_decides_the_remedy_rather_than_the_verdict() {
    // Two axes, because two separate rules gate the repair and one used to be
    // missing entirely. The version byte decides whether a repair may *ever*
    // clear this damage. Whether the discarded image's fencing marks survive
    // the adoption decides whether this particular repair may proceed.
    //
    // `workload()` ends in an acquisition, so its live image carries a strictly
    // higher mark than its stale partner: giving it up regresses an
    // acknowledged fencing mark, and that is refused by both entry points. The
    // repair used to perform it, report a generation delta, and leave a guarded
    // resource accepting two independent tenures under one token.
    let mut transcript = Vec::new();
    for (raises_a_mark, commands) in [(false, workload_ending_in_a_release()), (true, workload())] {
        for version in [1_u8, 2] {
            let scratch = ScratchDir::new(&format!(
                "probe-divergence-version-{version}-mark-{raises_a_mark}"
            ));
            let mut app = open(scratch.path());
            apply_all(&mut app, &commands);
            let live = app.store().live_slot().expect("the workload committed");
            drop(app);

            let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
            bytes[0] = 0x00;
            bytes[4] = version;
            raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");

            let damage = match LockStore::open(scratch.path(), config(2, 4)) {
                Err(LockStoreError::UnreadableSlot { damage, .. }) => damage,
                Err(other) => panic!("unexpected refusal at version {version}: {other}"),
                Ok(store) => panic!(
                    "an unsealed whole image must not be resolved by a read at version \
                     {version}: the store opened at generation {}",
                    store.generation()
                ),
            };
            let repaired = LockStore::open_and_repair(scratch.path(), config(2, 4));
            if raises_a_mark && version == 1 {
                let refusal = repaired.err().unwrap_or_else(|| {
                    panic!(
                        "a repair that discards the only image holding an acknowledged mark \
                         must refuse rather than report the loss afterwards"
                    )
                });
                assert!(
                    matches!(refusal, LockStoreError::DiscardWouldRegressMark { .. }),
                    "the refusal must name the marks rather than the generations: {refusal}"
                );
                transcript.push((raises_a_mark, version, damage, false));
            } else {
                transcript.push((raises_a_mark, version, damage, repaired.is_ok()));
            }
        }
    }

    let shapes: Vec<_> = transcript
        .iter()
        .map(|(raises_a_mark, version, damage, repairable)| {
            (
                *raises_a_mark,
                *version,
                matches!(damage, SlotDamage::UnsealedCompleteImage { .. }),
                matches!(damage, SlotDamage::UnsupportedFormatVersion { version: 2 }),
                *repairable,
            )
        })
        .collect();
    assert_eq!(
        shapes,
        vec![
            // This build's own version, and the adopted image carries every mark
            // the discarded one does: a whole image whose mark reads unsealed.
            // Recovery cannot tell the written-but-not-committed window from a
            // live slot whose mark rotted, and it does not have to — both
            // readings agree on every mark a client could hold, so a caller who
            // has decided may repair it and lose nothing observable.
            (false, 1, true, false, true),
            // A version this build cannot read. Also a refusal, and deliberately
            // not repairable: the bytes may be a newer build's committed image.
            (false, 2, false, true, false),
            // Same damage, same version, and the adopted image would drop an
            // acknowledged fencing mark. Now the two readings disagree about the
            // one fact this design protects, nothing in the bytes or in the
            // caller decides which holds, and both entry points refuse.
            (true, 1, true, false, false),
            // The version gate is above the mark check and stays there: it names
            // a different remedy, not a different amount of loss.
            (true, 2, false, true, false),
        ],
        "the unsealed mark must not be the whole of either verdict: {transcript:?}"
    );
}
