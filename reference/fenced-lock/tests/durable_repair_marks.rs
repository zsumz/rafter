//! What a repair is allowed to discard, and what it must say about it.
//!
//! Generation three moved the mark's hole out of `open` and gave the refusal a
//! way forward: `LockStore::open_and_repair`. The refusal it gives up is
//! [`SlotDamage::UnsealedCompleteImage`], whose own documentation says two
//! histories leave those exact bytes — a publication that wrote the whole image,
//! reached its durability barrier, and died before the one byte that seals it;
//! or a committed, adopted, acknowledged image whose one mark byte later rotted.
//!
//! `open` refuses because it cannot tell them apart. `open_and_repair` did not
//! tell them apart either: it picked the first reading unconditionally. Under
//! the second reading that is the generation-one defect, byte for byte, reached
//! through the entry point added to give the refusal a way forward — and by that
//! commit's own account the *ordinary* crash lands in the same refusal, so the
//! entry point a deployment must run after an ordinary crash was the one that
//! regressed an acknowledged fencing mark after a one-byte rot. Nothing in the
//! report distinguished which it had just done, because `Repair` carried the
//! generation delta and never the marks.
//!
//! The rule now: **wherever an image is discarded or set aside, its fencing
//! marks are compared against the image adopted in its place**, and a discard
//! the adopted image cannot dominate is refused by both entry points. The
//! argument for refusing rather than repairing-and-reporting is on
//! `verify_discard_preserves_marks`. The tests below pin the rule, the two
//! places it did not run, and the two boundaries of its scope: what it does not
//! check (session progress) and what it cannot check (an image this build cannot
//! read).

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_fenced_lock::{
    store::{raw_slot, LockStore, LockStoreError, SlotDamage},
    ApplyOutcome, Command, DurableLockError, DurableLockStateMachine, FencingToken,
    GuardedResource, GuardedWrite, LockResponse, OperationResult,
};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, resource, submit};

const RESOURCE: &str = "orders/shard-0";

/// One sector, which is the unit a drive returns zeros for when it cannot read
/// a block.
const SECTOR: usize = 512;

/// A workload whose last publication raises the resource's fencing mark.
///
/// The live image therefore carries a mark the stale one does not, which is the
/// case where the two readings of an unsealed whole image disagree about
/// something a client can hold.
fn workload_ending_in_an_acquisition() -> Vec<Command> {
    vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
        submit(0, 1, 4, release(RESOURCE, 2)),
        submit(0, 1, 5, acquire(RESOURCE, 10)),
    ]
}

/// The same workload with one more release, so its last publication leaves the
/// fencing mark exactly where it was.
///
/// Both images then carry the same marks, the two readings agree on every mark
/// a client could hold, and adopting either is correct under both.
fn workload_ending_in_a_release() -> Vec<Command> {
    let mut commands = workload_ending_in_an_acquisition();
    commands.push(submit(0, 1, 6, release(RESOURCE, 3)));
    commands
}

fn open(directory: &Path) -> DurableLockStateMachine {
    let store = LockStore::open(directory, config(2, 4)).expect("a lock store opens");
    DurableLockStateMachine::new(store, directory.join("raft/snapshots"))
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

fn mark(store: &LockStore) -> Option<FencingToken> {
    store.acknowledged_mark(resource(RESOURCE))
}

/// Commits `commands`, then zeroes the first `run` bytes of the live slot.
///
/// Returns the directory, the mark that was acknowledged before the damage, and
/// the applied index it stood at.
fn damage_the_live_slot(
    label: &str,
    commands: &[Command],
    run: usize,
) -> (ScratchDir, FencingToken, LogIndex) {
    let scratch = ScratchDir::new(label);
    let mut app = open(scratch.path());
    apply_all(&mut app, commands);
    let live = app.store().live_slot().expect("the workload committed");
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    let applied = app.store().applied_index();
    drop(app);

    let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    assert_ne!(bytes[0], 0x00, "the live slot is sealed");
    let end = run.min(bytes.len());
    for byte in &mut bytes[..end] {
        *byte = 0;
    }
    raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");
    (scratch, acknowledged, applied)
}

// ---------------------------------------------------------------------------
// The finding. One byte of the live slot is lost — the mark — and the repair
// entry point used to answer by adopting the stale partner.
// ---------------------------------------------------------------------------

/// Both entry points refuse, and the refusal names the marks.
///
/// The pre-fix behaviour, for the record: `open_and_repair` returned
/// `Repair { slot: One, damage: UnsealedCompleteImage { len: 180, generation: 6 },
/// adopted: Zero, adopted_generation: 5 }`, the acknowledged mark went from
/// `FencingToken(3)` to `FencingToken(2)`, the next acquisition reissued
/// `FencingToken(3)`, and a guarded resource accepted it — two independent
/// tenures under one token.
#[test]
fn a_repair_that_would_regress_a_mark_is_refused_by_both_entry_points() {
    let (scratch, acknowledged, _) = damage_the_live_slot(
        "repair-marks-regress",
        &workload_ending_in_an_acquisition(),
        1,
    );

    for entry_point in ["open", "open_and_repair"] {
        let opened = if entry_point == "open" {
            LockStore::open(scratch.path(), config(2, 4))
        } else {
            LockStore::open_and_repair(scratch.path(), config(2, 4))
        };
        match opened {
            Err(LockStoreError::UnreadableSlot { damage, .. }) => {
                assert_eq!(entry_point, "open", "only `open` refuses without deciding");
                assert!(
                    matches!(damage, SlotDamage::UnsealedCompleteImage { .. }),
                    "a one-byte mark rot leaves a whole image: {damage:?}"
                );
            }
            Err(LockStoreError::DiscardWouldRegressMark {
                resource: named,
                acknowledged: was,
                offered,
                damage,
                ..
            }) => {
                assert_eq!(entry_point, "open_and_repair");
                assert_eq!(
                    named,
                    resource(RESOURCE),
                    "the refusal named the wrong resource"
                );
                assert_eq!(
                    was, acknowledged,
                    "the refusal must carry the mark that would be lost"
                );
                assert!(
                    offered.is_some_and(|offered| offered < acknowledged),
                    "the refusal must carry the mark that would replace it: {offered:?}"
                );
                assert!(
                    matches!(damage, SlotDamage::UnsealedCompleteImage { .. }),
                    "the refusal must carry the damage that provoked the repair: {damage:?}"
                );
            }
            Err(other) => panic!("`{entry_point}` refused for the wrong reason: {other}"),
            Ok(store) => panic!(
                "`{entry_point}` adopted the stale slot after the live slot lost one byte: \
                 acknowledged mark {:?} (was {acknowledged:?}), repair {:?}",
                mark(&store),
                store.recovery().repair(),
            ),
        }
    }

    // A refusal writes nothing, so every byte is still on the medium and the
    // store stays refused rather than healing itself into the stale generation.
    // That is what makes refusing recoverable under both readings.
    assert!(
        LockStore::open(scratch.path(), config(2, 4)).is_err(),
        "the store must stay refused until somebody with the downstream evidence acts"
    );
}

/// The other half of "regress": a resource that disappears entirely.
///
/// A resource enters the state at its first acquisition, so a discarded image
/// can hold a resource the adopted one has never heard of. Dropping it does not
/// lower that resource's mark — it removes it — and the next acquisition then
/// issues token 1 for a resource whose guarded downstream has already accepted
/// one. That is the same failure by subtraction, and the comparison has to treat
/// a missing resource as a regression rather than as nothing to compare.
#[test]
fn a_repair_that_would_drop_a_resource_entirely_is_refused_too() {
    const FRESH: &str = "orders/shard-1";

    let scratch = ScratchDir::new("repair-marks-vanished-resource");
    let mut app = open(scratch.path());
    // The final publication is the *first* acquisition of `FRESH`, so only the
    // live image tracks it at all.
    apply_all(
        &mut app,
        &[
            open_session(0, 1),
            submit(0, 1, 1, acquire(RESOURCE, 10)),
            submit(0, 1, 2, release(RESOURCE, 1)),
            submit(0, 1, 3, acquire(FRESH, 10)),
        ],
    );
    let live = app.store().live_slot().expect("the workload committed");
    let acknowledged = app
        .store()
        .acknowledged_mark(resource(FRESH))
        .expect("the fresh resource has a mark");
    drop(app);

    let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    bytes[0] = 0x00;
    raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");

    let refusal = LockStore::open_and_repair(scratch.path(), config(2, 4))
        .err()
        .unwrap_or_else(|| {
            panic!("a repair that removes {FRESH} from the state entirely must refuse")
        });
    let LockStoreError::DiscardWouldRegressMark {
        resource: named,
        acknowledged: was,
        offered,
        ..
    } = refusal
    else {
        panic!("the repair refused for the wrong reason: {refusal}");
    };
    assert_eq!(named, resource(FRESH));
    assert_eq!(was, acknowledged);
    assert_eq!(
        offered, None,
        "the adopted image does not track the resource at all, and the refusal must say so"
    );
}

/// The end-to-end consequence, so the refusal is tied to the failure it stops.
///
/// A guarded resource must never accept two independent tenures under one token.
/// The pre-fix repair produced exactly that; here the store refuses, so no fresh
/// tenure is ever issued and the departed owner's token stays unique.
#[test]
fn no_fresh_tenure_can_reissue_a_departed_owners_token() {
    let (scratch, acknowledged, _) = damage_the_live_slot(
        "repair-marks-guarded",
        &workload_ending_in_an_acquisition(),
        1,
    );

    let mut guard = GuardedResource::new(resource(RESOURCE));
    guard
        .apply(GuardedWrite {
            resource: resource(RESOURCE),
            token: acknowledged,
            value: 7,
        })
        .expect("the departed owner's token is accepted");

    assert!(
        LockStore::open_and_repair(scratch.path(), config(2, 4)).is_err(),
        "the repair that would let a fresh tenure reissue {acknowledged:?} must refuse"
    );
}

// ---------------------------------------------------------------------------
// The scope of the rule, on both sides.
// ---------------------------------------------------------------------------

/// What the rule deliberately does **not** check: session progress.
///
/// Session progress advances on every applied entry, so a discarded image almost
/// always holds more of it than the adopted one. Requiring it to be preserved
/// would refuse every repair and leave the ordinary crash with no way forward at
/// all. The asymmetry is argued on `verify_discard_preserves_marks`: a
/// session-cache regression is bounded by the applied index this store reports
/// and is restored by replaying from it; a fencing token has already left the
/// cluster and no replay reaches the guarded resource that accepted it.
///
/// So this repair proceeds — and reports what it gave up, in the dimension it
/// can speak about.
#[test]
fn a_repair_that_regresses_only_session_progress_is_allowed_and_reported() {
    let (scratch, acknowledged, applied) = damage_the_live_slot(
        "repair-marks-session-only",
        &workload_ending_in_a_release(),
        1,
    );

    assert!(
        LockStore::open(scratch.path(), config(2, 4)).is_err(),
        "`open` still refuses to choose between the two readings"
    );

    let store = LockStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("a repair losing no mark is correct under both readings and needs no operator");
    let repair = store
        .recovery()
        .repair()
        .expect("a repair that gave up a slot records it");

    assert_eq!(
        mark(&store),
        Some(acknowledged),
        "no acknowledged fencing mark may be lost by a repair that was allowed to proceed"
    );
    assert!(
        store.applied_index() < applied,
        "the repair still costs a publication, and that is what it reports"
    );

    // The report, in the dimension that matters. This is what `Repair` could not
    // say before: it carried the generation delta and never the marks.
    assert!(
        repair.marks_cross_checked(),
        "a repair that discarded a decodable image must have compared its marks"
    );
    assert!(
        repair
            .discarded_generation()
            .is_some_and(|discarded| discarded > repair.adopted_generation()),
        "a decodable discarded image must be named by generation too: {repair:?}"
    );
    assert!(
        store.recovery().cross_checked_marks(),
        "the recovery report must say a comparison ran, not only the repair"
    );
    assert!(
        !store.recovery().is_clean(),
        "a repair is never a clean opening"
    );
}

/// What the rule **cannot** check: an image this build cannot read.
///
/// A zeroed sector destroys the slot's identity, so there is no image to decode
/// and no marks to compare. The repair still proceeds — that is the disclosed
/// cost of the entry point, and it predates this rule — but the report now says
/// the comparison did not run rather than leaving a caller to assume it did.
/// This is the boundary of the mechanism, stated as one.
#[test]
fn a_repair_that_cannot_read_the_discarded_image_says_so() {
    let (scratch, _, _) = damage_the_live_slot(
        "repair-marks-unreadable",
        &workload_ending_in_an_acquisition(),
        SECTOR,
    );

    let store = LockStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("an unreadable slot beside an intact one is what a repair is for");
    let repair = store
        .recovery()
        .repair()
        .expect("a repair that gave up a slot records it");

    assert!(
        matches!(repair.damage(), SlotDamage::NotALockImage { .. }),
        "a zeroed sector destroys the slot's identity: {repair:?}"
    );
    assert!(
        !repair.marks_cross_checked(),
        "nothing was decodable, so the report must not imply a comparison ran"
    );
    assert_eq!(
        repair.discarded_generation(),
        None,
        "reading the slot is exactly what failed, so its generation is unknown"
    );
    assert!(
        !store.recovery().cross_checked_marks(),
        "the recovery report must agree with the repair about what was checked"
    );
}

// ---------------------------------------------------------------------------
// The second place the comparison did not run: an image set aside rather than
// given up. `open` returns Ok on this path, so nothing about it needed an
// operator and nothing about it was checked.
// ---------------------------------------------------------------------------

/// A whole verified image dropped by `open` is cross-checked like any other.
///
/// Recovery sets aside a slot holding an unsealed whole image when the partner
/// holds a *sealed* image of a strictly greater generation, on the argument that
/// adopting the partner is then correct under both readings. That argument is a
/// claim about what a caller observes, and the marks are what a caller observes,
/// so it is now checked instead of asserted.
#[test]
fn setting_a_whole_image_aside_cross_checks_its_marks() {
    let scratch = ScratchDir::new("repair-marks-set-aside");
    let mut app = open(scratch.path());
    apply_all(&mut app, &workload_ending_in_an_acquisition());
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    drop(app);

    // Rot the mark of the *stale* slot, which holds a whole older image. The
    // partner's generation is strictly greater, so recovery sets it aside.
    let mut bytes = raw_slot::read(scratch.path(), stale).expect("the stale slot reads");
    assert_ne!(bytes[0], 0x00, "the stale slot is sealed");
    bytes[0] = 0x00;
    raw_slot::write(scratch.path(), stale, &bytes).expect("the stale slot rewrites");

    let store =
        LockStore::open(scratch.path(), config(2, 4)).expect("a set-aside older image still opens");
    let report = *store.recovery();
    assert_eq!(store.live_slot(), Some(live), "the newer image is adopted");
    assert_eq!(mark(&store), Some(acknowledged), "no mark is lost");
    assert!(
        report.cross_checked_marks(),
        "a whole verified image was dropped without the comparison the two-intact path \
         always runs: damaged = {:?}, repair = {:?}",
        report.damaged_slot(),
        report.repair(),
    );
    assert_eq!(
        report.repair(),
        None,
        "setting aside is not a repair: `open` performed it and wrote nothing"
    );
}

// ---------------------------------------------------------------------------
// The multi-byte sweep, which is the sibling ledger suite's shape over this
// store's format. This is the datum that made the ledger's verdict an asymmetry
// rather than a shared limitation, so it is kept as a regression test.
// ---------------------------------------------------------------------------

/// A zero run over the live slot is refused by `open` at every length.
///
/// `verify_identity` reads the magic above the mark test, so one zeroed byte is
/// the mark and two reach the identity — two different rules, both refusals, and
/// no length at which the store resolves it by itself.
#[test]
fn a_zero_run_over_the_live_slot_is_refused_at_every_length() {
    let mut transcript = Vec::new();
    for run in [1_usize, 2, 3, SECTOR] {
        let (scratch, _, _) = damage_the_live_slot(
            &format!("lock-zero-run-{run}"),
            &workload_ending_in_an_acquisition(),
            run,
        );
        let verdict = match LockStore::open(scratch.path(), config(2, 4)) {
            Err(LockStoreError::UnreadableSlot { damage, .. }) => damage,
            Err(other) => panic!("{run} zero bytes refused for the wrong reason: {other}"),
            Ok(store) => panic!(
                "{run} zero bytes over the live slot must not be resolved by a read: the \
                 store opened at generation {}",
                store.generation()
            ),
        };
        transcript.push((run, verdict));
    }

    let shapes: Vec<_> = transcript
        .iter()
        .map(|(run, damage)| {
            (
                *run,
                matches!(damage, SlotDamage::UnsealedCompleteImage { .. }),
                matches!(damage, SlotDamage::NotALockImage { .. }),
            )
        })
        .collect();
    assert_eq!(
        shapes,
        vec![
            // One byte is the mark alone, and the re-read finds a whole image.
            (1, true, false),
            // Two reach the identity, which no publication ever writes wrong.
            (2, false, true),
            (3, false, true),
            (SECTOR, false, true),
        ],
        "a zero run over the live slot must be refused at every length: {transcript:?}"
    );
}

/// The other direction, so the fix cannot be "refuse everything": a fresh
/// tenure is still issued and still guarded after a repair that lost no mark.
#[test]
fn a_permitted_repair_still_opens_a_working_store() {
    let (scratch, acknowledged, applied) =
        damage_the_live_slot("repair-marks-usable", &workload_ending_in_a_release(), 1);

    let store = LockStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("a repair losing no mark proceeds");
    let mut app = DurableLockStateMachine::new(store, scratch.path().join("raft/snapshots"));

    let mut index = applied.0;
    let mut step = |command: Command, app: &mut DurableLockStateMachine| {
        index += 1;
        apply_one(app, LogIndex(index), command)
            .unwrap_or_else(|error| panic!("after the repair, `{command:?}`: {error}"))
    };
    // The repair discarded the publication that recorded the release, so the
    // adopted state still holds the tenure the acknowledged mark belongs to.
    // Releasing it is the ordinary way forward and is itself evidence the
    // adopted state is coherent.
    step(open_session(0, 100), &mut app);
    step(
        submit(0, 100, 1, release(RESOURCE, acknowledged.get())),
        &mut app,
    );
    let acquired = step(submit(0, 100, 2, acquire(RESOURCE, 10)), &mut app);
    let LockResponse::Operation(OperationResult::Acquired { token, .. }) = acquired.response else {
        panic!("a free resource did not acquire after the repair: {acquired:?}");
    };
    assert!(
        token > acknowledged,
        "a fresh tenure must outrank every acknowledged mark: {token:?} after {acknowledged:?}"
    );

    let mut guard = GuardedResource::new(resource(RESOURCE));
    guard
        .apply(GuardedWrite {
            resource: resource(RESOURCE),
            token: acknowledged,
            value: 7,
        })
        .expect("the departed owner's token is accepted once");
    guard
        .apply(GuardedWrite {
            resource: resource(RESOURCE),
            token,
            value: 99,
        })
        .expect("the fresh tenure outranks it");
    assert!(
        guard
            .apply(GuardedWrite {
                resource: resource(RESOURCE),
                token: acknowledged,
                value: 11,
            })
            .is_err(),
        "the departed owner must not write behind the fresh tenure"
    );
}
