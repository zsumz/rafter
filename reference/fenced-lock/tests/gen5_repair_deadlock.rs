//! Gen-5 probe: does the repair entry point reach the crash it was added for?
//!
//! `LockStore::open_and_repair`'s own rationale:
//!
//! > it is that [`SlotDamage::UnsealedCompleteImage`] turns an ordinary crash —
//! > a publication interrupted between its barrier and its seal — into a
//! > refusal, and a store whose ordinary crash residue needs an operator with no
//! > documented way forward is worse than one that names the way forward and
//! > reports what it costs.
//!
//! `WriteFault::BeforeSeal` is that crash, exactly: every byte of the new image
//! is on the medium and the byte that seals it is not. The two tests below run
//! it twice over the same store, differing in one thing — whether the
//! interrupted transaction was an acquisition or a release.
//!
//! A release leaves the fencing marks where they were, the stale partner
//! dominates them, and the repair works. An acquisition raises one, the partner
//! cannot dominate it, and `verify_discard_preserves_marks` refuses — in both
//! entry points, by design, with no override. Acquisition is what a fencing lock
//! publishes.
//!
//! The refusal was right and it stands. What was missing is where the refused
//! store goes: the rule named re-seeding from the group as the way out, which
//! was a statement about the cluster that no call in this crate implemented, so
//! the way out began with deleting files by hand. `LockStore::discard_and_reseed`
//! is that call, and the third test below is the pair — this store has no entry
//! point that *reads* it, and exactly one that opens it.
//!
//! These began as a hunt for a way to make the refusal wrong. It is not wrong,
//! so they are kept as the boundary: two crashes that differ in one command,
//! and the one call that resolves the half neither reader can.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_fenced_lock::{
    store::{FaultPlan, LockStore, LockStoreError, SlotDamage, WriteFault},
    ApplyOutcome, Command, DurableLockError, DurableLockStateMachine,
};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, submit};

const RESOURCE: &str = "orders/shard-0";

fn open(directory: &Path, faults: FaultPlan) -> DurableLockStateMachine {
    let store = LockStore::open_with_faults(directory, config(2, 4), faults)
        .expect("a lock store opens under a plan that only fires on a publication");
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

/// Commits every command but the last, then crashes between the last one's
/// durability barrier and its seal.
fn crash_before_the_seal(label: &str, commands: &[Command]) -> ScratchDir {
    let scratch = ScratchDir::new(label);
    let plan = FaultPlan::at(commands.len() as u64, WriteFault::BeforeSeal);
    let mut app = open(scratch.path(), plan);
    for (position, command) in commands[..commands.len() - 1].iter().enumerate() {
        apply_one(&mut app, LogIndex(position as u64 + 1), *command)
            .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
    apply_one(
        &mut app,
        LogIndex(commands.len() as u64),
        commands[commands.len() - 1],
    )
    .expect_err("an image that was not sealed is not a transaction");
    assert_eq!(
        app.store().fired_fault(),
        Some(WriteFault::BeforeSeal),
        "the plan must have fired where it aimed"
    );
    drop(app);
    scratch
}

/// The control: the interrupted publication moved no fencing mark.
///
/// The stale partner dominates every mark the discarded image holds, the two
/// readings of the unsealed bytes agree on everything a client could be
/// holding, and the repair opens the store one generation back.
#[test]
fn gen5_the_ordinary_crash_on_a_release_is_repairable() {
    let commands = vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
    ];
    let scratch = crash_before_the_seal("gen5-release-crash", &commands);

    let refused = LockStore::open(scratch.path(), config(2, 4))
        .expect_err("a whole unsealed image is not residue `open` may skip");
    assert!(
        matches!(
            refused,
            LockStoreError::UnreadableSlot {
                damage: SlotDamage::UnsealedCompleteImage { .. },
                ..
            }
        ),
        "the ordinary crash refuses in `open`: {refused}"
    );

    let repaired = LockStore::open_and_repair(scratch.path(), config(2, 4))
        .expect("the repair entry point resolves the ordinary crash");
    println!(
        "release crash: repaired, live slot {:?}, generation {}",
        repaired.live_slot(),
        repaired.generation()
    );
}

/// The finding: the same crash, one command earlier, and there is no way
/// forward at all.
///
/// This is the *whole* reason a fencing lock publishes. `open` refuses because
/// it cannot tell an interrupted publication from a rotted mark, and
/// `open_and_repair` refuses because the partner it would adopt cannot dominate
/// the mark the discarded image raised — which is precisely the situation an
/// ordinary crash on an acquisition creates, every time.
#[test]
fn gen5_the_ordinary_crash_on_an_acquisition_has_no_way_forward() {
    let commands = vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
    ];
    let scratch = crash_before_the_seal("gen5-acquire-crash", &commands);

    let refused = LockStore::open(scratch.path(), config(2, 4))
        .expect_err("a whole unsealed image is not residue `open` may skip");
    println!("acquire crash, `open`:            {refused}");

    let repair = LockStore::open_and_repair(scratch.path(), config(2, 4));
    match repair {
        Ok(store) => panic!(
            "the repair entry point resolved it after all: live slot {:?}, generation {}",
            store.live_slot(),
            store.generation()
        ),
        Err(error) => {
            println!("acquire crash, `open_and_repair`: {error}");
            assert!(
                matches!(error, LockStoreError::DiscardWouldRegressMark { .. }),
                "the entry point added to give the ordinary crash a way forward \
                 refuses the ordinary crash: {error}"
            );
        }
    }
}

/// No entry point that reads this store opens it, and exactly one that does not
/// read it does.
///
/// The reading entry points are idempotent about their refusal — they move no
/// byte, so retrying lands in the same place however many times it is tried,
/// which is what makes this a wedge rather than a transient. The re-seed is the
/// one call that resolves it, and it resolves it by keeping nothing: the store
/// it returns is empty, at applied index zero, with the applied floor the
/// deleted store had reached reported so the caller knows how far the log has
/// to carry it back.
#[test]
fn gen5_a_wedged_store_has_exactly_one_way_forward() {
    let commands = vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
    ];
    let scratch = crash_before_the_seal("gen5-wedged", &commands);

    for attempt in 1..=3 {
        assert!(
            LockStore::open(scratch.path(), config(2, 4)).is_err(),
            "`open` attempt {attempt}"
        );
        assert!(
            LockStore::open_and_repair(scratch.path(), config(2, 4)).is_err(),
            "`open_and_repair` attempt {attempt}"
        );
        assert!(
            LockStore::open_with_faults(scratch.path(), config(2, 4), FaultPlan::none()).is_err(),
            "`open_with_faults` attempt {attempt}"
        );
    }
    println!("three reading entry points, three attempts each, no store opened");

    let reseeded = LockStore::discard_and_reseed(scratch.path(), config(2, 4))
        .expect("the re-seed opens a store no reader can");
    let reseed = reseeded
        .recovery()
        .reseed()
        .expect("a re-seed reports what it deleted");
    println!("`discard_and_reseed`:             {reseed}");

    assert_eq!(reseeded.applied_index(), LogIndex::ZERO);
    assert_eq!(reseeded.live_slot(), None);
    assert_eq!(reseeded.generation(), 0);
    assert_eq!(
        reseeded.acknowledged_mark(
            rafter_reference_fenced_lock::ResourceName::new(RESOURCE).expect("admissible")
        ),
        None,
        "a re-seeded store holds no mark of its own; the log is the authority now"
    );
    assert_eq!(
        reseed.discarded_applied_index(),
        Some(LogIndex(4)),
        "the report must name how far the deleted store had applied"
    );
    assert!(reseed.discarded_bytes() > 0);

    // And it is a read afterwards: the directory a re-seed leaves is one the
    // ordinary entry point opens, which is what makes the wedge cleared rather
    // than merely stepped over.
    let reopened =
        LockStore::open(scratch.path(), config(2, 4)).expect("the re-seeded directory reopens");
    assert_eq!(reopened.applied_index(), LogIndex::ZERO);
    assert_eq!(
        reopened.recovery().reseed(),
        None,
        "reopening is not a re-seed"
    );
}
