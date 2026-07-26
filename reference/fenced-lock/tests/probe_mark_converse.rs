//! Regression: the publication mark's converse.
//!
//! `SlotDamage::is_publication_residue` is used in one direction only — a slot
//! may be skipped **because** it is residue — so the implication that has to
//! hold is the one the caller relies on:
//!
//! > If it returns `true`, the slot was never the live image.
//!
//! An earlier shape of this store proved `interrupted => unsealed`, took the
//! contrapositive `sealed => not interrupted`, and skipped on an unsealed mark,
//! which follows from neither. The counterexample is one byte: a slot that
//! *was* sealed, adopted, and acknowledged, whose byte zero later reads `0x00`.
//!
//! This is the same fixture as
//! `a_live_slot_that_lost_its_tail_must_not_be_mistaken_for_publication_residue`
//! in `durable_recovery_proof.rs`, moved from the image's last byte to its
//! first. Both pass now: `classify_unsealed` re-reads the slot with the mark
//! restored, finds a whole image, and the store refuses rather than rolling
//! back to the partner.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_fenced_lock::{
    store::{raw_slot, LockStore, LockStoreError},
    ApplyOutcome, Command, DurableLockError, DurableLockStateMachine, FencingToken,
    GuardedResource, GuardedWrite, LockResponse, OperationResult,
};

use scratch::ScratchDir;
use support::{acquire, config, open_session, release, resource, submit};

const RESOURCE: &str = "orders/shard-0";

fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
        submit(0, 1, 4, release(RESOURCE, 2)),
        submit(0, 1, 5, acquire(RESOURCE, 10)),
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

fn mark(store: &LockStore) -> Option<FencingToken> {
    store.acknowledged_mark(resource(RESOURCE))
}

// ---------------------------------------------------------------------------
// One byte of the medium is lost, and it is the mark byte. The image it names
// was sealed by a completed publication whose `sync_data` returned, and the
// caller was told `Ok`. Every other byte of the same header is protected by a
// checksum that refuses the store. This one is called "residue" and the stale
// partner is adopted in its place — which is a fencing high-water mark
// regressing, and the guarded downstream then accepts two tenures under one
// token.
// ---------------------------------------------------------------------------

#[test]
fn a_live_slot_whose_mark_byte_was_lost_must_not_be_mistaken_for_publication_residue() {
    let scratch = ScratchDir::new("probe-mark-converse-live-slot");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    let generation = app.store().generation();
    let applied = app.store().applied_index();
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    drop(app);

    let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    assert_ne!(bytes[0], 0x00, "the live slot is sealed");
    let original = bytes[0];
    bytes[0] = 0x00;
    raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");

    match LockStore::open(scratch.path(), config(2, 4)) {
        Err(LockStoreError::UnreadableSlot { .. }) => {}
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => {
            let report = *store.recovery();
            let (damaged, damage) = report.damaged_slot().expect("the live slot is damaged");
            let banner = format!(
                "recovery adopted the stale slot after the LIVE slot lost ONE byte, its mark.\n\
                 corrupted byte      slot {live} byte 0 (0x{original:02x} -> 0x00)\n\
                 damaged slot        = {damaged} ({damage}), is_publication_residue = {}\n\
                 adopted slot        = {:?} (expected the live slot {live})\n\
                 generation          = {} (was {generation})\n\
                 applied index       = {} (was {applied})\n\
                 acknowledged mark   = {:?} (was {acknowledged:?})\n\
                 cross_checked_marks = {}\n\
                 recovery is_clean   = {}\n\
                 stale slot was      = {stale}",
                damage.is_publication_residue(),
                store.live_slot(),
                store.generation(),
                store.applied_index(),
                mark(&store),
                report.cross_checked_marks(),
                report.is_clean(),
            );

            // The departed owner held `acknowledged`, and an independent
            // guarded resource accepts a write under it.
            let mut guard = GuardedResource::new(resource(RESOURCE));
            guard
                .apply(GuardedWrite {
                    resource: resource(RESOURCE),
                    token: acknowledged,
                    value: 7,
                })
                .expect("the departed owner's token is accepted");

            let mut app = DurableLockStateMachine::new(store);
            let base = applied.0;
            let epoch = 100 + base;
            let mut index = base;
            let mut step = |command: Command, app: &mut DurableLockStateMachine| {
                index += 1;
                apply_one(app, LogIndex(index), command)
                    .unwrap_or_else(|error| panic!("after the rollback, `{command:?}`: {error}"))
            };
            step(open_session(0, epoch), &mut app);
            let acquired = step(submit(0, epoch, 1, acquire(RESOURCE, 10)), &mut app);
            let LockResponse::Operation(OperationResult::Acquired { token, .. }) =
                acquired.response
            else {
                panic!("a free resource did not acquire after the rollback: {acquired:?}");
            };

            let reissue = guard.apply(GuardedWrite {
                resource: resource(RESOURCE),
                token,
                value: 99,
            });
            panic!(
                "{banner}\n\
                 fresh tenure issued  = {token:?}\n\
                 departed owner held  = {acknowledged:?}\n\
                 guard accepted it    = {reissue:?}  <- two independent tenures under one token"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The uniformity sweep. Under the corrected rule every byte of a sealed image's
// head refuses the store, so losing one decides nothing on its own.
//
// This was a control while the defect stood: byte zero — the one byte no
// checksum was ever consulted for — opened, and its neighbours refused. The fix
// collapsed the contrast, which is what makes the sweep worth keeping.
// ---------------------------------------------------------------------------

#[test]
fn no_single_byte_loss_in_the_live_images_head_lets_the_store_open() {
    let mut verdicts = Vec::new();
    for byte in 0..5_usize {
        let scratch = ScratchDir::new(&format!("probe-mark-neighbour-{byte}"));
        let mut app = open(scratch.path());
        apply_all(&mut app, &workload());
        let live = app.store().live_slot().expect("the workload committed");
        let generation = app.store().generation();
        let acknowledged = mark(app.store());
        drop(app);

        let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
        let original = bytes[byte];
        bytes[byte] = 0x00;
        raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");

        let verdict = match LockStore::open(scratch.path(), config(2, 4)) {
            Err(error) => format!("REFUSED  ({error})"),
            Ok(store) => format!(
                "OPENED   generation {generation} -> {}, mark {acknowledged:?} -> {:?}, \
                 damage = {:?}",
                store.generation(),
                mark(&store),
                store.recovery().damaged_slot().map(|(_, damage)| damage),
            ),
        };
        verdicts.push(format!(
            "  image byte {byte} (0x{original:02x} -> 0x00): {verdict}"
        ));
    }

    let opened = verdicts
        .iter()
        .filter(|line| line.contains("OPENED"))
        .count();
    assert_eq!(
        opened,
        0,
        "a one-byte loss inside the sealed live image produced two different verdicts \
         depending on which byte it hit:\n{}",
        verdicts.join("\n")
    );
}
