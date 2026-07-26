//! What recovery is allowed to skip, asserted from the attacker's side.
//!
//! These began as an adversarial hunt against the store's stated recovery
//! claims and every one of them failed. They are kept because the claims they
//! attack are the ones a shape test can appear to satisfy while being false:
//! each fixture presents bytes that *look* like an interrupted publication and
//! are not one, and asserts that recovery says so.
//!
//! The single fact under all of them is the publication mark. A publication
//! writes byte zero unsealed before any other byte and promotes it only after
//! the rest of the image is durable, so residue can be proved rather than
//! inferred — and a sealed image that lost bytes afterwards, which is a strict
//! prefix of something this build wrote and passes every shape test, is not
//! residue. Each test here is one way of losing that distinction, and each one
//! ends in the same place: a fencing high-water mark regressing and a token
//! being reissued.

#[allow(dead_code)]
mod support;

#[path = "support/scratch.rs"]
mod scratch;

use std::path::Path;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_fenced_lock::{
    store::{raw_slot, LockStore, LockStoreError, SlotDamage, SlotIndex},
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
// The one-byte case, which is the whole argument in miniature.
//
// A sealed image that loses its last byte is a strict prefix of an image this
// build wrote, carries this store's magic and version, and fails no checksum
// over the bytes present. Every shape test says "interrupted publication"; it
// is the live image. The publication mark is what tells them apart, and this
// test follows the failure all the way to the end: recovery adopting the stale
// partner regresses the acknowledged mark, and a guarded resource then accepts
// two independent tenures under one token.
// ---------------------------------------------------------------------------

#[test]
fn a_live_slot_that_lost_its_tail_must_not_be_mistaken_for_publication_residue() {
    let scratch = ScratchDir::new("probe-truncated-live-slot");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    let generation = app.store().generation();
    let applied = app.store().applied_index();
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    drop(app);

    // Both slots hold a sealed image: the live one at `generation`, the stale
    // one at `generation - 1`. Drop exactly one byte off the end of the LIVE
    // slot. This is not a publication: this slot was closed and this store
    // never reopens it for writing.
    let mut bytes = raw_slot::read(scratch.path(), live).expect("the live slot reads");
    bytes.pop();
    raw_slot::write(scratch.path(), live, &bytes).expect("the live slot rewrites");

    let opened = LockStore::open(scratch.path(), config(2, 4));

    match opened {
        Err(LockStoreError::UnreadableSlot { .. }) => {}
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => {
            let report = *store.recovery();
            let (damaged, damage) = report.damaged_slot().expect("the live slot is damaged");
            let banner = format!(
                "recovery adopted the stale slot after the LIVE slot lost one byte.\n\
                 damaged slot        = {damaged} ({damage}), is_publication_residue = {}\n\
                 adopted slot        = {:?} (expected the live slot {live})\n\
                 generation          = {} (was {generation})\n\
                 applied index       = {} (was {applied})\n\
                 acknowledged mark   = {:?} (was {acknowledged:?})\n\
                 cross_checked_marks = {}\n\
                 stale slot was      = {stale}",
                damage.is_publication_residue(),
                store.live_slot(),
                store.generation(),
                store.applied_index(),
                mark(&store),
                report.cross_checked_marks(),
            );

            // The departed owner held `acknowledged` and an independent guarded
            // resource accepted a write under it.
            let mut guard = GuardedResource::new(resource(RESOURCE));
            guard
                .apply(GuardedWrite {
                    resource: resource(RESOURCE),
                    token: acknowledged,
                    value: 7,
                })
                .expect("the departed owner's token is accepted");

            // Take a fresh tenure on the rolled-back store.
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
// Attributing bytes to this build requires reading the fields that say which
// build wrote them.
//
// "A prefix always carries this store's magic and this build's version byte" is
// only an argument if those fields are actually consulted. They used to sit
// behind a full-header slice, so a short slot was assumed to be this build's
// rather than shown to be.
// ---------------------------------------------------------------------------

#[test]
fn a_short_slot_with_a_foreign_magic_is_not_this_builds_publication_residue() {
    let scratch = ScratchDir::new("probe-foreign-short-slot");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let stale = app.store().next_slot();
    drop(app);

    // Twenty bytes that are not a prefix of anything this build writes: a
    // foreign magic and a format version this build refuses at full length.
    let foreign = b"ZZZZ\x09xxxxxxxxxxxxxxx".to_vec();
    assert_eq!(foreign.len(), 20, "the fragment is shorter than one header");
    raw_slot::write(scratch.path(), stale, &foreign).expect("the stale slot rewrites");

    // The magic is tested on every slot long enough to carry it, before
    // anything classifies the slot by its length, so twenty foreign bytes are
    // named as foreign rather than adopted as this build's own residue.
    let damage = match LockStore::open(scratch.path(), config(2, 4)) {
        Err(LockStoreError::UnreadableSlot { damage, .. }) => damage,
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => {
            store
                .recovery()
                .damaged_slot()
                .expect("the fragment is damage")
                .1
        }
    };
    assert_eq!(
        damage,
        SlotDamage::NotALockImage { magic: [b'Z'; 4] },
        "a 20-byte fragment carrying magic {:?} and version {} must be named foreign at its own \
         length",
        &foreign[..4],
        foreign[4],
    );
    assert!(
        !damage.is_publication_residue(),
        "a foreign fragment is not this build's publication residue: {damage:?}"
    );
}

// ---------------------------------------------------------------------------
// The same bytes must get the same verdict at every length.
//
// A version-2 image was refused at full length and waved through as this
// build's own residue when truncated, which made the version gate a statement
// about length rather than about the field.
// ---------------------------------------------------------------------------

#[test]
fn the_version_gate_is_length_dependent() {
    let scratch = ScratchDir::new("probe-version-gate-length");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let stale = app.store().next_slot();
    drop(app);

    // The stale slot's own sealed image, with only the version byte moved to a
    // build this one cannot read, then truncated to 20 bytes. At full length
    // this is `UnsupportedFormatVersion` and a refusal; short, it is benign.
    let mut bytes = raw_slot::read(scratch.path(), stale).expect("the stale slot reads");
    bytes[4] = 2;
    raw_slot::write(scratch.path(), stale, &bytes).expect("the stale slot rewrites");
    let full = LockStore::open(scratch.path(), config(2, 4));
    assert!(
        matches!(full, Err(LockStoreError::UnreadableSlot { .. })),
        "a version-2 slot at full length must refuse"
    );

    bytes.truncate(20);
    raw_slot::write(scratch.path(), stale, &bytes).expect("the stale slot rewrites");
    let short = LockStore::open(scratch.path(), config(2, 4));
    let Ok(store) = short else {
        return; // consistent: refused at both lengths.
    };
    let (_, damage) = store.recovery().damaged_slot().expect("damage is reported");
    panic!(
        "the same version-2 bytes are refused at 37+ bytes and accepted as benign at 20 bytes: \
         {damage:?}, is_publication_residue = {}. The version byte is byte 4 and is present in \
         both artifacts.",
        damage.is_publication_residue()
    );
}

// ---------------------------------------------------------------------------
// An emptied store is not a new one.
//
// Two zero-length slot files used to open as a brand-new lock service with
// `is_clean()` true and every fencing high-water mark gone. The sibling ledger
// already refused the identical situation by name — "a file truncated to
// nothing is unreadable, not absent" — and this store now says the same thing
// in its own vocabulary: creation writes a mark into each slot, so an empty
// slot file is not a state this store ever leaves behind.
// ---------------------------------------------------------------------------

#[test]
fn two_emptied_slot_files_must_not_open_as_a_store_that_never_committed() {
    let scratch = ScratchDir::new("probe-emptied-slots");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    let applied = app.store().applied_index();
    drop(app);

    for slot in [
        rafter_reference_fenced_lock::store::SlotIndex::Zero,
        rafter_reference_fenced_lock::store::SlotIndex::One,
    ] {
        raw_slot::write(scratch.path(), slot, &[]).expect("the slot empties");
    }

    match LockStore::open(scratch.path(), config(2, 4)) {
        Err(_) => {}
        Ok(store) => {
            let report = *store.recovery();
            panic!(
                "a store whose two slot files were emptied opened as a fresh lock service.\n\
                 applied index     = {} (was {applied})\n\
                 acknowledged mark = {:?} (was {acknowledged:?})\n\
                 is_clean()        = {}\n\
                 created()         = {}\n\
                 damaged_slot()    = {:?}",
                store.applied_index(),
                mark(&store),
                report.is_clean(),
                report.created(),
                report.damaged_slot(),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// A slot that should exist and does not is unreadable rather than absent.
//
// `open` used to recreate it and adopt the older partner: a one-generation
// rollback reported as a clean opening.
// ---------------------------------------------------------------------------

#[test]
fn a_missing_live_slot_file_must_not_silently_roll_the_store_back() {
    let scratch = ScratchDir::new("probe-missing-live-slot");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let live = app.store().live_slot().expect("the workload committed");
    let generation = app.store().generation();
    let applied = app.store().applied_index();
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    drop(app);

    std::fs::remove_file(scratch.path().join(live.file_name())).expect("the live slot is removed");

    match LockStore::open(scratch.path(), config(2, 4)) {
        Err(_) => {}
        Ok(store) => {
            let report = *store.recovery();
            assert!(
                store.generation() >= generation && mark(&store) >= Some(acknowledged),
                "removing the live slot file rolled the store back one generation.\n\
                 generation        = {} (was {generation})\n\
                 applied index     = {} (was {applied})\n\
                 acknowledged mark = {:?} (was {acknowledged:?})\n\
                 is_clean()        = {}\n\
                 created()         = {}\n\
                 damaged_slot()    = {:?}",
                store.generation(),
                store.applied_index(),
                mark(&store),
                report.is_clean(),
                report.created(),
                report.damaged_slot(),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// PROBE 6: the equal-index session-cache floor is exactly one index wide, and
// that is the scope the store documents and defends rather than a gap.
//
// The check answers "two images name the same commit point and disagree about
// which requests completed; which one is poorer?" — a question only an
// unchanged applied index can pose, because at any higher index the model has
// legitimately advanced and is the authority on the sessions it retired,
// expired, or reused along the way. Widening the floor above the index would
// refuse those legitimate installs, which is a live-lock rather than a
// soundness fix. This test pins the boundary so a later widening is a decision
// somebody makes rather than one that happens.
// ---------------------------------------------------------------------------

#[test]
fn the_session_cache_floor_covers_exactly_the_index_it_can_judge() {
    use rafter_reference_fenced_lock::LockService;
    use support::{expire_through, renew};

    let lock_config = config(2, 4);
    let scratch = ScratchDir::new("probe-session-floor-offset");
    // The suite's own workload: its last command is a `renew`, which moves the
    // session cache and nothing else, so the mark floor cannot see it.
    let commands = [
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
        open_session(1, 4),
        submit(1, 4, 1, acquire("audit/log", 3)),
        submit(1, 4, 2, expire_through(3)),
        submit(0, 1, 4, renew(RESOURCE, 2, 20)),
    ];

    let mut poorer = LockService::new(lock_config);
    for command in &commands[..commands.len() - 1] {
        poorer.apply(*command);
    }
    let mut richer = poorer.clone();
    richer.apply(commands[commands.len() - 1]);
    let at = LogIndex(commands.len() as u64);

    let mut store = LockStore::open(scratch.path(), lock_config).expect("a fresh store opens");
    store.commit(&richer, at).expect("the transaction commits");

    // At the same index this is refused: nothing legitimately changed, so an
    // image that lost a completion is simply the poorer picture of one commit
    // point.
    store
        .install(&poorer, at)
        .expect_err("the equal-index floor refuses the poorer image");

    // One index higher the model has advanced, and it — not the durability
    // boundary — is the authority on which sessions it retired getting there.
    // The mark floor still applies at every publication, so the fencing
    // high-water marks are defended here whatever the session cache does.
    store
        .install(&poorer, LogIndex(at.0 + 1))
        .expect("an advanced index is the model's own account of what it retired");
}

// ---------------------------------------------------------------------------
// The rules above, from the other side: the cases the fail-closed reading must
// *not* catch, and the one shape of a half-created directory that is safe to
// finish.
// ---------------------------------------------------------------------------

#[test]
fn one_emptied_slot_beside_an_intact_partner_is_damage_rather_than_residue() {
    let scratch = ScratchDir::new("emptied-live-slot");
    let commands = workload();

    let mut app = open(scratch.path());
    apply_all(&mut app, &commands);
    let live = app.store().live_slot().expect("the workload committed");
    let acknowledged = mark(app.store()).expect("the resource has a mark");
    drop(app);

    // Emptying the *live* slot is indistinguishable, by length alone, from a
    // publication that emptied the stale one before writing it — which is
    // exactly why no publication ever empties a slot.
    raw_slot::write(scratch.path(), live, &[]).expect("the live slot empties");

    match LockStore::open(scratch.path(), config(2, 4)) {
        Err(LockStoreError::UnreadableSlot {
            slot,
            damage: SlotDamage::SlotEmptied,
            ..
        }) => assert_eq!(slot, live, "the refusal names the slot that was emptied"),
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => panic!(
            "emptying the live slot rolled the store back: mark {:?} was {acknowledged:?}",
            mark(&store)
        ),
    }
}

#[test]
fn a_creation_interrupted_between_its_two_files_is_finished_rather_than_refused() {
    let scratch = ScratchDir::new("half-created");
    // Opening creates both slot files and nothing else. Removing slot one
    // reproduces a creation that died between them.
    let store = LockStore::open(scratch.path(), config(2, 4)).expect("a fresh store opens");
    assert!(store.recovery().created(), "a fresh directory is created");
    drop(store);
    std::fs::remove_file(scratch.path().join("lock-state.1")).expect("slot one is removed");

    let store =
        LockStore::open(scratch.path(), config(2, 4)).expect("an interrupted creation finishes");
    assert!(
        store.recovery().created(),
        "finishing a creation is still a creation, and still reaches the caller"
    );
    assert_eq!(store.applied_index(), LogIndex::ZERO);
}

#[test]
fn a_missing_slot_zero_is_refused_even_when_slot_one_has_never_been_published_to() {
    let scratch = ScratchDir::new("missing-slot-zero");
    let mut app = open(scratch.path());
    // One transaction: generation 1 goes to slot zero, so slot one still holds
    // nothing but its creation mark.
    apply_one(&mut app, LogIndex(1), open_session(0, 1)).expect("the first transaction commits");
    assert_eq!(
        app.store().live_slot(),
        Some(SlotIndex::Zero),
        "the first publication writes slot zero"
    );
    drop(app);

    std::fs::remove_file(scratch.path().join("lock-state.0")).expect("slot zero is removed");

    // Slot one has never been published to, which is the shape a half-finished
    // creation leaves — but it is the mirror image of the safe one. Slot zero
    // is where the first generation lives, so an unpublished slot one says
    // nothing at all about what was lost.
    match LockStore::open(scratch.path(), config(2, 4)) {
        Err(LockStoreError::MissingSlot { slot, .. }) => {
            assert_eq!(slot, SlotIndex::Zero, "the refusal names the missing slot");
        }
        Err(other) => panic!("unexpected refusal: {other}"),
        Ok(store) => panic!(
            "recreating slot zero discarded generation {} and opened at applied index {}",
            store.generation(),
            store.applied_index()
        ),
    }
}

#[test]
fn creating_a_stores_slot_files_is_not_a_clean_opening() {
    let scratch = ScratchDir::new("creation-is-reported");
    let store = LockStore::open(scratch.path(), config(2, 4)).expect("a fresh store opens");
    let report = *store.recovery();
    assert!(report.created(), "the directory held no store");
    assert!(
        !report.is_clean(),
        "creation must reach a caller through the predicate callers actually branch on; a \
         directory that was supposed to hold a store and does not looks exactly like a fresh \
         replica from in here, and only the caller can tell them apart"
    );
    drop(store);

    let reopened = LockStore::open(scratch.path(), config(2, 4)).expect("the store reopens");
    assert!(!reopened.recovery().created());
    assert!(
        reopened.recovery().is_clean(),
        "a second opening of the same directory creates nothing and reports nothing"
    );
}
