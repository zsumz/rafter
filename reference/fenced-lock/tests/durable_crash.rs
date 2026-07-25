//! Application crash points over the durable transactional backend.
//!
//! Every test here interrupts a real publication at a named boundary, reopens
//! the store, and asks the same question: is the recovered state exactly the one
//! before the transaction or exactly the one after it? "Exactly" is load
//! bearing. The comparison is over a whole [`DurableState`] — the lock table,
//! every fencing high-water mark, sessions with their cached operation,
//! fingerprint, and result, the replicated logical time, and the applied Raft
//! index together — so a recovery that moved a lock without its cached result,
//! or an applied index without its data, fails here rather than being caught by
//! whichever later assertion happened to look.
//!
//! One question is asked more insistently than the rest, because this
//! application exists to answer it: can a recovered store ever issue a fencing
//! token at or below a mark it already made durable?
//! `a_recovered_store_never_issues_a_token_at_or_below_an_acknowledged_mark`
//! answers it against an independent [`GuardedResource`], which knows nothing
//! about locks and refuses a stale token on its own authority.
//!
//! Injection is deterministic and per-store. Every failure message carries the
//! [`FaultPlan`] that produced it, which is the whole reproduction input.
//!
//! The suite is also required to prove that its own injections bite: a crash
//! test that silently stopped interrupting anything would assert only that an
//! uninterrupted store works. Each scenario asserts that its fault fired, and
//! the byte sweep asserts that it reached every slot-damage shape the format
//! can produce.

#[allow(dead_code)]
mod support;

// The driver is shared with `adapter_cluster.rs`, which is where its read path,
// its unknown-outcome accounting, and its in-memory construction are exercised.
// This suite drives the durable composition and uses a smaller part of it.
#[allow(dead_code)]
#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/durable.rs"]
mod durable;
#[path = "support/scratch.rs"]
mod scratch;
#[allow(dead_code)]
#[path = "support/transport.rs"]
mod transport;

use std::{collections::BTreeSet, path::Path};

use rafter::{LogIndex, NodeId, Term};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyEntry, ReplicatedStateMachine,
};
use rafter_reference_fenced_lock::{
    store::{
        overwrite_header_applied_index, read_slot_bytes, write_slot_bytes, FaultPlan, LockStore,
        LockStoreError, SlotDamage, SlotIndex, SlotState, WriteFault, SLOT_HEADER_LEN,
        SLOT_TRAILER_LEN,
    },
    ApplyDisposition, ApplyOutcome, Command, DurableLockError, DurableLockStateMachine,
    FencingToken, GuardedResource, GuardedWrite, LockResponse, LockService, OperationResult,
    ReferenceLockService, ServiceView,
};

use cluster::LockCluster;
use durable::DurableLockApps;
use scratch::ScratchDir;
use support::{
    acquire, config, expire_through, open_session, release, renew, resource, submit, time,
};

const RESOURCE: &str = "orders/shard-0";
const AUDIT: &str = "audit/log";

/// Everything one transaction is required to move, compared as one value.
///
/// The contract names six things the transaction commits together. Five of them
/// live in the view — lock table mutations, the high-water marks, the session
/// and deduplication mutation, the cached command result, and the replicated
/// logical time — and the sixth is the applied index beside it. Comparing the
/// pair is how "together" is asserted: there is no way to be equal on one half
/// and not the other.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableState {
    applied_index: LogIndex,
    view: ServiceView,
}

// ---------------------------------------------------------------------------
// Store-level crash points
// ---------------------------------------------------------------------------

#[test]
fn a_crash_at_every_byte_of_a_publication_recovers_to_exactly_one_side_of_it() {
    let commands = workload();
    let interrupted = commands.len();
    let prefix = &commands[..interrupted - 1];

    // Establish the two legal answers, and the exact image the interrupted
    // store would have written, by running the same workload uninterrupted.
    let reference = ScratchDir::new("sweep-reference");
    let mut app = open(reference.path(), FaultPlan::none());
    apply_all(&mut app, prefix);
    let before = durable_state(&app);
    let live_before = app.store().live_slot().expect("the prefix committed");
    let stale_before = app.store().next_slot();
    apply_one(&mut app, index_of(interrupted), commands[interrupted - 1])
        .expect("an uninterrupted transaction commits");
    let after = durable_state(&app);
    let image_len = LockStore::planned_image_len(config(2, 4), app.service(), after.applied_index)
        .expect("the image the sweep interrupts is encodable");
    drop(app);

    assert_ne!(
        before, after,
        "a sweep whose two answers were equal would prove nothing"
    );
    let payload_len = image_len - as_u64(SLOT_HEADER_LEN) - as_u64(SLOT_TRAILER_LEN);
    let mut observed = BTreeSet::new();

    for stop in 0..=image_len {
        let plan = FaultPlan::at(as_u64(interrupted), WriteFault::AfterBytes(stop));
        let scratch = ScratchDir::new("sweep");
        let mut app = open(scratch.path(), plan.clone());
        apply_all(&mut app, prefix);
        let outcome = apply_one(&mut app, index_of(interrupted), commands[interrupted - 1]);

        let committed = stop == image_len;
        assert_interrupted_publication(&app, outcome, stop, committed, &before, &plan);
        // Dropping the handle is the crash: everything below is what a fresh
        // process can read back.
        drop(app);

        let recovered = open(scratch.path(), FaultPlan::none());
        let report = recovered.store().recovery();
        assert_eq!(
            report.damaged_slot(),
            expected_damage(stop, payload_len, image_len),
            "`{plan}` left a residue the format does not describe"
        );
        observed.insert(damage_kind(report.damaged_slot()));

        if committed {
            assert_eq!(
                report.live_slot(),
                Some(stale_before),
                "a committed publication makes the slot it wrote authoritative (`{plan}`)"
            );
        } else {
            // The interesting half: the slot being written held a real older
            // image, and it must never outrank the live one just by being the
            // most recently touched file.
            assert_eq!(
                report.live_slot(),
                Some(live_before),
                "recovery must order slots by generation, not by recency (`{plan}`)"
            );
            if stop == 0 {
                assert_eq!(
                    report.slot(stale_before),
                    SlotState::Empty,
                    "a publication that truncated and died leaves an empty slot (`{plan}`)"
                );
            }
        }
        assert_eq!(
            durable_state(&recovered),
            if committed {
                after.clone()
            } else {
                before.clone()
            },
            "`{plan}` recovered to neither side of the transaction"
        );
    }

    // The sweep is only evidence if it actually produced every shape the format
    // can leave behind.
    assert_eq!(
        observed,
        BTreeSet::from([
            "sealed or empty",
            "header incomplete",
            "payload incomplete",
            "missing commit checksum",
            "partial commit checksum",
        ]),
        "the sweep missed a slot-damage shape"
    );
}

#[test]
fn a_crash_before_the_publication_begins_leaves_both_slots_untouched() {
    let commands = workload();
    let scratch = ScratchDir::new("before-first-byte");

    // Two clean transactions first, so the slot the third one would have
    // written already holds a real image that must survive untouched.
    let plan = FaultPlan::at(3, WriteFault::BeforeFirstByte);
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..2]);
    let before = durable_state(&app);
    let stale = app.store().next_slot();

    apply_one(&mut app, index_of(3), commands[2]).expect_err("the third transaction never starts");
    assert_eq!(app.store().fired_fault(), Some(WriteFault::BeforeFirstByte));
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    let report = recovered.store().recovery();
    assert_eq!(
        report.damaged_slot(),
        None,
        "a publication that emitted nothing damaged nothing (`{plan}`)"
    );
    assert!(
        matches!(report.slot(stale), SlotState::Intact { generation: 1, .. }),
        "the slot the publication would have overwritten kept its own image (`{plan}`)"
    );
    assert!(
        report.cross_checked_marks(),
        "two intact slots let recovery re-check the marks across the commit between them"
    );
    assert_eq!(durable_state(&recovered), before);
}

#[test]
fn a_written_but_uncommitted_image_is_not_a_transaction() {
    let commands = workload();
    let scratch = ScratchDir::new("no-commit-checksum");
    let (before, payload_len) = state_and_payload_len(&commands, commands.len());

    // Stop exactly where the payload ends: everything the transaction changes
    // is on disk, and the checksum that says it counts is not. The answer has
    // to be that nothing happened.
    let stop = as_u64(SLOT_HEADER_LEN) + payload_len;
    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(stop));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1],
    )
    .expect_err("the transaction was never committed");
    assert_eq!(
        app.store().fired_fault(),
        Some(WriteFault::AfterBytes(stop))
    );
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        recovered.store().recovery().damaged_slot(),
        Some(SlotDamage::MissingCommitChecksum),
        "the residue is a written image with no commit checksum (`{plan}`)"
    );
    assert_eq!(
        durable_state(&recovered),
        before,
        "an uncommitted image changed nothing (`{plan}`)"
    );
}

#[test]
fn a_torn_commit_checksum_does_not_commit() {
    let commands = workload();
    let scratch = ScratchDir::new("torn-commit-checksum");
    let (before, payload_len) = state_and_payload_len(&commands, commands.len());

    // One byte short of a whole commit checksum. Every other check in the slot
    // passes; the only thing missing is the seal.
    let stop = as_u64(SLOT_HEADER_LEN) + payload_len + as_u64(SLOT_TRAILER_LEN) - 1;
    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(stop));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1],
    )
    .expect_err("a torn commit checksum is not a commit");
    drop(app);

    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        recovered.store().recovery().damaged_slot(),
        Some(SlotDamage::PartialCommitChecksum { present: 3 }),
        "under `{plan}`"
    );
    assert_eq!(
        durable_state(&recovered),
        before,
        "an image missing one byte of its seal is not committed (`{plan}`)"
    );
}

#[test]
fn a_failed_durability_barrier_leaves_the_outcome_to_recovery() {
    let commands = workload();
    let scratch = ScratchDir::new("failed-sync");
    let (before, _) = state_and_payload_len(&commands, commands.len());
    let after = uninterrupted_state(&commands);

    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AtSlotSync);
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1],
    )
    .expect_err("a failed barrier is a failed transaction");
    assert_eq!(app.store().fired_fault(), Some(WriteFault::AtSlotSync));
    assert!(
        app.store().requires_reopen(),
        "a caller cannot infer from `Err` that no bytes changed (`{plan}`)"
    );
    drop(app);

    // The contract does not promise which side a failed barrier lands on. It
    // promises the answer is one of the two, and that recovery is what decides.
    let recovered = open(scratch.path(), FaultPlan::none());
    let state = durable_state(&recovered);
    assert!(
        state == before || state == after,
        "a failed barrier recovered to a state that is neither side of the transaction (`{plan}`)"
    );
}

#[test]
fn a_poisoned_store_refuses_every_later_transaction() {
    let commands = workload();
    let scratch = ScratchDir::new("poisoned");
    let plan = FaultPlan::at(1, WriteFault::AtSlotSync);
    let mut app = open(scratch.path(), plan.clone());

    apply_one(&mut app, LogIndex(1), commands[0]).expect_err("the first transaction fails");
    assert!(app.store().requires_reopen(), "under `{plan}`");

    let error = apply_one(&mut app, LogIndex(2), commands[1])
        .expect_err("a poisoned store accepts nothing");
    assert!(
        matches!(
            error,
            DurableLockError::Store(LockStoreError::StoreRequiresReopen)
        ),
        "a poisoned store must say so rather than fail some other way: {error}"
    );
}

#[test]
fn an_acknowledged_command_is_never_re_executed_after_recovery() {
    let commands = workload();
    let scratch = ScratchDir::new("no-re-execution");
    // The acquisition at sequence 3 minted token 2 and was acknowledged before
    // the crash. Re-executing it would mint token 3 for a tenure that already
    // has a token, which is precisely the double-issue fencing forbids.
    let acknowledged = commands[3];

    let plan = FaultPlan::at(as_u64(commands.len()), WriteFault::AfterBytes(40));
    let mut app = open(scratch.path(), plan.clone());
    apply_all(&mut app, &commands[..commands.len() - 1]);
    apply_one(
        &mut app,
        index_of(commands.len()),
        commands[commands.len() - 1],
    )
    .expect_err("the last transaction is interrupted");
    drop(app);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    let floor = recovered
        .applied_index()
        .expect("a durable lock service reports its applied index");
    let mark_before = recovered.store().acknowledged_mark(resource(RESOURCE));

    let replayed = apply_one(&mut recovered, LogIndex(floor.0 + 1), acknowledged)
        .expect("a fresh index applies");
    assert_eq!(
        replayed.disposition,
        ApplyDisposition::Replayed,
        "an acknowledged command must not execute a second time (`{plan}`)"
    );
    assert_eq!(
        replayed.response,
        LockResponse::Operation(OperationResult::Acquired {
            token: token(2),
            expiry: time(10),
        }),
        "the replay returns the token the original acquisition minted"
    );
    assert_eq!(
        recovered.store().acknowledged_mark(resource(RESOURCE)),
        mark_before,
        "the replay issued no new token (`{plan}`)"
    );

    // And the entry itself can never come back at or below the floor.
    let error = apply_one(&mut recovered, floor, commands[0])
        .expect_err("an entry at the applied floor is refused");
    assert!(
        matches!(
            error,
            DurableLockError::Adapter(
                rafter_reference_fenced_lock::LockAdapterError::AppliedIndexRegression { .. }
            )
        ),
        "unexpected refusal: {error}"
    );
}

#[test]
fn a_crash_after_the_commit_point_but_before_the_reply_is_answered_by_the_cache() {
    let commands = workload();
    let scratch = ScratchDir::new("commit-without-reply");
    let mut app = open(scratch.path(), FaultPlan::none());
    // Everything up to, but not including, the acquisition that mints token 2.
    apply_all(&mut app, &commands[..3]);

    // This transaction commits. Its result is then dropped on the floor, which
    // is exactly what a process death between the commit point and the client
    // reply looks like: a fencing token exists and no client has heard it.
    let unreplied = apply_one(&mut app, index_of(4), commands[3]).expect("the transaction commits");
    let committed = durable_state(&app);
    assert_eq!(
        unreplied.response,
        LockResponse::Operation(OperationResult::Acquired {
            token: token(2),
            expiry: time(10),
        })
    );
    drop(app);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        durable_state(&recovered),
        committed,
        "a committed transaction survives a crash that swallowed its reply"
    );

    // The client never heard an answer, so it retries the same request
    // identity. The cache the transaction committed alongside the lock table is
    // what hands it the same token instead of minting a second one.
    let retried = apply_one(
        &mut recovered,
        LogIndex(committed.applied_index.0 + 1),
        commands[3],
    )
    .expect("a fresh index applies");
    assert_eq!(retried.disposition, ApplyDisposition::Replayed);
    assert_eq!(
        retried.response, unreplied.response,
        "the retry returns the token the crashed run minted"
    );
    assert_eq!(
        durable_state(&recovered).view,
        committed.view,
        "the retry moved no lock and minted no token"
    );
}

#[test]
fn recovery_then_replay_reconstructs_the_uninterrupted_run() {
    let commands = workload();
    let uninterrupted = uninterrupted_state(&commands);

    // Interrupt each transaction in turn, recover, replay the rest of the log
    // from the recovered floor, and land in the same place every time.
    for interrupted in 1..=commands.len() {
        let plan = FaultPlan::at(as_u64(interrupted), WriteFault::AfterBytes(35));
        let scratch = ScratchDir::new("replay-equivalence");
        let mut app = open(scratch.path(), plan.clone());
        for (position, command) in commands.iter().enumerate().take(interrupted) {
            let outcome = apply_one(&mut app, index_of(position + 1), *command);
            if position + 1 == interrupted {
                outcome.expect_err(&format!(
                    "`{plan}` must interrupt transaction {interrupted}"
                ));
            } else {
                outcome.expect("earlier transactions commit");
            }
        }
        drop(app);

        let mut recovered = open(scratch.path(), FaultPlan::none());
        let floor = recovered
            .applied_index()
            .expect("a durable lock service reports its applied index");
        assert!(
            floor.0 < as_u64(interrupted),
            "the interrupted transaction must not have committed (`{plan}`)"
        );

        for (position, command) in commands.iter().enumerate() {
            let index = index_of(position + 1);
            if index > floor {
                apply_one(&mut recovered, index, *command)
                    .unwrap_or_else(|error| panic!("replay after `{plan}` failed: {error}"));
            }
        }

        assert_eq!(
            durable_state(&recovered),
            uninterrupted,
            "replay from the recovered floor did not reconstruct the uninterrupted run (`{plan}`)"
        );
        assert_eq!(
            recovered.service().view(),
            replay_through_oracle(&commands).view(),
            "the reconstructed lock service disagrees with the independent oracle (`{plan}`)"
        );
    }
}

// ---------------------------------------------------------------------------
// Fencing high-water marks
// ---------------------------------------------------------------------------

/// The property this whole application exists to keep, asserted against a
/// downstream that knows nothing about locks.
///
/// A [`GuardedResource`] records the highest token it has accepted and refuses
/// anything older. If any recovery path let the store forget a mark, the next
/// acquisition would mint a token the guard has already seen and the guard
/// would refuse the *current* owner — which is the observable form of the
/// failure fencing exists to prevent.
#[test]
fn a_recovered_store_never_issues_a_token_at_or_below_an_acknowledged_mark() {
    let commands = workload();
    let scratch = ScratchDir::new("mark-monotonicity");
    let mut guard = GuardedResource::new(resource(RESOURCE));
    let mut highest_acknowledged = FencingToken::first();

    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    // The current owner writes downstream under the mark the run established.
    let mark = app
        .store()
        .acknowledged_mark(resource(RESOURCE))
        .expect("the workload acquired this resource");
    assert_eq!(mark, token(2));
    accept(&mut guard, mark, 1);
    highest_acknowledged = highest_acknowledged.max(mark);
    let mut floor = app
        .applied_index()
        .expect("a durable lock service reports its applied index");
    drop(app);

    // Four recovery paths, each followed by a fresh tenure. After every one the
    // new token must strictly exceed every mark ever acknowledged, and the
    // guard — which was never told any of this happened — must accept it.
    for (label, path) in [
        ("a plain restart", RecoveryPath::Restart),
        ("a crash mid-publication", RecoveryPath::CrashMidPublication),
        ("a snapshot install", RecoveryPath::SnapshotInstall),
        ("a crash mid-publication", RecoveryPath::CrashMidPublication),
    ] {
        floor = walk_recovery_path(scratch.path(), path, floor, label);

        let mut recovered = open(scratch.path(), FaultPlan::none());
        assert!(
            recovered
                .store()
                .acknowledged_mark(resource(RESOURCE))
                .is_some_and(|mark| mark >= highest_acknowledged),
            "{label} lost the mark {highest_acknowledged:?}"
        );

        let (issued, next_floor) = take_a_fresh_tenure(&mut recovered, floor, label);
        floor = next_floor;
        assert!(
            issued > highest_acknowledged,
            "after {label} the store issued token {issued:?}, at or below the acknowledged \
             {highest_acknowledged:?}"
        );
        accept(&mut guard, issued, 1);
        highest_acknowledged = issued;
        drop(recovered);
    }

    // A former owner from before any of this is still refused, which is the
    // property stated the way the contract states it.
    assert!(
        guard
            .apply(GuardedWrite {
                resource: resource(RESOURCE),
                token: token(2),
                value: 99,
            })
            .is_err(),
        "a stale former owner must be refused once a later owner has written"
    );
}

#[test]
fn a_publication_that_would_lower_a_mark_is_refused_before_a_byte_is_written() {
    let scratch = ScratchDir::new("mark-guard");
    let lock_config = config(2, 4);
    let mut store = LockStore::open(scratch.path(), lock_config).expect("a fresh store opens");

    // Two committed states: one where the resource has been acquired twice, and
    // an earlier one where it had only been acquired once.
    let mut early = LockService::new(lock_config);
    for command in &workload()[..2] {
        early.apply(*command);
    }
    let mut later = early.clone();
    for command in &workload()[2..4] {
        later.apply(*command);
    }
    store
        .commit(&later, LogIndex(4))
        .expect("the first publication commits");
    assert_eq!(store.acknowledged_mark(resource(RESOURCE)), Some(token(2)));

    let generation = store.generation();
    for (label, offered, expected) in [
        ("a state whose mark is lower", early, Some(token(1))),
        (
            "a state that never tracked the resource",
            LockService::new(lock_config),
            None,
        ),
    ] {
        let error = store
            .commit(&offered, LogIndex(5))
            .expect_err(&format!("{label} must be refused"));
        assert!(
            matches!(
                error,
                LockStoreError::MarkRegression { acknowledged, offered, .. }
                    if acknowledged == token(2) && offered == expected
            ),
            "{label} was refused for the wrong reason: {error}"
        );
    }
    assert_eq!(
        store.generation(),
        generation,
        "a refused publication must not have written anything"
    );
    assert_eq!(
        store.acknowledged_mark(resource(RESOURCE)),
        Some(token(2)),
        "a refused publication left the acknowledged mark where it was"
    );
}

#[test]
fn recovery_re_checks_the_marks_across_the_commit_boundary_it_recovers() {
    let lock_config = config(2, 4);
    let commands = workload();

    // A store whose live image carries mark 2 for the resource.
    let target = ScratchDir::new("mark-cross-check-target");
    let mut app = open(target.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..4]);
    let stale = app.store().next_slot();
    let generation = app.store().generation();
    assert_eq!(
        app.store().acknowledged_mark(resource(RESOURCE)),
        Some(token(2))
    );
    drop(app);

    // A second store, driven only far enough to hold mark 1, but pushed to a
    // strictly higher generation and applied index. Its live image is a real,
    // correctly sealed slot: nothing about it is forged.
    let source = ScratchDir::new("mark-cross-check-source");
    let mut donor = open(source.path(), FaultPlan::none());
    apply_all(&mut donor, &commands[..2]);
    for extra in 0..=generation {
        // Sessions the target never opened, so each one is a fresh transaction
        // that advances the generation without touching the resource.
        apply_one(&mut donor, LogIndex(3 + extra), open_session(1, 9 + extra))
            .expect("a session transaction commits");
    }
    assert!(
        donor.store().generation() > generation,
        "the donor image must outrank the target's live one to be adopted"
    );
    assert_eq!(
        donor.store().acknowledged_mark(resource(RESOURCE)),
        Some(token(1)),
        "the donor must carry the lower mark for this test to mean anything"
    );
    let donor_slot = donor.store().live_slot().expect("the donor committed");
    drop(donor);

    let image = read_slot_bytes(source.path(), donor_slot).expect("the donor slot reads back");
    write_slot_bytes(target.path(), stale, &image).expect("the target slot rewrites");

    // The higher generation wins the ordering, and then the older slot the
    // design preserves is what proves the marks went backwards.
    let error = LockStore::open(target.path(), lock_config)
        .expect_err("a store must not adopt an image that loses a mark");
    assert!(
        matches!(
            error,
            LockStoreError::MarkRegression {
                acknowledged, offered: Some(offered), ..
            } if acknowledged == token(2) && offered == token(1)
        ),
        "unexpected refusal: {error}"
    );
}

#[test]
fn two_damaged_slots_fail_closed_rather_than_opening_an_empty_lock_service() {
    let commands = workload();
    let scratch = ScratchDir::new("both-damaged");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..4]);
    drop(app);

    for slot in [SlotIndex::Zero, SlotIndex::One] {
        let mut bytes = read_slot_bytes(scratch.path(), slot).expect("the slot reads back");
        assert!(!bytes.is_empty(), "{slot} must hold an image to damage");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        write_slot_bytes(scratch.path(), slot, &bytes).expect("the slot rewrites");
    }

    // Opening empty here would hand out token 1 for a resource whose guarded
    // downstream has already accepted token 2.
    let error = LockStore::open(scratch.path(), config(2, 4))
        .expect_err("a store with no readable image must not open");
    assert!(
        matches!(error, LockStoreError::NoReadableImage { .. }),
        "unexpected refusal: {error}"
    );
}

#[test]
fn a_store_that_never_committed_opens_empty_even_with_a_damaged_slot() {
    let commands = workload();
    let scratch = ScratchDir::new("first-transaction-torn");
    let plan = FaultPlan::at(1, WriteFault::AfterBytes(20));
    let mut app = open(scratch.path(), plan.clone());
    apply_one(&mut app, LogIndex(1), commands[0])
        .expect_err("the first transaction is interrupted");
    drop(app);

    // This is the case the fail-closed rule must *not* catch: there were never
    // any marks to lose, so an empty store is the correct recovery.
    let recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(
        recovered.store().recovery().damaged_slot(),
        Some(SlotDamage::HeaderIncomplete { present: 20 }),
        "under `{plan}`"
    );
    assert_eq!(recovered.store().recovery().live_slot(), None);
    assert_eq!(
        durable_state(&recovered),
        DurableState {
            applied_index: LogIndex::ZERO,
            view: LockService::new(config(2, 4)).view(),
        },
        "a store that never committed recovers to an empty lock service"
    );
}

// ---------------------------------------------------------------------------
// Format integrity
// ---------------------------------------------------------------------------

#[test]
fn a_corrupted_sealed_image_is_detected_rather_than_trusted() {
    let commands = workload();
    let (before, payload_len) = state_and_payload_len(&commands, commands.len());

    // One offset in each record, named by the check that is supposed to catch
    // it. The header's own checksum has to catch its corruption before the
    // commit checksum does, because the generation it protects is what recovery
    // orders by.
    let cases: [(u64, DamageTest); 3] = [
        (5, |damage| {
            matches!(damage, SlotDamage::HeaderChecksumMismatch { .. })
        }),
        (as_u64(SLOT_HEADER_LEN) + 2, |damage| {
            matches!(damage, SlotDamage::CommitChecksumMismatch { .. })
        }),
        (as_u64(SLOT_HEADER_LEN) + payload_len + 1, |damage| {
            matches!(damage, SlotDamage::CommitChecksumMismatch { .. })
        }),
    ];

    for (offset, expected) in cases {
        let scratch = ScratchDir::new("corrupt-image");
        let mut app = open(scratch.path(), FaultPlan::none());
        apply_all(&mut app, &commands);
        let live = app.store().live_slot().expect("the workload committed");
        drop(app);

        let mut bytes = read_slot_bytes(scratch.path(), live).expect("the slot reads back");
        let target = usize::try_from(offset).expect("offsets are small");
        bytes[target] ^= 0xFF;
        write_slot_bytes(scratch.path(), live, &bytes).expect("the slot rewrites");

        let recovered = open(scratch.path(), FaultPlan::none());
        let damage = recovered
            .store()
            .recovery()
            .damaged_slot()
            .unwrap_or_else(|| panic!("flipping byte {offset} was not caught at all"));
        assert!(
            expected(damage),
            "flipping byte {offset} was caught as the wrong shape: {damage}"
        );
        assert_eq!(
            durable_state(&recovered),
            before,
            "a corrupt image is discarded, leaving the one before it"
        );
    }
}

#[test]
fn a_commit_checksum_seals_only_the_header_it_was_written_under() {
    let commands = workload();
    let lock_config = config(2, 4);
    let scratch = ScratchDir::new("splice-header");

    // Republishing the identical state leaves two slots whose payloads are
    // byte-identical and whose headers differ only in generation. Moving a seal
    // between them is therefore the narrowest possible test that the seal binds
    // its header — and with it the generation recovery orders by, which is what
    // stops an older image being resurrected under a newer number.
    let mut store = LockStore::open(scratch.path(), lock_config).expect("a fresh store opens");
    let mut service = LockService::new(lock_config);
    for command in &commands {
        service.apply(*command);
    }
    let at = index_of(commands.len());
    store
        .commit(&service, at)
        .expect("the first publication commits");
    let older = store.live_slot().expect("the publication committed");
    store
        .install(&service, at)
        .expect("republishing the same state commits");
    let newer = store.live_slot().expect("the republication committed");
    assert_ne!(older, newer, "a publication alternates slots");
    drop(store);

    let mut old_image = read_slot_bytes(scratch.path(), older).expect("the slot reads back");
    let new_image = read_slot_bytes(scratch.path(), newer).expect("the slot reads back");
    let seal = old_image.len() - SLOT_TRAILER_LEN;
    assert_eq!(
        old_image[SLOT_HEADER_LEN..seal],
        new_image[SLOT_HEADER_LEN..seal],
        "republishing one state must produce one payload, or this splice is not minimal"
    );
    assert_ne!(
        old_image[..SLOT_HEADER_LEN],
        new_image[..SLOT_HEADER_LEN],
        "the two headers must differ, or the splice changes nothing"
    );
    old_image[seal..].copy_from_slice(&new_image[seal..]);
    write_slot_bytes(scratch.path(), older, &old_image).expect("the slot rewrites");

    let recovered =
        LockStore::open(scratch.path(), lock_config).expect("the newer slot is still readable");
    assert!(
        matches!(
            recovered.recovery().damaged_slot(),
            Some(SlotDamage::CommitChecksumMismatch { .. })
        ),
        "a seal written under another header must not seal this one"
    );
    assert_eq!(recovered.recovery().live_slot(), Some(newer));
}

#[test]
fn a_commit_checksum_seals_only_the_payload_it_was_written_for() {
    // Two runs that differ only in a lease: the lock's fields are fixed width,
    // so they encode to the same number of bytes and, at the same applied index
    // and generation, to byte-identical headers. The only thing distinguishing
    // the two images is the payload, so the seal is the only check that can
    // catch the splice.
    let mine = ScratchDir::new("splice-target");
    let theirs = ScratchDir::new("splice-source");
    let commands = workload();
    let mut divergent = commands.clone();
    divergent[7] = submit(0, 1, 4, renew(RESOURCE, 2, 21));

    let (before, _) = state_and_payload_len(&commands, commands.len());

    let mut app = open(mine.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let target_slot = app.store().live_slot().expect("the workload committed");
    drop(app);
    let mut app = open(theirs.path(), FaultPlan::none());
    apply_all(&mut app, &divergent);
    let source_slot = app.store().live_slot().expect("the workload committed");
    drop(app);

    let mut target = read_slot_bytes(mine.path(), target_slot).expect("the slot reads back");
    let source = read_slot_bytes(theirs.path(), source_slot).expect("the slot reads back");
    assert_eq!(
        target.len(),
        source.len(),
        "the two images must be the same shape for this splice to be interesting"
    );
    assert_eq!(
        target[..SLOT_HEADER_LEN],
        source[..SLOT_HEADER_LEN],
        "the two headers must match, or a weaker check could catch the splice"
    );

    let seal = target.len() - SLOT_TRAILER_LEN;
    assert_ne!(
        target[seal..],
        source[seal..],
        "the two seals must differ, or the splice changes nothing"
    );
    target[seal..].copy_from_slice(&source[seal..]);
    write_slot_bytes(mine.path(), target_slot, &target).expect("the slot rewrites");

    let recovered = open(mine.path(), FaultPlan::none());
    assert!(
        matches!(
            recovered.store().recovery().damaged_slot(),
            Some(SlotDamage::CommitChecksumMismatch { .. })
        ),
        "a seal written for another payload must not seal this one"
    );
    assert_eq!(durable_state(&recovered), before);
}

#[test]
fn the_slot_header_binds_a_store_to_its_format_and_its_bounds() {
    let scratch = ScratchDir::new("header");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &workload());
    let live = app.store().live_slot().expect("the workload committed");
    let stale = app.store().next_slot();
    drop(app);

    // Different resource bounds decide which images are valid, so they are
    // refused rather than reinterpreted. A lock service opened under a smaller
    // resource bound than its marks were written for would be a service that
    // could evict a mark.
    let error = LockStore::open(scratch.path(), config(2, 3))
        .expect_err("a store is bound to the configuration its images were written under");
    assert!(
        matches!(error, LockStoreError::ConfigMismatch { .. }),
        "unexpected refusal: {error}"
    );

    let original = read_slot_bytes(scratch.path(), live).expect("the slot reads back");

    // Damaging only the live slot leaves the stale one, which is intact and one
    // generation older, so each of these is observed as damage rather than as a
    // store that cannot open.
    let cases: [(&str, ImageMutation, DamageTest); 3] = [
        (
            "foreign magic",
            |bytes| bytes[1] = b'X',
            |damage| matches!(damage, SlotDamage::NotALockImage { .. }),
        ),
        (
            "an unreadable version",
            |bytes| bytes[4] = 9,
            |damage| matches!(damage, SlotDamage::UnsupportedFormatVersion { version: 9 }),
        ),
        (
            "bytes beyond the seal",
            |bytes| bytes.push(0),
            |damage| matches!(damage, SlotDamage::TrailingBytes { extra: 1 }),
        ),
    ];
    for (label, mutate, expected) in cases {
        let mut bytes = original.clone();
        mutate(&mut bytes);
        write_slot_bytes(scratch.path(), live, &bytes).expect("the slot rewrites");

        let recovered = open(scratch.path(), FaultPlan::none());
        let damage = recovered
            .store()
            .recovery()
            .damaged_slot()
            .unwrap_or_else(|| panic!("{label} was not caught"));
        assert!(expected(damage), "{label} was caught as {damage}");
        assert_eq!(
            recovered.store().recovery().live_slot(),
            Some(stale),
            "{label} left the other slot as the only readable image"
        );
    }
}

#[test]
fn a_slot_whose_two_applied_indexes_disagree_is_refused_rather_than_reconciled() {
    let commands = workload();
    let scratch = ScratchDir::new("index-disagreement");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..4]);
    let live = app.store().live_slot().expect("the workload committed");
    let floor = app
        .applied_index()
        .expect("a durable lock service reports its applied index");
    drop(app);

    // The header's copy of the applied index and the payload's are written from
    // one argument, so a correct store cannot make them disagree. Resealing is
    // the only way to reach the check, and the check exists because an artifact
    // whose two copies disagree is not the artifact it claims to be.
    let image = read_slot_bytes(scratch.path(), live).expect("the slot reads back");
    let forged = overwrite_header_applied_index(image, LogIndex(floor.0 + 7));
    write_slot_bytes(scratch.path(), live, &forged).expect("the slot rewrites");

    let error = LockStore::open(scratch.path(), config(2, 4))
        .expect_err("a self-contradicting image must not be adopted");
    assert!(
        matches!(
            error,
            LockStoreError::AppliedIndexDisagreement {
                header_index,
                payload_index,
                ..
            } if header_index == LogIndex(floor.0 + 7) && payload_index == floor
        ),
        "unexpected refusal: {error}"
    );
}

#[test]
fn two_slots_at_one_generation_leave_recovery_no_rule_for_choosing() {
    let commands = workload();
    let scratch = ScratchDir::new("ambiguous-generation");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..4]);
    let live = app.store().live_slot().expect("the workload committed");
    drop(app);

    // Two slots claiming one generation cannot happen from this store's own
    // writes, so it is corruption, and there is no rule that picks between them.
    let image = read_slot_bytes(scratch.path(), live).expect("the slot reads back");
    write_slot_bytes(scratch.path(), live.other(), &image).expect("the slot rewrites");

    let error = LockStore::open(scratch.path(), config(2, 4))
        .expect_err("two images of one generation must not be ranked");
    assert!(
        matches!(error, LockStoreError::AmbiguousGeneration { .. }),
        "unexpected refusal: {error}"
    );
}

#[test]
fn the_store_refuses_a_transaction_that_does_not_advance_the_applied_floor() {
    // The state machine already refuses a replayed entry, so this is the store
    // saying the same thing on its own. It matters that both do: the store is
    // the thing recovery reads, and a pair of slots at the same applied index
    // with different content has no rule for choosing between them.
    let scratch = ScratchDir::new("monotonic");
    let lock_config = config(2, 4);
    let mut store = LockStore::open(scratch.path(), lock_config).expect("a fresh store opens");
    let mut service = LockService::new(lock_config);
    service.apply(open_session(0, 1));

    store
        .commit(&service, LogIndex(1))
        .expect("the first transaction advances the floor");

    for repeated in [LogIndex(1), LogIndex::ZERO] {
        let error = store
            .commit(&service, repeated)
            .expect_err("a non-advancing commit must be refused");
        assert!(
            matches!(error, LockStoreError::AppliedIndexRegression { .. }),
            "unexpected refusal at {repeated}: {error}"
        );
    }

    // An install is the exception, and only at the current index or above:
    // installing the state a replica already holds must not require inventing
    // an index.
    store
        .install(&service, LogIndex(1))
        .expect("an install may republish the current index");
    let error = store
        .install(&service, LogIndex::ZERO)
        .expect_err("an install must not move the floor backwards");
    assert!(
        matches!(error, LockStoreError::AppliedIndexRegression { .. }),
        "unexpected refusal: {error}"
    );
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

#[test]
fn a_durable_snapshot_round_trip_preserves_everything_the_contract_lists() {
    let commands = workload();
    let source = ScratchDir::new("snapshot-source");
    let destination = ScratchDir::new("snapshot-destination");

    let mut app = open(source.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let expected = durable_state(&app);
    let snapshot = app
        .build_snapshot(expected.applied_index)
        .expect("a durable lock service snapshots its own applied index");
    drop(app);

    let mut installed = open(destination.path(), FaultPlan::none());
    installed
        .install_snapshot(snapshot)
        .expect("a matching snapshot installs");
    assert_eq!(
        durable_state(&installed),
        expected,
        "locks, every high-water mark, sessions with their cached operation, \
         fingerprint, and result, logical time, and the applied floor all survive"
    );
    assert_eq!(
        installed.store().acknowledged_mark(resource(RESOURCE)),
        Some(token(2))
    );
    drop(installed);

    // The install has to have been a transaction, not an in-memory adoption.
    let reopened = open(destination.path(), FaultPlan::none());
    assert_eq!(
        durable_state(&reopened),
        expected,
        "an installed snapshot is durable without any further write"
    );
    assert_eq!(
        reopened.store().generation(),
        1,
        "an install is one publication, like any other"
    );
}

#[test]
fn a_crash_during_a_snapshot_install_leaves_exactly_one_side_of_it() {
    let commands = workload();
    let source = ScratchDir::new("install-crash-source");
    let mut app = open(source.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let installed_state = durable_state(&app);
    let snapshot = app
        .build_snapshot(installed_state.applied_index)
        .expect("a durable lock service snapshots its own applied index");
    let image_len =
        LockStore::planned_image_len(config(2, 4), app.service(), installed_state.applied_index)
            .expect("the installed image is encodable");
    drop(app);

    // The destination already holds a shorter prefix of the same history, so
    // "pre-install" is a real state with real marks rather than an empty one.
    let prefix = &commands[..4];
    let mut observed_pre_install = 0;
    let mut observed_post_install = 0;

    // Every fault here stops short of the last byte, so every one of them must
    // land on the pre-install side. `AtSlotSync` is deliberately not in this
    // list: it emits the whole image and only fails the barrier, and this suite
    // cannot say which side that lands on — see
    // `a_failed_durability_barrier_leaves_the_outcome_to_recovery`, and the
    // store's own note that a same-process test reads its own writes back
    // through the page cache.
    let short_faults = [
        WriteFault::BeforeFirstByte,
        WriteFault::AfterBytes(0),
        WriteFault::AfterBytes(as_u64(SLOT_HEADER_LEN) - 1),
        WriteFault::AfterBytes(as_u64(SLOT_HEADER_LEN) + 4),
        WriteFault::AfterBytes(image_len - 1),
    ];
    for fault in short_faults {
        let destination = ScratchDir::new("install-crash");
        // Four clean transactions, then the install is the fifth publication.
        let plan = FaultPlan::at(5, fault);
        let mut app = open(destination.path(), plan.clone());
        apply_all(&mut app, prefix);
        let before = durable_state(&app);

        app.install_snapshot(clone_snapshot(&snapshot))
            .expect_err(&format!("`{plan}` must interrupt the install"));
        assert_eq!(
            app.store().fired_fault(),
            Some(fault),
            "`{plan}` never reached its boundary"
        );
        drop(app);

        let recovered = open(destination.path(), FaultPlan::none());
        let state = durable_state(&recovered);
        if state == before {
            observed_pre_install += 1;
        } else {
            assert_eq!(
                state, installed_state,
                "`{plan}` recovered to neither side of the install"
            );
            observed_post_install += 1;
        }
        // Whichever side it landed on, the marks never went backwards.
        assert!(
            recovered
                .store()
                .acknowledged_mark(resource(RESOURCE))
                .is_some_and(|mark| mark >= token(2)),
            "`{plan}` lost a fencing high-water mark across the install"
        );
    }

    // An install is sealed by its last four bytes and published by the barrier
    // that follows them, so a fault that stopped short can only leave the
    // pre-install state. A run that saw a post-install state here would mean
    // the commit point had moved earlier than the seal.
    assert_eq!(
        (observed_pre_install, observed_post_install),
        (short_faults.len(), 0),
        "an image that was never sealed must never be adopted"
    );
}

#[test]
fn an_install_never_makes_an_acknowledged_command_executable_again() {
    let commands = workload();
    let scratch = ScratchDir::new("install-dedup");
    // The highest completed sequence is the only one a session still answers
    // from cache, so the command to retry is the last one the run acknowledged.
    let acknowledged = commands[commands.len() - 1];

    let source = ScratchDir::new("install-dedup-source");
    let mut app = open(source.path(), FaultPlan::none());
    apply_all(&mut app, &commands);
    let expected = durable_state(&app);
    let snapshot = app
        .build_snapshot(expected.applied_index)
        .expect("a durable lock service snapshots its own applied index");
    drop(app);

    let mut installed = open(scratch.path(), FaultPlan::none());
    installed
        .install_snapshot(snapshot)
        .expect("a matching snapshot installs");
    drop(installed);

    let mut recovered = open(scratch.path(), FaultPlan::none());
    assert_eq!(durable_state(&recovered), expected);

    // The deduplication state is the thing an install must not drop: without
    // it, this acknowledged renewal would run again against a lock table it no
    // longer matches.
    let replayed = apply_one(
        &mut recovered,
        LogIndex(expected.applied_index.0 + 1),
        acknowledged,
    )
    .expect("a fresh index applies");
    assert_eq!(replayed.disposition, ApplyDisposition::Replayed);
    assert_eq!(
        durable_state(&recovered).view,
        expected.view,
        "the replay after an install moved nothing"
    );
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

#[test]
fn a_replica_that_crashed_mid_transaction_recovers_and_rejoins() {
    let scratch = ScratchDir::new("cluster-crash");
    let lock_config = config(2, 4);
    let mut apps = DurableLockApps::new(scratch.path(), lock_config);

    // Node 3 loses power part way through the payload of its third durable
    // transaction. The payload is never shorter than its fixed prologue, so a
    // stop a few bytes into it lands inside the payload for any lock service
    // this test can build.
    let stop = as_u64(SLOT_HEADER_LEN) + 5;
    let plan = FaultPlan::at(3, WriteFault::AfterBytes(stop));
    apps.arm(NodeId(3), plan.clone());

    let mut cluster = LockCluster::with_apps(lock_config, apps);
    let leader = cluster.elect_leader();
    assert_eq!(leader, NodeId(1), "the lowest election timeout wins");

    let commands = workload();
    for command in &commands {
        cluster.submit(leader, *command);
    }

    assert_eq!(
        cluster
            .crashed()
            .into_iter()
            .map(|(node_id, _)| node_id)
            .collect::<Vec<_>>(),
        vec![NodeId(3)],
        "the armed replica must be the one that died, and the only one (`{plan}`)"
    );

    // The surviving quorum kept serving while node 3 was down, so the cluster
    // is strictly ahead of anything node 3 can recover on its own.
    cluster.settle();
    let quorum_view = cluster.service_view(NodeId(2));
    let quorum_mark = cluster
        .lock_status(NodeId(2), resource(RESOURCE))
        .token_floor
        .expect("the quorum acquired this resource");

    cluster.restart(NodeId(3));
    let recovered_mark = cluster
        .lock_status(NodeId(3), resource(RESOURCE))
        .token_floor;
    assert_ne!(
        cluster.service_view(NodeId(3)),
        quorum_view,
        "a replica that came back already caught up would make the catch-up vacuous"
    );
    assert!(
        cluster.crashed().is_empty(),
        "the restarted replica is alive again"
    );

    // Catching up is the leader's work, not recovery's: a restarted replica
    // knows only what its own durable state said, and the entries committed
    // while it was gone reach it through ordinary replication.
    cluster.submit(leader, submit(1, 4, 3, acquire("reports/daily", 4)));
    cluster.run_rounds(8);
    cluster.settle();

    let converged = cluster.service_view(leader);
    assert_ne!(
        converged, quorum_view,
        "the post-restart command must have moved the cluster on"
    );
    for node_id in cluster.node_ids() {
        assert_eq!(
            cluster.service_view(node_id),
            converged,
            "replica {} did not converge with the rest (`{plan}`)",
            node_id.0
        );
    }
    assert!(
        cluster.crashed().is_empty(),
        "no replica died during the catch-up (`{plan}`)"
    );
    // The mark is the fact that had to survive the crash, the recovery, and the
    // catch-up. It may only ever have gone up.
    assert!(
        recovered_mark.is_none_or(|mark| mark <= quorum_mark),
        "a crashed replica recovered a mark above the quorum's"
    );
    assert!(
        cluster
            .lock_status(NodeId(3), resource(RESOURCE))
            .token_floor
            .is_some_and(|mark| mark >= quorum_mark),
        "the rejoined replica must hold at least the mark the quorum established"
    );
}

// ---------------------------------------------------------------------------
// Scenario support
// ---------------------------------------------------------------------------

/// The command sequence these scenarios replicate, at indexes 1 upward.
///
/// It opens two sessions, acquires a resource, releases it, and reacquires it
/// so its high-water mark outruns its first tenure, tracks a second resource,
/// advances logical time past that second resource's expiry, and renews the
/// first. The committed state therefore exercises every field the transaction
/// has to carry: a held lock, a tracked but free resource, two distinct marks,
/// two sessions with cached operations and results, and a non-zero logical
/// time.
fn workload() -> Vec<Command> {
    vec![
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(0, 1, 2, release(RESOURCE, 1)),
        submit(0, 1, 3, acquire(RESOURCE, 10)),
        open_session(1, 4),
        submit(1, 4, 1, acquire(AUDIT, 3)),
        submit(1, 4, 2, expire_through(3)),
        submit(0, 1, 4, renew(RESOURCE, 2, 20)),
    ]
}

/// How a scenario got from one durable state to the next.
#[derive(Clone, Copy, Debug)]
enum RecoveryPath {
    /// Close the store and reopen it, changing nothing.
    Restart,
    /// Interrupt one publication part way through its payload.
    CrashMidPublication,
    /// Build the current state as a snapshot and install it back.
    SnapshotInstall,
}

/// Drives one recovery path over `directory`, returning the new applied floor.
fn walk_recovery_path(
    directory: &Path,
    path: RecoveryPath,
    floor: LogIndex,
    label: &str,
) -> LogIndex {
    match path {
        RecoveryPath::Restart => floor,
        RecoveryPath::CrashMidPublication => {
            let mut app = open(
                directory,
                FaultPlan::at(1, WriteFault::AfterBytes(as_u64(SLOT_HEADER_LEN) + 3)),
            );
            apply_one(&mut app, LogIndex(floor.0 + 1), open_session(1, 40))
                .expect_err(&format!("{label} must interrupt its transaction"));
            assert!(app.store().requires_reopen(), "{label} left a live store");
            floor
        }
        RecoveryPath::SnapshotInstall => {
            let mut app = open(directory, FaultPlan::none());
            let snapshot = app
                .build_snapshot(floor)
                .unwrap_or_else(|error| panic!("{label} could not build a snapshot: {error}"));
            app.install_snapshot(snapshot)
                .unwrap_or_else(|error| panic!("{label} could not install: {error}"));
            floor
        }
    }
}

/// Releases the current tenure and takes a fresh one, returning its token.
///
/// This is what makes a lost mark observable: a new tenure asks the store for
/// the next token, and only a store that still knows the old mark can answer
/// correctly.
fn take_a_fresh_tenure(
    app: &mut DurableLockStateMachine,
    floor: LogIndex,
    label: &str,
) -> (FencingToken, LogIndex) {
    let held = app
        .service()
        .status(resource(RESOURCE))
        .holder
        .expect("the resource is held before a fresh tenure is taken");
    let epoch = 100 + floor.0;
    let mut index = floor.0;
    let mut step = |command: Command, app: &mut DurableLockStateMachine| {
        index += 1;
        apply_one(app, LogIndex(index), command)
            .unwrap_or_else(|error| panic!("after {label}, `{command:?}` failed: {error}"))
    };

    step(open_session(0, epoch), app);
    step(
        submit(0, epoch, 1, release(RESOURCE, held.token.get())),
        app,
    );
    let acquired = step(submit(0, epoch, 2, acquire(RESOURCE, 10)), app);
    let LockResponse::Operation(OperationResult::Acquired { token, .. }) = acquired.response else {
        panic!("after {label}, a free resource did not acquire: {acquired:?}");
    };
    (token, LogIndex(index))
}

/// Offers a write to the guarded resource and requires it to be accepted.
fn accept(guard: &mut GuardedResource, token: FencingToken, value: u64) {
    guard
        .apply(GuardedWrite {
            resource: resource(RESOURCE),
            token,
            value,
        })
        .unwrap_or_else(|rejection| {
            panic!(
                "the guarded resource refused the current owner's token {token:?}: {rejection:?}"
            )
        });
}

/// Opens a durable lock service over `directory` under `faults`.
fn open(directory: &Path, faults: FaultPlan) -> DurableLockStateMachine {
    let armed = faults.to_string();
    let store =
        LockStore::open_with_faults(directory, config(2, 4), faults).unwrap_or_else(|error| {
            panic!(
                "a lock store opens at {} under `{armed}`: {error}",
                directory.display()
            )
        });
    DurableLockStateMachine::new(store)
}

/// Applies one command at `index`, returning its outcome.
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
    .map(|mut results| {
        results
            .pop()
            .expect("a one-entry batch returns one result")
            .result
    })
}

/// Applies `commands` at consecutive indexes from one, expecting each to commit.
fn apply_all(app: &mut DurableLockStateMachine, commands: &[Command]) {
    for (position, command) in commands.iter().enumerate() {
        apply_one(app, index_of(position + 1), *command)
            .unwrap_or_else(|error| panic!("transaction {} must commit: {error}", position + 1));
    }
}

/// Returns everything a transaction moves, as one comparable value.
fn durable_state(app: &DurableLockStateMachine) -> DurableState {
    DurableState {
        applied_index: app
            .applied_index()
            .expect("a durable lock service reports its applied index"),
        view: app.service().view(),
    }
}

/// Returns the state before the `interrupted`-th command, and that command's
/// payload length.
fn state_and_payload_len(commands: &[Command], interrupted: usize) -> (DurableState, u64) {
    let scratch = ScratchDir::new("measure");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, &commands[..interrupted - 1]);
    let before = durable_state(&app);
    apply_one(&mut app, index_of(interrupted), commands[interrupted - 1])
        .expect("an uninterrupted transaction commits");
    let image_len = LockStore::planned_image_len(
        config(2, 4),
        app.service(),
        app.applied_index()
            .expect("a durable lock service reports its applied index"),
    )
    .expect("the measured image is encodable");
    (
        before,
        image_len - as_u64(SLOT_HEADER_LEN) - as_u64(SLOT_TRAILER_LEN),
    )
}

/// Returns the state an uninterrupted run of `commands` reaches.
fn uninterrupted_state(commands: &[Command]) -> DurableState {
    let scratch = ScratchDir::new("uninterrupted");
    let mut app = open(scratch.path(), FaultPlan::none());
    apply_all(&mut app, commands);
    durable_state(&app)
}

/// Replays `commands` through the structurally independent oracle.
fn replay_through_oracle(commands: &[Command]) -> ReferenceLockService {
    let mut oracle = ReferenceLockService::new(config(2, 4));
    for command in commands {
        oracle.apply(*command);
    }
    oracle
}

/// Asserts what one interrupted publication reported before the process died.
///
/// Split out of the sweep so the sweep itself reads as the loop it is. The
/// point of every assertion here is that a store which failed mid-publication
/// must say so, and must not have moved anything it reports as durable.
fn assert_interrupted_publication(
    app: &DurableLockStateMachine,
    outcome: Result<ApplyOutcome, DurableLockError>,
    stop: u64,
    committed: bool,
    before: &DurableState,
    plan: &FaultPlan,
) {
    if committed {
        outcome.unwrap_or_else(|error| panic!("a whole image commits under `{plan}`: {error}"));
        assert_eq!(
            app.store().fired_fault(),
            None,
            "a fault that stops after the last byte stops nothing (`{plan}`)"
        );
        return;
    }

    let error = outcome.expect_err(&format!("`{plan}` must interrupt the transaction"));
    assert!(
        matches!(
            error,
            DurableLockError::Store(LockStoreError::InjectedFault { .. })
        ),
        "`{plan}` failed for the wrong reason: {error}"
    );
    assert_eq!(
        app.store().fired_fault(),
        Some(WriteFault::AfterBytes(stop)),
        "`{plan}` never reached its boundary"
    );
    assert!(
        app.store().requires_reopen(),
        "a store that failed mid-publication cannot say what its stale slot holds (`{plan}`)"
    );
    // The machine reports a *durable* applied index, so a transaction that did
    // not commit must not have moved it — nor the lock service beside it. A
    // machine that adopted state before publishing it would report a floor
    // above what recovery can reach.
    assert_eq!(
        &durable_state(app),
        before,
        "a failed transaction moved the reported state (`{plan}`)"
    );
}

/// Predicate naming the damage shape a corruption case must produce.
type DamageTest = fn(SlotDamage) -> bool;

/// A byte-level corruption applied to one slot image.
type ImageMutation = fn(&mut Vec<u8>);

/// Returns the slot damage a stop after `stop` bytes of an image must leave.
fn expected_damage(stop: u64, payload_len: u64, image_len: u64) -> Option<SlotDamage> {
    let header = as_u64(SLOT_HEADER_LEN);
    if stop == 0 || stop == image_len {
        None
    } else if stop < header {
        Some(SlotDamage::HeaderIncomplete { present: stop })
    } else if stop < header + payload_len {
        Some(SlotDamage::PayloadIncomplete {
            declared: payload_len,
            present: stop - header,
        })
    } else if stop == header + payload_len {
        Some(SlotDamage::MissingCommitChecksum)
    } else {
        Some(SlotDamage::PartialCommitChecksum {
            present: stop - header - payload_len,
        })
    }
}

/// Names the shape of a damage without its byte counts, so a sweep can assert
/// it produced every one of them.
fn damage_kind(damage: Option<SlotDamage>) -> &'static str {
    match damage {
        None => "sealed or empty",
        Some(SlotDamage::HeaderIncomplete { .. }) => "header incomplete",
        Some(SlotDamage::NotALockImage { .. }) => "foreign magic",
        Some(SlotDamage::UnsupportedFormatVersion { .. }) => "unreadable version",
        Some(SlotDamage::HeaderChecksumMismatch { .. }) => "corrupt header",
        Some(SlotDamage::PayloadIncomplete { .. }) => "payload incomplete",
        Some(SlotDamage::MissingCommitChecksum) => "missing commit checksum",
        Some(SlotDamage::PartialCommitChecksum { .. }) => "partial commit checksum",
        Some(SlotDamage::CommitChecksumMismatch { .. }) => "unsealed image",
        Some(SlotDamage::TrailingBytes { .. }) => "bytes beyond the seal",
    }
}

/// Copies a snapshot so one built value can be installed repeatedly.
fn clone_snapshot(snapshot: &ApplicationSnapshot) -> ApplicationSnapshot {
    ApplicationSnapshot {
        applied_index: snapshot.applied_index,
        payload: snapshot.payload.clone(),
        raft_snapshot: None,
    }
}

fn token(value: u64) -> FencingToken {
    FencingToken::new(value).expect("test token is nonzero")
}

/// The one-based log index of the `position`-th command.
fn index_of(position: usize) -> LogIndex {
    LogIndex(as_u64(position))
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).expect("test sizes fit a u64")
}
