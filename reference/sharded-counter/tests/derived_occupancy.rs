//! What the audit derives about workers, rather than believes about them.
//!
//! Worker occupancy used to be two scheduler-authored bits: a dispatch set a
//! group's `servicing` flag and a release cleared it, and the audit checked
//! neither. That made the fairness bound's fifth readiness condition something
//! the audited party defined for itself — omit one release and the starved
//! group is permanently un-ready, permanently owed nothing, and permanently
//! invisible to a `widest_gap` of zero.
//!
//! Every test here is a case that used to pass. An occupancy now opens at the
//! cost its serviced items add up to, comes due at a tick anyone can compute,
//! and is judged on arrival: early, late, unpaired, or beyond the worker pool
//! are each a named fault. The last two tests are the positive controls, so the
//! derivation is shown to accept correct schedulers as well as reject broken
//! ones.

mod support;

use rafter_reference_sharded_counter::{
    GroupAvailability, GroupId, GroupLifecycle, HistoryEvent, LifecycleOutcome, LifecycleRequest,
    OfferOutcome, ReferenceScheduler, SchedulerConfig, SchedulingViolation, SystemClass, TickIndex,
};
use support::{config, create, first, group, system, tick, History, Recorder};

/// Two groups, a quota of two, and items costing one tick each, so a full turn
/// occupies a worker for exactly two ticks and passes fall on even ticks.
const QUOTA: u32 = 2;
const TURN: u64 = QUOTA as u64;

fn bounds() -> SchedulerConfig {
    config(4, 2, 2, 64, 512)
}

/// Builds the starvation shape: both groups take pass one, and every later
/// pass names only `favored` while `starved` keeps a standing backlog.
///
/// `passes` counts the plans armed. `release` decides whether the worker
/// `starved` took in pass one is ever reported free again — the single event
/// that used to decide whether the audit saw anything at all.
fn starvation(favored: GroupId, starved: GroupId, passes: u64, release: bool) -> History {
    let mut history = History::new();
    for id in [favored, starved] {
        history.open_group(id, QUOTA);
    }
    // The favoured backlog is submitted first, so its work identifiers run
    // from one and the starved group's follow.
    let favored_items = passes * TURN;
    for _ in 0..favored_items {
        history.submit(favored, system(SystemClass::Bulk, 1));
    }
    for _ in 0..40 {
        history.submit(starved, system(SystemClass::Bulk, 1));
    }

    let mut next_favored = 1;
    let mut next_starved = favored_items + 1;
    for index in 0..passes {
        let pass = index + 1;
        let at = (index + 1) * TURN;
        if index > 0 {
            history.released(at, favored);
            if release && index == 1 {
                history.released(at, starved);
            }
        }
        let plan = if index == 0 {
            vec![favored, starved]
        } else {
            vec![favored]
        };
        history.armed(pass, at, plan);

        history.dispatched(pass, at, favored, QUOTA, TURN);
        for _ in 0..QUOTA {
            history.serviced(pass, favored, next_favored);
            next_favored += 1;
        }
        if index == 0 {
            history.dispatched(pass, at, starved, QUOTA, TURN);
            for _ in 0..QUOTA {
                history.serviced(pass, starved, next_starved);
                next_starved += 1;
            }
        }
        history.retired(pass, at);
    }
    history
}

// ---------------------------------------------------------------------------
// The starvation the audit could not see
// ---------------------------------------------------------------------------

/// The payload of the whole derivation: an occupancy that has outlived the
/// cost that opened it stops excluding its group from the ready set, so a plan
/// that omits the group is denying it a turn and the gap says so.
///
/// This history contains no `WorkerReleased` for the starved group at all. It
/// ends at the tick its occupancy came due, so nothing is yet overdue and the
/// only thing left to report is the fairness failure itself. Before the
/// derivation this audit was `Ok` with `widest_gap == 0`.
#[test]
fn an_expired_occupancy_stops_excusing_a_plan_that_omits_its_group() {
    let favored = group(0);
    let starved = group(1);
    let history = starvation(favored, starved, 2, false);

    let violation = history
        .audit(bounds())
        .expect_err("a plan armed at the tick an occupancy expired owes that group a turn");
    println!("expired occupancy, no release recorded: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: support::pass(2),
            denied_passes: 1,
        },
        "the group was ready again the instant its cost was paid, released or not"
    );
}

/// The other half: a history that runs on past the tick a release was due
/// reports the unreleased worker directly, naming the pass that took it, the
/// tick it was due back, and how far the history had run.
#[test]
fn an_occupancy_that_outlives_its_cost_is_reported_against_the_pass_that_took_it() {
    let favored = group(0);
    let starved = group(1);
    let history = starvation(favored, starved, 12, false);

    let violation = history
        .audit(bounds())
        .expect_err("a worker that is never released is a fault in its own right");
    println!("worker never released across twelve passes: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::WorkerHeldPastCost {
            pass: support::pass(1),
            group: starved,
            due: tick(2 * TURN),
            observed: tick(3 * TURN),
        },
        "the occupancy was due one turn after the dispatch that opened it"
    );
}

/// And the control: the identical history with the release recorded where it
/// belongs still fails, as the same starvation, through the gap. The fix did
/// not trade a fairness failure for a structural one — it added a second way
/// to catch a scheduler that was already cheating.
#[test]
fn recording_the_release_leaves_the_same_starvation_to_the_gap() {
    let favored = group(0);
    let starved = group(1);
    let passes = 12;
    let history = starvation(favored, starved, passes, true);

    let violation = history
        .audit(bounds())
        .expect_err("releasing the worker exposes the gap it was hiding");
    println!("worker released on time, plans still exclude the group: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: support::pass(2),
            denied_passes: u32::try_from(passes - 1).expect("twelve passes fit in u32"),
        }
    );
}

// ---------------------------------------------------------------------------
// Releases in every wrong direction
// ---------------------------------------------------------------------------

/// A release for a group holding no worker is refused rather than absorbed.
/// Absorbing it let a scheduler clear an occupancy it never opened.
#[test]
fn a_release_that_pairs_with_no_dispatch_is_refused() {
    let mut recorder = Recorder::new(bounds());
    for id in [group(0), group(1)] {
        recorder.open_group(id, QUOTA);
        for _ in 0..4 {
            recorder.submit(id, first(), system(SystemClass::Bulk, 1));
        }
    }
    recorder.run(12);
    recorder.assert_agreement(&"a correct workload first");

    // Ahead of every dispatch, so it cannot be a late report of anything.
    let mut leading = ReferenceScheduler::new(bounds());
    leading.observe(HistoryEvent::WorkerReleased {
        tick: TickIndex::ZERO,
        group: group(0),
    });
    leading.observe_all(recorder.oracle().history().iter().cloned());
    assert_eq!(
        leading
            .audit()
            .expect_err("a release before any dispatch pairs with nothing"),
        SchedulingViolation::SpuriousWorkerRelease {
            tick: TickIndex::ZERO,
            group: group(0),
        }
    );

    // And after every occupancy has already ended.
    let mut trailing = ReferenceScheduler::new(bounds());
    trailing.observe_all(recorder.oracle().history().iter().cloned());
    trailing.observe(HistoryEvent::WorkerReleased {
        tick: tick(999),
        group: group(1),
    });
    assert_eq!(
        trailing
            .audit()
            .expect_err("a second release of a worker already free pairs with nothing"),
        SchedulingViolation::SpuriousWorkerRelease {
            tick: tick(999),
            group: group(1),
        }
    );
}

/// An early release is not a harmless generosity. It returns a worker that is
/// still busy to the pool, so the host can hold more dispatches at once than it
/// has workers, and it returns its group to the ready set having paid less than
/// its work cost.
#[test]
fn a_release_before_its_cost_is_paid_is_refused() {
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, QUOTA);
    history.submit(id, system(SystemClass::Bulk, 4));
    history.submit(id, system(SystemClass::Bulk, 3));
    // One turn of two items costing four and three ticks: due at 1 + 7 == 8.
    history.armed(1, 1, vec![id]);
    history.dispatched(1, 1, id, 2, 7);
    history.serviced(1, id, 1);
    history.serviced(1, id, 2);
    history.retired(1, 1);
    history.released(5, id);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("a worker released three ticks early was never free"),
        SchedulingViolation::WorkerReleasedEarly {
            pass: support::pass(1),
            group: id,
            due: tick(8),
            observed: tick(5),
        }
    );
}

/// Faking releases to run more dispatches than there are workers is caught by
/// counting the occupancies rather than the release reports.
#[test]
fn more_concurrent_dispatches_than_workers_is_refused() {
    let single_worker = config(4, 1, 2, 64, 512);
    let mut history = History::new();
    for id in [group(0), group(1)] {
        history.open_group(id, 1);
        history.submit(id, system(SystemClass::Bulk, 5));
    }
    history.armed(1, 1, vec![group(0), group(1)]);
    history.dispatched(1, 1, group(0), 1, 5);
    history.serviced(1, group(0), 1);
    // The pool has one worker and its occupancy runs to tick six.
    history.dispatched(1, 1, group(1), 1, 5);

    assert_eq!(
        history
            .audit(single_worker)
            .expect_err("two occupancies cannot come out of a pool of one"),
        SchedulingViolation::WorkerCountExceeded {
            pass: support::pass(1),
            group: group(1),
            workers: 1,
        }
    );
}

// ---------------------------------------------------------------------------
// The clock the deadlines are measured against
// ---------------------------------------------------------------------------

/// Every occupancy deadline is a tick, so a history that walks its clock
/// backwards could hold a deadline permanently in the future. The clock is
/// checked rather than trusted.
#[test]
fn a_recorded_tick_may_never_precede_one_already_recorded() {
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, 1);
    history.submit(id, system(SystemClass::Bulk, 1));
    history.submit(id, system(SystemClass::Bulk, 1));
    history.armed(1, 9, vec![id]);
    history.dispatched(1, 9, id, 1, 1);
    history.serviced(1, id, 1);
    history.retired(1, 9);
    history.released(10, id);
    history.armed(2, 4, vec![id]);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("a clock that runs backwards measures nothing"),
        SchedulingViolation::TickWentBackwards {
            current: tick(10),
            observed: tick(4),
        }
    );
}

/// The rule that stops the clock being stopped. A tick arms at most one plan,
/// so a scheduler cannot arm plan after plan — denying a group every one of
/// them — while the occupancies it holds never come due. Without it, running
/// the clock in place is a way to starve a group forever without a tick ever
/// passing for a deadline to fall due in.
///
/// The second plan is empty, and necessarily so: everything ready was just
/// dispatched by the first. That is the whole shape of the abuse — arming
/// again at the same tick can only be a way of accumulating plans without
/// spending time.
#[test]
fn one_tick_arms_at_most_one_plan() {
    let mut history = History::new();
    for id in [group(0), group(1)] {
        history.open_group(id, 1);
        history.submit(id, system(SystemClass::Bulk, 1));
    }
    history.armed(1, 3, vec![group(0), group(1)]);
    history.dispatched(1, 3, group(0), 1, 1);
    history.serviced(1, group(0), 1);
    history.dispatched(1, 3, group(1), 1, 1);
    history.serviced(1, group(1), 2);
    history.retired(1, 3);
    history.armed(2, 3, vec![]);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("a second plan armed at the same tick"),
        SchedulingViolation::PassBoundaryReused {
            pass: support::pass(2),
            tick: tick(3),
        }
    );
}

/// And the counterpart: a tick retires at most one plan. This one is reachable
/// without arming twice, because a suspended pass retires later than it was
/// armed — so a pass that retires at tick four leaves room for a pass armed at
/// tick four to try to retire there too.
#[test]
fn one_tick_retires_at_most_one_plan() {
    let mut history = History::new();
    for id in [group(0), group(1), group(2)] {
        history.open_group(id, 1);
    }
    // Two slow items fill both workers, so the third group waits three ticks
    // for its turn and the pass retires long after it was armed.
    history.submit(group(0), system(SystemClass::Bulk, 3));
    history.submit(group(1), system(SystemClass::Bulk, 3));
    history.submit(group(2), system(SystemClass::Bulk, 1));
    history.submit(group(0), system(SystemClass::Bulk, 1));

    history.armed(1, 1, vec![group(0), group(1), group(2)]);
    history.dispatched(1, 1, group(0), 1, 3);
    history.serviced(1, group(0), 1);
    history.dispatched(1, 1, group(1), 1, 3);
    history.serviced(1, group(1), 2);
    history.released(4, group(0));
    history.released(4, group(1));
    history.dispatched(1, 4, group(2), 1, 1);
    history.serviced(1, group(2), 3);
    history.retired(1, 4);

    // A second pass armed and retired inside the same tick.
    history.armed(2, 4, vec![group(0)]);
    history.dispatched(2, 4, group(0), 1, 1);
    history.serviced(2, group(0), 4);
    history.retired(2, 4);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("a second plan retired at the same tick"),
        SchedulingViolation::PassBoundaryReused {
            pass: support::pass(2),
            tick: tick(4),
        }
    );
}

// ---------------------------------------------------------------------------
// The price of a turn
// ---------------------------------------------------------------------------

/// A dispatch's cost is derived from the items it serviced, so rewriting the
/// reported figure is caught. Before, the fold folded the number without ever
/// looking at it, and any value at all was accepted.
#[test]
fn a_dispatch_that_misreports_its_cost_is_refused() {
    let mut recorder = Recorder::new(bounds());
    for id in [group(0), group(1)] {
        recorder.open_group(id, QUOTA);
        for _ in 0..6 {
            recorder.submit(id, first(), system(SystemClass::Bulk, 2));
        }
    }
    recorder.run(20);
    recorder.assert_agreement(&"a correct workload first");

    let mut rewritten = 0_u32;
    let mut corrupted = ReferenceScheduler::new(bounds());
    for event in recorder.oracle().history() {
        corrupted.observe(match event {
            HistoryEvent::GroupOffered {
                pass,
                tick: at,
                group: id,
                outcome: OfferOutcome::Dispatched { serviced, cost },
            } => {
                rewritten += 1;
                HistoryEvent::GroupOffered {
                    pass: *pass,
                    tick: *at,
                    group: *id,
                    outcome: OfferOutcome::Dispatched {
                        serviced: *serviced,
                        cost: cost + 1,
                    },
                }
            }
            other => other.clone(),
        });
    }
    assert!(rewritten > 0, "the workload produced dispatches to rewrite");

    let violation = corrupted
        .audit()
        .expect_err("a turn cannot charge a worker anything it likes");
    println!("rewrote {rewritten} dispatch costs: {violation:?}");
    assert!(
        matches!(
            violation,
            SchedulingViolation::DispatchCostMismatch {
                expected: 4,
                observed: 5,
                ..
            }
        ),
        "two items of cost two are worth four ticks, whatever the dispatch says: {violation:?}"
    );
}

/// Deriving the cost is only half of it: the occupancy is *opened* at the
/// derived figure too, so a dispatch that under-reports its cost is still held
/// for exactly as long as its work was worth. A scheduler that could shorten
/// its own occupancy by lying about the price would be back to naming its own
/// readiness, one indirection further away.
#[test]
fn a_misreported_cost_still_holds_its_worker_for_what_the_work_was_worth() {
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, 1);
    history.submit(id, system(SystemClass::Bulk, 5));
    history.armed(1, 1, vec![id]);
    // The item is worth five ticks. The dispatch claims one.
    history.dispatched(1, 1, id, 1, 1);
    history.serviced(1, id, 1);
    history.retired(1, 1);
    // Carry the clock to tick three: past the claimed deadline of two, well
    // short of the derived deadline of six.
    history.reported(3, id, GroupAvailability::Available);

    let replay = history.replay(bounds());
    assert!(
        matches!(
            replay.fairness,
            Err(SchedulingViolation::DispatchCostMismatch {
                expected: 5,
                observed: 1,
                ..
            })
        ),
        "the lie itself is reported: {:?}",
        replay.fairness
    );
    assert!(
        replay
            .view
            .groups
            .iter()
            .any(|view| view.group == id && view.servicing),
        "the occupancy runs on the work's cost, not the dispatch's claim"
    );
}

/// A turn's occupancy no longer saturates. Eight items of cost `2^31` are worth
/// `2^34` ticks, and a `u32` accumulator reported `u32::MAX` — under-charging
/// the worker by more than fifteen billion ticks, silently.
#[test]
fn a_large_turn_charges_its_whole_cost_rather_than_saturating() {
    let mut recorder = Recorder::new(config(2, 1, 2, 8, 16));
    let id = group(0);
    recorder.open_group(id, 8);
    let each = 1_u32 << 31;
    for _ in 0..8 {
        recorder.submit(id, first(), system(SystemClass::Bulk, each));
    }

    let report = recorder.step(&[]);
    let expected = u64::from(each) * 8;
    assert_eq!(
        report.offers[0].outcome,
        OfferOutcome::Dispatched {
            serviced: 8,
            cost: expected,
        },
        "the turn is worth the sum of its items, which is above u32::MAX"
    );
    assert!(
        expected > u64::from(u32::MAX),
        "the case is only interesting above the old accumulator's ceiling"
    );
    println!("eight items of cost {each} charge {expected} ticks");
    recorder.assert_agreement(&"a turn above the u32 ceiling");
}

// ---------------------------------------------------------------------------
// Positive controls
// ---------------------------------------------------------------------------

/// The derivation accepts a correct scheduler under sustained pressure: sixteen
/// groups, four workers, mixed costs, eight hundred ticks. Every occupancy is
/// opened, timed, and closed by the fold, and the workload is checked against
/// it throughout — so this is the assertion that the new faults are not merely
/// unreachable.
#[test]
fn a_correct_scheduler_keeps_every_occupancy_it_opens() {
    let workers = 4;
    let mut recorder = Recorder::new(config(16, workers, 2, 16, 256));
    for index in 0..16 {
        recorder.open_group(group(index), 1 + index % 3);
    }

    let mut occupied: std::collections::BTreeSet<GroupId> = std::collections::BTreeSet::new();
    let mut dispatches = 0_u64;
    let mut seed = 7_u64;
    let mut next = move || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        u32::try_from(seed >> 33).expect("a 33-bit shift fits in u32")
    };
    for step in 0..800 {
        for _ in 0..3 {
            let id = group(next() % 16);
            recorder.submit(id, first(), system(SystemClass::Bulk, 1 + next() % 5));
        }
        let report = recorder.step(&[]);
        for released in &report.released {
            assert!(
                occupied.remove(released),
                "step {step}: {released:?} was released without holding a worker"
            );
        }
        for offer in &report.offers {
            if matches!(offer.outcome, OfferOutcome::Dispatched { .. }) {
                dispatches += 1;
                assert!(
                    occupied.insert(offer.group),
                    "step {step}: {:?} was dispatched while already holding a worker",
                    offer.group
                );
                assert!(
                    u32::try_from(occupied.len()).expect("a bounded count fits in u32") <= workers,
                    "step {step}: more groups held workers than the pool has"
                );
            }
        }
    }
    recorder.assert_agreement(&"sustained occupancy pressure");
    let report = recorder
        .oracle()
        .audit()
        .expect("a correct scheduler keeps the bound");
    println!(
        "{dispatches} dispatches over {} passes, widest plan {}, gap {}",
        report.passes_completed, report.widest_plan, report.widest_gap
    );
    assert!(dispatches > 0);
    assert!(
        report.passes_completed >= 8,
        "the workload must complete enough passes to mean something: {report:?}"
    );
}

/// An occupancy belongs to the worker, not to the slot's incarnation. A group
/// removed and created again while its predecessor's work is outstanding stays
/// out of the ready set until that cost is paid, because the worker it is
/// holding does not care that the slot was reopened.
#[test]
fn a_reopened_slot_keeps_its_predecessors_occupancy() {
    let mut recorder = Recorder::new(config(2, 1, 2, 8, 16));
    let id = group(0);
    recorder.open_group(id, 1);
    recorder.submit(id, first(), system(SystemClass::Bulk, 6));
    let dispatch = recorder.step(&[]);
    assert_eq!(
        dispatch.offers[0].outcome,
        OfferOutcome::Dispatched {
            serviced: 1,
            cost: 6,
        }
    );

    recorder.lifecycle(id, LifecycleRequest::Drain);
    recorder.lifecycle(id, LifecycleRequest::Remove);
    let reopened = recorder.lifecycle(id, create(1));
    assert_eq!(
        reopened.outcome,
        LifecycleOutcome::Created {
            incarnation: support::incarnation(2)
        }
    );
    assert!(
        recorder
            .scheduler()
            .group(id)
            .is_some_and(|view| view.servicing && view.state == GroupLifecycle::Creating),
        "the fresh incarnation is still holding the old one's worker"
    );

    // It stays out of every plan until the cost that opened the occupancy is
    // paid, and then it rejoins on its own.
    recorder.lifecycle(id, LifecycleRequest::Recover);
    recorder.lifecycle(id, LifecycleRequest::Serve);
    recorder.submit(id, support::incarnation(2), system(SystemClass::Bulk, 1));
    recorder.step(&[]);
    assert!(
        recorder.scheduler().ready_groups().is_empty(),
        "an unpaid occupancy keeps the reopened slot out of the ready set"
    );
    recorder.run(8);
    assert_eq!(
        recorder.scheduler().summary().queued,
        0,
        "it recovers later"
    );
    recorder.assert_agreement(&"occupancy across a reopening");
}
