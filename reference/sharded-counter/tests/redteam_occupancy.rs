//! Red-team probes against the derived-occupancy rule and the freedoms it
//! leaves the scheduler.
//!
//! Each test states what CONTRACT.md promises and lets the audit answer. Every
//! one of them was written as a *hole* — a schedule the contract says it forbids
//! and the audit accepted — and each now names the fault that catches it.
//!
//! The generation this file closed derived a turn's occupancy from the items the
//! group *owed*, read off its queue when the turn was offered, and never checked
//! that the matching `WorkServiced` events arrived. So a dispatch that serviced
//! nothing was charged full price, and the occupancy it bought kept its group
//! out of the ready set for the whole of it:
//!
//! ```text
//! audit: passes_completed=25 opportunities=25 widest_gap=0; group queued=1 serviced host-wide=0
//! ```
//!
//! Twenty-five opportunities, zero items serviced, and a clean bill of health.
//! Why the rest of the suite could not see it is the important part, and it is
//! why `redteam_controls.rs` exists: `ManagedScheduler` always services what it
//! dispatches, so no history the `Recorder` can produce distinguishes "the audit
//! checks the service stream" from "the audit ignores it". The positive control
//! was the vacuity.

mod support;

use rafter_reference_sharded_counter::{
    GroupAvailability, ReferenceScheduler, SchedulingViolation, SystemClass,
};
use support::{config, group, pass, system, work, History};

/// CONTRACT.md, "Occupancy is derived, not reported":
///
/// ```text
/// cost(turn) = sum of ServiceCost over the items the turn serviced
/// ```
///
/// and invariant 6a: "A turn's worker occupancy is the sum of the
/// `ServiceCost`s of the items it **serviced**".
///
/// A dispatch that services nothing is now priced at nothing, and the queue it
/// left behind is the fault: the worker was held for work that never moved.
#[test]
fn a_dispatch_that_services_nothing_buys_no_occupancy_at_all() {
    const HOLD: u64 = 10;
    const PASSES: u64 = 31;
    let favored = group(0);
    let starved = group(1);

    let mut history = History::new();
    history.open_group(favored, 1);
    history.open_group(starved, 1);
    for _ in 0..PASSES {
        history.submit(favored, system(SystemClass::Bulk, 1));
    }
    // One item, submitted once, and never serviced by anybody.
    history.submit(
        starved,
        system(SystemClass::Bulk, u32::try_from(HOLD).expect("fits")),
    );

    for index in 0..PASSES {
        let pass = index + 1;
        let at = index + 1;
        if index > 0 {
            history.released(at, favored);
        }
        // The starved group's occupancy came due every HOLD ticks, so it was
        // genuinely ready at those instants, was planned and dispatched, and no
        // gap ever accrued. That was the whole trick.
        let renews = at % HOLD == 1;
        let plan = if renews {
            vec![favored, starved]
        } else {
            vec![favored]
        };
        history.armed(pass, at, plan);
        history.dispatched(pass, at, favored, 1, 1);
        // The favoured group's backlog was submitted first, so its work
        // identifiers run from one in pass order.
        history.serviced(pass, favored, pass);
        if renews {
            // Full price, zero work. No `WorkServiced`, and no
            // `WorkerReleased` for this group anywhere in the history.
            history.dispatched(pass, at, starved, 1, HOLD);
        }
        history.retired(pass, at);
    }

    let violation = history
        .audit(bounds())
        .expect_err("a turn charged ten ticks must have serviced ten ticks of work");
    println!("full-price dispatch, zero work: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::DispatchLeftWorkUnserviced {
            pass: pass(1),
            group: starved,
            owed: 1,
            serviced: 0,
        },
        "the first turn that bought an occupancy with nothing is the one named"
    );
}

/// The same hole reached without omitting a single event: a second dispatch in
/// one pass used to silently discard the first dispatch's owed items, because
/// the fold kept one dispatch slot for a pool of `workers`.
///
/// Two workers means two turns may be open in one pass, and each now keeps its
/// own owed set and its own deadline. The first turn is settled by the second
/// one's arrival, and settling it is what reports that it moved nothing.
#[test]
fn a_second_dispatch_in_one_pass_settles_the_first_rather_than_discarding_it() {
    let first = group(0);
    let second = group(1);
    let mut history = History::new();
    for id in [first, second] {
        history.open_group(id, 2);
        history.submit(id, system(SystemClass::Bulk, 1));
        history.submit(id, system(SystemClass::Bulk, 1));
    }

    history.armed(1, 1, vec![first, second]);
    history.dispatched(1, 1, first, 2, 2);
    // Both workers are taken before either turn records its work — the natural
    // shape of a two-worker host, and what used to drop the first turn's items.
    history.dispatched(1, 1, second, 2, 2);
    history.serviced(1, second, 3);
    history.serviced(1, second, 4);
    history.retired(1, 1);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("a turn that reported `serviced: 2, cost: 2` serviced nothing"),
        SchedulingViolation::DispatchLeftWorkUnserviced {
            pass: pass(1),
            group: first,
            owed: 2,
            serviced: 0,
        }
    );

    // And the control on the control: the identical pass with both turns doing
    // their work is accepted, so what the fault caught is the missing service
    // rather than the concurrency.
    let mut honest = History::new();
    for id in [first, second] {
        honest.open_group(id, 2);
        honest.submit(id, system(SystemClass::Bulk, 1));
        honest.submit(id, system(SystemClass::Bulk, 1));
    }
    honest.armed(1, 1, vec![first, second]);
    honest.dispatched(1, 1, first, 2, 2);
    honest.serviced(1, first, 1);
    honest.serviced(1, first, 2);
    honest.dispatched(1, 1, second, 2, 2);
    honest.serviced(1, second, 3);
    honest.serviced(1, second, 4);
    honest.retired(1, 1);

    let report = honest
        .audit(bounds())
        .expect("two concurrent turns on a two-worker host are legal");
    assert_eq!(report.serviced, 4, "both turns did their work: {report:?}");
    assert_eq!(report.widest_gap, 0);
}

/// The purest form of the same hole, with no occupancy window involved at all.
///
/// The group is ready at every arming, is named in every plan, and is
/// dispatched in every pass. It satisfies the required bound perfectly, in the
/// strongest sense the audit can express — and it used to be serviced exactly
/// never, at twenty-five opportunities and a `widest_gap` of zero.
#[test]
fn a_group_offered_in_every_pass_must_actually_be_serviced() {
    const PASSES: u64 = 25;
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, 1);
    history.submit(id, system(SystemClass::Bulk, 1));

    for index in 0..PASSES {
        let pass = index + 1;
        let at = index + 1;
        history.armed(pass, at, vec![id]);
        // Priced at the one item it was owed, which it then does not service.
        history.dispatched(pass, at, id, 1, 1);
        history.retired(pass, at);
    }

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("an opportunity that services nothing is not an opportunity"),
        SchedulingViolation::DispatchLeftWorkUnserviced {
            pass: pass(1),
            group: id,
            owed: 1,
            serviced: 0,
        }
    );
}

/// The mirror: a turn that services more than the queue it was offered against
/// held. A turn is one act over a fixed head of the queue, so running past it
/// is a group taking throughput share the pass never granted.
#[test]
fn a_turn_that_services_past_its_own_work_is_refused() {
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, 1);
    history.submit(id, system(SystemClass::Bulk, 1));
    history.submit(id, system(SystemClass::Bulk, 1));

    history.armed(1, 1, vec![id]);
    // A quota of one, honestly reported, and two items serviced under it.
    history.dispatched(1, 1, id, 1, 1);
    history.serviced(1, id, 1);
    history.serviced(1, id, 2);
    history.retired(1, 1);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("a turn cannot service work its offer never covered"),
        SchedulingViolation::DispatchServicedBeyondItsWork {
            pass: pass(1),
            group: id,
            owed: 1,
            work: work(2),
        }
    );
}

/// A turn is settled by the first event that is not one of its own services,
/// and the end of the record is such an event.
///
/// Without that, a scheduler's last turn in any history is never priced at all:
/// stop recording immediately after the dispatch and the fold has nothing left
/// to settle it against. It is the cheapest possible evasion — do nothing —
/// and it is caught by pricing the turn where the history runs out rather than
/// only where the next decision arrives.
#[test]
fn a_history_that_ends_inside_a_turn_still_prices_it() {
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, 2);
    history.submit(id, system(SystemClass::Bulk, 1));
    history.submit(id, system(SystemClass::Bulk, 1));

    history.armed(1, 1, vec![id]);
    // The last decision in the record, and nothing follows to settle it.
    history.dispatched(1, 1, id, 2, 2);

    assert_eq!(
        history
            .audit(bounds())
            .expect_err("the end of the record settles the turn it ends inside"),
        SchedulingViolation::DispatchLeftWorkUnserviced {
            pass: pass(1),
            group: id,
            owed: 2,
            serviced: 0,
        }
    );

    // The control: the same turn with its work recorded, still with nothing
    // after it, is accepted and priced at what that work was worth.
    let mut honest = History::new();
    honest.open_group(id, 2);
    honest.submit(id, system(SystemClass::Bulk, 1));
    honest.submit(id, system(SystemClass::Bulk, 1));
    honest.armed(1, 1, vec![id]);
    honest.dispatched(1, 1, id, 2, 2);
    honest.serviced(1, id, 1);
    honest.serviced(1, id, 2);

    let replay = honest.replay(bounds());
    let report = replay
        .fairness
        .expect("a turn that did its work is priced by it, recorded last or not");
    assert_eq!(report.serviced, 2);
    assert!(
        replay
            .view
            .groups
            .iter()
            .any(|view| view.group == id && view.servicing),
        "and the occupancy it bought runs to tick three"
    );
}

/// CONTRACT.md: "An occupancy ends at `due`, and ends only there."
///
/// It ends there whether or not anything says so, and this history is the proof
/// rather than the counterexample: six dispatches, six turns that did their
/// work, and no `WorkerReleased` anywhere. The audit is green because the
/// release is a *report* of an instant the audit already computed, never a
/// grant of one — which is the entire content of deriving the occupancy.
///
/// What needed retiring was the reading of that sentence as "a release must be
/// recorded". Nothing required one, and nothing now pretends to. What the
/// sentence does forbid is dispatching before `due`, and the second half shows
/// that caught with no release either.
#[test]
fn an_occupancy_ends_at_its_deadline_with_no_release_ever_recorded() {
    let id = group(0);
    let mut history = History::new();
    history.open_group(id, 1);
    for _ in 0..6 {
        history.submit(id, system(SystemClass::Bulk, 2));
    }
    // Turns of one item costing two ticks: dispatch at 1, due at 3; dispatch
    // again at 3, due at 5; and so on. No `WorkerReleased` anywhere.
    for index in 0..6 {
        let pass = index + 1;
        let at = 1 + index * 2;
        history.armed(pass, at, vec![id]);
        history.dispatched(pass, at, id, 1, 2);
        history.serviced(pass, id, index + 1);
        history.retired(pass, at);
    }

    let mut oracle = ReferenceScheduler::new(bounds());
    oracle.observe_all(history.events().iter().cloned());
    let report = oracle
        .audit()
        .expect("the derivation needs no release to time an occupancy");
    println!(
        "six dispatches, zero releases: passes_completed={} serviced={} widest_gap={}",
        report.passes_completed, report.serviced, report.widest_gap
    );
    assert_eq!(report.passes_completed, 6);
    assert_eq!(
        report.serviced, 6,
        "and the floor that says the run proved something is the work it moved"
    );

    // Dispatching a tick early is what the sentence actually forbids, and it is
    // caught without a release either: the group is still holding a worker.
    let mut early = History::new();
    early.open_group(id, 1);
    for _ in 0..2 {
        early.submit(id, system(SystemClass::Bulk, 2));
    }
    early.armed(1, 1, vec![id]);
    early.dispatched(1, 1, id, 1, 2);
    early.serviced(1, id, 1);
    early.retired(1, 1);
    early.armed(2, 2, vec![id]);

    assert_eq!(
        early
            .audit(bounds())
            .expect_err("an occupancy due at three does not end at two"),
        SchedulingViolation::PlanIncludedUnreadyGroup {
            pass: pass(2),
            group: id,
        }
    );
}

/// CONTRACT.md: external availability "is sticky: a stalled group stays out of
/// the ready set until it is reported available again, however much work it
/// accumulates."
///
/// A stall held unbroken is therefore the one legitimate way a group holding
/// work receives nothing across a long run, and the audit says so rather than
/// inventing a fault. That is not a hole; it is the external input doing its
/// job. What makes it safe is the next test: a stall that *moves* buys nothing.
#[test]
fn a_stall_held_unbroken_is_the_one_legitimate_way_a_group_receives_nothing() {
    const PASSES: u64 = 20;
    let favored = group(0);
    let starved = group(1);

    let mut history = History::new();
    history.open_group(favored, 1);
    history.open_group(starved, 1);
    for _ in 0..PASSES {
        history.submit(favored, system(SystemClass::Bulk, 1));
    }
    for _ in 0..PASSES {
        history.submit(starved, system(SystemClass::Bulk, 1));
    }
    history.reported(1, starved, GroupAvailability::Stalled);

    for index in 0..PASSES {
        let pass = index + 1;
        let at = index + 1;
        if index > 0 {
            history.released(at, favored);
        }
        history.armed(pass, at, vec![favored]);
        history.dispatched(pass, at, favored, 1, 1);
        // The favoured group's backlog was submitted first, so its work
        // identifiers run from one in pass order.
        history.serviced(pass, favored, pass);
        history.retired(pass, at);
    }

    let replay = history.replay(bounds());
    let report = replay
        .fairness
        .expect("an unbroken external stall excuses every plan it covers");
    let view = replay
        .view
        .groups
        .iter()
        .find(|view| view.group == starved)
        .copied()
        .expect("the starved group exists");
    println!(
        "unbroken stall: passes_completed={} serviced={} widest_gap={}; \
         stalled group holds {} items, stalled={}",
        report.passes_completed, report.serviced, report.widest_gap, view.queued, view.stalled
    );
    assert_eq!(report.widest_gap, 0);
    assert!(
        view.stalled,
        "the group the audit excused is the one the history still says is stalled"
    );
    assert_eq!(
        u64::from(view.queued),
        PASSES,
        "it kept every item, which is what sticky means"
    );
    // And the run is not vacuous: the other group's work is what the service
    // floor counts, so this green audit proved something about a live host.
    assert_eq!(report.serviced, PASSES);
}

/// CONTRACT.md, after this generation: "A stall excuses omission only while it
/// is unbroken."
///
/// Readiness is sampled at each arming, and the stall bit used to be free to
/// move between them. A group reported stalled immediately before every arm and
/// available immediately after was never *sampled* as ready, so it was never
/// owed a plan and never accrued gap — while the audit's own final view said it
/// was available and holding a backlog. That was the third freedom over
/// readiness, and it is the one the "exactly two freedoms" sentence did not
/// enumerate.
///
/// Breaking the stall at an instant when a plan could have named the group now
/// makes it owed the next plan, whatever its availability by the time that plan
/// is armed. The debt is settled by a plan naming it, and by nothing else.
#[test]
fn flickering_the_stall_bit_across_arm_instants_is_owed_every_plan_it_denied() {
    const PASSES: u64 = 20;
    let favored = group(0);
    let starved = group(1);

    let mut history = History::new();
    history.open_group(favored, 1);
    history.open_group(starved, 1);
    for _ in 0..PASSES {
        history.submit(favored, system(SystemClass::Bulk, 1));
    }
    history.submit(starved, system(SystemClass::Bulk, 1));

    for index in 0..PASSES {
        let pass = index + 1;
        let at = index + 1;
        if index > 0 {
            history.released(at, favored);
        }
        // Stalled for the instant the plan is taken...
        history.reported(at, starved, GroupAvailability::Stalled);
        history.armed(pass, at, vec![favored]);
        history.dispatched(pass, at, favored, 1, 1);
        // The favoured group's backlog was submitted first, so its work
        // identifiers run from one in pass order.
        history.serviced(pass, favored, pass);
        history.retired(pass, at);
        // ...and available again the moment the plan is closed, at an instant
        // when a plan could have named it and none did.
        history.reported(at, starved, GroupAvailability::Available);
    }

    let violation = history
        .audit(bounds())
        .expect_err("a stall that moves excuses nothing");
    println!("stall flickered around every arming: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: pass(2),
            denied_passes: u32::try_from(PASSES - 1).expect("twenty passes fit in u32"),
        },
        "the debt begins at the first plan armed after the stall was broken"
    );
}

/// `ServiceCost` is bounded only by `u32`, and nothing in `SchedulerConfig`
/// bounds it at all. One admitted item of cost `u32::MAX` on a one-worker host
/// takes the only worker for `4_294_967_295` ticks, and this history is entirely
/// legal: nothing lies, the one turn services exactly what it was offered, and
/// the audit is right to accept it.
///
/// What it is *not* is a run that proved anything, and the mitigation the
/// previous generation recommended for the vacuity hole — a floor on
/// `passes_completed` — certifies it anyway, because an empty plan names
/// exactly the ready set when nothing is ready and arming is free. So the floor
/// moved to the work the history did. That is the choice this crate makes over
/// bounding `ServiceCost`: a numeric ceiling on cost only rescales the wedge,
/// and it would make "a group with slow storage" unrepresentable past a number
/// this contract has no basis to pick.
#[test]
fn the_vacuity_floor_is_on_service_because_arming_is_free() {
    let id = group(0);
    let single_worker = config(2, 1, 2, 8, 16);
    let mut history = History::new();
    history.open_group(id, 1);
    // Control sorts first, so it is the head of the queue whatever follows it.
    history.submit(id, system(SystemClass::Control, u32::MAX));
    for _ in 0..6 {
        history.submit(id, system(SystemClass::Bulk, 1));
    }

    history.armed(1, 1, vec![id]);
    history.dispatched(1, 1, id, 1, u64::from(u32::MAX));
    history.serviced(1, id, 1);
    history.retired(1, 1);
    // Empty plans forever after: nothing is ready, so nothing is owed.
    for index in 1..40 {
        let pass = index + 1;
        let at = index + 1;
        history.armed(pass, at, vec![]);
        history.retired(pass, at);
    }

    let replay = history.replay(single_worker);
    let report = replay.fairness.expect("no recorded decision broke a rule");
    let view = replay
        .view
        .groups
        .iter()
        .find(|view| view.group == id)
        .copied()
        .expect("the group exists");
    println!(
        "audit: passes_completed={} serviced={} widest_gap={}; group holds {} \
         items and is servicing={} until tick {}",
        report.passes_completed,
        report.serviced,
        report.widest_gap,
        view.queued,
        view.servicing,
        1 + u64::from(u32::MAX)
    );
    assert!(
        report.passes_completed >= 8,
        "the floor on arming is satisfied by a host doing nothing"
    );
    assert_eq!(
        report.serviced,
        1,
        "and the floor on service is what refuses it: one item moved across {} \
         completed passes, with the only worker booked out to tick {}",
        report.passes_completed,
        1 + u64::from(u32::MAX)
    );
    assert_eq!(view.queued, 6, "six items are still waiting on that worker");
}

fn bounds() -> rafter_reference_sharded_counter::SchedulerConfig {
    config(4, 2, 2, 64, 512)
}
