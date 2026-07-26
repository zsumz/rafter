//! Red-team probes against the derived-occupancy rule and the freedoms it
//! leaves the scheduler.
//!
//! Each test states what CONTRACT.md promises and lets the audit answer. Most
//! were written as a *hole* — a schedule the contract says it forbids and the
//! audit accepted — and each of those now names the fault that catches it. The
//! rest are the acceptance cases those holes are bounded by: a schedule the
//! contract permits, which the audit must go on accepting for the faults above
//! to mean anything narrower than "refuses everything".
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

/// The dual of the test above, reached from the other side of the same pass.
///
/// `flickering_...` closes the window *between* passes. This closes the window
/// *inside* one. The rule used to gate the debt on `self.open.is_none()`,
/// justified by "a stall raised while a pass was already open still excuses,
/// because no plan could have been armed to name the group" — a claim about the
/// instant a stall is *raised*, applied to the instant availability is
/// *restored*. The scheduler authors both the availability reports and the pass
/// boundaries, so it chose the window, and the audit called this clean:
///
/// ```text
/// passes_completed = 20   opportunities = 20   serviced = 20 host-wide, 0 for the starved group
/// widest_gap       = 0    starved group: queued 1, stalled true, servicing false
/// ```
///
/// The `serviced > 0` floor does not catch it either: twenty items moved.
#[test]
fn availability_restored_inside_an_open_pass_is_owed_every_plan_it_denied() {
    const PASSES: u64 = 20;
    // Each pass occupies ten ticks: armed at `b`, retired at `b + 9`.
    const SPAN: u64 = 10;
    let favored = group(0);
    let starved = group(1);

    let mut history = History::new();
    history.open_group(favored, 1);
    history.open_group(starved, 1);
    for _ in 0..PASSES {
        history.submit(
            favored,
            system(SystemClass::Bulk, u32::try_from(SPAN).expect("fits")),
        );
    }
    history.submit(starved, system(SystemClass::Bulk, 1));
    // Stalled before the first plan is armed, so nothing is owed at pass one.
    history.reported(1, starved, GroupAvailability::Stalled);

    for index in 0..PASSES {
        let pass = index + 1;
        let base = index * SPAN + 1;
        if index > 0 {
            // The favoured turn cost exactly one span, so its worker is due
            // back at the instant the next plan is armed.
            history.released(base, favored);
        }
        history.armed(pass, base, vec![favored]);
        history.dispatched(pass, base, favored, 1, SPAN);
        // The favoured group's backlog was submitted first, so its work
        // identifiers run from one in pass order.
        history.serviced(pass, favored, pass);

        // Available for the whole interior of the open pass. The plan has
        // nothing left pending, so the pass is being *held* open rather than
        // traversed, and holding it open buys the omission nothing.
        history.reported(base + 1, starved, GroupAvailability::Available);
        // Stalled again one tick before the pass retires, so the next arming
        // samples a stalled group — and is owed it anyway.
        history.reported(base + SPAN - 1, starved, GroupAvailability::Stalled);
        history.retired(pass, base + SPAN - 1);
    }

    let violation = history
        .audit(bounds())
        .expect_err("a stall broken inside a held-open pass excuses nothing");
    println!("stall broken inside every open pass: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: pass(2),
            denied_passes: u32::try_from(PASSES - 1).expect("twenty passes fit in u32"),
        },
        "the debt begins at the first plan armed after the stall was first broken"
    );
}

/// The first boundary of the rule above: a restoration that lands while the
/// group is holding a worker opens no debt, and can deny it one window and no
/// more.
///
/// The qualification is kept because the worker *was* what kept the group out,
/// and that worker's price is work the group actually did. What makes the
/// residual bounded is that the dodge cannot be repeated: to be occupied again
/// the group must be dispatched, which takes a plan naming it. The second half
/// of this test is that follow-on, and it is owed.
#[test]
fn restoring_availability_inside_an_occupancy_denies_one_window_and_no_more() {
    const PASSES: u64 = 12;
    let id = group(0);

    // A turn costing six ticks, taken at tick one and due back at tick seven.
    let prefix = |history: &mut History| {
        history.open_group(id, 1);
        history.submit(id, system(SystemClass::Bulk, 6));
        history.submit(id, system(SystemClass::Bulk, 1));
        history.armed(1, 1, vec![id]);
        history.dispatched(1, 1, id, 1, 6);
        history.serviced(1, id, 1);
        history.retired(1, 1);
    };

    let mut excused = History::new();
    prefix(&mut excused);
    // The whole oscillation lands inside the occupancy.
    excused.reported(2, id, GroupAvailability::Stalled);
    excused.reported(3, id, GroupAvailability::Available);
    excused.reported(4, id, GroupAvailability::Stalled);
    excused.released(7, id);
    for index in 1..PASSES {
        excused.armed(index + 1, 7 + index, vec![]);
        excused.retired(index + 1, 7 + index);
    }

    let report = excused
        .audit(bounds())
        .expect("a stall raised at four and never broken again excuses what follows it");
    println!("restored inside an occupancy: {report:?}");
    assert_eq!(report.widest_gap, 0);
    assert_eq!(report.serviced, 1, "and the run is not vacuous");

    // The same history with the oscillation moved one tick past the deadline.
    // Nothing else differs, and the group is owed every plan it is left out of.
    let mut owed = History::new();
    prefix(&mut owed);
    owed.reported(2, id, GroupAvailability::Stalled);
    owed.released(7, id);
    owed.reported(8, id, GroupAvailability::Available);
    owed.reported(9, id, GroupAvailability::Stalled);
    for index in 1..PASSES {
        owed.armed(index + 1, 9 + index, vec![]);
        owed.retired(index + 1, 9 + index);
    }

    assert_eq!(
        owed.audit(bounds())
            .expect_err("the worker was gone, so it was the stall keeping the group out"),
        SchedulingViolation::OpportunityGap {
            group: id,
            from_pass: pass(2),
            denied_passes: u32::try_from(PASSES - 1).expect("twelve passes fit in u32"),
        }
    );
}

/// The second boundary: a restoration that lands while the group holds no work
/// opens no debt, and can deny it one window and no more.
///
/// A group with an empty queue was not being kept out by its stall, so
/// returning it to availability grants it nothing it did not have. The dodge is
/// bounded for the same reason as the one above: to empty the queue again the
/// group has to be serviced.
#[test]
fn restoring_availability_with_an_empty_queue_denies_one_window_and_no_more() {
    const PASSES: u64 = 12;
    let id = group(0);

    let mut excused = History::new();
    excused.open_group(id, 1);
    // Stalled and restored while it holds nothing at all.
    excused.reported(1, id, GroupAvailability::Stalled);
    excused.reported(2, id, GroupAvailability::Available);
    excused.reported(3, id, GroupAvailability::Stalled);
    excused.submit(id, system(SystemClass::Bulk, 1));
    for index in 0..PASSES {
        excused.armed(index + 1, 4 + index, vec![]);
        excused.retired(index + 1, 4 + index);
    }

    let replay = excused.replay(bounds());
    let report = replay
        .fairness
        .expect("a stall raised at three over an empty queue excuses what follows it");
    println!("restored over an empty queue: {report:?}");
    assert_eq!(report.widest_gap, 0);
    // Without this the acceptance would be uninteresting: a group holding
    // nothing is owed nothing for reasons that have no bearing on the stall.
    assert!(
        replay
            .view
            .groups
            .iter()
            .any(|view| view.group == id && view.queued == 1 && view.stalled),
        "the group the audit excused holds work and is still stalled"
    );

    // The same oscillation once the group holds work is owed every plan.
    let mut owed = History::new();
    owed.open_group(id, 1);
    owed.reported(1, id, GroupAvailability::Stalled);
    owed.submit(id, system(SystemClass::Bulk, 1));
    owed.reported(2, id, GroupAvailability::Available);
    owed.reported(3, id, GroupAvailability::Stalled);
    for index in 0..PASSES {
        owed.armed(index + 1, 4 + index, vec![]);
        owed.retired(index + 1, 4 + index);
    }

    assert_eq!(
        owed.audit(bounds())
            .expect_err("with work queued, the stall is what is keeping the group out"),
        SchedulingViolation::OpportunityGap {
            group: id,
            from_pass: pass(1),
            denied_passes: u32::try_from(PASSES).expect("twelve passes fit in u32"),
        }
    );
}

/// The third boundary, and the one that is *not* bounded to a single window.
///
/// A pass with more entries still pending than the pool has free workers cannot
/// be finished, cannot be retired, and therefore stands between the scheduler
/// and the next arming. That is the required bound's own precondition — "absent
/// global resource exhaustion" — so availability that appears only there is
/// excused, and a host willing to keep one entry pending behind a long turn can
/// arrange that every time.
///
/// This test asserts the acceptance rather than a fault, because the acceptance
/// is the honest answer: at the level of recorded decisions this host is not
/// distinguishable from one that is simply busy, and the entry it holds pending
/// is a ready group it must still offer. What refuses to certify it is the
/// service floor, which is why the floor is reported beside the gap.
#[test]
fn a_pass_the_host_cannot_finish_excuses_the_availability_it_spans() {
    const PASSES: u64 = 12;
    // Armed at `b`, retired at `b + 3`, next plan armed at `b + 4`.
    const SPAN: u64 = 4;
    let single_worker = config(4, 1, 2, 64, 512);
    let favored = group(0);
    let filler = group(1);
    let starved = group(2);

    let mut history = History::new();
    history.open_group(favored, 1);
    history.open_group(filler, 1);
    history.open_group(starved, 1);
    for _ in 0..PASSES {
        history.submit(favored, system(SystemClass::Bulk, 3));
    }
    for _ in 0..PASSES {
        history.submit(filler, system(SystemClass::Bulk, 1));
    }
    history.submit(starved, system(SystemClass::Bulk, 1));
    history.reported(1, starved, GroupAvailability::Stalled);

    for index in 0..PASSES {
        let pass = index + 1;
        let base = index * SPAN + 1;
        if index > 0 {
            history.released(base, filler);
        }
        history.armed(pass, base, vec![favored, filler]);
        // The only worker goes to the favoured group for three ticks, so the
        // plan's remaining entry cannot be offered and the pass cannot retire.
        history.dispatched(pass, base, favored, 1, 3);
        history.serviced(pass, favored, pass);
        // Available for exactly the span the host could not act over.
        history.reported(base + 1, starved, GroupAvailability::Available);
        history.reported(base + 2, starved, GroupAvailability::Stalled);
        history.released(base + 3, favored);
        history.dispatched(pass, base + 3, filler, 1, 1);
        history.serviced(pass, filler, PASSES + pass);
        history.retired(pass, base + 3);
    }

    let replay = history.replay(single_worker);
    let report = replay
        .fairness
        .expect("a pass the host cannot finish excuses the availability it spans");
    let view = replay
        .view
        .groups
        .iter()
        .find(|view| view.group == starved)
        .copied()
        .expect("the starved group exists");
    println!(
        "unfinishable pass: passes_completed={} serviced={} widest_gap={}; \
         starved group holds {} items and received nothing",
        report.passes_completed, report.serviced, report.widest_gap, view.queued
    );
    assert_eq!(report.widest_gap, 0, "this is the limit, stated not hidden");
    assert_eq!(view.queued, 1, "and the starved group still holds its item");
    assert_eq!(
        report.serviced,
        2 * PASSES,
        "the host was busy for every tick it was excused over"
    );
}

/// The other side of that boundary, and what keeps it from swallowing the rule
/// it qualifies: a pass with an entry still pending and a worker free to offer
/// it with is not a pass the host *cannot* finish. It is one it chose not to.
///
/// Without the free-worker term this is the whole evasion back again, one
/// qualification later: plan a decoy beside the favoured group, leave it
/// pending across every availability window, and every `Available` report lands
/// inside an "unfinishable" pass while a worker sits idle.
#[test]
fn a_pass_held_open_with_a_worker_to_spare_excuses_nothing() {
    const PASSES: u64 = 6;
    // Armed at `b`, retired at `b + 3`, next plan armed at `b + 4`.
    const SPAN: u64 = 4;
    let favored = group(0);
    let decoy = group(1);
    let starved = group(2);

    let mut history = History::new();
    history.open_group(favored, 1);
    history.open_group(decoy, 1);
    history.open_group(starved, 1);
    for _ in 0..PASSES {
        history.submit(favored, system(SystemClass::Bulk, 3));
    }
    for _ in 0..PASSES {
        history.submit(decoy, system(SystemClass::Bulk, 1));
    }
    history.submit(starved, system(SystemClass::Bulk, 1));
    history.reported(1, starved, GroupAvailability::Stalled);

    for index in 0..PASSES {
        let pass = index + 1;
        let base = index * SPAN + 1;
        if index > 0 {
            history.released(base, decoy);
        }
        history.armed(pass, base, vec![favored, decoy]);
        history.dispatched(pass, base, favored, 1, 3);
        history.serviced(pass, favored, pass);
        // One entry pending out of a pool of two, so the second worker is free
        // the whole time the decoy is held back.
        history.reported(base + 1, starved, GroupAvailability::Available);
        history.reported(base + 2, starved, GroupAvailability::Stalled);
        history.released(base + 3, favored);
        history.dispatched(pass, base + 3, decoy, 1, 1);
        history.serviced(pass, decoy, PASSES + pass);
        history.retired(pass, base + 3);
    }

    let violation = history
        .audit(bounds())
        .expect_err("a spare worker is not global resource exhaustion");
    println!("pass held open with a worker to spare: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: pass(2),
            denied_passes: u32::try_from(PASSES - 1).expect("six passes fit in u32"),
        }
    );
}

/// The fourth boundary: the debt is opened by a *transition*, so an `Available`
/// report naming a group that was not stalled opens nothing.
///
/// A group that is already available is owed every plan by ordinary readiness,
/// so the transition requirement takes nothing away while it holds. What it
/// does deny is the window before a group's *first* stall — and, like the
/// occupancy and empty-queue boundaries, only once: every later restoration is
/// a transition out of a stall, and opens the debt.
#[test]
fn availability_reported_for_a_group_that_was_never_stalled_opens_no_debt() {
    const PASSES: u64 = 12;
    let id = group(0);

    let mut excused = History::new();
    excused.open_group(id, 1);
    excused.submit(id, system(SystemClass::Bulk, 1));
    // Told it is available when it already was, then stalled before any plan is
    // armed. The stall that follows is never broken, so it excuses everything.
    excused.reported(1, id, GroupAvailability::Available);
    excused.reported(2, id, GroupAvailability::Stalled);
    for index in 0..PASSES {
        excused.armed(index + 1, 3 + index, vec![]);
        excused.retired(index + 1, 3 + index);
    }

    let replay = excused.replay(bounds());
    let report = replay
        .fairness
        .expect("a group that was never stalled is owed by readiness, not by a debt");
    println!("redundant availability report: {report:?}");
    assert_eq!(report.widest_gap, 0);
    assert!(
        replay
            .view
            .groups
            .iter()
            .any(|view| view.group == id && view.queued == 1 && view.stalled),
        "the group the audit excused holds work and is still stalled"
    );

    // And the second oscillation, which *is* a transition, is owed.
    let mut owed = excused.clone();
    owed.reported(3 + PASSES, id, GroupAvailability::Available);
    owed.reported(4 + PASSES, id, GroupAvailability::Stalled);
    for index in 0..PASSES {
        owed.armed(PASSES + index + 1, 5 + PASSES + index, vec![]);
        owed.retired(PASSES + index + 1, 5 + PASSES + index);
    }

    assert_eq!(
        owed.audit(bounds())
            .expect_err("the second report broke a stall, and that is the debt"),
        SchedulingViolation::OpportunityGap {
            group: id,
            from_pass: pass(PASSES + 1),
            denied_passes: u32::try_from(PASSES).expect("twelve passes fit in u32"),
        }
    );
}

/// "The debt is discharged by a plan naming the group, and by nothing else" is
/// a claim about every other event in the history, and this is the one that
/// tries hardest to be an exception: a second `Available` report, saying what
/// the first already said.
///
/// It opens no debt of its own — it is not a transition out of a stall — and
/// the mistake is to conclude from that that it *closes* one. Accumulating the
/// qualification rather than assigning it is the whole difference, and without
/// this test nothing in the suite could tell the two apart.
#[test]
fn a_debt_already_open_is_discharged_by_a_plan_and_by_nothing_else() {
    const PASSES: u64 = 12;
    let id = group(0);

    let mut history = History::new();
    history.open_group(id, 1);
    history.submit(id, system(SystemClass::Bulk, 1));
    history.reported(1, id, GroupAvailability::Stalled);
    // The transition that opens the debt.
    history.reported(2, id, GroupAvailability::Available);
    // The same thing said twice. Nothing about the group changed, so nothing
    // about what it is owed may change either.
    history.reported(3, id, GroupAvailability::Available);
    history.reported(4, id, GroupAvailability::Stalled);
    for index in 0..PASSES {
        history.armed(index + 1, 5 + index, vec![]);
        history.retired(index + 1, 5 + index);
    }

    let violation = history
        .audit(bounds())
        .expect_err("a repeated availability report is not a discharge");
    println!("debt survives a repeated report: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: id,
            from_pass: pass(1),
            denied_passes: u32::try_from(PASSES).expect("twelve passes fit in u32"),
        }
    );

    // The control: the same repeated report, and a first plan that names the
    // group. That discharges the debt, and the unbroken stall that follows
    // excuses every plan after it.
    let mut discharged = History::new();
    discharged.open_group(id, 1);
    discharged.submit(id, system(SystemClass::Bulk, 1));
    discharged.submit(id, system(SystemClass::Bulk, 1));
    discharged.reported(1, id, GroupAvailability::Stalled);
    discharged.reported(2, id, GroupAvailability::Available);
    discharged.reported(3, id, GroupAvailability::Available);
    discharged.armed(1, 4, vec![id]);
    discharged.dispatched(1, 4, id, 1, 1);
    discharged.serviced(1, id, 1);
    discharged.retired(1, 4);
    discharged.released(5, id);
    discharged.reported(5, id, GroupAvailability::Stalled);
    for index in 1..PASSES {
        discharged.armed(index + 1, 5 + index, vec![]);
        discharged.retired(index + 1, 5 + index);
    }

    let report = discharged
        .audit(bounds())
        .expect("a plan naming the group is what settles it");
    assert_eq!(report.widest_gap, 0);
    assert_eq!(report.serviced, 1, "and the discharge was a real turn");
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
