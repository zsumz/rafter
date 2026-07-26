//! A deliberately-violating scheduler for every rule the audit enforces.
//!
//! This suite exists because of what the last three generations had in common:
//! prose more confident than the code had earned. The audit grew a rule, the
//! rule grew a paragraph, and the paragraph was checked by a workload that
//! could not break it — because [`ManagedScheduler`] always services what it
//! dispatches, always releases what it takes, and always plans what is ready.
//!
//! **No history the `Recorder` can produce distinguishes a rule the audit
//! checks from one it ignores.** That is not a gap in the workloads; it is a
//! property of driving the audit with a correct scheduler, and no amount of
//! scale fixes it. A check whose only positive evidence is a green run against
//! an honest party has been tested against its own vacuity.
//!
//! So every rule has a control here, and the mapping is checked by the compiler
//! rather than by a reader.
//!
//! ## A rule is a site, and exhaustive matching closes variants
//!
//! The previous generation of this file matched [`SchedulingViolation`]
//! exhaustively and concluded that no rule could exist without a control. That
//! conclusion is one scope wider than the mechanism reaches. The fold decides
//! twenty-nine rules at twenty-nine places, and reports them through
//! twenty-four variants: five checks report a variant another check already
//! reports. Adding a rule that reuses a variant added no match arm, broke no
//! build, and needed no control — and three rules had arrived that way and were
//! never exercised at all:
//!
//! - the retire-side pass-ordering checks, both reporting `PassOutOfOrder`,
//!   which the arm-side check also reports;
//! - the retire-side tick-reuse check, reporting `PassBoundaryReused`, which
//!   the arm-side check also reports; and
//! - the release-side held-past-cost check, reporting `WorkerHeldPastCost`,
//!   which the deadline sweep also reports.
//!
//! [`control_for`] therefore matches [`FaultSite`] — one variant per place the
//! fold decides a rule — with no catch-all. A rule added by reusing a variant
//! still needs a site, a site still needs an arm here, and an arm still needs
//! either a scheduler or an argument. Two further mechanisms close the two
//! directions a match cannot: `FaultSite::ALL` is derived from an exhaustive
//! chain and const-checked against its own declaration order, so a site the
//! chain does not reach fails to compile; and
//! `oracle::tests::every_fault_site_is_raised_by_exactly_one_check` scans the
//! fold's source, because two checks passing *one* site is a false statement no
//! type system here can catch.
//!
//! ## What none of this proves
//!
//! It proves that each rule the audit names fires on a history that breaks it,
//! and that the near-miss beside it is accepted. **It does not prove the rule
//! set is complete**, and that is not a small remainder: the starvation this
//! generation closed —
//! `redteam_occupancy::availability_restored_inside_an_open_pass_is_owed_every_plan_it_denied`
//! — broke no rule the audit named. Twenty passes, twenty items serviced, a
//! group available for eight ticks in ten holding a backlog throughout, and
//! every control in this file green. A total mapping from rules to controls
//! says nothing about a rule that was never written.
//!
//! ## Each control is a pair
//!
//! `RedTeam::run(cheat)` takes one bad decision; `RedTeam::control(cheat)`
//! takes that same decision correctly and changes nothing else. Both are
//! asserted, because a negative control on its own proves that some history
//! somewhere fails, and the pair proves that the rule under test is the one
//! doing the work.
//!
//! One rule has no fault of its own and so no site below: a stall that is
//! broken while the host could have reached an arming no longer excuses the
//! omissions it covers, and it reports the ordinary
//! [`SchedulingViolation::OpportunityGap`] when it does. Its pairs are in
//! `redteam_occupancy.rs`, which is the same two-history discipline with the
//! argument written out.
//!
//! [`ManagedScheduler`]: rafter_reference_sharded_counter::ManagedScheduler

mod support;

use rafter_reference_sharded_counter::{FaultSite, HistoryEvent, SchedulingViolation, SystemClass};
use support::{config, group, pass, system, Cheat, History, RedTeam};

/// Whether a rule has a red-team scheduler, and which.
enum Control {
    /// The cheat that must trip this check, and only this check.
    RedTeam(Cheat),
    /// No history can trip it, with the argument for why.
    ///
    /// CONTRACT.md's standard for an unreachable answer is stated twice, about
    /// the absent session-table capacity refusal and the absent
    /// sequence-exhaustion refusal: "a refusal for a state that cannot be
    /// reached is a promise about behavior no test could observe." A site
    /// landing here has to survive that argument rather than sit behind a
    /// comparison nothing satisfies.
    Unreachable(&'static str),
}

/// Maps every place the audit decides a rule to the scheduler that provokes it.
///
/// The match is exhaustive and has no catch-all, on purpose. A rule added to
/// the fold without a red-team scheduler beside it does not silently go
/// unchecked — it fails to compile, which is the difference between a claim
/// this suite backs and a claim it merely states. Indexing by site rather than
/// by violation is what extends that from "every answer the audit can give" to
/// "every rule the audit decides".
fn control_for(site: FaultSite) -> Control {
    match site {
        FaultSite::ClockWalkedBackwards => Control::RedTeam(Cheat::WalkTheClockBackwards),
        FaultSite::OccupancyOutlivedItsCost => Control::RedTeam(Cheat::HoldAWorkerPastItsCost),
        FaultSite::ReleaseWithoutOccupancy => Control::RedTeam(Cheat::ReleaseAWorkerNobodyTook),
        FaultSite::ReleaseBeforeDue => Control::RedTeam(Cheat::ReleaseAWorkerBeforeItsCostIsPaid),
        FaultSite::ReleaseAfterDue => Control::RedTeam(Cheat::ReleaseAWorkerAfterItsCostIsPaid),
        FaultSite::ArmedOverAnOpenPass => Control::RedTeam(Cheat::ArmOverAnOpenPlan),
        FaultSite::ArmedOutOfOrder => Control::RedTeam(Cheat::SkipAPassIndex),
        FaultSite::ArmedTickReused => Control::RedTeam(Cheat::ArmTwoPlansInOneTick),
        FaultSite::PlanRepeatedAGroup => Control::RedTeam(Cheat::NameAGroupTwiceInOnePlan),
        FaultSite::PlanNamedAnUnreadyGroup => Control::RedTeam(Cheat::PlanAGroupThatIsNotReady),
        FaultSite::OfferOutsideThePlan => Control::RedTeam(Cheat::OfferAGroupThePlanDidNotName),
        FaultSite::OfferedTwiceInOnePass => Control::RedTeam(Cheat::OfferAGroupTwiceInOnePass),
        // Defensive, and shadowed by a rule that runs first. Reaching it needs
        // a turn offered to a group that was never created; the plan that named
        // that group was judged at its arming, where an uncreated group is not
        // ready, so `PlanNamedAnUnreadyGroup` is always the fault a history
        // like that reports. `an_offer_to_a_group_that_never_existed_is_caught_
        // when_the_plan_named_it` is the history, and it is asserted rather
        // than asserted-about: the argument is only as good as the check.
        FaultSite::OfferedAnUnknownGroup => Control::Unreachable(
            "an offer reaches this only for a group the open plan named, and a plan naming \
             a group that was never created is reported at the arming instead",
        ),
        FaultSite::SkippedWhileAvailable => Control::RedTeam(Cheat::SkipAGroupThatIsAvailable),
        FaultSite::DispatchedWhileUnready => Control::RedTeam(Cheat::DispatchAGroupThatIsNotReady),
        FaultSite::DispatchOverQuota => Control::RedTeam(Cheat::ServiceMoreThanTheQuota),
        FaultSite::DispatchOverWorkerPool => Control::RedTeam(Cheat::TakeMoreWorkersThanThePoolHas),
        FaultSite::TurnLeftWorkQueued => Control::RedTeam(Cheat::LeaveOwedWorkUnserviced),
        FaultSite::TurnMiscountedItsWork => Control::RedTeam(Cheat::MiscountTheWorkATurnDid),
        FaultSite::TurnMispricedItsWork => Control::RedTeam(Cheat::MisreportTheTurnsCost),
        FaultSite::ServiceOutsideATurn => Control::RedTeam(Cheat::ServiceWorkOutsideAnyDispatch),
        FaultSite::ServicePastTheTurnsWork => Control::RedTeam(Cheat::ServicePastTheEndOfATurn),
        FaultSite::ServiceOutOfOrder => Control::RedTeam(Cheat::ServiceOutOfArrivalOrder),
        FaultSite::RetiredTickReused => Control::RedTeam(Cheat::RetireTwoPlansInOneTick),
        FaultSite::RetiredWithNoPassOpen => Control::RedTeam(Cheat::RetireAPassWithNoPlanOpen),
        FaultSite::RetiredADifferentPass => Control::RedTeam(Cheat::RetireAPassOtherThanTheOpenOne),
        FaultSite::RetiredWithATurnOwing => Control::RedTeam(Cheat::RetireAPlanWithATurnOwing),
        // The one rule with no red-team scheduler, and the argument for why it
        // keeps its place anyway. `admitted = serviced + failed + queued` is not
        // a property of a history; it is arithmetic the fold performs over its
        // own four counters, each of which it moves itself and in step. No
        // sequence of recorded decisions can put them out of step, because a
        // service the fold cannot locate in a queue changes none of them and a
        // removal is refused while a queue is not empty.
        //
        // That makes it a self-check on the fold rather than a judgement of a
        // scheduler, and this comment is the honest form of that: it is not
        // being claimed as a rule the audit enforces. CONTRACT.md's invariant
        // 10 is asserted where it can be — over the model's summary, in
        // `model_contract::work_is_conserved_across_a_poisoned_drain_a_removal_and_a_reopening`.
        FaultSite::WorkUnaccountedFor => Control::Unreachable(
            "the fold moves admitted, serviced, failed, and queued together, so no \
             recorded history can separate them; the law is asserted over the model instead",
        ),
        FaultSite::WidestOpportunityGap => Control::RedTeam(Cheat::StarveAReadyGroup),
        FaultSite::EndOfSites => {
            Control::Unreachable("the end marker is not a site and no check raises it")
        }
    }
}

/// Every cheat trips exactly the check it was written to trip, produces exactly
/// the fault it was written to produce, and the same history without that one
/// decision is accepted.
///
/// The pair is the assertion. Half of it — the failing half — is what the
/// previous generations had, and it is why "the audit derives the occupancy"
/// survived a suite in which the audit derived it from work that was never
/// done: the derivation was never asked a question whose answer it could get
/// wrong.
#[test]
fn every_audit_rule_has_a_scheduler_that_breaks_exactly_it() {
    for cheat in Cheat::ALL {
        let variant = RedTeam::run(cheat);
        let expected = variant
            .expected_violation()
            .expect("a cheat names the fault it must produce");
        let expected_site = variant
            .expected_site()
            .expect("a cheat names the check it must trip");
        let replay = variant.replay();
        let observed = match replay.fairness {
            Err(violation) => violation,
            Ok(report) => panic!("{cheat:?} was accepted: {report:?}"),
        };
        assert_eq!(
            observed, expected,
            "{cheat:?} produced the wrong fault, so it is not a control for the rule it names"
        );
        assert_eq!(
            replay.fault,
            Some(expected_site),
            "{cheat:?} produced the right fault from the wrong check, so the rule it \
             controls is not the rule it names"
        );

        let control = RedTeam::control(cheat);
        let report = control.audit().unwrap_or_else(|violation| {
            panic!("{cheat:?}'s control is not a correct schedule: {violation:?}")
        });
        assert_eq!(
            report.widest_gap, 0,
            "{cheat:?}'s control must differ from it by one decision, not by being unfair"
        );
        assert!(
            report.serviced > 0,
            "{cheat:?}'s control must do work, or it agrees with the cheat by doing nothing: \
             {report:?}"
        );
        println!(
            "{cheat:?} -> {expected_site:?} / {observed:?} (control serviced {})",
            report.serviced
        );
    }
}

/// The mapping from rule to control is total, and the compiler is what keeps it
/// that way.
///
/// [`control_for`] matches every site with no catch-all, so this test is really
/// three claims: that each check a cheat trips is the check that cheat was named
/// for, that no two cheats claim one check, and — enforced at compile time
/// rather than here — that no check exists without a control at all.
#[test]
fn every_rule_the_audit_decides_names_the_control_that_provokes_it() {
    let mut claimed: Vec<FaultSite> = Vec::new();
    for cheat in Cheat::ALL {
        let site = RedTeam::run(cheat)
            .expected_site()
            .expect("a cheat names the check it must trip");
        match control_for(site) {
            Control::RedTeam(named) => assert_eq!(
                named, cheat,
                "{site:?} is tripped by {cheat:?} and attributed to {named:?}"
            ),
            Control::Unreachable(reason) => {
                panic!("{cheat:?} trips {site:?}, which is declared unreachable: {reason}")
            }
        }
        assert!(
            !claimed.contains(&site),
            "{site:?} is claimed by two cheats, so one of them controls nothing"
        );
        claimed.push(site);
    }

    // And the other direction: every site is either claimed above or carries an
    // argument. Without this, a site whose `control_for` arm names a cheat that
    // trips a *different* site would go unexercised while the match stayed
    // exhaustive.
    let mut argued = 0_usize;
    for site in FaultSite::ALL {
        match control_for(site) {
            Control::RedTeam(cheat) => assert!(
                claimed.contains(&site),
                "{site:?} names {cheat:?} as its control, but {cheat:?} trips something else"
            ),
            Control::Unreachable(_) => argued += 1,
        }
    }
    assert_eq!(
        claimed.len() + argued,
        FaultSite::ALL.len(),
        "every rule the audit decides is either controlled or argued"
    );
    println!(
        "{} rules across {} violation variants: {} controlled, {} argued unreachable",
        FaultSite::ALL.len(),
        24,
        claimed.len(),
        argued
    );
}

/// The argument behind the one *shadowed* site, asserted rather than asserted
/// about.
///
/// `OfferedAnUnknownGroup` is declared unreachable because a plan naming a group
/// that was never created is judged at the arming, where such a group is not
/// ready. That claim is only worth what the check is worth, so here is the
/// history it is about: the fold reports the arm-side rule, and the offer-side
/// one never gets to speak.
#[test]
fn an_offer_to_a_group_that_never_existed_is_caught_when_the_plan_named_it() {
    let served = group(0);
    let ghost = group(1);
    let mut history = History::new();
    history.open_group(served, 1);
    history.submit(served, system(SystemClass::Bulk, 1));
    history.armed(1, 1, vec![served, ghost]);
    history.dispatched(1, 1, served, 1, 1);
    history.serviced(1, served, 1);
    // A turn handed to a slot that was never created. The offer-side check for
    // it exists, and this history never reaches it.
    history.dispatched(1, 1, ghost, 1, 1);

    let replay = history.replay(config(4, 2, 2, 64, 512));
    assert_eq!(
        replay.fairness,
        Err(SchedulingViolation::PlanIncludedUnreadyGroup {
            pass: pass(1),
            group: ghost,
        })
    );
    assert_eq!(
        replay.fault,
        Some(FaultSite::PlanNamedAnUnreadyGroup),
        "the arming is where an uncreated group is refused, so the offer-side \
         check is shadowed and has no history of its own"
    );
}

/// The base the whole family is cut from is a correct schedule.
///
/// Without this, a green control proves nothing: every cheat's control could be
/// green because the base is trivially green — no plans, no turns, no work.
#[test]
fn the_red_team_base_is_a_correct_schedule_that_does_real_work() {
    let honest = RedTeam::honest();
    let report = honest
        .audit()
        .expect("the base every cheat is cut from keeps the bound");
    println!(
        "red-team base: passes_completed={} opportunities={} serviced={} widest_gap={}",
        report.passes_completed, report.opportunities, report.serviced, report.widest_gap
    );
    assert_eq!(report.widest_gap, 0);
    assert_eq!(report.passes_completed, 2);
    assert_eq!(report.opportunities, 6, "three groups, two passes each");
    assert_eq!(
        report.serviced, 12,
        "and every one of those turns moved its full quota"
    );
    assert!(
        honest.expected_violation().is_none(),
        "the base cheats at nothing"
    );
    assert!(honest.expected_site().is_none());

    // The base drives three concurrent turns out of a three-worker pool, which
    // is what makes `TakeMoreWorkersThanThePoolHas` a one-worker edit rather
    // than a different history.
    assert_eq!(honest.bounds().workers(), 3);
    let head = group(0);
    assert_eq!(
        honest
            .history()
            .iter()
            .filter(|event| matches!(
                event,
                HistoryEvent::WorkServiced { group, .. } if *group == head
            ))
            .count(),
        4,
        "each group's whole backlog leaves over the two passes"
    );
}
