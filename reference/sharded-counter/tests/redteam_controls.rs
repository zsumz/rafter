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
//! So every fault has a control here, and the mapping is checked by the
//! compiler rather than by a reader: [`control_for`] matches
//! `SchedulingViolation` exhaustively with no catch-all, so adding a fault to
//! the audit without a scheduler that provokes it stops this file compiling.
//!
//! Each control is a *pair*. `RedTeam::run(cheat)` takes one bad decision;
//! `RedTeam::control(cheat)` takes that same decision correctly and changes
//! nothing else. Both are asserted, because a negative control on its own
//! proves that some history somewhere fails, and the pair proves that the rule
//! under test is the one doing the work.
//!
//! One rule has no fault of its own and so no entry below: a stall that is
//! broken between armings no longer excuses the omissions it covers, and it
//! reports the ordinary [`SchedulingViolation::OpportunityGap`] when it does.
//! Its pair is
//! `redteam_occupancy::flickering_the_stall_bit_across_arm_instants_is_owed_every_plan_it_denied`
//! against `redteam_occupancy::a_stall_held_unbroken_is_the_one_legitimate_way_a_group_receives_nothing`,
//! which is the same two-history discipline with the argument written out.
//!
//! [`ManagedScheduler`]: rafter_reference_sharded_counter::ManagedScheduler

mod support;

use rafter_reference_sharded_counter::{HistoryEvent, SchedulingViolation};
use support::{group, Cheat, RedTeam};

/// Whether a fault has a red-team scheduler, and which.
enum Control {
    /// The cheat that must produce this fault, and only this fault.
    RedTeam(Cheat),
    /// No history can produce it, with the argument for why.
    ///
    /// CONTRACT.md's standard for an unreachable answer is stated twice, about
    /// the absent session-table capacity refusal and the absent
    /// sequence-exhaustion refusal: "a refusal for a state that cannot be
    /// reached is a promise about behavior no test could observe." A fault
    /// landing here has to survive that argument rather than sit behind a
    /// comparison nothing satisfies.
    Unreachable(&'static str),
}

/// Maps every fault the audit can report to the scheduler that provokes it.
///
/// The match is exhaustive and has no catch-all, on purpose. A fault added to
/// [`SchedulingViolation`] without a red-team scheduler beside it does not
/// silently go unchecked — it fails to compile, which is the difference between
/// a claim this suite backs and a claim it merely states.
fn control_for(violation: &SchedulingViolation) -> Control {
    match violation {
        SchedulingViolation::OpportunityGap { .. } => Control::RedTeam(Cheat::StarveAReadyGroup),
        SchedulingViolation::PassArmedWhileOpen { .. } => {
            Control::RedTeam(Cheat::ArmOverAnOpenPlan)
        }
        SchedulingViolation::PassOutOfOrder { .. } => Control::RedTeam(Cheat::SkipAPassIndex),
        SchedulingViolation::PlanIncludedUnreadyGroup { .. } => {
            Control::RedTeam(Cheat::PlanAGroupThatIsNotReady)
        }
        SchedulingViolation::PlanRepeatedGroup { .. } => {
            Control::RedTeam(Cheat::NameAGroupTwiceInOnePlan)
        }
        SchedulingViolation::OfferOutsidePlan { .. } => {
            Control::RedTeam(Cheat::OfferAGroupThePlanDidNotName)
        }
        SchedulingViolation::GroupOfferedTwice { .. } => {
            Control::RedTeam(Cheat::OfferAGroupTwiceInOnePass)
        }
        SchedulingViolation::PassCompletedWithUnofferedGroup { .. } => {
            Control::RedTeam(Cheat::RetireAPlanWithATurnOwing)
        }
        SchedulingViolation::DispatchedUnreadyGroup { .. } => {
            Control::RedTeam(Cheat::DispatchAGroupThatIsNotReady)
        }
        SchedulingViolation::SkippedAvailableGroup { .. } => {
            Control::RedTeam(Cheat::SkipAGroupThatIsAvailable)
        }
        SchedulingViolation::QuotaExceeded { .. } => {
            Control::RedTeam(Cheat::ServiceMoreThanTheQuota)
        }
        SchedulingViolation::ServiceOrderViolation { .. } => {
            Control::RedTeam(Cheat::ServiceOutOfArrivalOrder)
        }
        SchedulingViolation::ServiceCountMismatch { .. } => {
            Control::RedTeam(Cheat::MiscountTheWorkATurnDid)
        }
        SchedulingViolation::DispatchLeftWorkUnserviced { .. } => {
            Control::RedTeam(Cheat::LeaveOwedWorkUnserviced)
        }
        SchedulingViolation::DispatchServicedBeyondItsWork { .. } => {
            Control::RedTeam(Cheat::ServicePastTheEndOfATurn)
        }
        SchedulingViolation::DispatchCostMismatch { .. } => {
            Control::RedTeam(Cheat::MisreportTheTurnsCost)
        }
        SchedulingViolation::WorkerHeldPastCost { .. } => {
            Control::RedTeam(Cheat::HoldAWorkerPastItsCost)
        }
        SchedulingViolation::WorkerReleasedEarly { .. } => {
            Control::RedTeam(Cheat::ReleaseAWorkerBeforeItsCostIsPaid)
        }
        SchedulingViolation::SpuriousWorkerRelease { .. } => {
            Control::RedTeam(Cheat::ReleaseAWorkerNobodyTook)
        }
        SchedulingViolation::WorkerCountExceeded { .. } => {
            Control::RedTeam(Cheat::TakeMoreWorkersThanThePoolHas)
        }
        SchedulingViolation::TickWentBackwards { .. } => {
            Control::RedTeam(Cheat::WalkTheClockBackwards)
        }
        SchedulingViolation::PassBoundaryReused { .. } => {
            Control::RedTeam(Cheat::ArmTwoPlansInOneTick)
        }
        SchedulingViolation::ServiceOutsideDispatch { .. } => {
            Control::RedTeam(Cheat::ServiceWorkOutsideAnyDispatch)
        }
        // The one fault with no red-team scheduler, and the argument for why it
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
        SchedulingViolation::WorkNotConserved { .. } => Control::Unreachable(
            "the fold moves admitted, serviced, failed, and queued together, so no \
             recorded history can separate them; the law is asserted over the model instead",
        ),
    }
}

/// Every cheat produces exactly the fault it was written to produce, and the
/// same history without that one decision is accepted.
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
        let observed = match variant.audit() {
            Err(violation) => violation,
            Ok(report) => panic!("{cheat:?} was accepted: {report:?}"),
        };
        assert_eq!(
            observed, expected,
            "{cheat:?} produced the wrong fault, so it is not a control for the rule it names"
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
            "{cheat:?} -> {observed:?} (control serviced {})",
            report.serviced
        );
    }
}

/// The mapping from fault to control is total, and the compiler is what keeps
/// it that way.
///
/// [`control_for`] matches every variant of [`SchedulingViolation`] with no
/// catch-all, so this test is really two claims: that each fault a cheat
/// produces is the fault that cheat was named for, and — enforced at compile
/// time rather than here — that no fault exists without a control at all.
#[test]
fn every_fault_the_audit_can_report_names_the_control_that_provokes_it() {
    let mut covered = 0_usize;
    for cheat in Cheat::ALL {
        let violation = RedTeam::run(cheat)
            .expected_violation()
            .expect("a cheat names the fault it must produce");
        match control_for(&violation) {
            Control::RedTeam(named) => {
                assert_eq!(
                    named, cheat,
                    "{violation:?} is produced by {cheat:?} and attributed to {named:?}"
                );
                covered += 1;
            }
            Control::Unreachable(reason) => {
                panic!("{cheat:?} produced {violation:?}, which is declared unreachable: {reason}")
            }
        }
    }
    assert_eq!(
        covered,
        Cheat::ALL.len(),
        "every cheat maps to itself, so the family covers what it claims to"
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
