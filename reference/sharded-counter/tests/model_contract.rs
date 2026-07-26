mod support;

use rafter_reference_sharded_counter::{
    AdmissionOutcome, AdmissionRejection, CounterCommand, CounterRejection, CounterResult, Delta,
    GroupAvailability, GroupId, GroupLifecycle, HistoryEvent, LifecycleOutcome, LifecycleRejection,
    LifecycleRequest, ManagedScheduler, OfferOutcome, Operation, OperationId, OperationOutcome,
    PassProgress, PassSuspension, ReadinessSignal, ReferenceScheduler, RequestFingerprint,
    SchedulerConfig, SchedulerConfigError, SchedulingViolation, ServiceCost, SessionOutcome,
    SkipReason, SystemClass, WorkClass, WorkFailure, WorkId, WorkQuota, WORK_CLASS_ORDER,
};
use support::{
    add, client, config, counter_with_fingerprint, create, delta, epoch, faulty, first, group,
    incarnation, pass, quota, read, sequence, system, work, Recorder, UnfairScheduler,
};

/// Bounds wide enough that nothing in a lifecycle or session test hits a queue
/// limit by accident.
fn roomy() -> SchedulerConfig {
    config(8, 2, 4, 32, 256)
}

fn counter_of(recorder: &Recorder, id: GroupId) -> i64 {
    recorder
        .scheduler()
        .counter(id)
        .unwrap_or_else(|| panic!("{id:?} has no counter"))
}

fn lifecycle_outcome(
    recorder: &mut Recorder,
    id: GroupId,
    request: LifecycleRequest,
) -> LifecycleOutcome {
    recorder.lifecycle(id, request).outcome
}

// ---------------------------------------------------------------------------
// The fairness bound
// ---------------------------------------------------------------------------

/// The bound, stated as the assertion the contract makes: while a group is
/// ready every time a plan is armed, no plan omits it. Hot, cold, slow, and
/// poisoned groups all run at once so that nothing here is proved on a
/// uniform workload.
#[test]
fn every_continuously_ready_group_is_offered_a_turn_in_every_pass() {
    let mut recorder = Recorder::new(config(8, 2, 4, 64, 512));
    let hot = group(0);
    let cold = group(1);
    let slow = group(2);
    let poisoned = group(3);
    for id in [hot, cold, slow, poisoned] {
        recorder.open_group(id, 2);
    }

    // The hot group always has more than its quota; the cold group is fed one
    // item every eight ticks; the slow group's work occupies a worker for six
    // ticks at a time; the poisoned group breaks on its first dispatch.
    recorder.submit(poisoned, first(), faulty(SystemClass::Bulk, 1));
    for _ in 0..8 {
        recorder.submit(slow, first(), system(SystemClass::Bulk, 6));
    }

    for step in 0..120_u64 {
        for _ in 0..3 {
            recorder.submit(hot, first(), system(SystemClass::Bulk, 1));
        }
        if step % 8 == 0 {
            recorder.submit(cold, first(), system(SystemClass::Bulk, 1));
        }
        recorder.step(&[]);
        recorder.assert_agreement(&step);
    }

    let report = recorder
        .oracle()
        .audit()
        .expect("a fair scheduler keeps the bound");
    assert_eq!(report.widest_gap, 0);
    assert!(
        report.passes_completed >= 8,
        "the workload must complete enough passes to mean something: {report:?}"
    );

    // Every group but the poisoned one made progress, and the poisoned one made
    // none without taking anyone else's turn with it.
    let serviced = |id: GroupId| {
        recorder
            .services()
            .iter()
            .filter(|record| record.group == id)
            .count()
    };
    assert!(serviced(hot) > 0 && serviced(cold) > 0 && serviced(slow) > 0);
    assert_eq!(serviced(poisoned), 1, "only the faulty item ever ran");
}

/// The bound survives a shortage of workers. One worker and many ready groups
/// means a pass takes many ticks, and the contract's answer is that the pass
/// suspends and resumes rather than restarting at the head.
#[test]
fn a_pass_suspends_under_worker_exhaustion_and_never_restarts() {
    let mut recorder = Recorder::new(config(6, 1, 2, 16, 64));
    for index in 0..6 {
        let id = group(index);
        recorder.open_group(id, 1);
        recorder.submit(id, first(), system(SystemClass::Bulk, 1));
    }

    let armed = recorder.step(&[]);
    let plan = armed
        .armed
        .clone()
        .expect("a plan is armed over six groups");
    assert_eq!(plan.len(), 6);
    assert_eq!(
        armed.progress,
        PassProgress::Suspended(PassSuspension::NoFreeWorker)
    );
    assert_eq!(armed.offers.len(), 1, "one worker gives out one turn");
    assert_eq!(
        recorder.scheduler().open_pass(),
        armed.pass,
        "a suspended pass stays open across ticks"
    );

    let mut offered = vec![armed.offers[0].group];
    for _ in 0..5 {
        let report = recorder.step(&[]);
        assert!(report.armed.is_none(), "a suspended pass is never rearmed");
        assert_eq!(report.pass, armed.pass);
        offered.extend(report.offers.iter().map(|offer| offer.group));
    }
    offered.sort_unstable();
    assert_eq!(offered, plan);
    assert_eq!(
        recorder.scheduler().open_pass(),
        None,
        "a plan that owes nothing is retired rather than left open"
    );
    recorder.assert_agreement(&"suspension");
}

/// The negative control. A scheduler that plans only its busiest group breaks
/// the bound, and the audit names the group, the pass the denial began at, and
/// how many complete passes it lasted.
#[test]
fn a_deliberately_unfair_scheduler_is_caught_with_the_exact_gap() {
    let starved = group(1);
    let variant = UnfairScheduler::run(group(0), starved, 5, 2);
    let mut oracle = ReferenceScheduler::new(config(4, 1, 2, 32, 128));
    oracle.observe_all(variant.history().iter().cloned());

    let violation = oracle
        .audit()
        .expect_err("a scheduler that never plans a ready group is unfair");
    assert_eq!(violation, variant.expected_violation());
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: pass(1),
            denied_passes: 5,
        },
        "the audit must name the starved group and the exact size of its gap"
    );

    // The same history with one more unfair pass produces a strictly wider gap,
    // so the number is measuring something rather than merely being nonzero.
    let longer = UnfairScheduler::run(group(0), starved, 9, 2);
    let mut oracle = ReferenceScheduler::new(config(4, 1, 2, 32, 128));
    oracle.observe_all(longer.history().iter().cloned());
    assert_eq!(
        oracle.audit().expect_err("still unfair"),
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: pass(1),
            denied_passes: 9,
        }
    );
}

/// A plan that is abandoned proves nothing about the groups it had not reached,
/// so rearming over an open plan is a violation in its own right.
#[test]
fn rearming_a_plan_that_still_owes_a_turn_is_a_violation() {
    let mut oracle = ReferenceScheduler::new(config(4, 1, 2, 16, 64));
    let mut events: Vec<HistoryEvent> = Vec::new();
    let mut next = 0_u64;
    let mut invoke = |events: &mut Vec<HistoryEvent>, operation: Operation| {
        next += 1;
        events.push(HistoryEvent::Invoked {
            operation_id: OperationId::new(next),
            operation,
        });
    };
    for id in [group(0), group(1)] {
        for request in [
            create(2),
            LifecycleRequest::Recover,
            LifecycleRequest::Serve,
        ] {
            invoke(&mut events, Operation::Lifecycle { group: id, request });
        }
        invoke(
            &mut events,
            Operation::Submit {
                group: id,
                incarnation: first(),
                work: system(SystemClass::Bulk, 1),
            },
        );
    }
    events.push(HistoryEvent::PassArmed {
        pass: pass(1),
        tick: support::tick(1),
        plan: vec![group(0), group(1)],
    });
    events.push(HistoryEvent::GroupOffered {
        pass: pass(1),
        tick: support::tick(1),
        group: group(0),
        outcome: OfferOutcome::Dispatched {
            serviced: 1,
            cost: 1,
        },
    });
    events.push(HistoryEvent::WorkServiced {
        pass: pass(1),
        group: group(0),
        work: work(1),
    });
    // Group 1 is still owed its turn, and the scheduler arms a fresh plan.
    events.push(HistoryEvent::PassArmed {
        pass: pass(2),
        tick: support::tick(2),
        plan: vec![group(1)],
    });
    oracle.observe_all(events);

    assert_eq!(
        oracle.audit().expect_err("an abandoned plan is unfair"),
        SchedulingViolation::PassArmedWhileOpen {
            open: pass(1),
            armed: pass(2),
        }
    );
}

// ---------------------------------------------------------------------------
// Quota, class priority, and readiness
// ---------------------------------------------------------------------------

/// A quota decides throughput share; the pass decides opportunity share. A
/// group with ten times the backlog takes exactly its quota per turn.
#[test]
fn one_opportunity_services_exactly_the_quota_and_no_more() {
    let mut recorder = Recorder::new(config(4, 4, 2, 64, 256));
    let generous = group(0);
    let modest = group(1);
    recorder.open_group(generous, 5);
    recorder.open_group(modest, 1);
    for _ in 0..20 {
        recorder.submit(generous, first(), system(SystemClass::Bulk, 1));
        recorder.submit(modest, first(), system(SystemClass::Bulk, 1));
    }

    let report = recorder.step(&[]);
    let dispatched = |id: GroupId| {
        report
            .offers
            .iter()
            .find(|offer| offer.group == id)
            .map(|offer| offer.outcome)
    };
    assert_eq!(
        dispatched(generous),
        Some(OfferOutcome::Dispatched {
            serviced: 5,
            cost: 5
        })
    );
    assert_eq!(
        dispatched(modest),
        Some(OfferOutcome::Dispatched {
            serviced: 1,
            cost: 1
        })
    );
    // Both were offered exactly one turn regardless of their backlogs.
    assert_eq!(report.offers.len(), 2);
    recorder.assert_agreement(&"quota");
}

/// A turn takes everything the group has when the quota is not the binding
/// constraint, which is the service floor the bound implies.
#[test]
fn a_turn_services_the_smaller_of_the_quota_and_the_backlog() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    recorder.open_group(id, 4);
    recorder.submit(id, first(), system(SystemClass::Bulk, 1));
    recorder.submit(id, first(), system(SystemClass::Bulk, 1));

    let report = recorder.step(&[]);
    assert_eq!(
        report.offers[0].outcome,
        OfferOutcome::Dispatched {
            serviced: 2,
            cost: 2
        }
    );
    assert_eq!(report.serviced.len(), 2);
    recorder.assert_agreement(&"service floor");
}

/// Class priority fills a group's own quota. It never reorders the pass, so a
/// group with urgent control work does not take another group's turn.
#[test]
fn control_work_is_serviced_first_within_a_turn_and_never_jumps_the_pass() {
    let mut recorder = Recorder::new(config(4, 1, 2, 32, 128));
    let bulky = group(0);
    let urgent = group(1);
    recorder.open_group(bulky, 4);
    recorder.open_group(urgent, 4);

    // The bulk group is submitted first and every class is present out of
    // priority order, so nothing here is decided by arrival alone.
    for class in [
        SystemClass::Bulk,
        SystemClass::Snapshot,
        SystemClass::Bulk,
        SystemClass::Control,
    ] {
        recorder.submit(bulky, first(), system(class, 1));
    }
    recorder.submit(urgent, first(), system(SystemClass::Control, 1));

    let first_tick = recorder.step(&[]);
    assert_eq!(
        first_tick.armed.as_deref(),
        Some([bulky, urgent].as_slice()),
        "both groups are in the plan; priority does not decide membership"
    );
    let classes: Vec<WorkClass> = first_tick
        .serviced
        .iter()
        .map(|record| record.class)
        .collect();
    assert_eq!(
        classes,
        vec![
            WorkClass::Control,
            WorkClass::Snapshot,
            WorkClass::Bulk,
            WorkClass::Bulk
        ],
        "a turn services its own classes in priority order, arrival order within a class"
    );

    // One worker, and the bulk group's turn cost four ticks, so the urgent
    // group waits. It waits inside the same pass: no plan is armed while it
    // does, and its turn is the one the original plan already owed it.
    let mut waited = 0;
    let mut urgent_turn = recorder.step(&[]);
    while urgent_turn.offers.is_empty() {
        assert!(
            urgent_turn.armed.is_none(),
            "a suspended pass is never rearmed around a busy worker"
        );
        assert_eq!(
            urgent_turn.progress,
            PassProgress::Suspended(PassSuspension::NoFreeWorker)
        );
        waited += 1;
        urgent_turn = recorder.step(&[]);
    }
    assert!(waited > 0, "the single worker was genuinely contended");
    assert!(urgent_turn.armed.is_none());
    assert_eq!(urgent_turn.pass, first_tick.pass);
    assert_eq!(urgent_turn.offers[0].group, urgent);
    assert_eq!(urgent_turn.serviced[0].class, WorkClass::Control);
    recorder.assert_agreement(&"class priority");
}

/// The classes are a total order, and it is the one the contract names.
#[test]
fn work_classes_order_by_descending_service_priority() {
    assert_eq!(
        WORK_CLASS_ORDER,
        [
            WorkClass::Control,
            WorkClass::Command,
            WorkClass::Snapshot,
            WorkClass::Bulk
        ]
    );
    assert!(WorkClass::Control < WorkClass::Command);
    assert!(WorkClass::Command < WorkClass::Snapshot);
    assert!(WorkClass::Snapshot < WorkClass::Bulk);
    assert_eq!(
        WORK_CLASS_ORDER.map(WorkClass::rank),
        [0, 1, 2, 3],
        "rank must agree with the declared order that indexes the queues"
    );
    assert_eq!(system(SystemClass::Control, 1).class(), WorkClass::Control);
    assert_eq!(
        faulty(SystemClass::Snapshot, 1).class(),
        WorkClass::Snapshot
    );
    assert_eq!(add(0, 1, 1, 1, 1).class(), WorkClass::Command);
}

/// A group holding a worker is not starved; it is being served. It leaves the
/// ready set for as long as its cost says, which is what lets a slow group be
/// slow without being unfair.
#[test]
fn a_group_occupying_a_worker_is_out_of_the_ready_set_until_its_cost_is_paid() {
    let mut recorder = Recorder::new(config(4, 2, 2, 32, 128));
    let slow = group(0);
    recorder.open_group(slow, 1);
    recorder.submit(slow, first(), system(SystemClass::Bulk, 4));
    recorder.submit(slow, first(), system(SystemClass::Bulk, 1));

    let dispatch = recorder.step(&[]);
    assert_eq!(
        dispatch.offers[0].outcome,
        OfferOutcome::Dispatched {
            serviced: 1,
            cost: 4
        }
    );
    assert!(recorder.scheduler().ready_groups().is_empty());

    // Four ticks of occupancy, and the group is out of every plan armed during
    // them. It is not starved; it is being served.
    for _ in 0..3 {
        let report = recorder.step(&[]);
        assert!(report.released.is_empty(), "the worker is still occupied");
        assert!(recorder.scheduler().ready_groups().is_empty());
        assert_eq!(report.progress, PassProgress::Idle);
    }
    let release = recorder.step(&[]);
    assert_eq!(release.released, vec![slow]);
    assert_eq!(
        release.offers.first().map(|offer| offer.group),
        Some(slow),
        "the tick that frees the worker is the tick the group rejoins a plan"
    );
    assert_eq!(recorder.scheduler().summary().queued, 0);
    recorder.assert_agreement(&"slow group");
}

/// External backpressure is the one thing that can revoke a plan entry's
/// readiness after the plan is armed, and a skipped turn is still a turn.
#[test]
fn an_external_stall_skips_a_planned_turn_without_costing_the_pass() {
    let mut recorder = Recorder::new(config(4, 1, 2, 32, 128));
    let steady = group(0);
    let stalling = group(1);
    for id in [steady, stalling] {
        recorder.open_group(id, 1);
        recorder.submit(id, first(), system(SystemClass::Bulk, 1));
    }

    let armed = recorder.step(&[]);
    assert_eq!(armed.armed.as_deref(), Some([steady, stalling].as_slice()));
    assert_eq!(armed.offers[0].group, steady);

    let stalled = recorder.step(&[ReadinessSignal::stalled(stalling)]);
    assert_eq!(
        stalled.offers[0].outcome,
        OfferOutcome::Skipped(SkipReason::Stalled)
    );
    assert_eq!(stalled.progress, PassProgress::Completed);
    assert!(
        recorder
            .scheduler()
            .group(stalling)
            .is_some_and(|view| view.stalled && view.queued == 1),
        "a stalled group keeps its work"
    );

    // It rejoins the ready set the moment the host says it may, and the very
    // next plan owes it a turn.
    let resumed = recorder.step(&[ReadinessSignal::available(stalling)]);
    assert_eq!(resumed.armed.as_deref(), Some([stalling].as_slice()));
    assert_eq!(
        resumed.offers[0].outcome,
        OfferOutcome::Dispatched {
            serviced: 1,
            cost: 1
        }
    );
    assert_eq!(recorder.scheduler().summary().queued, 0);
    recorder.assert_agreement(&"stall");
}

// ---------------------------------------------------------------------------
// Poison and isolation
// ---------------------------------------------------------------------------

/// The isolation property: a group that its own work destroyed takes nothing
/// else down, and cannot keep taking turns it can no longer use.
#[test]
fn a_poisoned_group_stops_itself_and_nothing_else() {
    let mut recorder = Recorder::new(config(4, 2, 2, 32, 128));
    let broken = group(0);
    let healthy = group(1);
    recorder.open_group(broken, 4);
    recorder.open_group(healthy, 2);

    recorder.submit(broken, first(), system(SystemClass::Control, 1));
    recorder.submit(broken, first(), faulty(SystemClass::Bulk, 1));
    recorder.submit(broken, first(), system(SystemClass::Bulk, 1));
    for _ in 0..6 {
        recorder.submit(healthy, first(), system(SystemClass::Bulk, 1));
    }

    let poisoning = recorder.step(&[]);
    assert_eq!(
        poisoning.offers[0].outcome,
        OfferOutcome::Dispatched {
            serviced: 2,
            cost: 2
        },
        "the turn stops at the item that broke the group, short of its quota of four"
    );
    assert!(recorder
        .scheduler()
        .group(broken)
        .is_some_and(|view| view.poisoned && view.queued == 1));

    // The healthy group keeps taking turns for as long as it has work, and the
    // poisoned one is never in a plan again.
    for _ in 0..12 {
        recorder.step(&[]);
    }
    assert_eq!(counter_of(&recorder, healthy), 0);
    assert!(recorder
        .scheduler()
        .group(healthy)
        .is_some_and(|view| view.queued == 0));
    assert!(!recorder.scheduler().ready_groups().contains(&broken));

    // Nothing new may be admitted to it, but its accepted item is still there.
    assert_eq!(
        recorder.submit(broken, first(), system(SystemClass::Control, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupPoisoned)
    );
    assert!(recorder
        .scheduler()
        .group(broken)
        .is_some_and(|view| view.queued == 1));
    recorder.assert_agreement(&"poison isolation");
}

/// Accepted work is allowed to fail and never allowed to disappear. Draining a
/// poisoned group reports every item it could not service, by name.
#[test]
fn draining_a_poisoned_group_reports_every_item_it_retires() {
    let mut recorder = Recorder::new(roomy());
    let broken = group(0);
    recorder.open_group(broken, 1);
    // One quota, one class, so the faulty item is the first thing serviced and
    // the two behind it are stranded with nowhere to go.
    recorder.submit(broken, first(), faulty(SystemClass::Bulk, 1));
    let stranded = [
        recorder.submit(broken, first(), system(SystemClass::Bulk, 1)),
        recorder.submit(broken, first(), system(SystemClass::Bulk, 1)),
    ];
    recorder.step(&[]);
    assert!(recorder
        .scheduler()
        .group(broken)
        .is_some_and(|view| view.poisoned && view.queued == 2));

    // Removal cannot outrun the queue.
    assert_eq!(
        lifecycle_outcome(&mut recorder, broken, LifecycleRequest::Remove),
        LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
            current: GroupLifecycle::Serving,
            requested: GroupLifecycle::Removed,
        })
    );

    let drain = recorder.lifecycle(broken, LifecycleRequest::Drain);
    let retired: Vec<WorkId> = drain.failed.iter().map(|record| record.work).collect();
    let expected: Vec<WorkId> = stranded
        .iter()
        .map(|outcome| match outcome {
            AdmissionOutcome::Queued { work } => *work,
            other => panic!("expected a queue slot, observed {other:?}"),
        })
        .collect();
    assert_eq!(retired.len(), 2);
    assert_eq!(
        retired
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        expected
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        "every accepted item is named in the report that retires it"
    );
    assert!(drain
        .failed
        .iter()
        .all(|record| record.reason == WorkFailure::GroupPoisoned));
    assert_eq!(recorder.scheduler().summary().queued, 0);
    assert_eq!(recorder.scheduler().summary().failed, 2);

    // With the queue accounted for, removal proceeds.
    assert!(matches!(
        lifecycle_outcome(&mut recorder, broken, LifecycleRequest::Remove),
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Removed,
            ..
        }
    ));
    recorder.assert_agreement(&"poisoned drain");
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// The whole transition table, asserted as a table: every legal edge applies,
/// every repeat is idempotent, and every other request is an explicit conflict
/// naming both states.
#[test]
fn the_lifecycle_table_is_idempotent_or_explicitly_conflicting() {
    use GroupLifecycle::{Creating, Draining, Recovering, Removed, Serving, Tombstoned};
    let requests = [
        create(1),
        LifecycleRequest::Recover,
        LifecycleRequest::Serve,
        LifecycleRequest::Drain,
        LifecycleRequest::Remove,
        LifecycleRequest::Tombstone,
    ];
    // The path each state is reached by, then the state itself.
    let states: [(GroupLifecycle, &[LifecycleRequest]); 6] = [
        (Creating, &[create(1)]),
        (Recovering, &[create(1), LifecycleRequest::Recover]),
        (
            Serving,
            &[
                create(1),
                LifecycleRequest::Recover,
                LifecycleRequest::Serve,
            ],
        ),
        (Draining, &[create(1), LifecycleRequest::Drain]),
        (
            Removed,
            &[create(1), LifecycleRequest::Drain, LifecycleRequest::Remove],
        ),
        (
            Tombstoned,
            &[
                create(1),
                LifecycleRequest::Drain,
                LifecycleRequest::Remove,
                LifecycleRequest::Tombstone,
            ],
        ),
    ];

    for (state, path) in states {
        for request in requests {
            let mut recorder = Recorder::new(roomy());
            let id = group(0);
            for step in path {
                recorder.lifecycle(id, *step);
            }
            assert_eq!(
                recorder.scheduler().group(id).map(|view| view.state),
                Some(state),
                "the path to {state:?} must reach it"
            );

            let outcome = lifecycle_outcome(&mut recorder, id, request);
            let target = request.target();
            match (state, request) {
                // Repeating the state you are in changes nothing.
                (current, _) if current == target && current != Creating => assert!(
                    matches!(outcome, LifecycleOutcome::Idempotent { state: s, .. } if s == current),
                    "{current:?} + {request:?} must be idempotent, observed {outcome:?}"
                ),
                // A creation repeated on a creating slot is idempotent only
                // when it names the same quota.
                (Creating, LifecycleRequest::Create { .. }) => assert!(
                    matches!(
                        outcome,
                        LifecycleOutcome::Idempotent {
                            state: Creating,
                            ..
                        }
                    ),
                    "observed {outcome:?}"
                ),
                // A removed slot reopens as a strictly greater incarnation.
                (Removed, LifecycleRequest::Create { .. }) => assert_eq!(
                    outcome,
                    LifecycleOutcome::Created {
                        incarnation: incarnation(2)
                    }
                ),
                // A tombstone answers with itself, never with a conflict.
                (Tombstoned, _) => assert_eq!(
                    outcome,
                    LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
                ),
                // Legal successors apply.
                (Creating, LifecycleRequest::Recover)
                | (Recovering, LifecycleRequest::Serve)
                | (Creating | Recovering | Serving, LifecycleRequest::Drain)
                | (Draining, LifecycleRequest::Remove)
                | (Removed, LifecycleRequest::Tombstone) => assert!(
                    matches!(outcome, LifecycleOutcome::Applied { from, to, .. } if from == state && to == target),
                    "{state:?} + {request:?} is a legal edge, observed {outcome:?}"
                ),
                // Everything else conflicts, and says so with both states.
                _ => assert_eq!(
                    outcome,
                    LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                        current: state,
                        requested: target,
                    }),
                    "{state:?} + {request:?} must conflict explicitly"
                ),
            }
            recorder.assert_agreement(&(state, request));
        }
    }
}

/// A quota belongs to an incarnation, so a repeated creation that names a
/// different one is refused rather than absorbed.
#[test]
fn a_repeated_creation_naming_another_quota_is_refused() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    recorder.lifecycle(id, create(3));

    assert_eq!(
        lifecycle_outcome(&mut recorder, id, create(3)),
        LifecycleOutcome::Idempotent {
            state: GroupLifecycle::Creating,
            incarnation: first(),
        }
    );
    assert_eq!(
        lifecycle_outcome(&mut recorder, id, create(7)),
        LifecycleOutcome::Rejected(LifecycleRejection::QuotaConflict { current: quota(3) })
    );
    assert_eq!(
        recorder.scheduler().group(id).map(|view| view.quota),
        Some(quota(3))
    );
    recorder.assert_agreement(&"quota conflict");
}

/// Removal cannot outrun accepted work: a healthy group's queue leaves by being
/// serviced, and until it does, removal is refused with the count still owed.
#[test]
fn removal_is_refused_while_accepted_work_is_still_queued() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    recorder.open_group(id, 1);
    recorder.submit(id, first(), system(SystemClass::Bulk, 1));
    recorder.submit(id, first(), system(SystemClass::Bulk, 1));
    recorder.lifecycle(id, LifecycleRequest::Drain);

    assert_eq!(
        lifecycle_outcome(&mut recorder, id, LifecycleRequest::Remove),
        LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained { pending: 2 })
    );
    // A draining group is still serviceable; that is how the queue empties.
    recorder.run(4);
    assert_eq!(recorder.scheduler().summary().queued, 0);
    assert!(matches!(
        lifecycle_outcome(&mut recorder, id, LifecycleRequest::Remove),
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Removed,
            ..
        }
    ));
    assert_eq!(recorder.scheduler().summary().serviced, 2);
    recorder.assert_agreement(&"drain by service");
}

// ---------------------------------------------------------------------------
// Late messages, incarnations, and tombstones
// ---------------------------------------------------------------------------

/// Late traffic is refused whether the slot is gone, reopened, or tombstoned.
/// None of the three resurrects anything, and the refusals are distinguishable
/// so a caller can tell which world it woke up in.
#[test]
fn late_messages_are_rejected_and_never_resurrect_a_removed_group() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let original = recorder.open_group(id, 2);
    recorder.open_session(id, original, client(0), epoch(1));
    recorder.submit(id, original, add(0, 1, 1, 5, 1));
    recorder.run(2);
    assert_eq!(counter_of(&recorder, id), 5);

    recorder.lifecycle(id, LifecycleRequest::Drain);
    recorder.lifecycle(id, LifecycleRequest::Remove);

    // Removal took the counter and the sessions with it, so a late retry has no
    // cache to recognize it and must be refused rather than executed.
    assert_eq!(counter_of(&recorder, id), 0);
    assert_eq!(
        recorder.submit(id, original, add(0, 1, 1, 5, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Removed,
            class: WorkClass::Command,
        })
    );

    // Reopened under a greater incarnation, the same late message names a slot
    // generation that has retired.
    let reopened = recorder.open_group(id, 2);
    assert_eq!(reopened, incarnation(2));
    assert_eq!(
        recorder.submit(id, original, add(0, 1, 1, 5, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::StaleIncarnation { current: reopened })
    );
    assert_eq!(
        recorder.open_session(id, original, client(0), epoch(1)),
        SessionOutcome::Rejected(AdmissionRejection::StaleIncarnation { current: reopened })
    );
    assert_eq!(counter_of(&recorder, id), 0, "a reopened slot starts empty");

    // A message from the future is refused too, so a caller cannot address a
    // generation the slot has not reached.
    assert_eq!(
        recorder.submit(id, incarnation(9), system(SystemClass::Bulk, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::FutureIncarnation { current: reopened })
    );
    recorder.assert_agreement(&"late messages");
}

/// A tombstone is terminal for the identity, not just for the incarnation.
#[test]
fn a_tombstoned_group_refuses_everything_forever() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 1);
    recorder.lifecycle(id, LifecycleRequest::Drain);
    recorder.lifecycle(id, LifecycleRequest::Remove);
    recorder.lifecycle(id, LifecycleRequest::Tombstone);

    for incarnation_named in [live, incarnation(2)] {
        assert_eq!(
            recorder.submit(id, incarnation_named, system(SystemClass::Control, 1)),
            AdmissionOutcome::Rejected(AdmissionRejection::GroupTombstoned),
            "a tombstone outranks every incarnation question"
        );
        assert_eq!(
            recorder.open_session(id, incarnation_named, client(0), epoch(1)),
            SessionOutcome::Rejected(AdmissionRejection::GroupTombstoned)
        );
    }
    assert_eq!(
        lifecycle_outcome(&mut recorder, id, create(1)),
        LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned),
        "a tombstoned identity is never created again"
    );
    assert_eq!(
        lifecycle_outcome(&mut recorder, id, LifecycleRequest::Tombstone),
        LifecycleOutcome::Idempotent {
            state: GroupLifecycle::Tombstoned,
            incarnation: live,
        }
    );
    recorder.assert_agreement(&"tombstone");
}

/// Work addressed to a slot that has never existed is refused by identity, not
/// by lifecycle, so a caller cannot probe which IDs have been used.
#[test]
fn unknown_and_out_of_range_groups_are_refused_at_the_gate() {
    let mut recorder = Recorder::new(config(2, 1, 2, 8, 16));
    assert_eq!(
        recorder.submit(group(0), first(), system(SystemClass::Bulk, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupUnknown)
    );
    assert_eq!(
        recorder.submit(group(9), first(), system(SystemClass::Bulk, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupOutOfRange)
    );
    assert_eq!(
        lifecycle_outcome(&mut recorder, group(9), create(1)),
        LifecycleOutcome::Rejected(LifecycleRejection::GroupOutOfRange)
    );
    assert_eq!(
        lifecycle_outcome(&mut recorder, group(0), LifecycleRequest::Serve),
        LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown)
    );
    recorder.assert_agreement(&"unknown groups");
}

/// A recovering group takes its own replication traffic and refuses client
/// commands, because parking commands behind a recovery of unknown length turns
/// one slow group into a queue outage for the host.
#[test]
fn a_recovering_group_takes_system_traffic_and_refuses_client_commands() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    recorder.lifecycle(id, create(2));
    recorder.lifecycle(id, LifecycleRequest::Recover);
    recorder.open_session(id, first(), client(0), epoch(1));

    assert!(matches!(
        recorder.submit(id, first(), system(SystemClass::Bulk, 1)),
        AdmissionOutcome::Queued { .. }
    ));
    assert_eq!(
        recorder.submit(id, first(), add(0, 1, 1, 3, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Recovering,
            class: WorkClass::Command,
        })
    );
    assert!(
        recorder.scheduler().ready_groups().contains(&id),
        "a recovering group is schedulable for the traffic that recovers it"
    );

    recorder.lifecycle(id, LifecycleRequest::Serve);
    assert!(matches!(
        recorder.submit(id, first(), add(0, 1, 1, 3, 1)),
        AdmissionOutcome::Queued { .. }
    ));
    recorder.assert_agreement(&"recovery admission");
}

// ---------------------------------------------------------------------------
// Queue and quota bounds
// ---------------------------------------------------------------------------

/// Both bounds fail closed, in the order the contract names them, and neither
/// touches work that was already accepted.
#[test]
fn queue_bounds_fail_closed_without_discarding_accepted_work() {
    let mut recorder = Recorder::new(config(4, 1, 2, 3, 5));
    let first_group = group(0);
    let second_group = group(1);
    recorder.open_group(first_group, 1);
    recorder.open_group(second_group, 1);

    for _ in 0..3 {
        assert!(matches!(
            recorder.submit(first_group, first(), system(SystemClass::Bulk, 1)),
            AdmissionOutcome::Queued { .. }
        ));
    }
    assert_eq!(
        recorder.submit(first_group, first(), system(SystemClass::Bulk, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupQueueFull { limit: 3 }),
        "a group over its own bound learns which bound it hit"
    );

    for _ in 0..2 {
        assert!(matches!(
            recorder.submit(second_group, first(), system(SystemClass::Bulk, 1)),
            AdmissionOutcome::Queued { .. }
        ));
    }
    assert_eq!(
        recorder.submit(second_group, first(), system(SystemClass::Bulk, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GlobalQueueFull { limit: 5 }),
        "a group under its own bound learns the host is full"
    );

    // Five items were accepted and five are still there. Nothing a rejection
    // touched was already ours.
    assert_eq!(recorder.scheduler().summary().queued, 5);
    assert_eq!(recorder.scheduler().summary().admitted, 5);
    recorder.run(12);
    assert_eq!(recorder.scheduler().summary().serviced, 5);
    assert_eq!(recorder.scheduler().summary().queued, 0);
    recorder.assert_agreement(&"queue bounds");
}

/// An acknowledged request stays confirmable when the queue is full, because
/// the session cache answers before any bound is consulted.
#[test]
fn a_full_queue_still_replays_an_acknowledged_request() {
    let mut recorder = Recorder::new(config(2, 1, 2, 2, 4));
    let id = group(0);
    let live = recorder.open_group(id, 1);
    recorder.open_session(id, live, client(0), epoch(1));
    recorder.submit(id, live, add(0, 1, 1, 7, 1));
    recorder.run(2);
    assert_eq!(counter_of(&recorder, id), 7);

    // Fill the queue with traffic that has nothing to do with the client.
    for _ in 0..2 {
        assert!(matches!(
            recorder.submit(id, live, system(SystemClass::Bulk, 4)),
            AdmissionOutcome::Queued { .. }
        ));
    }
    assert_eq!(
        recorder.submit(id, live, add(0, 1, 2, 3, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::GroupQueueFull { limit: 2 }),
        "new work is refused while the queue is full"
    );
    assert_eq!(
        recorder.submit(id, live, add(0, 1, 1, 7, 1)),
        AdmissionOutcome::Replayed {
            result: CounterResult::Added { value: 7 }
        },
        "an acknowledged request must not look unacknowledged because the queue is busy"
    );
    assert_eq!(counter_of(&recorder, id), 7);
    recorder.assert_agreement(&"replay under pressure");
}

// ---------------------------------------------------------------------------
// Sessions and deduplication
// ---------------------------------------------------------------------------

/// The session protocol, stated in counter terms: one outstanding mutation per
/// client per group, an exact retry that costs nothing, and explicit refusals
/// for every other reuse.
#[test]
fn one_request_identity_changes_one_counter_at_most_once() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 4);
    recorder.open_session(id, live, client(0), epoch(1));

    let mutation = add(0, 1, 1, 10, 1);
    let queued = recorder.submit(id, live, mutation);
    let AdmissionOutcome::Queued { work: slot } = queued else {
        panic!("expected a queue slot, observed {queued:?}")
    };

    // Retried while still queued, the request joins the slot it already has.
    assert_eq!(
        recorder.submit(id, live, mutation),
        AdmissionOutcome::AlreadyQueued { work: slot }
    );
    assert_eq!(recorder.scheduler().summary().admitted, 1);

    recorder.run(2);
    assert_eq!(counter_of(&recorder, id), 10);

    // Retried after completion, it replays the cached result and adds nothing.
    assert_eq!(
        recorder.submit(id, live, mutation),
        AdmissionOutcome::Replayed {
            result: CounterResult::Added { value: 10 }
        }
    );
    recorder.run(2);
    assert_eq!(counter_of(&recorder, id), 10, "one identity, one effect");
    recorder.assert_agreement(&"deduplication");
}

/// Reusing an identity with different bytes is refused, whether the original is
/// still queued or already cached.
#[test]
fn conflicting_reuse_of_an_identity_is_refused_in_both_states() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 4);
    recorder.open_session(id, live, client(0), epoch(1));
    recorder.submit(id, live, add(0, 1, 1, 4, 1));

    assert_eq!(
        recorder.submit(id, live, add(0, 1, 1, 9, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::ConflictingRetry),
        "an outstanding identity cannot change its mind"
    );
    recorder.run(2);
    assert_eq!(
        recorder.submit(id, live, read(0, 1, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::ConflictingRetry),
        "a cached identity cannot change its mind either"
    );
    assert_eq!(counter_of(&recorder, id), 4);
    recorder.assert_agreement(&"conflicting retry");
}

/// Stale sequences, gaps, and a client that runs ahead of its own outstanding
/// request are all refused with the sequence the session will accept next.
#[test]
fn stale_sequences_and_gaps_fail_closed() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 4);
    recorder.open_session(id, live, client(0), epoch(1));

    assert_eq!(
        recorder.submit(id, live, add(0, 1, 2, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::SequenceGap {
            expected: sequence(1)
        })
    );
    recorder.submit(id, live, add(0, 1, 1, 1, 1));
    assert_eq!(
        recorder.submit(id, live, add(0, 1, 3, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::SequenceGap {
            expected: sequence(1)
        }),
        "the expected sequence does not move until the outstanding one completes"
    );

    recorder.run(2);
    assert_eq!(
        recorder.submit(id, live, add(0, 1, 3, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::SequenceGap {
            expected: sequence(2)
        })
    );
    recorder.submit(id, live, add(0, 1, 2, 1, 1));
    recorder.run(2);
    assert_eq!(
        recorder.submit(id, live, read(0, 1, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::StaleSequence {
            highest: sequence(2)
        })
    );
    assert_eq!(counter_of(&recorder, id), 2);
    recorder.assert_agreement(&"sequence discipline");
}

/// A request whose fingerprint does not describe its own command is malformed
/// wherever its sequence falls, and consumes nothing.
#[test]
fn a_fingerprint_that_does_not_describe_its_command_is_refused() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 2);
    recorder.open_session(id, live, client(0), epoch(1));

    let command = CounterCommand::Add { delta: delta(6) };
    // A digest a client computed by some other rule entirely. The gate must
    // reject the envelope rather than quietly recompute it into agreement.
    let unrelated = RequestFingerprint::from_digest(0xdead_beef);
    assert_eq!(
        recorder.submit(
            id,
            live,
            counter_with_fingerprint(0, 1, 1, unrelated, command, 1)
        ),
        AdmissionOutcome::Rejected(AdmissionRejection::FingerprintMismatch {
            expected: RequestFingerprint::of(&command)
        })
    );
    // The malformed envelope consumed no sequence.
    assert!(matches!(
        recorder.submit(id, live, add(0, 1, 1, 6, 1)),
        AdmissionOutcome::Queued { .. }
    ));
    recorder.assert_agreement(&"fingerprint");
}

/// Sessions are scoped to a group, so one client can hold one outstanding
/// mutation in each of several groups at once.
#[test]
fn sessions_are_scoped_to_a_group_and_bounded_within_it() {
    let mut recorder = Recorder::new(config(4, 2, 2, 16, 64));
    let left = group(0);
    let right = group(1);
    let left_live = recorder.open_group(left, 2);
    let right_live = recorder.open_group(right, 2);
    recorder.open_session(left, left_live, client(0), epoch(1));
    recorder.open_session(right, right_live, client(0), epoch(1));

    assert!(matches!(
        recorder.submit(left, left_live, add(0, 1, 1, 3, 1)),
        AdmissionOutcome::Queued { .. }
    ));
    assert!(
        matches!(
            recorder.submit(right, right_live, add(0, 1, 1, 8, 1)),
            AdmissionOutcome::Queued { .. }
        ),
        "one client's outstanding request in one group does not block another"
    );
    recorder.run(3);
    assert_eq!(counter_of(&recorder, left), 3);
    assert_eq!(counter_of(&recorder, right), 8);

    // A group's session table is bounded by the addressable client range, and
    // needs no capacity refusal of its own because of it.
    assert_eq!(recorder.scheduler().config().max_clients_per_group(), 2);
    assert_eq!(
        recorder.open_session(left, left_live, client(1), epoch(1)),
        SessionOutcome::Opened {
            session_epoch: epoch(1)
        }
    );
    assert_eq!(
        recorder.open_session(left, left_live, client(2), epoch(1)),
        SessionOutcome::Rejected(AdmissionRejection::ClientOutOfRange)
    );
    recorder.assert_agreement(&"scoped sessions");
}

/// A greater epoch clears a client's deduplication state and nothing else. Work
/// the old epoch had accepted still takes effect: a client restart must not
/// silently cancel a command the service accepted.
#[test]
fn a_greater_session_epoch_clears_the_cache_and_never_cancels_accepted_work() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 1);
    recorder.open_session(id, live, client(0), epoch(1));
    recorder.submit(id, live, add(0, 1, 1, 11, 1));

    assert_eq!(
        recorder.open_session(id, live, client(0), epoch(2)),
        SessionOutcome::Replaced {
            session_epoch: epoch(2)
        }
    );
    assert_eq!(
        recorder.open_session(id, live, client(0), epoch(1)),
        SessionOutcome::Rejected(AdmissionRejection::StaleSession { current: epoch(2) })
    );
    assert_eq!(
        recorder.submit(id, live, add(0, 1, 2, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::StaleSession { current: epoch(2) })
    );

    recorder.run(2);
    assert_eq!(
        counter_of(&recorder, id),
        11,
        "the accepted command still ran under the epoch that admitted it"
    );
    // The new epoch starts at sequence one with an empty cache.
    assert_eq!(
        recorder.submit(id, live, add(0, 2, 2, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::SequenceGap {
            expected: sequence(1)
        })
    );
    assert!(matches!(
        recorder.submit(id, live, add(0, 2, 1, 4, 1)),
        AdmissionOutcome::Queued { .. }
    ));
    recorder.run(2);
    assert_eq!(counter_of(&recorder, id), 15);
    recorder.assert_agreement(&"epoch replacement");
}

/// Submitting under an epoch the slot has not opened is refused rather than
/// treated as an implicit open.
#[test]
fn a_future_or_absent_session_is_refused() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 1);

    assert_eq!(
        recorder.submit(id, live, add(0, 1, 1, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::SessionNotOpen)
    );
    recorder.open_session(id, live, client(0), epoch(2));
    assert_eq!(
        recorder.submit(id, live, add(0, 3, 1, 1, 1)),
        AdmissionOutcome::Rejected(AdmissionRejection::FutureSession { current: epoch(2) })
    );
    assert_eq!(
        recorder.open_session(id, live, client(0), epoch(2)),
        SessionOutcome::AlreadyOpen {
            session_epoch: epoch(2)
        }
    );
    recorder.assert_agreement(&"session admission");
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// The counter is the sum of the deltas that reached it, reads see the value at
/// service time, and overflow fails closed rather than saturating.
#[test]
fn counters_sum_their_deltas_and_refuse_to_saturate() {
    let mut recorder = Recorder::new(roomy());
    let id = group(0);
    let live = recorder.open_group(id, 1);
    recorder.open_session(id, live, client(0), epoch(1));

    for (index, amount) in [7_i64, -3, 20].into_iter().enumerate() {
        let seq = u64::try_from(index).expect("three commands fit in u64") + 1;
        recorder.submit(id, live, add(0, 1, seq, amount, 1));
        recorder.run(2);
    }
    assert_eq!(counter_of(&recorder, id), 24);

    recorder.submit(id, live, read(0, 1, 4, 1));
    recorder.run(2);
    assert_eq!(
        recorder.services().last().map(|record| record.result),
        Some(Some(CounterResult::Value { value: 24 }))
    );

    recorder.submit(id, live, add(0, 1, 5, i64::MAX, 1));
    recorder.run(2);
    assert_eq!(
        recorder.services().last().map(|record| record.result),
        Some(Some(CounterResult::Rejected(
            CounterRejection::CounterOverflow { current: 24 }
        ))),
        "an overflow that saturated would satisfy every aggregate check while losing the add"
    );
    assert_eq!(counter_of(&recorder, id), 24);

    // The refusal consumed and cached its sequence like any other outcome.
    assert_eq!(
        recorder.submit(id, live, add(0, 1, 5, i64::MAX, 1)),
        AdmissionOutcome::Replayed {
            result: CounterResult::Rejected(CounterRejection::CounterOverflow { current: 24 })
        }
    );
    recorder.assert_agreement(&"counter arithmetic");
}

/// Counters are per group and share nothing. Work and failure in one group do
/// not reach another.
#[test]
fn counters_are_independent_across_groups() {
    let mut recorder = Recorder::new(config(4, 2, 2, 16, 64));
    let ids = [group(0), group(1), group(2)];
    for (index, id) in ids.iter().enumerate() {
        let live = recorder.open_group(*id, 2);
        recorder.open_session(*id, live, client(0), epoch(1));
        let amount = (i64::try_from(index).expect("three groups fit in i64") + 1) * 100;
        recorder.submit(*id, live, add(0, 1, 1, amount, 1));
    }
    recorder.run(6);

    assert_eq!(counter_of(&recorder, ids[0]), 100);
    assert_eq!(counter_of(&recorder, ids[1]), 200);
    assert_eq!(counter_of(&recorder, ids[2]), 300);
    recorder.assert_agreement(&"independence");
}

// ---------------------------------------------------------------------------
// Bounded schema
// ---------------------------------------------------------------------------

#[test]
fn zero_is_unrepresentable_where_it_would_mean_nothing() {
    assert_eq!(
        Delta::new(0),
        None,
        "a delta that changes nothing is not a mutation"
    );
    assert_eq!(
        WorkQuota::new(0),
        None,
        "a zero quota is starvation in configuration's clothing"
    );
    assert_eq!(
        ServiceCost::new(0),
        None,
        "free work never releases its worker"
    );
    assert_eq!(support::sequence(1).get(), 1);
    assert_eq!(WorkId::new(0), None);
    assert_eq!(rafter_reference_sharded_counter::PassIndex::new(0), None);
    assert_eq!(
        rafter_reference_sharded_counter::GroupIncarnation::new(0),
        None
    );
    assert_eq!(
        rafter_reference_sharded_counter::GroupIncarnation::new(u32::MAX)
            .and_then(rafter_reference_sharded_counter::GroupIncarnation::successor),
        None,
        "an exhausted incarnation space fails closed rather than wrapping onto a retired one"
    );
    assert_eq!(
        support::sequence(u64::MAX).successor(),
        None,
        "an exhausted sequence space fails closed rather than wrapping onto a cached completion"
    );
}

#[test]
fn configuration_bounds_reject_the_states_they_would_make_unreachable() {
    assert_eq!(
        SchedulerConfig::new(0, 1, 1, 1, 1),
        Err(SchedulerConfigError::ZeroGroups)
    );
    assert_eq!(
        SchedulerConfig::new(1, 0, 1, 1, 1),
        Err(SchedulerConfigError::ZeroWorkers)
    );
    assert_eq!(
        SchedulerConfig::new(1, 1, 0, 1, 1),
        Err(SchedulerConfigError::ZeroClients)
    );
    assert_eq!(
        SchedulerConfig::new(1, 1, 1, 0, 1),
        Err(SchedulerConfigError::ZeroGroupQueue)
    );
    assert_eq!(
        SchedulerConfig::new(1, 1, 1, 8, 4),
        Err(SchedulerConfigError::GlobalQueueBelowGroupQueue),
        "a group bound the host can never reach would hide which limit a workload hits"
    );
}

/// A configuration that permits a large number of groups costs nothing until
/// the groups exist, and an idle host does no work proportional to its bound.
#[test]
fn a_large_group_bound_costs_nothing_until_the_groups_exist() {
    let mut scheduler = ManagedScheduler::new(config(1_000_000, 4, 2, 16, 4096));
    assert_eq!(scheduler.summary().live_groups, 0);
    assert_eq!(scheduler.view().groups.len(), 0);

    let report = scheduler.step(&[]);
    assert_eq!(report.progress, PassProgress::Idle);
    assert!(report.pass.is_none(), "an idle tick arms nothing");

    scheduler.lifecycle(group(999_999), create(1));
    assert_eq!(scheduler.view().groups.len(), 1);
    assert_eq!(
        scheduler.lifecycle(group(1_000_000), create(1)).outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::GroupOutOfRange)
    );
}

// ---------------------------------------------------------------------------
// History vocabulary
// ---------------------------------------------------------------------------

/// The vocabulary represents completion, refusal, and both kinds of lost
/// outcome, and keeps the two kinds apart.
#[test]
fn history_vocabulary_represents_completion_rejection_and_lost_outcomes() {
    let operation_id = OperationId::new(11);
    let operation = Operation::Submit {
        group: group(2),
        incarnation: first(),
        work: add(0, 1, 1, 5, 1),
    };
    let events = [
        HistoryEvent::Invoked {
            operation_id,
            operation,
        },
        HistoryEvent::Completed {
            operation_id,
            outcome: OperationOutcome::Admission(AdmissionOutcome::Rejected(
                AdmissionRejection::SessionNotOpen,
            )),
        },
        HistoryEvent::Unknown { operation_id },
        HistoryEvent::NotAdmitted { operation_id },
    ];

    assert!(events
        .iter()
        .all(|event| event.operation_id() == Some(operation_id)));
    assert_eq!(operation.group(), group(2));
    assert!(operation.request_identity().is_some());
    assert!(events[0].is_request() && !events[0].is_observation());
    assert!(events[1].is_observation() && !events[1].is_request());
    // The two lost-outcome events are separate terminal claims. Collapsing them
    // would let a provable refusal be read as a possible effect.
    assert_ne!(events[2], events[3]);

    let scheduling = HistoryEvent::PassCompleted {
        pass: pass(3),
        tick: support::tick(9),
    };
    assert_eq!(scheduling.operation_id(), None);
    assert_eq!(scheduling.pass(), Some(pass(3)));
    assert!(!scheduling.is_request() && !scheduling.is_observation());
}

/// The oracle folds requests and decisions and ignores conclusions, so a
/// history whose observations are all wrong still replays to the truth.
#[test]
fn the_oracle_ignores_the_conclusions_it_is_meant_to_be_checking() {
    let bounds = roomy();
    let mut recorder = Recorder::new(bounds);
    let id = group(0);
    let live = recorder.open_group(id, 2);
    recorder.open_session(id, live, client(0), epoch(1));
    recorder.submit(id, live, add(0, 1, 1, 42, 1));
    recorder.run(2);
    let truth = recorder.oracle().replay();

    let mut corrupted = ReferenceScheduler::new(bounds);
    for event in recorder.oracle().history() {
        corrupted.observe(match event {
            HistoryEvent::Completed { operation_id, .. } => HistoryEvent::Completed {
                operation_id: *operation_id,
                outcome: OperationOutcome::Serviced(Some(CounterResult::Value { value: -1 })),
            },
            other => other.clone(),
        });
    }

    assert_eq!(corrupted.view(), truth.view);
    assert_eq!(corrupted.summary(), truth.summary);
    assert_eq!(corrupted.replay().services, truth.services);
    assert!(!corrupted.is_empty() && corrupted.len() == recorder.oracle().len());
    assert_eq!(
        recorder.scheduler().counter(id),
        Some(42),
        "a lie in the history's observations cannot move the replayed counter"
    );
}

/// Availability reports are sticky and are the only external input the
/// scheduler does not derive.
#[test]
fn availability_reports_are_sticky_across_ticks() {
    let mut recorder = Recorder::new(config(2, 1, 2, 8, 16));
    let id = group(0);
    recorder.open_group(id, 1);
    recorder.submit(id, first(), system(SystemClass::Bulk, 1));
    recorder.step(&[ReadinessSignal {
        group: id,
        availability: GroupAvailability::Stalled,
    }]);

    for _ in 0..4 {
        let report = recorder.step(&[]);
        assert_eq!(report.progress, PassProgress::Idle);
        assert!(recorder.scheduler().ready_groups().is_empty());
    }
    assert_eq!(recorder.scheduler().summary().queued, 1);

    recorder.step(&[ReadinessSignal::available(id)]);
    recorder.run(2);
    assert_eq!(recorder.scheduler().summary().serviced, 1);
    recorder.assert_agreement(&"sticky availability");
}
