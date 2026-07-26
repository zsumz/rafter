//! Shared construction and recording helpers for this crate's tests.
//!
//! Nothing here decides anything the contract cares about. The builders name
//! bounded values, [`Recorder`] drives the scheduler and writes down what it
//! did, and [`UnfairScheduler`] is the negative control that proves the audit
//! has teeth.

// Two suites declare this module — `model_contract` and `differential` — and
// each compiles its own copy of the whole thing while using a different subset.
// `dependency_boundary` reads manifests only and does not declare it at all.
// The unused half of each copy is the cost of that arrangement, not dead code.
#![allow(dead_code)]

use std::{collections::BTreeMap, fmt};

use rafter_reference_sharded_counter::{
    AdmissionOutcome, ClientId, CounterCommand, Delta, FailureRecord, GroupId, GroupIncarnation,
    HistoryEvent, LifecycleRequest, LifecycleTransition, ManagedScheduler, Offer, OfferOutcome,
    Operation, OperationId, OperationOutcome, PassIndex, PassProgress, ReadinessSignal,
    ReferenceScheduler, RequestFingerprint, RequestIdentity, SchedulerConfig, SchedulingViolation,
    Sequence, ServiceCost, ServiceRecord, SessionEpoch, SessionOutcome, SystemClass, TickIndex,
    TickReport, Work, WorkId, WorkQuota,
};

/// Builds a scheduler configuration, panicking on inadmissible bounds.
#[must_use]
pub fn config(
    max_groups: u32,
    workers: u32,
    max_clients_per_group: u32,
    max_group_queue: u32,
    max_global_queue: u32,
) -> SchedulerConfig {
    SchedulerConfig::new(
        max_groups,
        workers,
        max_clients_per_group,
        max_group_queue,
        max_global_queue,
    )
    .expect("test configurations are admissible")
}

#[must_use]
pub const fn group(id: u32) -> GroupId {
    GroupId::new(id)
}

#[must_use]
pub fn incarnation(value: u32) -> GroupIncarnation {
    GroupIncarnation::new(value).expect("test incarnations are nonzero")
}

#[must_use]
pub const fn first() -> GroupIncarnation {
    GroupIncarnation::first()
}

#[must_use]
pub const fn client(id: u32) -> ClientId {
    ClientId::new(id)
}

#[must_use]
pub fn epoch(value: u64) -> SessionEpoch {
    SessionEpoch::new(value).expect("test epochs are nonzero")
}

#[must_use]
pub fn sequence(value: u64) -> Sequence {
    Sequence::new(value).expect("test sequences are nonzero")
}

#[must_use]
pub fn quota(value: u32) -> WorkQuota {
    WorkQuota::new(value).expect("test quotas are nonzero")
}

#[must_use]
pub fn cost(value: u32) -> ServiceCost {
    ServiceCost::new(value).expect("test costs are nonzero")
}

#[must_use]
pub fn delta(value: i64) -> Delta {
    Delta::new(value).expect("test deltas are nonzero")
}

#[must_use]
pub fn work(value: u64) -> WorkId {
    WorkId::new(value).expect("test work identifiers are nonzero")
}

#[must_use]
pub fn pass(value: u64) -> PassIndex {
    PassIndex::new(value).expect("test pass indices are nonzero")
}

#[must_use]
pub const fn tick(value: u64) -> TickIndex {
    TickIndex::new(value)
}

#[must_use]
pub fn create(work_quota: u32) -> LifecycleRequest {
    LifecycleRequest::Create {
        quota: quota(work_quota),
    }
}

/// Builds a counter submission whose fingerprint describes its own command.
#[must_use]
pub fn counter(
    client_id: u32,
    session_epoch: u64,
    seq: u64,
    command: CounterCommand,
    service_cost: u32,
) -> Work {
    Work::Counter {
        request: RequestIdentity {
            client_id: client(client_id),
            session_epoch: epoch(session_epoch),
            sequence: sequence(seq),
            fingerprint: RequestFingerprint::of(&command),
        },
        command,
        cost: cost(service_cost),
    }
}

/// Builds a counter submission carrying a fingerprint of the caller's choosing.
#[must_use]
pub fn counter_with_fingerprint(
    client_id: u32,
    session_epoch: u64,
    seq: u64,
    fingerprint: RequestFingerprint,
    command: CounterCommand,
    service_cost: u32,
) -> Work {
    Work::Counter {
        request: RequestIdentity {
            client_id: client(client_id),
            session_epoch: epoch(session_epoch),
            sequence: sequence(seq),
            fingerprint,
        },
        command,
        cost: cost(service_cost),
    }
}

#[must_use]
pub fn add(client_id: u32, session_epoch: u64, seq: u64, amount: i64, service_cost: u32) -> Work {
    counter(
        client_id,
        session_epoch,
        seq,
        CounterCommand::Add {
            delta: delta(amount),
        },
        service_cost,
    )
}

#[must_use]
pub fn read(client_id: u32, session_epoch: u64, seq: u64, service_cost: u32) -> Work {
    counter(
        client_id,
        session_epoch,
        seq,
        CounterCommand::Read,
        service_cost,
    )
}

#[must_use]
pub fn system(class: SystemClass, service_cost: u32) -> Work {
    Work::System {
        class,
        cost: cost(service_cost),
    }
}

#[must_use]
pub fn faulty(class: SystemClass, service_cost: u32) -> Work {
    Work::Faulty {
        class,
        cost: cost(service_cost),
    }
}

/// Records everything a scheduler was asked to do and everything it decided.
///
/// The recorder writes one history in the canonical order — external signals,
/// then worker releases, then the plan, then the turns and the work each took,
/// then the plan retiring — and feeds it to a [`ReferenceScheduler`] as it goes.
///
/// The model's answers do reach that history, as the observation events a real
/// caller would have seen. They cannot influence anything the oracle derives,
/// because the oracle folds no observation at all — which is the property
/// `the_oracle_ignores_the_conclusions_it_is_meant_to_be_checking` asserts. The
/// copies used for comparison are the separate ones in `outcomes`, `services`,
/// and `failures`, kept beside the history rather than read back out of it.
#[derive(Clone)]
pub struct Recorder {
    scheduler: ManagedScheduler,
    oracle: ReferenceScheduler,
    next_operation: u64,
    outcomes: Vec<OperationOutcome>,
    services: Vec<ServiceRecord>,
    failures: Vec<FailureRecord>,
    queued: BTreeMap<WorkId, OperationId>,
}

impl fmt::Debug for Recorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Recorder")
            .field("tick", &self.scheduler.tick())
            .field("events", &self.oracle.len())
            .finish_non_exhaustive()
    }
}

impl Recorder {
    #[must_use]
    pub fn new(bounds: SchedulerConfig) -> Self {
        Self {
            scheduler: ManagedScheduler::new(bounds),
            oracle: ReferenceScheduler::new(bounds),
            next_operation: 0,
            outcomes: Vec::new(),
            services: Vec::new(),
            failures: Vec::new(),
            queued: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn scheduler(&self) -> &ManagedScheduler {
        &self.scheduler
    }

    #[must_use]
    pub const fn oracle(&self) -> &ReferenceScheduler {
        &self.oracle
    }

    #[must_use]
    pub fn services(&self) -> &[ServiceRecord] {
        &self.services
    }

    #[must_use]
    pub fn failures(&self) -> &[FailureRecord] {
        &self.failures
    }

    fn record(&mut self, event: HistoryEvent) {
        self.oracle.observe(event);
    }

    fn invoke(&mut self, operation: Operation) -> OperationId {
        self.next_operation += 1;
        let operation_id = OperationId::new(self.next_operation);
        self.record(HistoryEvent::Invoked {
            operation_id,
            operation,
        });
        operation_id
    }

    pub fn lifecycle(&mut self, id: GroupId, request: LifecycleRequest) -> LifecycleTransition {
        let operation_id = self.invoke(Operation::Lifecycle { group: id, request });
        let transition = self.scheduler.lifecycle(id, request);
        let outcome = OperationOutcome::Lifecycle(transition.outcome);
        self.outcomes.push(outcome);
        self.record(HistoryEvent::Completed {
            operation_id,
            outcome,
        });
        for failure in transition.failed.clone() {
            self.failures.push(failure);
            if let Some(origin) = self.queued.remove(&failure.work) {
                self.record(HistoryEvent::Completed {
                    operation_id: origin,
                    outcome: OperationOutcome::Failed(failure.reason),
                });
            }
        }
        transition
    }

    pub fn open_session(
        &mut self,
        id: GroupId,
        group_incarnation: GroupIncarnation,
        client_id: ClientId,
        session_epoch: SessionEpoch,
    ) -> SessionOutcome {
        let operation_id = self.invoke(Operation::OpenSession {
            group: id,
            incarnation: group_incarnation,
            client_id,
            session_epoch,
        });
        let result = self
            .scheduler
            .open_session(id, group_incarnation, client_id, session_epoch);
        let outcome = OperationOutcome::Session(result);
        self.outcomes.push(outcome);
        self.record(HistoryEvent::Completed {
            operation_id,
            outcome,
        });
        result
    }

    pub fn submit(
        &mut self,
        id: GroupId,
        group_incarnation: GroupIncarnation,
        item: Work,
    ) -> AdmissionOutcome {
        let operation_id = self.invoke(Operation::Submit {
            group: id,
            incarnation: group_incarnation,
            work: item,
        });
        let result = self.scheduler.submit(id, group_incarnation, item);
        let outcome = OperationOutcome::Admission(result);
        self.outcomes.push(outcome);
        match result {
            // A queue slot is not a terminal outcome. The client learns what
            // its command did when the work is serviced or retired.
            AdmissionOutcome::Queued { work: id } => {
                self.queued.insert(id, operation_id);
            }
            _ => self.record(HistoryEvent::Completed {
                operation_id,
                outcome,
            }),
        }
        result
    }

    pub fn step(&mut self, signals: &[ReadinessSignal]) -> TickReport {
        let report = self.scheduler.step(signals);
        for signal in signals {
            self.record(HistoryEvent::AvailabilityReported {
                tick: report.tick,
                group: signal.group,
                availability: signal.availability,
            });
        }
        for released in report.released.clone() {
            self.record(HistoryEvent::WorkerReleased {
                tick: report.tick,
                group: released,
            });
        }
        let Some(current) = report.pass else {
            return report;
        };
        if let Some(plan) = report.armed.clone() {
            self.record(HistoryEvent::PassArmed {
                pass: current,
                tick: report.tick,
                plan,
            });
        }

        // Service records are flat and in dispatch order, so each turn claims
        // exactly the number of them it says it serviced.
        let mut serviced = report.serviced.iter().copied();
        for Offer { group: id, outcome } in report.offers.clone() {
            self.record(HistoryEvent::GroupOffered {
                pass: current,
                group: id,
                outcome,
            });
            for _ in 0..outcome.serviced() {
                let record = serviced
                    .next()
                    .expect("a turn reports exactly the work it serviced");
                self.record(HistoryEvent::WorkServiced {
                    pass: current,
                    group: record.group,
                    work: record.work,
                });
                self.services.push(record);
                if let Some(origin) = self.queued.remove(&record.work) {
                    self.record(HistoryEvent::Completed {
                        operation_id: origin,
                        outcome: OperationOutcome::Serviced(record.result),
                    });
                }
            }
        }
        assert!(
            serviced.next().is_none(),
            "a tick serviced work no turn accounted for"
        );
        if report.progress == PassProgress::Completed {
            self.record(HistoryEvent::PassCompleted {
                pass: current,
                tick: report.tick,
            });
        }
        report
    }

    /// Runs `ticks` plain ticks with no external readiness reports.
    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step(&[]);
        }
    }

    /// Asserts that the scheduler and the independent replay agree on
    /// everything either can observe, and that the decisions were fair.
    ///
    /// `context` is formatted only on failure, so a checkpoint costs nothing
    /// while it passes.
    pub fn assert_agreement(&self, context: &dyn fmt::Debug) {
        let replay = self.oracle.replay();
        assert_eq!(
            self.scheduler.view(),
            replay.view,
            "state disagreement after {context:?}"
        );
        assert_eq!(
            self.scheduler.summary(),
            replay.summary,
            "summary disagreement after {context:?}"
        );
        assert_eq!(
            self.outcomes, replay.outcomes,
            "outcome disagreement after {context:?}"
        );
        assert_eq!(
            self.services, replay.services,
            "service disagreement after {context:?}"
        );
        assert_eq!(
            self.failures, replay.failures,
            "failure disagreement after {context:?}"
        );
        if let Err(violation) = replay.fairness {
            panic!("scheduling violation after {context:?}: {violation:?}");
        }
    }

    /// Creates, recovers, and serves a group, returning its incarnation.
    pub fn open_group(&mut self, id: GroupId, work_quota: u32) -> GroupIncarnation {
        let transition = self.lifecycle(id, create(work_quota));
        self.lifecycle(id, LifecycleRequest::Recover);
        self.lifecycle(id, LifecycleRequest::Serve);
        match transition.outcome {
            rafter_reference_sharded_counter::LifecycleOutcome::Created { incarnation } => {
                incarnation
            }
            other => panic!("expected a creation, observed {other:?}"),
        }
    }
}

/// A deliberately unfair scheduler, written down as the decisions it makes.
///
/// This variant always plans the group with the most queued work and nothing
/// else, which is the shape a throughput-chasing scheduler naturally takes and
/// the shape the fairness bound exists to forbid. It is expressed as a history
/// rather than as a second scheduler because the bound is a property of
/// decisions, and a history is exactly a record of decisions.
///
/// Everything else it does is correct: its passes are ordered, its turns are
/// taken once, its quota is respected, and its work is serviced in priority and
/// arrival order. That is deliberate. A negative control that broke several
/// rules at once would not show which rule the audit caught.
pub struct UnfairScheduler {
    favored: GroupId,
    starved: GroupId,
    passes: u32,
    quota: WorkQuota,
    events: Vec<HistoryEvent>,
    next_operation: u64,
    now: u64,
}

impl fmt::Debug for UnfairScheduler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnfairScheduler")
            .field("favored", &self.favored)
            .field("starved", &self.starved)
            .field("passes", &self.passes)
            .finish_non_exhaustive()
    }
}

impl UnfairScheduler {
    /// Runs the unfair variant and returns the history it produced.
    #[must_use]
    pub fn run(favored: GroupId, starved: GroupId, passes: u32, work_quota: u32) -> Self {
        let mut variant = Self {
            favored,
            starved,
            passes,
            quota: quota(work_quota),
            events: Vec::new(),
            next_operation: 0,
            now: 0,
        };
        variant.build();
        variant
    }

    #[must_use]
    pub fn history(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Returns the violation this history must produce.
    #[must_use]
    pub fn expected_violation(&self) -> SchedulingViolation {
        SchedulingViolation::OpportunityGap {
            group: self.starved,
            from_pass: PassIndex::first(),
            denied_passes: self.passes,
        }
    }

    fn invoke(&mut self, operation: Operation) {
        self.next_operation += 1;
        self.events.push(HistoryEvent::Invoked {
            operation_id: OperationId::new(self.next_operation),
            operation,
        });
    }

    fn build(&mut self) {
        for id in [self.favored, self.starved] {
            self.invoke(Operation::Lifecycle {
                group: id,
                request: LifecycleRequest::Create { quota: self.quota },
            });
            self.invoke(Operation::Lifecycle {
                group: id,
                request: LifecycleRequest::Recover,
            });
            self.invoke(Operation::Lifecycle {
                group: id,
                request: LifecycleRequest::Serve,
            });
        }

        // The favored group is given enough work to fill its quota in every
        // pass, so it is legitimately ready throughout and the audit has no
        // structural complaint to make instead of the fairness one.
        let backlog = self.passes * self.quota.get();
        for _ in 0..backlog {
            self.invoke(Operation::Submit {
                group: self.favored,
                incarnation: GroupIncarnation::first(),
                work: system(SystemClass::Bulk, 1),
            });
        }
        self.invoke(Operation::Submit {
            group: self.starved,
            incarnation: GroupIncarnation::first(),
            work: system(SystemClass::Bulk, 1),
        });

        let mut next_work = 1_u64;
        for index in 0..self.passes {
            self.now += 1;
            let current = pass(u64::from(index) + 1);
            if index > 0 {
                self.events.push(HistoryEvent::WorkerReleased {
                    tick: TickIndex::new(self.now),
                    group: self.favored,
                });
            }
            self.events.push(HistoryEvent::PassArmed {
                pass: current,
                tick: TickIndex::new(self.now),
                plan: vec![self.favored],
            });
            self.events.push(HistoryEvent::GroupOffered {
                pass: current,
                group: self.favored,
                outcome: OfferOutcome::Dispatched {
                    serviced: self.quota.get(),
                    cost: self.quota.get(),
                },
            });
            for _ in 0..self.quota.get() {
                self.events.push(HistoryEvent::WorkServiced {
                    pass: current,
                    group: self.favored,
                    work: work(next_work),
                });
                next_work += 1;
            }
            self.events.push(HistoryEvent::PassCompleted {
                pass: current,
                tick: TickIndex::new(self.now),
            });
        }
    }
}
