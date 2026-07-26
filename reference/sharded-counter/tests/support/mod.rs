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
    AdmissionOutcome, ClientId, CounterCommand, Delta, FailureRecord, FairnessReport, FaultSite,
    GroupAvailability, GroupId, GroupIncarnation, HistoryEvent, LifecycleRequest,
    LifecycleTransition, ManagedScheduler, Offer, OfferOutcome, Operation, OperationId,
    OperationOutcome, PassIndex, PassProgress, ReadinessSignal, ReferenceScheduler, Replay,
    RequestFingerprint, RequestIdentity, SchedulerConfig, SchedulingViolation, Sequence,
    ServiceCost, ServiceRecord, SessionEpoch, SessionOutcome, SkipReason, SystemClass, TickIndex,
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
                tick: report.tick,
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

/// A history written by hand, decision by decision.
///
/// [`Recorder`] can only produce histories the model produces, which is
/// precisely why it cannot express the ones worth auditing: a scheduler that
/// omits a release, prices a turn wrongly, or arms two plans in one tick. This
/// builder writes those down directly, so the oracle can be asked what it makes
/// of decisions no correct scheduler would take.
///
/// It decides nothing. Every method appends the event it names, exactly as
/// named — a builder that corrected its caller would be unable to state the
/// case under test.
#[derive(Clone, Default)]
pub struct History {
    events: Vec<HistoryEvent>,
    next_operation: u64,
}

impl fmt::Debug for History {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("History")
            .field("events", &self.events.len())
            .finish_non_exhaustive()
    }
}

impl History {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Folds the history under `bounds` and returns the audit's verdict.
    pub fn audit(&self, bounds: SchedulerConfig) -> Result<FairnessReport, SchedulingViolation> {
        self.replay(bounds).fairness
    }

    /// Folds the history under `bounds` and returns everything it implies.
    #[must_use]
    pub fn replay(&self, bounds: SchedulerConfig) -> Replay {
        let mut oracle = ReferenceScheduler::new(bounds);
        oracle.observe_all(self.events.iter().cloned());
        oracle.replay()
    }

    pub fn invoke(&mut self, operation: Operation) -> &mut Self {
        self.next_operation += 1;
        self.events.push(HistoryEvent::Invoked {
            operation_id: OperationId::new(self.next_operation),
            operation,
        });
        self
    }

    /// Creates, recovers, and serves a group.
    pub fn open_group(&mut self, id: GroupId, work_quota: u32) -> &mut Self {
        for request in [
            create(work_quota),
            LifecycleRequest::Recover,
            LifecycleRequest::Serve,
        ] {
            self.invoke(Operation::Lifecycle { group: id, request });
        }
        self
    }

    pub fn submit(&mut self, id: GroupId, item: Work) -> &mut Self {
        self.invoke(Operation::Submit {
            group: id,
            incarnation: GroupIncarnation::first(),
            work: item,
        })
    }

    pub fn armed(&mut self, index: u64, at: u64, plan: Vec<GroupId>) -> &mut Self {
        self.events.push(HistoryEvent::PassArmed {
            pass: pass(index),
            tick: TickIndex::new(at),
            plan,
        });
        self
    }

    pub fn dispatched(
        &mut self,
        index: u64,
        at: u64,
        id: GroupId,
        serviced: u32,
        turn_cost: u64,
    ) -> &mut Self {
        self.events.push(HistoryEvent::GroupOffered {
            pass: pass(index),
            tick: TickIndex::new(at),
            group: id,
            outcome: OfferOutcome::Dispatched {
                serviced,
                cost: turn_cost,
            },
        });
        self
    }

    /// Records a turn the group was offered and could not take.
    pub fn skipped(&mut self, index: u64, at: u64, id: GroupId) -> &mut Self {
        self.events.push(HistoryEvent::GroupOffered {
            pass: pass(index),
            tick: TickIndex::new(at),
            group: id,
            outcome: OfferOutcome::Skipped(SkipReason::Stalled),
        });
        self
    }

    pub fn serviced(&mut self, index: u64, id: GroupId, item: u64) -> &mut Self {
        self.events.push(HistoryEvent::WorkServiced {
            pass: pass(index),
            group: id,
            work: work(item),
        });
        self
    }

    pub fn reported(&mut self, at: u64, id: GroupId, availability: GroupAvailability) -> &mut Self {
        self.events.push(HistoryEvent::AvailabilityReported {
            tick: TickIndex::new(at),
            group: id,
            availability,
        });
        self
    }

    pub fn released(&mut self, at: u64, id: GroupId) -> &mut Self {
        self.events.push(HistoryEvent::WorkerReleased {
            tick: TickIndex::new(at),
            group: id,
        });
        self
    }

    pub fn retired(&mut self, index: u64, at: u64) -> &mut Self {
        self.events.push(HistoryEvent::PassCompleted {
            pass: pass(index),
            tick: TickIndex::new(at),
        });
        self
    }
}

/// One rule the audit enforces, and the decision that breaks exactly it.
///
/// [`RedTeam`] turns each of these into two histories that differ in one
/// decision: the cheating one, which must produce the named fault, and the
/// control, which must be accepted. That pairing is the point. `ManagedScheduler`
/// always services what it dispatches, always releases what it takes, and always
/// plans what is ready, so **no history the [`Recorder`] can produce
/// distinguishes a rule the audit checks from one it ignores**. A check with no
/// deliberate violator is a check whose positive control is its own vacuity, and
/// that is how a whole generation of "the audit derives the occupancy" survived
/// while the audit derived it from work that was never done.
#[allow(
    clippy::manual_non_exhaustive,
    reason = "the marker's discriminant is the family size, which `#[non_exhaustive]` \
              does not provide — and `#[non_exhaustive]` would force a wildcard arm on \
              every match over this type, which is the exact guarantee it exists to give"
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Cheat {
    /// Plan the busy group and leave a ready one out.
    StarveAReadyGroup,
    /// Arm a fresh plan while the last one still owes a turn.
    ArmOverAnOpenPlan,
    /// Number a pass something other than its predecessor's successor.
    SkipAPassIndex,
    /// Name a group in a plan that was not ready when the plan was armed.
    PlanAGroupThatIsNotReady,
    /// Name one group twice in one plan.
    NameAGroupTwiceInOnePlan,
    /// Hand a turn to a group the plan did not name.
    OfferAGroupThePlanDidNotName,
    /// Hand one group two turns in one pass.
    OfferAGroupTwiceInOnePass,
    /// Retire a plan that still owes a group its turn.
    RetireAPlanWithATurnOwing,
    /// Dispatch a group an external report stalled after the plan was armed.
    DispatchAGroupThatIsNotReady,
    /// Skip a group as stalled when nothing reported it stalled.
    SkipAGroupThatIsAvailable,
    /// Service more items in one turn than the group's quota allows.
    ServiceMoreThanTheQuota,
    /// Service a turn's items out of arrival order.
    ServiceOutOfArrivalOrder,
    /// Claim a different number of items than the turn recorded.
    MiscountTheWorkATurnDid,
    /// End a turn with the work it was offered against still queued.
    LeaveOwedWorkUnserviced,
    /// Service past the end of the work a turn was offered against.
    ServicePastTheEndOfATurn,
    /// Charge a worker something other than what the turn's items were worth.
    MisreportTheTurnsCost,
    /// Run the history past the tick an occupancy was due back at.
    HoldAWorkerPastItsCost,
    /// Report a worker free before the turn holding it was paid for.
    ReleaseAWorkerBeforeItsCostIsPaid,
    /// Report a worker free for a group that is holding none.
    ReleaseAWorkerNobodyTook,
    /// Run the same decisions out of a pool one worker short of holding them.
    TakeMoreWorkersThanThePoolHas,
    /// Record a tick earlier than one already recorded.
    WalkTheClockBackwards,
    /// Arm a second plan within one tick.
    ArmTwoPlansInOneTick,
    /// Record work serviced when no turn was open to service it.
    ServiceWorkOutsideAnyDispatch,
    /// Report a worker free a tick after its turn's cost came due.
    ///
    /// Produces the same [`SchedulingViolation`] value as
    /// [`Self::HoldAWorkerPastItsCost`] from a different check, which is the
    /// whole reason controls are indexed by site rather than by variant.
    ReleaseAWorkerAfterItsCostIsPaid,
    /// Retire a second plan within one tick.
    RetireTwoPlansInOneTick,
    /// Retire a plan when none is open.
    RetireAPassWithNoPlanOpen,
    /// Retire a plan other than the one that is open.
    RetireAPassOtherThanTheOpenOne,
    /// **Not a cheat.** The end marker, and the only thing that makes
    /// [`Self::ALL`] closed under adding one.
    ///
    /// Its discriminant *is* the size of the family, so a cheat added above it
    /// changes `ALL`'s length and fails the const check below unless it is also
    /// threaded onto [`Self::next`]. It must stay last, and `ALL` never
    /// contains it. A variant declared *after* it escapes — a statement that
    /// this marker is not the end, rather than an omission.
    ///
    /// The literal array this replaced was closed by nothing: a twenty-fourth
    /// variant left out of `[Self; 23]` compiled, ran, and was never exercised,
    /// while the suite above it asserted the family covered what it claimed to.
    #[doc(hidden)]
    EndOfFamily,
}

impl Cheat {
    /// The number of cheats, taken from the end marker's discriminant.
    pub const COUNT: usize = Self::EndOfFamily as usize;

    /// Every cheat, so a suite can run the whole family rather than a sample.
    pub const ALL: [Self; Self::COUNT] = Self::all();

    /// The next cheat in declaration order, or `None` past the last.
    ///
    /// Exhaustive, with no catch-all: a cheat added without an arm here does
    /// not compile.
    const fn next(self) -> Option<Self> {
        Some(match self {
            Self::StarveAReadyGroup => Self::ArmOverAnOpenPlan,
            Self::ArmOverAnOpenPlan => Self::SkipAPassIndex,
            Self::SkipAPassIndex => Self::PlanAGroupThatIsNotReady,
            Self::PlanAGroupThatIsNotReady => Self::NameAGroupTwiceInOnePlan,
            Self::NameAGroupTwiceInOnePlan => Self::OfferAGroupThePlanDidNotName,
            Self::OfferAGroupThePlanDidNotName => Self::OfferAGroupTwiceInOnePass,
            Self::OfferAGroupTwiceInOnePass => Self::RetireAPlanWithATurnOwing,
            Self::RetireAPlanWithATurnOwing => Self::DispatchAGroupThatIsNotReady,
            Self::DispatchAGroupThatIsNotReady => Self::SkipAGroupThatIsAvailable,
            Self::SkipAGroupThatIsAvailable => Self::ServiceMoreThanTheQuota,
            Self::ServiceMoreThanTheQuota => Self::ServiceOutOfArrivalOrder,
            Self::ServiceOutOfArrivalOrder => Self::MiscountTheWorkATurnDid,
            Self::MiscountTheWorkATurnDid => Self::LeaveOwedWorkUnserviced,
            Self::LeaveOwedWorkUnserviced => Self::ServicePastTheEndOfATurn,
            Self::ServicePastTheEndOfATurn => Self::MisreportTheTurnsCost,
            Self::MisreportTheTurnsCost => Self::HoldAWorkerPastItsCost,
            Self::HoldAWorkerPastItsCost => Self::ReleaseAWorkerBeforeItsCostIsPaid,
            Self::ReleaseAWorkerBeforeItsCostIsPaid => Self::ReleaseAWorkerNobodyTook,
            Self::ReleaseAWorkerNobodyTook => Self::TakeMoreWorkersThanThePoolHas,
            Self::TakeMoreWorkersThanThePoolHas => Self::WalkTheClockBackwards,
            Self::WalkTheClockBackwards => Self::ArmTwoPlansInOneTick,
            Self::ArmTwoPlansInOneTick => Self::ServiceWorkOutsideAnyDispatch,
            Self::ServiceWorkOutsideAnyDispatch => Self::ReleaseAWorkerAfterItsCostIsPaid,
            Self::ReleaseAWorkerAfterItsCostIsPaid => Self::RetireTwoPlansInOneTick,
            Self::RetireTwoPlansInOneTick => Self::RetireAPassWithNoPlanOpen,
            Self::RetireAPassWithNoPlanOpen => Self::RetireAPassOtherThanTheOpenOne,
            Self::RetireAPassOtherThanTheOpenOne | Self::EndOfFamily => return None,
        })
    }

    const fn all() -> [Self; Self::COUNT] {
        let mut cheats = [Self::StarveAReadyGroup; Self::COUNT];
        let mut index = 0;
        let mut current = Some(Self::StarveAReadyGroup);
        while index < Self::COUNT {
            let Some(cheat) = current else { break };
            cheats[index] = cheat;
            current = cheat.next();
            index += 1;
        }
        cheats
    }

    /// The check in the fold that this cheat's one decision must trip.
    ///
    /// Exhaustive with no catch-all, so a cheat added without a site does not
    /// compile — the other half of the closure `control_for` provides.
    #[must_use]
    pub const fn site(self) -> FaultSite {
        match self {
            Self::StarveAReadyGroup => FaultSite::WidestOpportunityGap,
            Self::ArmOverAnOpenPlan => FaultSite::ArmedOverAnOpenPass,
            Self::SkipAPassIndex => FaultSite::ArmedOutOfOrder,
            Self::PlanAGroupThatIsNotReady => FaultSite::PlanNamedAnUnreadyGroup,
            Self::NameAGroupTwiceInOnePlan => FaultSite::PlanRepeatedAGroup,
            Self::OfferAGroupThePlanDidNotName => FaultSite::OfferOutsideThePlan,
            Self::OfferAGroupTwiceInOnePass => FaultSite::OfferedTwiceInOnePass,
            Self::RetireAPlanWithATurnOwing => FaultSite::RetiredWithATurnOwing,
            Self::DispatchAGroupThatIsNotReady => FaultSite::DispatchedWhileUnready,
            Self::SkipAGroupThatIsAvailable => FaultSite::SkippedWhileAvailable,
            Self::ServiceMoreThanTheQuota => FaultSite::DispatchOverQuota,
            Self::ServiceOutOfArrivalOrder => FaultSite::ServiceOutOfOrder,
            Self::MiscountTheWorkATurnDid => FaultSite::TurnMiscountedItsWork,
            Self::LeaveOwedWorkUnserviced => FaultSite::TurnLeftWorkQueued,
            Self::ServicePastTheEndOfATurn => FaultSite::ServicePastTheTurnsWork,
            Self::MisreportTheTurnsCost => FaultSite::TurnMispricedItsWork,
            Self::HoldAWorkerPastItsCost => FaultSite::OccupancyOutlivedItsCost,
            Self::ReleaseAWorkerBeforeItsCostIsPaid => FaultSite::ReleaseBeforeDue,
            Self::ReleaseAWorkerNobodyTook => FaultSite::ReleaseWithoutOccupancy,
            Self::TakeMoreWorkersThanThePoolHas => FaultSite::DispatchOverWorkerPool,
            Self::WalkTheClockBackwards => FaultSite::ClockWalkedBackwards,
            Self::ArmTwoPlansInOneTick => FaultSite::ArmedTickReused,
            Self::ServiceWorkOutsideAnyDispatch => FaultSite::ServiceOutsideATurn,
            Self::ReleaseAWorkerAfterItsCostIsPaid => FaultSite::ReleaseAfterDue,
            Self::RetireTwoPlansInOneTick => FaultSite::RetiredTickReused,
            Self::RetireAPassWithNoPlanOpen => FaultSite::RetiredWithNoPassOpen,
            Self::RetireAPassOtherThanTheOpenOne => FaultSite::RetiredADifferentPass,
            Self::EndOfFamily => panic!("the end marker is not a cheat and is never run"),
        }
    }
}

// `ALL` walks `next` from the first variant, and this is what makes that walk a
// closure claim rather than a hope: every entry must sit at its own declaration
// index, so a chain that stops early, repeats itself, or skips a variant leaves
// a slot holding the wrong cheat and fails to compile.
const _: () = {
    let mut index = 0;
    while index < Cheat::COUNT {
        assert!(
            Cheat::ALL[index] as usize == index,
            "Cheat::next must visit every cheat once, in declaration order"
        );
        index += 1;
    }
};

/// Groups the red-team base history drives.
const RED_TEAM_GROUPS: [u32; 3] = [0, 1, 2];
/// Items each of them is given, two per turn over two passes.
const RED_TEAM_BACKLOG: u64 = 4;
/// Quota every red-team group is created with.
const RED_TEAM_QUOTA: u32 = 2;
/// Workers the base history's three concurrent turns come out of.
const RED_TEAM_WORKERS: u32 = 3;
/// A created slot with no work, so a plan can name a group that exists and is
/// not ready without inventing an unknown one.
const RED_TEAM_EMPTY_SLOT: GroupId = GroupId::new(3);
/// Ticks `MisreportTheTurnsCost` overcharges its worker by.
const MISREPORTED_COST: u64 = 3;

/// A scheduler that breaks exactly one rule, written down as the decisions it
/// makes.
///
/// The base is an honest two-pass history over three groups on a three-worker
/// host: each pass plans all three, each turn services its full quota in arrival
/// order, and every worker is released at exactly the tick its cost comes due.
/// A [`Cheat`] changes one decision in it and nothing else, so the fault the
/// audit reports is attributable to that decision rather than to the shape of
/// the history.
///
/// [`Self::control`] is the same history with that one decision taken
/// correctly, and it must be accepted. Without it a "negative control" proves
/// only that some history somewhere fails; with it, the pair proves that the
/// rule under test is the one doing the work.
///
/// Cheats that need a setup — a stall report, say — carry it in *both*
/// histories, so the difference between them stays one decision wide.
pub struct RedTeam {
    cheat: Option<Cheat>,
    staged: Option<Cheat>,
    bounds: SchedulerConfig,
    history: History,
}

impl fmt::Debug for RedTeam {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedTeam")
            .field("cheat", &self.cheat)
            .field("staged", &self.staged)
            .finish_non_exhaustive()
    }
}

impl RedTeam {
    /// Builds the history in which `cheat` is taken.
    #[must_use]
    pub fn run(cheat: Cheat) -> Self {
        Self::build(Some(cheat), Some(cheat))
    }

    /// Builds the same history with `cheat`'s one decision taken correctly.
    #[must_use]
    pub fn control(cheat: Cheat) -> Self {
        Self::build(None, Some(cheat))
    }

    /// Builds the base history, which cheats nothing and stages nothing.
    #[must_use]
    pub fn honest() -> Self {
        Self::build(None, None)
    }

    #[must_use]
    pub fn bounds(&self) -> SchedulerConfig {
        self.bounds
    }

    #[must_use]
    pub fn history(&self) -> &[HistoryEvent] {
        self.history.events()
    }

    /// Folds the history and returns the audit's verdict.
    pub fn audit(&self) -> Result<FairnessReport, SchedulingViolation> {
        self.history.audit(self.bounds)
    }

    /// Folds the history and returns everything it implies, including which
    /// check reported the fault.
    #[must_use]
    pub fn replay(&self) -> Replay {
        self.history.replay(self.bounds)
    }

    /// Returns the check this history must trip, or `None` for a control.
    ///
    /// A cheat names a site rather than only a violation because five of the
    /// fold's checks report a variant another check already reports. Asserting
    /// the variant alone cannot tell a control for one from a control for the
    /// other, which is how three checks came to have none.
    #[must_use]
    pub fn expected_site(&self) -> Option<FaultSite> {
        self.cheat.map(Cheat::site)
    }

    /// Returns the fault this history must produce, or `None` for a control.
    #[must_use]
    pub fn expected_violation(&self) -> Option<SchedulingViolation> {
        let cheat = self.cheat?;
        Some(
            Self::planning_fault(cheat)
                .or_else(|| Self::turn_fault(cheat))
                .expect("every cheat names the one fault it must produce"),
        )
    }

    /// The faults about which groups a pass planned, offered, and retired with.
    fn planning_fault(cheat: Cheat) -> Option<SchedulingViolation> {
        let (first, third) = (group(0), group(2));
        Some(match cheat {
            Cheat::StarveAReadyGroup => SchedulingViolation::OpportunityGap {
                group: third,
                from_pass: pass(2),
                denied_passes: 1,
            },
            Cheat::ArmOverAnOpenPlan => SchedulingViolation::PassArmedWhileOpen {
                open: pass(1),
                armed: pass(2),
            },
            // Three checks report pass ordering — one at the arming and two at
            // the retirement — and all three answer with these exact fields.
            Cheat::SkipAPassIndex
            | Cheat::RetireAPassWithNoPlanOpen
            | Cheat::RetireAPassOtherThanTheOpenOne => SchedulingViolation::PassOutOfOrder {
                expected: pass(2),
                observed: pass(3),
            },
            Cheat::PlanAGroupThatIsNotReady => SchedulingViolation::PlanIncludedUnreadyGroup {
                pass: pass(2),
                group: group(3),
            },
            Cheat::NameAGroupTwiceInOnePlan => SchedulingViolation::PlanRepeatedGroup {
                pass: pass(2),
                group: first,
            },
            Cheat::OfferAGroupThePlanDidNotName => SchedulingViolation::OfferOutsidePlan {
                pass: pass(2),
                group: third,
            },
            Cheat::OfferAGroupTwiceInOnePass => SchedulingViolation::GroupOfferedTwice {
                pass: pass(2),
                group: first,
            },
            Cheat::RetireAPlanWithATurnOwing => {
                SchedulingViolation::PassCompletedWithUnofferedGroup {
                    pass: pass(2),
                    group: third,
                }
            }
            Cheat::DispatchAGroupThatIsNotReady => SchedulingViolation::DispatchedUnreadyGroup {
                pass: pass(2),
                group: third,
            },
            Cheat::SkipAGroupThatIsAvailable => SchedulingViolation::SkippedAvailableGroup {
                pass: pass(2),
                group: third,
            },
            // The arm-side and retire-side halves of "a tick arms at most one
            // plan and retires at most one". Two checks, one variant, and
            // field-for-field the same answer: before controls were indexed by
            // site, the second of them had no scheduler at all and an
            // exhaustive match over `SchedulingViolation` said otherwise.
            Cheat::ArmTwoPlansInOneTick | Cheat::RetireTwoPlansInOneTick => {
                SchedulingViolation::PassBoundaryReused {
                    pass: pass(2),
                    tick: tick(1),
                }
            }
            Cheat::WalkTheClockBackwards => SchedulingViolation::TickWentBackwards {
                current: tick(1 + u64::from(RED_TEAM_QUOTA)),
                observed: tick(2),
            },
            _ => return None,
        })
    }

    /// The faults about what a turn did with the worker it took.
    fn turn_fault(cheat: Cheat) -> Option<SchedulingViolation> {
        let (first, second, third) = (group(0), group(1), group(2));
        let turn = u64::from(RED_TEAM_QUOTA);
        Some(match cheat {
            Cheat::ServiceMoreThanTheQuota => SchedulingViolation::QuotaExceeded {
                pass: pass(2),
                group: first,
                serviced: RED_TEAM_QUOTA + 1,
                quota: RED_TEAM_QUOTA,
            },
            Cheat::ServiceOutOfArrivalOrder => SchedulingViolation::ServiceOrderViolation {
                pass: pass(1),
                group: first,
                expected: work(1),
                observed: work(2),
            },
            Cheat::MiscountTheWorkATurnDid => SchedulingViolation::ServiceCountMismatch {
                pass: pass(1),
                group: first,
                expected: RED_TEAM_QUOTA,
                observed: 1,
            },
            Cheat::LeaveOwedWorkUnserviced => SchedulingViolation::DispatchLeftWorkUnserviced {
                pass: pass(1),
                group: first,
                owed: RED_TEAM_QUOTA,
                serviced: 0,
            },
            Cheat::ServicePastTheEndOfATurn => SchedulingViolation::DispatchServicedBeyondItsWork {
                pass: pass(1),
                group: first,
                owed: RED_TEAM_QUOTA,
                work: work(3),
            },
            Cheat::MisreportTheTurnsCost => SchedulingViolation::DispatchCostMismatch {
                pass: pass(1),
                group: first,
                expected: turn,
                observed: turn + MISREPORTED_COST,
            },
            // The deadline sweep and the release check, byte for byte the same
            // answer from two different rules. `HoldAWorkerPastItsCost` records
            // no release at all and lets the clock carry past the deadline;
            // `ReleaseAWorkerAfterItsCostIsPaid` records one, a tick late.
            Cheat::HoldAWorkerPastItsCost | Cheat::ReleaseAWorkerAfterItsCostIsPaid => {
                SchedulingViolation::WorkerHeldPastCost {
                    pass: pass(1),
                    group: first,
                    due: tick(1 + turn),
                    observed: tick(2 + turn),
                }
            }
            Cheat::ReleaseAWorkerBeforeItsCostIsPaid => SchedulingViolation::WorkerReleasedEarly {
                pass: pass(1),
                group: first,
                due: tick(1 + turn),
                observed: tick(2),
            },
            Cheat::ReleaseAWorkerNobodyTook => SchedulingViolation::SpuriousWorkerRelease {
                tick: tick(2),
                group: group(3),
            },
            Cheat::TakeMoreWorkersThanThePoolHas => SchedulingViolation::WorkerCountExceeded {
                pass: pass(1),
                group: third,
                workers: RED_TEAM_WORKERS - 1,
            },
            Cheat::ServiceWorkOutsideAnyDispatch => SchedulingViolation::ServiceOutsideDispatch {
                pass: pass(1),
                group: second,
            },
            _ => return None,
        })
    }

    fn build(cheat: Option<Cheat>, staged: Option<Cheat>) -> Self {
        let bounds = if cheat == Some(Cheat::TakeMoreWorkersThanThePoolHas) {
            // The one cheat that changes no decision: the same history, run out
            // of a pool one worker short of holding the turns it hands out.
            config(4, RED_TEAM_WORKERS - 1, 2, 64, 512)
        } else {
            config(4, RED_TEAM_WORKERS, 2, 64, 512)
        };

        let ids: Vec<GroupId> = RED_TEAM_GROUPS.iter().map(|id| group(*id)).collect();
        let mut history = History::new();
        for id in &ids {
            history.open_group(*id, RED_TEAM_QUOTA);
        }
        // A fourth slot, created and left empty, so a plan can name a group that
        // exists and is not ready without inventing an unknown one.
        history.open_group(RED_TEAM_EMPTY_SLOT, RED_TEAM_QUOTA);
        for id in &ids {
            for _ in 0..RED_TEAM_BACKLOG {
                history.submit(*id, system(SystemClass::Bulk, 1));
            }
        }

        if Self::first_pass(&mut history, cheat, &ids) {
            Self::second_pass(&mut history, cheat, staged, &ids);
        }
        Self {
            cheat,
            staged,
            bounds,
            history,
        }
    }

    /// Writes pass one, and reports whether a second pass follows it.
    ///
    /// Every group is planned, dispatched, and services its full quota in
    /// arrival order. The turn faults land here, because pass one is where a
    /// turn's work is unambiguous: nothing has moved yet.
    fn first_pass(history: &mut History, cheat: Option<Cheat>, ids: &[GroupId]) -> bool {
        let taken = |candidate: Cheat| cheat == Some(candidate);
        let turn = u64::from(RED_TEAM_QUOTA);
        history.armed(1, 1, ids.to_vec());
        for (index, id) in ids.iter().enumerate() {
            let head = index == 0;
            if head && taken(Cheat::LeaveOwedWorkUnserviced) {
                history.dispatched(1, 1, *id, RED_TEAM_QUOTA, turn);
                continue;
            }
            if head && taken(Cheat::MiscountTheWorkATurnDid) {
                history.dispatched(1, 1, *id, 1, turn);
            } else if head && taken(Cheat::MisreportTheTurnsCost) {
                history.dispatched(1, 1, *id, RED_TEAM_QUOTA, turn + MISREPORTED_COST);
            } else {
                history.dispatched(1, 1, *id, RED_TEAM_QUOTA, turn);
            }
            if head && taken(Cheat::ServiceOutOfArrivalOrder) {
                history.serviced(1, *id, red_team_item(index, 1));
                history.serviced(1, *id, red_team_item(index, 0));
                continue;
            }
            for slot in 0..turn {
                history.serviced(1, *id, red_team_item(index, slot));
            }
            if head && taken(Cheat::ServicePastTheEndOfATurn) {
                history.serviced(1, *id, red_team_item(index, turn));
            }
        }
        if taken(Cheat::ServiceWorkOutsideAnyDispatch) {
            // Group one's turn is over and its worker is still held, so there is
            // no open turn for this to belong to.
            history.serviced(1, ids[1], red_team_item(1, turn));
        }
        if !taken(Cheat::ArmOverAnOpenPlan) {
            history.retired(1, 1);
        }
        if taken(Cheat::ArmTwoPlansInOneTick) {
            history.armed(2, 1, Vec::new());
            return false;
        }
        // The retire-side counterparts of the two rules above. Each has its own
        // check in the fold, and each reports a variant the arm-side check
        // already reports — so before controls were indexed by site, neither
        // had a scheduler that provoked it and the exhaustive match over
        // `SchedulingViolation` said otherwise.
        if taken(Cheat::RetireTwoPlansInOneTick) {
            history.retired(2, 1);
            return false;
        }
        if taken(Cheat::RetireAPassWithNoPlanOpen) {
            history.retired(3, 2);
            return false;
        }
        if taken(Cheat::ReleaseAWorkerBeforeItsCostIsPaid) {
            history.released(2, ids[0]);
        }
        if taken(Cheat::ReleaseAWorkerNobodyTook) {
            history.released(2, RED_TEAM_EMPTY_SLOT);
        }
        true
    }

    /// Writes pass two, at the tick pass one's occupancies come due.
    ///
    /// The planning faults land here, because a second pass is what makes a
    /// plan comparable to the one before it: an omission, a repeat, a rearm, or
    /// a group offered outside the plan all need a plan to have preceded them.
    fn second_pass(
        history: &mut History,
        cheat: Option<Cheat>,
        staged: Option<Cheat>,
        ids: &[GroupId],
    ) {
        let taken = |candidate: Cheat| cheat == Some(candidate);
        let carries = |candidate: Cheat| staged == Some(candidate);
        let turn = u64::from(RED_TEAM_QUOTA);
        let at = if taken(Cheat::HoldAWorkerPastItsCost) {
            // No releases, and the clock carried one tick past the deadline.
            2 + turn
        } else if carries(Cheat::ReleaseAWorkerAfterItsCostIsPaid) {
            // Every worker is due back at `1 + turn`. The cheat reports the
            // head group's one tick late and the control reports it on time;
            // both arm the next plan at the same tick, so the difference stays
            // one decision wide. The fault this produces is byte-for-byte the
            // one `HoldAWorkerPastItsCost` produces, from the other check.
            for id in &ids[1..] {
                history.released(1 + turn, *id);
            }
            let head_at = if taken(Cheat::ReleaseAWorkerAfterItsCostIsPaid) {
                2 + turn
            } else {
                1 + turn
            };
            history.released(head_at, ids[0]);
            2 + turn
        } else {
            for id in ids {
                history.released(1 + turn, *id);
            }
            if taken(Cheat::WalkTheClockBackwards) {
                2
            } else {
                1 + turn
            }
        };
        let index_of = if taken(Cheat::SkipAPassIndex) { 3 } else { 2 };
        let plan = if taken(Cheat::StarveAReadyGroup) || taken(Cheat::OfferAGroupThePlanDidNotName)
        {
            vec![ids[0], ids[1]]
        } else if taken(Cheat::PlanAGroupThatIsNotReady) {
            vec![ids[0], ids[1], ids[2], RED_TEAM_EMPTY_SLOT]
        } else if taken(Cheat::NameAGroupTwiceInOnePlan) {
            vec![ids[0], ids[1], ids[2], ids[0]]
        } else {
            ids.to_vec()
        };
        history.armed(index_of, at, plan);
        // A plan entry's readiness can only be revoked after the plan is armed,
        // so the stall lands a tick later and the pass resumes there. Both the
        // cheat and its control carry it, which keeps the difference between
        // them one decision wide: dispatch the stalled group, or skip it.
        let last_at = if carries(Cheat::DispatchAGroupThatIsNotReady) {
            at + 1
        } else {
            at
        };

        for (index, id) in ids.iter().enumerate() {
            let tail = index + 1 == ids.len();
            // Three cheats hinge on the last group's turn: one never offers it,
            // one offers it outside the plan afterwards, and one retires the
            // plan while it is still owed.
            if tail
                && (taken(Cheat::RetireAPlanWithATurnOwing)
                    || taken(Cheat::StarveAReadyGroup)
                    || taken(Cheat::OfferAGroupThePlanDidNotName))
            {
                break;
            }
            if tail && carries(Cheat::DispatchAGroupThatIsNotReady) {
                history.reported(last_at, *id, GroupAvailability::Stalled);
                if taken(Cheat::DispatchAGroupThatIsNotReady) {
                    history.dispatched(index_of, last_at, *id, RED_TEAM_QUOTA, turn);
                    for slot in 0..turn {
                        history.serviced(index_of, *id, red_team_item(index, turn + slot));
                    }
                } else {
                    history.skipped(index_of, last_at, *id);
                }
                continue;
            }
            if tail && taken(Cheat::SkipAGroupThatIsAvailable) {
                history.skipped(index_of, at, *id);
                continue;
            }
            if index == 0 && taken(Cheat::ServiceMoreThanTheQuota) {
                history.dispatched(index_of, at, *id, RED_TEAM_QUOTA + 1, turn);
            } else {
                history.dispatched(index_of, at, *id, RED_TEAM_QUOTA, turn);
            }
            for slot in 0..turn {
                history.serviced(index_of, *id, red_team_item(index, turn + slot));
            }
            if index == 0 && taken(Cheat::OfferAGroupTwiceInOnePass) {
                // A second turn for a group that already took one, and no work
                // left for it to do with it.
                history.dispatched(index_of, at, *id, RED_TEAM_QUOTA, turn);
            }
        }
        if taken(Cheat::OfferAGroupThePlanDidNotName) {
            history.dispatched(index_of, at, ids[2], RED_TEAM_QUOTA, turn);
        }
        let retired_as = if taken(Cheat::RetireAPassOtherThanTheOpenOne) {
            index_of + 1
        } else {
            index_of
        };
        history.retired(retired_as, last_at);
    }
}

/// Identifier of the `slot`-th item submitted for the `index`-th red-team group.
///
/// Work identifiers run in submission order, so group zero holds `1..=4`, group
/// one `5..=8`, and group two `9..=12`.
fn red_team_item(index: usize, slot: u64) -> u64 {
    u64::try_from(index).expect("three groups fit in u64") * RED_TEAM_BACKLOG + slot + 1
}

/// A deliberately unfair scheduler, written down as the decisions it makes.
///
/// This variant always plans the group with the most queued work and nothing
/// else, which is the shape a throughput-chasing scheduler naturally takes and
/// the shape the fairness bound exists to forbid. It is expressed as a history
/// rather than as a second scheduler because the bound is a property of
/// decisions, and a history is exactly a record of decisions.
///
/// It is the template [`RedTeam`] generalizes: one rule broken, everything else
/// correct, and a control that differs by one decision.
///
/// Everything else it does is correct: its passes are ordered, its turns are
/// taken once, its quota is respected, its work is serviced in priority and
/// arrival order, and every worker it takes is released at exactly the tick
/// its cost comes due. That is deliberate, and the last of those is the point:
/// a negative control that also mishandled its occupancies would be caught by
/// the occupancy derivation instead, and would prove nothing about the gap.
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

        // Every item costs one tick, so a turn that fills the quota occupies a
        // worker for `quota` ticks and the release falls on the next pass's
        // tick. Spacing the passes by that cost is what keeps this variant
        // unfair in exactly one way.
        let turn_cost = u64::from(self.quota.get());
        let mut next_work = 1_u64;
        for index in 0..self.passes {
            self.now += turn_cost;
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
                tick: TickIndex::new(self.now),
                group: self.favored,
                outcome: OfferOutcome::Dispatched {
                    serviced: self.quota.get(),
                    cost: turn_cost,
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
