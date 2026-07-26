use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
};

use crate::{
    AdmissionOutcome, AdmissionRejection, ClientId, CounterCommand, CounterRejection,
    CounterResult, FailureRecord, GroupAvailability, GroupId, GroupIncarnation, GroupLifecycle,
    GroupView, HistoryEvent, LifecycleOutcome, LifecycleRejection, LifecycleRequest, OfferOutcome,
    Operation, OperationOutcome, PassIndex, RequestFingerprint, RequestIdentity, SchedulerConfig,
    SchedulerSummary, SchedulerView, Sequence, ServiceRecord, SessionEpoch, SessionOutcome,
    SkipReason, Work, WorkClass, WorkFailure, WorkId, WorkQuota,
};

/// A scheduling rule the recorded decisions did not keep.
///
/// Every variant names the group and the pass, because a fairness failure a
/// report could not point at is a benchmark impression rather than a proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingViolation {
    /// A ready group was left out of a pass plan.
    ///
    /// This is the fairness bound broken. `denied_passes` is the longest run of
    /// consecutive plans that omitted the group while it was ready, and
    /// `from_pass` is where that run began. The bound is `denied_passes == 0`,
    /// so any report at all is a failure; the size says how badly.
    OpportunityGap {
        /// Group that was denied its turn.
        group: GroupId,
        /// First pass of the longest denial run.
        from_pass: PassIndex,
        /// Length of that run in complete passes.
        denied_passes: u32,
    },
    /// A plan was armed while an earlier plan still owed a group its turn.
    ///
    /// This is the other half of the bound: a pass that may be abandoned proves
    /// nothing about the groups it had not reached.
    PassArmedWhileOpen {
        /// Pass still owed turns.
        open: PassIndex,
        /// Pass that was armed anyway.
        armed: PassIndex,
    },
    /// A pass index did not follow its predecessor.
    PassOutOfOrder {
        /// Pass that was expected next.
        expected: PassIndex,
        /// Pass that arrived.
        observed: PassIndex,
    },
    /// A plan named a group that was not ready when it was armed.
    PlanIncludedUnreadyGroup {
        /// Pass whose plan named it.
        pass: PassIndex,
        /// Group that was not ready.
        group: GroupId,
    },
    /// A plan named one group more than once.
    PlanRepeatedGroup {
        /// Pass whose plan repeated it.
        pass: PassIndex,
        /// Group that appeared twice.
        group: GroupId,
    },
    /// A turn was taken outside the open plan.
    OfferOutsidePlan {
        /// Pass the turn claimed.
        pass: PassIndex,
        /// Group that took it.
        group: GroupId,
    },
    /// A group took a second turn within one pass.
    GroupOfferedTwice {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that took two turns.
        group: GroupId,
    },
    /// A pass retired while a group in its plan still had no turn.
    PassCompletedWithUnofferedGroup {
        /// Pass that retired early.
        pass: PassIndex,
        /// Group that never got its turn.
        group: GroupId,
    },
    /// A group was dispatched although it was not ready.
    DispatchedUnreadyGroup {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that should not have been dispatched.
        group: GroupId,
    },
    /// A group was skipped as stalled although it was available.
    SkippedAvailableGroup {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that was skipped.
        group: GroupId,
    },
    /// An opportunity serviced more items than the group's quota allows.
    QuotaExceeded {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that overran.
        group: GroupId,
        /// Items the opportunity serviced.
        serviced: u32,
        /// Items its quota allowed.
        quota: u32,
    },
    /// An opportunity serviced work out of its priority or arrival order.
    ///
    /// Servicing a lower-priority item while a higher-priority one waits is the
    /// class rule broken; servicing a later item of the same class first is the
    /// arrival rule broken. Both look the same from here: the wrong item moved.
    ServiceOrderViolation {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that serviced the wrong item.
        group: GroupId,
        /// Item that was owed the slot.
        expected: WorkId,
        /// Item that took it.
        observed: WorkId,
    },
    /// An opportunity serviced fewer or more items than it claimed.
    ServiceCountMismatch {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that miscounted.
        group: GroupId,
        /// Items the group should have serviced.
        expected: u32,
        /// Items it reported.
        observed: u32,
    },
    /// Work was serviced outside any dispatch.
    ServiceOutsideDispatch {
        /// Pass the service claimed.
        pass: PassIndex,
        /// Group that claimed it.
        group: GroupId,
    },
    /// Accepted work neither serviced, retired, nor still queued.
    ///
    /// The conservation law: every admitted item reaches exactly one terminal
    /// disposition, and this is what it looks like when one does not.
    WorkNotConserved {
        /// Items admitted over the history.
        admitted: u64,
        /// Items serviced.
        serviced: u64,
        /// Items retired without service.
        failed: u64,
        /// Items still queued.
        queued: u32,
    },
}

/// Evidence that the recorded decisions kept the scheduling contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairnessReport {
    /// Plans armed over the history.
    pub passes_armed: u64,
    /// Plans that retired with every entry offered.
    pub passes_completed: u64,
    /// Turns handed out.
    pub opportunities: u64,
    /// Largest plan armed.
    pub widest_plan: u32,
    /// Largest run of complete passes any ready group went without a turn.
    ///
    /// The bound is that this is zero. It is reported rather than merely
    /// asserted so that a green run says which number it proved.
    pub widest_gap: u32,
}

/// Everything one replay of a recorded history derives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Replay {
    /// State the history implies.
    pub view: SchedulerView,
    /// Aggregate counts the history implies.
    pub summary: SchedulerSummary,
    /// Outcome each invoked operation should have produced, in invocation
    /// order.
    pub outcomes: Vec<OperationOutcome>,
    /// Service records the recorded dispatches should have produced, in order.
    pub services: Vec<ServiceRecord>,
    /// Failure records the recorded drains should have produced, in order.
    pub failures: Vec<FailureRecord>,
    /// Whether the recorded decisions kept the scheduling contract.
    pub fairness: Result<FairnessReport, SchedulingViolation>,
}

/// Structurally independent executable specification for the managed
/// scheduler.
///
/// This oracle schedules nothing. It keeps an append-only history and answers
/// every question by folding it: the counters, the lifecycles, the queues, the
/// ready set at each arming, and the per-group opportunity gaps are all
/// consequences of the recorded events rather than books kept alongside them.
/// [`crate::ManagedScheduler`] is the opposite in every one of those places — a
/// dense slot table, four class queues per group, an incrementally maintained
/// ready set, and no history at all — so a bookkeeping mistake in either has
/// nothing to hide behind in the other.
///
/// It folds exactly two of the history's three families: what callers asked for
/// and what the scheduler decided. It never folds what a caller concluded. A
/// conclusion it copied would be a conclusion it could not contradict.
///
/// The log is unbounded, which is affordable only because this type is an
/// oracle and never runs a service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceScheduler {
    config: SchedulerConfig,
    log: Vec<HistoryEvent>,
}

impl ReferenceScheduler {
    /// Creates a reference scheduler with an empty history.
    #[must_use]
    pub const fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            log: Vec::new(),
        }
    }

    /// Appends one observed event.
    pub fn observe(&mut self, event: HistoryEvent) {
        self.log.push(event);
    }

    /// Appends a recorded history.
    pub fn observe_all<I: IntoIterator<Item = HistoryEvent>>(&mut self, events: I) {
        self.log.extend(events);
    }

    /// Returns the recorded history in order.
    #[must_use]
    pub fn history(&self) -> &[HistoryEvent] {
        &self.log
    }

    /// Returns the number of recorded events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.log.len()
    }

    /// Returns whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }

    /// Folds the whole history into everything it implies.
    #[must_use]
    pub fn replay(&self) -> Replay {
        let mut fold = Fold::new(self.config);
        for event in &self.log {
            fold.absorb(event);
        }
        fold.finish()
    }

    /// Folds the history and returns only the state it implies.
    #[must_use]
    pub fn view(&self) -> SchedulerView {
        self.replay().view
    }

    /// Folds the history and returns only the aggregate counts it implies.
    #[must_use]
    pub fn summary(&self) -> SchedulerSummary {
        self.replay().summary
    }

    /// Folds the history and judges the scheduling decisions in it.
    ///
    /// # Errors
    ///
    /// Returns the first structural violation, or the widest opportunity gap
    /// when the decisions were structurally sound but unfair.
    pub fn audit(&self) -> Result<FairnessReport, SchedulingViolation> {
        self.replay().fairness
    }
}

/// One client's derived deduplication state within one group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DerivedSession {
    client_id: ClientId,
    epoch: SessionEpoch,
    outstanding: Option<(Sequence, CounterCommand, WorkId)>,
    completed: Option<(Sequence, CounterCommand, CounterResult)>,
}

/// One group as the history implies it.
///
/// The queue is one flat list in arrival order. The priority head is found by
/// scanning it, never by keeping a structure that already knows the answer.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DerivedGroup {
    incarnation: GroupIncarnation,
    state: GroupLifecycle,
    poisoned: bool,
    stalled: bool,
    servicing: bool,
    counter: i64,
    quota: WorkQuota,
    queue: Vec<(WorkId, Work)>,
    sessions: Vec<DerivedSession>,
}

impl DerivedGroup {
    fn new(incarnation: GroupIncarnation, quota: WorkQuota) -> Self {
        Self {
            incarnation,
            state: GroupLifecycle::Creating,
            poisoned: false,
            stalled: false,
            servicing: false,
            counter: 0,
            quota,
            queue: Vec::new(),
            sessions: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.counter = 0;
        self.sessions.clear();
        self.queue.clear();
        self.poisoned = false;
        self.stalled = false;
    }

    fn is_ready(&self) -> bool {
        matches!(
            self.state,
            GroupLifecycle::Recovering | GroupLifecycle::Serving | GroupLifecycle::Draining
        ) && !self.poisoned
            && !self.stalled
            && !self.servicing
            && !self.queue.is_empty()
    }

    fn admits(&self, class: WorkClass) -> bool {
        match self.state {
            GroupLifecycle::Serving => true,
            GroupLifecycle::Recovering => !matches!(class, WorkClass::Command),
            GroupLifecycle::Creating
            | GroupLifecycle::Draining
            | GroupLifecycle::Removed
            | GroupLifecycle::Tombstoned => false,
        }
    }

    /// Returns the position of the item a correct opportunity would take next,
    /// skipping positions already taken by this same opportunity.
    fn head(&self, taken: &[usize]) -> Option<usize> {
        self.queue
            .iter()
            .enumerate()
            .filter(|(index, _)| !taken.contains(index))
            .min_by_key(|(index, (_, work))| (work.class(), *index))
            .map(|(index, _)| index)
    }

    /// Returns the exact items an opportunity should service, in order.
    fn expected_dispatch(&self) -> Vec<WorkId> {
        let mut taken: Vec<usize> = Vec::new();
        let mut expected: Vec<WorkId> = Vec::new();
        for _ in 0..self.quota.get() {
            let Some(index) = self.head(&taken) else {
                break;
            };
            taken.push(index);
            expected.push(self.queue[index].0);
            if matches!(self.queue[index].1, Work::Faulty { .. }) {
                break;
            }
        }
        expected
    }

    fn session(&self, client_id: ClientId) -> Option<&DerivedSession> {
        self.sessions
            .iter()
            .find(|session| session.client_id == client_id)
    }
}

/// The plan a pass is still working through.
///
/// `pending` and `offered` partition the plan: a group starts in the first and
/// moves to the second when it takes its turn. The bound is that `pending` is
/// empty by the time the pass retires, and the two sets are how the fold says
/// so at any scale.
#[derive(Clone, Debug, Eq, PartialEq)]
struct OpenPass {
    pass: PassIndex,
    pending: BTreeSet<GroupId>,
    offered: BTreeSet<GroupId>,
    dispatch: Option<(GroupId, VecDeque<WorkId>)>,
}

impl OpenPass {
    fn planned(&self, group: GroupId) -> bool {
        self.pending.contains(&group) || self.offered.contains(&group)
    }
}

/// How long one group has gone without a turn it was owed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Gap {
    run: u32,
    run_from: Option<PassIndex>,
    worst: u32,
    worst_from: Option<PassIndex>,
}

impl Gap {
    fn deny(&mut self, pass: PassIndex) {
        if self.run == 0 {
            self.run_from = Some(pass);
        }
        self.run += 1;
        if self.run > self.worst {
            self.worst = self.run;
            self.worst_from = self.run_from;
        }
    }

    const fn satisfy(&mut self) {
        self.run = 0;
        self.run_from = None;
    }
}

struct Fold {
    config: SchedulerConfig,
    groups: BTreeMap<GroupId, DerivedGroup>,
    next_work: u64,
    admitted: u64,
    serviced: u64,
    failed: u64,
    queued: u32,
    outcomes: Vec<OperationOutcome>,
    services: Vec<ServiceRecord>,
    failures: Vec<FailureRecord>,
    open: Option<OpenPass>,
    next_pass: PassIndex,
    passes_armed: u64,
    passes_completed: u64,
    opportunities: u64,
    widest_plan: u32,
    gaps: BTreeMap<GroupId, Gap>,
    violation: Option<SchedulingViolation>,
}

impl Fold {
    fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            groups: BTreeMap::new(),
            next_work: 1,
            admitted: 0,
            serviced: 0,
            failed: 0,
            queued: 0,
            outcomes: Vec::new(),
            services: Vec::new(),
            failures: Vec::new(),
            open: None,
            next_pass: PassIndex::first(),
            passes_armed: 0,
            passes_completed: 0,
            opportunities: 0,
            widest_plan: 0,
            gaps: BTreeMap::new(),
            violation: None,
        }
    }

    fn absorb(&mut self, event: &HistoryEvent) {
        match event {
            HistoryEvent::Invoked { operation, .. } => {
                let outcome = self.invoke(*operation);
                self.outcomes.push(outcome);
            }
            HistoryEvent::AvailabilityReported {
                group,
                availability,
                ..
            } => {
                if let Some(state) = self.groups.get_mut(group) {
                    state.stalled = matches!(availability, GroupAvailability::Stalled);
                }
            }
            HistoryEvent::WorkerReleased { group, .. } => {
                if let Some(state) = self.groups.get_mut(group) {
                    state.servicing = false;
                }
            }
            HistoryEvent::PassArmed { pass, plan, .. } => self.arm(*pass, plan),
            HistoryEvent::GroupOffered {
                pass,
                group,
                outcome,
            } => self.offer(*pass, *group, *outcome),
            HistoryEvent::WorkServiced { pass, group, work } => self.service(*pass, *group, *work),
            HistoryEvent::PassCompleted { pass, .. } => self.complete(*pass),
            // Conclusions a caller drew are deliberately not folded.
            HistoryEvent::Completed { .. }
            | HistoryEvent::Unknown { .. }
            | HistoryEvent::NotAdmitted { .. } => {}
        }
    }

    fn invoke(&mut self, operation: Operation) -> OperationOutcome {
        match operation {
            Operation::Lifecycle { group, request } => {
                OperationOutcome::Lifecycle(self.lifecycle(group, request))
            }
            Operation::OpenSession {
                group,
                incarnation,
                client_id,
                session_epoch,
            } => OperationOutcome::Session(self.open_session(
                group,
                incarnation,
                client_id,
                session_epoch,
            )),
            Operation::Submit {
                group,
                incarnation,
                work,
            } => OperationOutcome::Admission(self.submit(group, incarnation, work)),
        }
    }

    fn lifecycle(&mut self, group: GroupId, request: LifecycleRequest) -> LifecycleOutcome {
        if !self.config.admits_group(group) {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupOutOfRange);
        }
        let target = request.target();
        let Some(state) = self.groups.get_mut(&group) else {
            let LifecycleRequest::Create { quota } = request else {
                return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
            };
            self.groups
                .insert(group, DerivedGroup::new(GroupIncarnation::first(), quota));
            return LifecycleOutcome::Created {
                incarnation: GroupIncarnation::first(),
            };
        };

        if state.state == GroupLifecycle::Tombstoned {
            return if target == GroupLifecycle::Tombstoned {
                LifecycleOutcome::Idempotent {
                    state: GroupLifecycle::Tombstoned,
                    incarnation: state.incarnation,
                }
            } else {
                LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
            };
        }

        if let LifecycleRequest::Create { quota } = request {
            return match state.state {
                GroupLifecycle::Creating if state.quota == quota => LifecycleOutcome::Idempotent {
                    state: GroupLifecycle::Creating,
                    incarnation: state.incarnation,
                },
                GroupLifecycle::Creating => {
                    LifecycleOutcome::Rejected(LifecycleRejection::QuotaConflict {
                        current: state.quota,
                    })
                }
                GroupLifecycle::Removed => {
                    let Some(next) = state.incarnation.successor() else {
                        return LifecycleOutcome::Rejected(
                            LifecycleRejection::IncarnationExhausted,
                        );
                    };
                    state.incarnation = next;
                    state.state = GroupLifecycle::Creating;
                    state.quota = quota;
                    state.clear();
                    LifecycleOutcome::Created { incarnation: next }
                }
                current => LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                    current,
                    requested: GroupLifecycle::Creating,
                }),
            };
        }

        if state.state == target {
            let outcome = LifecycleOutcome::Idempotent {
                state: target,
                incarnation: state.incarnation,
            };
            if target == GroupLifecycle::Draining {
                self.retire_poisoned(group);
            }
            return outcome;
        }

        let legal = match request {
            LifecycleRequest::Recover => state.state == GroupLifecycle::Creating,
            LifecycleRequest::Serve => state.state == GroupLifecycle::Recovering,
            LifecycleRequest::Drain => matches!(
                state.state,
                GroupLifecycle::Creating | GroupLifecycle::Recovering | GroupLifecycle::Serving
            ),
            LifecycleRequest::Remove => state.state == GroupLifecycle::Draining,
            LifecycleRequest::Tombstone => state.state == GroupLifecycle::Removed,
            LifecycleRequest::Create { .. } => false,
        };
        if !legal {
            return LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current: state.state,
                requested: target,
            });
        }
        if request == LifecycleRequest::Remove && !state.queue.is_empty() {
            return LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained {
                pending: queue_len(state),
            });
        }

        let from = state.state;
        state.state = target;
        let incarnation = state.incarnation;
        if target == GroupLifecycle::Removed {
            state.clear();
        }
        if target == GroupLifecycle::Draining {
            self.retire_poisoned(group);
        }
        LifecycleOutcome::Applied {
            from,
            to: target,
            incarnation,
        }
    }

    fn retire_poisoned(&mut self, group: GroupId) {
        let Some(state) = self.groups.get_mut(&group) else {
            return;
        };
        if !state.poisoned {
            return;
        }
        let mut order: Vec<usize> = (0..state.queue.len()).collect();
        order.sort_by_key(|index| (state.queue[*index].1.class(), *index));
        for index in order {
            self.failures.push(FailureRecord {
                work: state.queue[index].0,
                group,
                reason: WorkFailure::GroupPoisoned,
            });
        }
        let retired = queue_len(state);
        state.queue.clear();
        for session in &mut state.sessions {
            session.outstanding = None;
        }
        self.queued -= retired;
        self.failed += u64::from(retired);
    }

    fn open_session(
        &mut self,
        group: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> SessionOutcome {
        if let Err(rejection) = self.address(group, incarnation) {
            return SessionOutcome::Rejected(rejection);
        }
        let admits_client = self.config.admits_client(client_id);
        let Some(state) = self.groups.get_mut(&group) else {
            return SessionOutcome::Rejected(AdmissionRejection::GroupUnknown);
        };
        if !matches!(
            state.state,
            GroupLifecycle::Recovering | GroupLifecycle::Serving
        ) {
            return SessionOutcome::Rejected(AdmissionRejection::GroupNotAcceptingWork {
                state: state.state,
                class: WorkClass::Command,
            });
        }
        if state.poisoned {
            return SessionOutcome::Rejected(AdmissionRejection::GroupPoisoned);
        }
        if !admits_client {
            return SessionOutcome::Rejected(AdmissionRejection::ClientOutOfRange);
        }

        let existing = state
            .sessions
            .iter()
            .position(|session| session.client_id == client_id);
        let Some(position) = existing else {
            state.sessions.push(DerivedSession {
                client_id,
                epoch,
                outstanding: None,
                completed: None,
            });
            return SessionOutcome::Opened {
                session_epoch: epoch,
            };
        };

        let current = state.sessions[position].epoch;
        if epoch < current {
            return SessionOutcome::Rejected(AdmissionRejection::StaleSession { current });
        }
        if epoch == current {
            return SessionOutcome::AlreadyOpen {
                session_epoch: epoch,
            };
        }
        state.sessions[position] = DerivedSession {
            client_id,
            epoch,
            outstanding: None,
            completed: None,
        };
        SessionOutcome::Replaced {
            session_epoch: epoch,
        }
    }

    fn submit(
        &mut self,
        group: GroupId,
        incarnation: GroupIncarnation,
        work: Work,
    ) -> AdmissionOutcome {
        if let Err(rejection) = self.address(group, incarnation) {
            return AdmissionOutcome::Rejected(rejection);
        }
        let class = work.class();
        let group_limit = self.config.max_group_queue();
        let global_limit = self.config.max_global_queue();
        let global_queued = self.queued;
        let admits_client = work
            .request_identity()
            .is_none_or(|request| self.config.admits_client(request.client_id));
        let Some(state) = self.groups.get_mut(&group) else {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupUnknown);
        };

        if !state.admits(class) {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupNotAcceptingWork {
                state: state.state,
                class,
            });
        }
        if state.poisoned {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupPoisoned);
        }

        if let Work::Counter {
            request, command, ..
        } = work
        {
            if !admits_client {
                return AdmissionOutcome::Rejected(AdmissionRejection::ClientOutOfRange);
            }
            let Some(session) = state.session(request.client_id) else {
                return AdmissionOutcome::Rejected(AdmissionRejection::SessionNotOpen);
            };
            if let Some(answer) = session_verdict(session, request, command) {
                return answer;
            }
        }

        if queue_len(state) >= group_limit {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupQueueFull {
                limit: group_limit,
            });
        }
        if global_queued >= global_limit {
            return AdmissionOutcome::Rejected(AdmissionRejection::GlobalQueueFull {
                limit: global_limit,
            });
        }

        let id = WorkId::new(self.next_work).expect("work identifiers start at one");
        state.queue.push((id, work));
        if let Work::Counter {
            request, command, ..
        } = work
        {
            if let Some(session) = state
                .sessions
                .iter_mut()
                .find(|session| session.client_id == request.client_id)
            {
                session.outstanding = Some((request.sequence, command, id));
            }
        }
        self.next_work += 1;
        self.queued += 1;
        self.admitted += 1;
        AdmissionOutcome::Queued { work: id }
    }

    fn address(
        &self,
        group: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<(), AdmissionRejection> {
        if !self.config.admits_group(group) {
            return Err(AdmissionRejection::GroupOutOfRange);
        }
        let Some(state) = self.groups.get(&group) else {
            return Err(AdmissionRejection::GroupUnknown);
        };
        if state.state == GroupLifecycle::Tombstoned {
            return Err(AdmissionRejection::GroupTombstoned);
        }
        if incarnation < state.incarnation {
            return Err(AdmissionRejection::StaleIncarnation {
                current: state.incarnation,
            });
        }
        if incarnation > state.incarnation {
            return Err(AdmissionRejection::FutureIncarnation {
                current: state.incarnation,
            });
        }
        Ok(())
    }

    fn fault(&mut self, violation: SchedulingViolation) {
        if self.violation.is_none() {
            self.violation = Some(violation);
        }
    }

    /// Recomputes the ready set from first principles and judges the plan
    /// against it.
    fn arm(&mut self, pass: PassIndex, plan: &[GroupId]) {
        if let Some(open) = self.open.as_ref().map(|open| open.pass) {
            self.fault(SchedulingViolation::PassArmedWhileOpen { open, armed: pass });
        }
        if pass != self.next_pass {
            self.fault(SchedulingViolation::PassOutOfOrder {
                expected: self.next_pass,
                observed: pass,
            });
        }
        self.next_pass = pass.successor().unwrap_or(pass);
        self.passes_armed += 1;
        self.widest_plan = self
            .widest_plan
            .max(u32::try_from(plan.len()).unwrap_or(u32::MAX));

        let mut seen: BTreeSet<GroupId> = BTreeSet::new();
        for group in plan {
            if !seen.insert(*group) {
                self.fault(SchedulingViolation::PlanRepeatedGroup {
                    pass,
                    group: *group,
                });
            }
            if !self.groups.get(group).is_some_and(DerivedGroup::is_ready) {
                self.fault(SchedulingViolation::PlanIncludedUnreadyGroup {
                    pass,
                    group: *group,
                });
            }
        }

        // Every ready group is owed this plan's turn. A group left out has been
        // denied an opportunity, and the run of consecutive denials is the gap
        // the bound forbids. A group that is not ready is owed nothing, so its
        // run resets rather than accumulating while it has no work to do.
        for (group, state) in &self.groups {
            let gap = self.gaps.entry(*group).or_default();
            if !state.is_ready() || seen.contains(group) {
                gap.satisfy();
            } else {
                gap.deny(pass);
            }
        }

        self.open = Some(OpenPass {
            pass,
            pending: seen,
            offered: BTreeSet::new(),
            dispatch: None,
        });
    }

    fn offer(&mut self, pass: PassIndex, group: GroupId, outcome: OfferOutcome) {
        self.opportunities += 1;
        let planned = self
            .open
            .as_ref()
            .is_some_and(|open| open.pass == pass && open.planned(group));
        if !planned {
            self.fault(SchedulingViolation::OfferOutsidePlan { pass, group });
            return;
        }
        if self
            .open
            .as_ref()
            .is_some_and(|open| open.offered.contains(&group))
        {
            self.fault(SchedulingViolation::GroupOfferedTwice { pass, group });
            return;
        }
        if let Some(open) = self.open.as_mut() {
            open.pending.remove(&group);
            open.offered.insert(group);
        }

        let Some(state) = self.groups.get(&group) else {
            self.fault(SchedulingViolation::DispatchedUnreadyGroup { pass, group });
            return;
        };
        let stalled = state.stalled;
        let ready = state.is_ready();
        let quota = state.quota.get();
        let expected = state.expected_dispatch();

        match outcome {
            OfferOutcome::Skipped(SkipReason::Stalled) => {
                if !stalled {
                    self.fault(SchedulingViolation::SkippedAvailableGroup { pass, group });
                }
            }
            OfferOutcome::Dispatched { serviced, .. } => {
                if !ready {
                    self.fault(SchedulingViolation::DispatchedUnreadyGroup { pass, group });
                    return;
                }
                if serviced > quota {
                    self.fault(SchedulingViolation::QuotaExceeded {
                        pass,
                        group,
                        serviced,
                        quota,
                    });
                }
                let expected_count = u32::try_from(expected.len()).unwrap_or(u32::MAX);
                if expected_count != serviced {
                    self.fault(SchedulingViolation::ServiceCountMismatch {
                        pass,
                        group,
                        expected: expected_count,
                        observed: serviced,
                    });
                }
                if let Some(state) = self.groups.get_mut(&group) {
                    state.servicing = true;
                }
                if let Some(open) = self.open.as_mut() {
                    open.dispatch = Some((group, expected.into_iter().collect()));
                }
            }
        }
    }

    fn service(&mut self, pass: PassIndex, group: GroupId, work: WorkId) {
        let dispatched = self.open.as_ref().is_some_and(|open| {
            open.pass == pass
                && open
                    .dispatch
                    .as_ref()
                    .is_some_and(|(holder, _)| *holder == group)
        });
        if !dispatched {
            self.fault(SchedulingViolation::ServiceOutsideDispatch { pass, group });
            return;
        }
        let expected = self
            .open
            .as_mut()
            .and_then(|open| open.dispatch.as_mut())
            .and_then(|(_, pending)| pending.pop_front());
        let Some(expected) = expected else {
            self.fault(SchedulingViolation::ServiceOutsideDispatch { pass, group });
            return;
        };
        if expected != work {
            self.fault(SchedulingViolation::ServiceOrderViolation {
                pass,
                group,
                expected,
                observed: work,
            });
        }
        self.apply(group, work);
    }

    /// Recomputes what servicing one item does to a group.
    fn apply(&mut self, group: GroupId, work: WorkId) {
        let Some(state) = self.groups.get_mut(&group) else {
            return;
        };
        let Some(position) = state.queue.iter().position(|(id, _)| *id == work) else {
            return;
        };
        let (_, item) = state.queue.remove(position);
        self.queued -= 1;
        self.serviced += 1;

        let result = match item {
            Work::System { .. } => None,
            Work::Faulty { .. } => {
                state.poisoned = true;
                None
            }
            Work::Counter {
                request, command, ..
            } => {
                let outcome = match command {
                    CounterCommand::Add { delta } => match state.counter.checked_add(delta.get()) {
                        Some(value) => {
                            state.counter = value;
                            CounterResult::Added { value }
                        }
                        None => CounterResult::Rejected(CounterRejection::CounterOverflow {
                            current: state.counter,
                        }),
                    },
                    CounterCommand::Read => CounterResult::Value {
                        value: state.counter,
                    },
                };
                if let Some(session) = state
                    .sessions
                    .iter_mut()
                    .find(|session| session.client_id == request.client_id)
                {
                    if session.epoch == request.session_epoch
                        && session
                            .outstanding
                            .is_some_and(|(_, _, queued)| queued == work)
                    {
                        session.outstanding = None;
                        session.completed = Some((request.sequence, command, outcome));
                    }
                }
                Some(outcome)
            }
        };

        self.services.push(ServiceRecord {
            work,
            group,
            class: item.class(),
            result,
        });
    }

    fn complete(&mut self, pass: PassIndex) {
        let Some(open) = self.open.take() else {
            self.fault(SchedulingViolation::PassOutOfOrder {
                expected: self.next_pass,
                observed: pass,
            });
            return;
        };
        if open.pass != pass {
            self.fault(SchedulingViolation::PassOutOfOrder {
                expected: open.pass,
                observed: pass,
            });
        }
        for group in &open.pending {
            self.fault(SchedulingViolation::PassCompletedWithUnofferedGroup {
                pass,
                group: *group,
            });
        }
        self.passes_completed += 1;
    }

    fn finish(mut self) -> Replay {
        if u64::from(self.queued) + self.serviced + self.failed != self.admitted {
            self.fault(SchedulingViolation::WorkNotConserved {
                admitted: self.admitted,
                serviced: self.serviced,
                failed: self.failed,
                queued: self.queued,
            });
        }

        // Structural faults outrank the gap: decisions that were not a pass at
        // all cannot be judged for the fairness of a pass.
        let fairness = match self.violation {
            Some(violation) => Err(violation),
            None => match self.widest_gap() {
                Some(violation) => Err(violation),
                None => Ok(FairnessReport {
                    passes_armed: self.passes_armed,
                    passes_completed: self.passes_completed,
                    opportunities: self.opportunities,
                    widest_plan: self.widest_plan,
                    // Measured rather than assumed to be zero. A green run
                    // should report the number it proved, not a constant that
                    // happens to be right.
                    widest_gap: self.gaps.values().map(|gap| gap.worst).max().unwrap_or(0),
                }),
            },
        };

        let groups = self
            .groups
            .iter()
            .map(|(group, state)| GroupView {
                group: *group,
                incarnation: state.incarnation,
                state: state.state,
                poisoned: state.poisoned,
                stalled: state.stalled,
                counter: state.counter,
                queued: queue_len(state),
                quota: state.quota,
                servicing: state.servicing,
            })
            .collect::<Vec<_>>();
        let live_groups = groups
            .iter()
            .filter(|view| {
                !matches!(
                    view.state,
                    GroupLifecycle::Removed | GroupLifecycle::Tombstoned
                )
            })
            .count();
        let poisoned_groups = groups.iter().filter(|view| view.poisoned).count();
        let ready_groups = self
            .groups
            .values()
            .filter(|state| state.is_ready())
            .count();

        Replay {
            summary: SchedulerSummary {
                live_groups: u32::try_from(live_groups).unwrap_or(u32::MAX),
                poisoned_groups: u32::try_from(poisoned_groups).unwrap_or(u32::MAX),
                ready_groups: u32::try_from(ready_groups).unwrap_or(u32::MAX),
                queued: self.queued,
                admitted: self.admitted,
                serviced: self.serviced,
                failed: self.failed,
            },
            view: SchedulerView {
                groups,
                queued: self.queued,
            },
            outcomes: self.outcomes,
            services: self.services,
            failures: self.failures,
            fairness,
        }
    }

    /// Returns the worst opportunity gap the history produced, when any group
    /// went a complete pass without the turn it was owed. Ties go to the lowest
    /// group ID so that one history always names one group.
    fn widest_gap(&self) -> Option<SchedulingViolation> {
        self.gaps
            .iter()
            .filter(|(_, gap)| gap.worst > 0)
            .max_by_key(|(group, gap)| (gap.worst, std::cmp::Reverse(**group)))
            .map(|(group, gap)| SchedulingViolation::OpportunityGap {
                group: *group,
                from_pass: gap.worst_from.unwrap_or_else(PassIndex::first),
                denied_passes: gap.worst,
            })
    }
}

fn queue_len(state: &DerivedGroup) -> u32 {
    u32::try_from(state.queue.len()).unwrap_or(u32::MAX)
}

/// Recomputes what a client session owes one submission.
///
/// `Some` is the answer the gate must give; `None` means the request is new
/// work. The checks run in a fixed order — epoch, then envelope, then sequence
/// — because a request whose fingerprint does not describe its own command is
/// malformed wherever its sequence falls.
fn session_verdict(
    session: &DerivedSession,
    request: RequestIdentity,
    command: CounterCommand,
) -> Option<AdmissionOutcome> {
    let rejected = |rejection| Some(AdmissionOutcome::Rejected(rejection));
    if request.session_epoch < session.epoch {
        return rejected(AdmissionRejection::StaleSession {
            current: session.epoch,
        });
    }
    if request.session_epoch > session.epoch {
        return rejected(AdmissionRejection::FutureSession {
            current: session.epoch,
        });
    }
    let recomputed = RequestFingerprint::of(&command);
    if request.fingerprint != recomputed {
        return rejected(AdmissionRejection::FingerprintMismatch {
            expected: recomputed,
        });
    }

    let expected = match session.completed {
        None => Sequence::first(),
        Some((completed, cached, result)) => match request.sequence.cmp(&completed) {
            Ordering::Less => {
                return rejected(AdmissionRejection::StaleSequence { highest: completed })
            }
            Ordering::Equal if command == cached => {
                return Some(AdmissionOutcome::Replayed { result })
            }
            Ordering::Equal => return rejected(AdmissionRejection::ConflictingRetry),
            Ordering::Greater => match completed.successor() {
                Some(successor) => successor,
                None => return rejected(AdmissionRejection::SequenceExhausted),
            },
        },
    };

    match session.outstanding {
        Some((outstanding, queued, id)) if request.sequence == outstanding => {
            if command == queued {
                Some(AdmissionOutcome::AlreadyQueued { work: id })
            } else {
                rejected(AdmissionRejection::ConflictingRetry)
            }
        }
        _ if request.sequence == expected => None,
        _ => rejected(AdmissionRejection::SequenceGap { expected }),
    }
}
