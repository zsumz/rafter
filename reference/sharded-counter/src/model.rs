use std::collections::VecDeque;

use crate::{
    AdmissionOutcome, AdmissionRejection, ClientId, CounterCommand, CounterRejection,
    CounterResult, FailureRecord, GroupAvailability, GroupId, GroupIncarnation, GroupLifecycle,
    GroupView, LifecycleOutcome, LifecycleRejection, LifecycleRequest, LifecycleTransition, Offer,
    OfferOutcome, PassIndex, PassProgress, PassSuspension, ReadinessSignal, RequestFingerprint,
    RequestIdentity, SchedulerConfig, SchedulerSummary, SchedulerView, Sequence, ServiceRecord,
    SessionEpoch, SessionOutcome, SkipReason, TickIndex, TickReport, Work, WorkClass, WorkFailure,
    WorkId, WorkQuota, WORK_CLASS_ORDER,
};

/// One admitted item waiting for its group's turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueuedWork {
    id: WorkId,
    work: Work,
}

/// One client's deduplication state within one group.
///
/// A session records at most one outstanding request and at most one completed
/// one, which is what keeps deduplication bounded by client slots rather than
/// by traffic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Session {
    client_id: ClientId,
    epoch: SessionEpoch,
    outstanding: Option<(Sequence, CounterCommand, WorkId)>,
    completed: Option<(Sequence, CounterCommand, CounterResult)>,
}

/// One group slot's live state.
///
/// Every field here is bounded. The queues are the only part that grows, and
/// they are capped by the configured per-group bound, so a slot's footprint
/// does not depend on how long the scheduler has run.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Group {
    incarnation: GroupIncarnation,
    state: GroupLifecycle,
    poisoned: bool,
    stalled: bool,
    servicing: bool,
    counter: i64,
    quota: WorkQuota,
    queues: [VecDeque<QueuedWork>; WORK_CLASS_ORDER.len()],
    queued: u32,
    sessions: Vec<Session>,
    ready_position: Option<usize>,
}

impl Group {
    fn new(incarnation: GroupIncarnation, quota: WorkQuota) -> Self {
        Self {
            incarnation,
            state: GroupLifecycle::Creating,
            poisoned: false,
            stalled: false,
            servicing: false,
            counter: 0,
            quota,
            queues: std::array::from_fn(|_| VecDeque::new()),
            queued: 0,
            sessions: Vec::new(),
            ready_position: None,
        }
    }

    /// Returns whether the group would take a turn if it were offered one.
    ///
    /// Occupying a worker excludes a group deliberately: a group being serviced
    /// is not starved, it is being served, and offering it a second concurrent
    /// turn would let one group hold two workers while another holds none.
    const fn is_ready(&self) -> bool {
        self.state.is_serviceable()
            && !self.poisoned
            && !self.stalled
            && !self.servicing
            && self.queued > 0
    }

    /// Clears everything a removed slot must not keep.
    ///
    /// A removed group's counter, sessions, and queue are gone. That is what
    /// removal means, and it is exactly why a late retry addressed to the slot
    /// has to be refused rather than executed: there is no cache left to
    /// recognize it as a retry, so executing it would apply an acknowledged
    /// command a second time.
    ///
    /// Two fields are deliberately left alone. `servicing` tracks a *physical*
    /// worker, and the worker the departing incarnation was occupying is still
    /// occupied; clearing the flag would let a slot reopened before that
    /// occupancy ends be dispatched into a worker that is still busy. A slot
    /// reopened while its predecessor's work is outstanding therefore reports
    /// `servicing` until the release arrives, and stays out of the ready set
    /// until then. `ready_position` is owned by `refresh_ready`, which every
    /// caller of this method runs afterwards.
    fn clear(&mut self) {
        self.counter = 0;
        self.sessions.clear();
        for queue in &mut self.queues {
            queue.clear();
        }
        self.queued = 0;
        self.poisoned = false;
        self.stalled = false;
    }

    fn session(&self, client_id: ClientId) -> Option<&Session> {
        self.sessions
            .iter()
            .find(|session| session.client_id == client_id)
    }

    fn session_mut(&mut self, client_id: ClientId) -> Option<&mut Session> {
        self.sessions
            .iter_mut()
            .find(|session| session.client_id == client_id)
    }
}

/// One worker holding a group's opportunity until its cost is paid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Dispatch {
    group: GroupId,
    busy_until: TickIndex,
}

/// The plan a pass traverses.
///
/// A plan is armed once from the ready set and then retired only when every
/// entry has been offered. It is never rearmed early, never reordered, and
/// never truncated, and that single rule is the whole fairness bound.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Plan {
    pass: PassIndex,
    entries: Vec<GroupId>,
    next: usize,
}

/// Deterministic managed scheduler over many independent counter groups.
///
/// The scheduler owns three things: a lifecycle per group slot, a bounded queue
/// of classified work per live group, and a rotating pass over the ready set
/// that hands each ready group exactly one turn. It retains no history: a
/// [`TickReport`] is emitted once and forgotten, so a scheduler that has run for
/// a billion ticks is the same size as one that has just started.
///
/// Group state is allocated when a slot is first created, so a configuration
/// that permits a large number of groups costs nothing until the groups exist,
/// and arming a pass costs one traversal of the *ready* set rather than of every
/// configured slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedScheduler {
    config: SchedulerConfig,
    groups: Vec<Option<Group>>,
    ready: Vec<GroupId>,
    workers: Vec<Option<Dispatch>>,
    plan: Option<Plan>,
    tick: TickIndex,
    next_pass: PassIndex,
    next_work: u64,
    cursor: u32,
    queued: u32,
    admitted: u64,
    serviced: u64,
    failed: u64,
}

impl ManagedScheduler {
    /// Creates an empty scheduler with no groups and no work.
    #[must_use]
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            groups: Vec::new(),
            ready: Vec::new(),
            workers: (0..config.workers()).map(|_| None).collect(),
            plan: None,
            tick: TickIndex::ZERO,
            next_pass: PassIndex::first(),
            next_work: 1,
            cursor: 0,
            queued: 0,
            admitted: 0,
            serviced: 0,
            failed: 0,
        }
    }

    /// Returns the configured bounds.
    #[must_use]
    pub const fn config(&self) -> SchedulerConfig {
        self.config
    }

    /// Returns the number of ticks the scheduler has taken.
    #[must_use]
    pub const fn tick(&self) -> TickIndex {
        self.tick
    }

    /// Returns the pass a plan is currently open under, when one is open.
    #[must_use]
    pub fn open_pass(&self) -> Option<PassIndex> {
        self.plan.as_ref().map(|plan| plan.pass)
    }

    /// Returns the ready set in group order.
    #[must_use]
    pub fn ready_groups(&self) -> Vec<GroupId> {
        let mut ready = self.ready.clone();
        ready.sort_unstable();
        ready
    }

    /// Returns one group's public view, when the slot has been created.
    #[must_use]
    pub fn group(&self, group: GroupId) -> Option<GroupView> {
        self.slot(group).map(|state| group_view(group, state))
    }

    /// Returns one group's counter, when the slot holds one.
    #[must_use]
    pub fn counter(&self, group: GroupId) -> Option<i64> {
        self.slot(group).map(|state| state.counter)
    }

    /// Returns a canonical state view for differential assertions.
    ///
    /// This is a full traversal of every created slot. It is a checkpoint tool,
    /// not something the scheduler needs to run.
    #[must_use]
    pub fn view(&self) -> SchedulerView {
        let groups = self
            .groups
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| {
                slot.as_ref()
                    .map(|state| group_view(group_id(index), state))
            })
            .collect();
        SchedulerView {
            groups,
            queued: self.queued,
        }
    }

    /// Returns aggregate counts.
    #[must_use]
    pub fn summary(&self) -> SchedulerSummary {
        let live_groups = self
            .groups
            .iter()
            .flatten()
            .filter(|state| {
                !matches!(
                    state.state,
                    GroupLifecycle::Removed | GroupLifecycle::Tombstoned
                )
            })
            .count();
        let poisoned_groups = self
            .groups
            .iter()
            .flatten()
            .filter(|state| state.poisoned)
            .count();
        SchedulerSummary {
            live_groups: count(live_groups),
            poisoned_groups: count(poisoned_groups),
            ready_groups: count(self.ready.len()),
            queued: self.queued,
            admitted: self.admitted,
            serviced: self.serviced,
            failed: self.failed,
        }
    }

    /// Requests one administrative lifecycle transition.
    ///
    /// Administration addresses a *slot*, never an incarnation: the operator is
    /// the party that changed the incarnation, so there is no stale
    /// administrative request to detect. Only traffic carries an incarnation.
    pub fn lifecycle(&mut self, group: GroupId, request: LifecycleRequest) -> LifecycleTransition {
        if !self.config.admits_group(group) {
            return LifecycleTransition::rejected(LifecycleRejection::GroupOutOfRange);
        }
        let outcome = match request {
            LifecycleRequest::Create { quota } => self.create(group, quota),
            LifecycleRequest::Recover => {
                self.advance(group, GroupLifecycle::Creating, GroupLifecycle::Recovering)
            }
            LifecycleRequest::Serve => {
                self.advance(group, GroupLifecycle::Recovering, GroupLifecycle::Serving)
            }
            LifecycleRequest::Drain => self.drain(group),
            LifecycleRequest::Remove => self.remove(group),
            LifecycleRequest::Tombstone => {
                self.advance(group, GroupLifecycle::Removed, GroupLifecycle::Tombstoned)
            }
        };

        // A group can be poisoned by work it services *while* draining, so the
        // retirement is attached to the request rather than to the transition:
        // draining an already-draining poisoned group is how an operator clears
        // a backlog that `Remove` keeps refusing.
        let failed = if matches!(request, LifecycleRequest::Drain)
            && !matches!(outcome, LifecycleOutcome::Rejected(_))
        {
            self.retire_poisoned_queue(group)
        } else {
            Vec::new()
        };
        self.refresh_ready(group);
        LifecycleTransition { outcome, failed }
    }

    /// Opens or advances one client session inside one group.
    ///
    /// Session establishment is an admission-gate action rather than queued
    /// work. Queueing it would mean a client needed a queue slot to open the
    /// session that a queue-full rejection tells it to retry under, which is a
    /// circle with no exit.
    pub fn open_session(
        &mut self,
        group: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> SessionOutcome {
        if let Err(rejection) = self.admit_group(group, incarnation) {
            return SessionOutcome::Rejected(rejection);
        }
        let admits_client = self.config.admits_client(client_id);
        let Some(state) = self.slot_mut(group) else {
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

        let Some(session) = state.session_mut(client_id) else {
            // No capacity refusal is needed or possible here. The addressable
            // client range *is* the table's bound: a client outside it was
            // refused above, so at most `max_clients_per_group` distinct slots
            // can ever be inserted.
            state.sessions.push(Session {
                client_id,
                epoch,
                outstanding: None,
                completed: None,
            });
            return SessionOutcome::Opened {
                session_epoch: epoch,
            };
        };

        if epoch < session.epoch {
            return SessionOutcome::Rejected(AdmissionRejection::StaleSession {
                current: session.epoch,
            });
        }
        if epoch == session.epoch {
            return SessionOutcome::AlreadyOpen {
                session_epoch: epoch,
            };
        }

        // A greater epoch clears this slot's deduplication state and nothing
        // else. Work the previous epoch already had admitted stays queued and
        // still takes effect: a client restart must not silently cancel a
        // command the service accepted. It loses only its cache slot, so its
        // result is no longer replayable.
        session.epoch = epoch;
        session.outstanding = None;
        session.completed = None;
        SessionOutcome::Replaced {
            session_epoch: epoch,
        }
    }

    /// Submits one unit of work to one group incarnation.
    pub fn submit(
        &mut self,
        group: GroupId,
        incarnation: GroupIncarnation,
        work: Work,
    ) -> AdmissionOutcome {
        if let Err(rejection) = self.admit_group(group, incarnation) {
            return AdmissionOutcome::Rejected(rejection);
        }
        let class = work.class();
        let group_limit = self.config.max_group_queue();
        let global_limit = self.config.max_global_queue();
        let global_queued = self.queued;
        let admits_client = work
            .request_identity()
            .is_none_or(|request| self.config.admits_client(request.client_id));
        // Reserved, not yet spent: `next_work` advances only if this submission
        // reaches a queue slot, so the identifier sequence has no gaps and an
        // observer can mint the same identifiers from the same admissions.
        let work_id = work_id(self.next_work);

        let Some(state) = self.slot_mut(group) else {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupUnknown);
        };
        if !state.state.admits(class) {
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
            if let Err(answer) = admit_under_session(session, request, command) {
                return answer;
            }
        }

        if state.queued >= group_limit {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupQueueFull {
                limit: group_limit,
            });
        }
        if global_queued >= global_limit {
            return AdmissionOutcome::Rejected(AdmissionRejection::GlobalQueueFull {
                limit: global_limit,
            });
        }

        state.queues[class.rank()].push_back(QueuedWork { id: work_id, work });
        state.queued += 1;
        if let Work::Counter {
            request, command, ..
        } = work
        {
            if let Some(session) = state.session_mut(request.client_id) {
                session.outstanding = Some((request.sequence, command, work_id));
            }
        }

        self.next_work += 1;
        self.queued += 1;
        self.admitted += 1;
        self.refresh_ready(group);
        AdmissionOutcome::Queued { work: work_id }
    }

    /// Advances the scheduler by one tick.
    ///
    /// `signals` is the explicit external readiness input: a group reported
    /// [`GroupAvailability::Stalled`] leaves the ready set until it is reported
    /// available again, whatever its queue holds. Signals are applied before a
    /// plan is armed, so a stall observed at this tick keeps its group out of a
    /// plan armed at this tick, and strands it only in a plan armed earlier.
    pub fn step(&mut self, signals: &[ReadinessSignal]) -> TickReport {
        self.tick = TickIndex::new(self.tick.get() + 1);
        let mut report = TickReport {
            tick: self.tick,
            pass: None,
            armed: None,
            offers: Vec::new(),
            serviced: Vec::new(),
            released: Vec::new(),
            progress: PassProgress::Idle,
        };

        self.apply_signals(signals);
        report.released = self.release_workers();

        let Some(mut plan) = self.resume_or_arm(&mut report) else {
            return report;
        };
        report.pass = Some(plan.pass);

        while plan.next < plan.entries.len() {
            let Some(worker) = self.free_worker() else {
                report.progress = PassProgress::Suspended(PassSuspension::NoFreeWorker);
                self.plan = Some(plan);
                return report;
            };
            let group = plan.entries[plan.next];
            plan.next += 1;
            let outcome = self.offer(group, worker, &mut report);
            report.offers.push(Offer { group, outcome });
        }

        // The next pass starts after the group that went first, so a short
        // supply of workers does not always favor the same head. Order changes
        // who is served earliest; it never changes who is in the plan, which is
        // why the fairness bound does not mention it.
        self.cursor = plan.entries[0].get().wrapping_add(1);
        if self.cursor >= self.config.max_groups() {
            self.cursor = 0;
        }
        report.progress = PassProgress::Completed;
        report
    }

    /// Resumes the open plan, or arms a new one over the current ready set.
    ///
    /// Returning `None` is the scheduler's idle tick: nothing was ready, so no
    /// plan exists and no group is owed anything.
    fn resume_or_arm(&mut self, report: &mut TickReport) -> Option<Plan> {
        if let Some(open) = self.plan.take() {
            return Some(open);
        }
        let entries = self.arm();
        if entries.is_empty() {
            return None;
        }
        let pass = self.next_pass;
        self.next_pass = following_pass(pass);
        report.armed = Some(entries.clone());
        Some(Plan {
            pass,
            entries,
            next: 0,
        })
    }

    fn apply_signals(&mut self, signals: &[ReadinessSignal]) {
        for signal in signals {
            if !self.config.admits_group(signal.group) {
                continue;
            }
            let stalled = matches!(signal.availability, GroupAvailability::Stalled);
            if let Some(state) = self.slot_mut(signal.group) {
                state.stalled = stalled;
            }
            self.refresh_ready(signal.group);
        }
    }

    fn release_workers(&mut self) -> Vec<GroupId> {
        let now = self.tick;
        let mut released: Vec<GroupId> = Vec::new();
        for slot in &mut self.workers {
            if let Some(dispatch) = *slot {
                if dispatch.busy_until <= now {
                    *slot = None;
                    released.push(dispatch.group);
                }
            }
        }
        released.sort_unstable();
        for group in &released {
            if let Some(state) = self.slot_mut(*group) {
                state.servicing = false;
            }
            self.refresh_ready(*group);
        }
        released
    }

    /// Takes an ordered snapshot of the ready set.
    ///
    /// Cost is one traversal of the ready set, not of the configured group
    /// range, so a host with thousands of idle slots pays nothing to arm a pass
    /// over the handful that have work.
    fn arm(&self) -> Vec<GroupId> {
        let mut entries = self.ready.clone();
        entries.sort_unstable();
        let split = entries.partition_point(|group| group.get() < self.cursor);
        entries.rotate_left(split);
        entries
    }

    fn free_worker(&self) -> Option<usize> {
        self.workers.iter().position(Option::is_none)
    }

    fn offer(&mut self, group: GroupId, worker: usize, report: &mut TickReport) -> OfferOutcome {
        let state = self
            .slot(group)
            .expect("a planned group was created before it became ready");
        if state.stalled {
            return OfferOutcome::Skipped(SkipReason::Stalled);
        }

        let quota = state.quota.get();
        let mut serviced = 0_u32;
        let mut cost = 0_u32;
        while serviced < quota {
            let Some(item) = self.take_next(group) else {
                break;
            };
            cost = cost.saturating_add(item.work.cost().get());
            serviced += 1;
            let result = self.apply_work(group, item);
            report.serviced.push(result);
            self.serviced += 1;
            self.queued -= 1;
            if self.slot(group).is_some_and(|state| state.poisoned) {
                // The rest of this opportunity is not serviced. A poisoned group
                // stops working the instant its own work broke it, and the items
                // behind the failure stay queued for the drain that will report
                // them.
                break;
            }
        }

        self.workers[worker] = Some(Dispatch {
            group,
            busy_until: TickIndex::new(self.tick.get().saturating_add(u64::from(cost))),
        });
        if let Some(state) = self.slot_mut(group) {
            state.servicing = true;
        }
        self.refresh_ready(group);
        OfferOutcome::Dispatched { serviced, cost }
    }

    /// Removes the highest-priority queued item, which is the least class the
    /// group has anything in.
    fn take_next(&mut self, group: GroupId) -> Option<QueuedWork> {
        let state = self.slot_mut(group)?;
        for class in WORK_CLASS_ORDER {
            if let Some(item) = state.queues[class.rank()].pop_front() {
                state.queued -= 1;
                return Some(item);
            }
        }
        None
    }

    fn apply_work(&mut self, group: GroupId, item: QueuedWork) -> ServiceRecord {
        let class = item.work.class();
        let state = self
            .slot_mut(group)
            .expect("work is only serviced for a created group");
        let result = match item.work {
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

                // The cache belongs to the epoch that admitted the request. A
                // session replaced while this item waited has no claim on the
                // result, and the effect still stands.
                if let Some(session) = state.session_mut(request.client_id) {
                    if session.epoch == request.session_epoch
                        && session
                            .outstanding
                            .is_some_and(|(_, _, queued)| queued == item.id)
                    {
                        session.outstanding = None;
                        session.completed = Some((request.sequence, command, outcome));
                    }
                }
                Some(outcome)
            }
        };

        ServiceRecord {
            work: item.id,
            group,
            class,
            result,
        }
    }

    fn create(&mut self, group: GroupId, quota: WorkQuota) -> LifecycleOutcome {
        let index = slot_index(group);
        if index >= self.groups.len() {
            self.groups.resize(index + 1, None);
        }
        let Some(state) = self.groups[index].as_mut() else {
            self.groups[index] = Some(Group::new(GroupIncarnation::first(), quota));
            return LifecycleOutcome::Created {
                incarnation: GroupIncarnation::first(),
            };
        };

        match state.state {
            GroupLifecycle::Creating => {
                if state.quota == quota {
                    LifecycleOutcome::Idempotent {
                        state: state.state,
                        incarnation: state.incarnation,
                    }
                } else {
                    // A quota belongs to an incarnation. Accepting a differing
                    // one as idempotent would discard the number the caller
                    // asked for while reporting success.
                    LifecycleOutcome::Rejected(LifecycleRejection::QuotaConflict {
                        current: state.quota,
                    })
                }
            }
            GroupLifecycle::Removed => {
                let Some(next) = state.incarnation.successor() else {
                    return LifecycleOutcome::Rejected(LifecycleRejection::IncarnationExhausted);
                };
                state.incarnation = next;
                state.state = GroupLifecycle::Creating;
                state.quota = quota;
                state.clear();
                LifecycleOutcome::Created { incarnation: next }
            }
            GroupLifecycle::Tombstoned => {
                LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
            }
            current => LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current,
                requested: GroupLifecycle::Creating,
            }),
        }
    }

    /// Applies a transition that has exactly one legal predecessor.
    fn advance(
        &mut self,
        group: GroupId,
        from: GroupLifecycle,
        to: GroupLifecycle,
    ) -> LifecycleOutcome {
        let Some(state) = self.slot_mut(group) else {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
        };
        if state.state == to {
            return LifecycleOutcome::Idempotent {
                state: to,
                incarnation: state.incarnation,
            };
        }
        if state.state == GroupLifecycle::Tombstoned {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned);
        }
        if state.state != from {
            return LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current: state.state,
                requested: to,
            });
        }
        state.state = to;
        LifecycleOutcome::Applied {
            from,
            to,
            incarnation: state.incarnation,
        }
    }

    /// Draining has three legal predecessors, so it does not fit [`Self::advance`].
    fn drain(&mut self, group: GroupId) -> LifecycleOutcome {
        let Some(state) = self.slot_mut(group) else {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
        };
        match state.state {
            GroupLifecycle::Draining => LifecycleOutcome::Idempotent {
                state: GroupLifecycle::Draining,
                incarnation: state.incarnation,
            },
            GroupLifecycle::Tombstoned => {
                LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
            }
            current @ (GroupLifecycle::Creating
            | GroupLifecycle::Recovering
            | GroupLifecycle::Serving) => {
                state.state = GroupLifecycle::Draining;
                LifecycleOutcome::Applied {
                    from: current,
                    to: GroupLifecycle::Draining,
                    incarnation: state.incarnation,
                }
            }
            current => LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current,
                requested: GroupLifecycle::Draining,
            }),
        }
    }

    fn remove(&mut self, group: GroupId) -> LifecycleOutcome {
        let Some(state) = self.slot(group) else {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
        };
        if state.state == GroupLifecycle::Draining && state.queued > 0 {
            // Removal cannot outrun the queue. A healthy group's accepted work
            // leaves by being serviced; a poisoned group's leaves through the
            // failure records the drain already emitted. Neither vanishes.
            return LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained {
                pending: state.queued,
            });
        }
        let outcome = self.advance(group, GroupLifecycle::Draining, GroupLifecycle::Removed);
        if matches!(outcome, LifecycleOutcome::Applied { .. }) {
            if let Some(state) = self.slot_mut(group) {
                state.clear();
            }
        }
        outcome
    }

    /// Retires a poisoned group's queue when it begins draining.
    ///
    /// A poisoned group can service nothing, so the drain that would otherwise
    /// empty its queue has to report the loss instead. Each item gets its own
    /// record naming it: accepted work is allowed to fail, and is never allowed
    /// to disappear.
    fn retire_poisoned_queue(&mut self, group: GroupId) -> Vec<FailureRecord> {
        let Some(state) = self.slot_mut(group) else {
            return Vec::new();
        };
        if !state.poisoned {
            return Vec::new();
        }
        let mut failed: Vec<FailureRecord> = Vec::new();
        for class in WORK_CLASS_ORDER {
            while let Some(item) = state.queues[class.rank()].pop_front() {
                failed.push(FailureRecord {
                    work: item.id,
                    group,
                    reason: WorkFailure::GroupPoisoned,
                });
            }
        }
        state.queued = 0;
        for session in &mut state.sessions {
            session.outstanding = None;
        }
        let retired = count(failed.len());
        self.queued -= retired;
        self.failed += u64::from(retired);
        failed
    }

    /// Applies every gate that depends only on the addressed slot's identity.
    ///
    /// # Errors
    ///
    /// Returns the refusal a caller sees when the slot does not exist, has been
    /// tombstoned, or is not the incarnation the traffic named.
    fn admit_group(
        &self,
        group: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<(), AdmissionRejection> {
        if !self.config.admits_group(group) {
            return Err(AdmissionRejection::GroupOutOfRange);
        }
        let Some(state) = self.slot(group) else {
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

    fn slot(&self, group: GroupId) -> Option<&Group> {
        self.groups.get(slot_index(group))?.as_ref()
    }

    fn slot_mut(&mut self, group: GroupId) -> Option<&mut Group> {
        self.groups.get_mut(slot_index(group))?.as_mut()
    }

    /// Brings the ready set back in step with one group's state.
    ///
    /// Membership is maintained exactly, in constant time per change, so the
    /// scheduler never scans idle groups looking for work.
    fn refresh_ready(&mut self, group: GroupId) {
        let Some(state) = self.slot(group) else {
            return;
        };
        let should_be_ready = state.is_ready();
        let position = state.ready_position;
        match (should_be_ready, position) {
            (true, None) => {
                let index = self.ready.len();
                self.ready.push(group);
                if let Some(state) = self.slot_mut(group) {
                    state.ready_position = Some(index);
                }
            }
            (false, Some(index)) => {
                self.ready.swap_remove(index);
                if let Some(state) = self.slot_mut(group) {
                    state.ready_position = None;
                }
                if let Some(moved) = self.ready.get(index).copied() {
                    if let Some(state) = self.slot_mut(moved) {
                        state.ready_position = Some(index);
                    }
                }
            }
            (true, Some(_)) | (false, None) => {}
        }
    }
}

/// Decides one counter submission against its client session.
///
/// `Err` carries the answer the caller must return; `Ok` means the request is
/// new work that may proceed to the queue bounds.
///
/// The completed cache answers before any queue bound is consulted. An
/// acknowledged request has to stay confirmable while the queue is full, or a
/// client would be told to retry a command that already took effect.
fn admit_under_session(
    session: &Session,
    request: RequestIdentity,
    command: CounterCommand,
) -> Result<(), AdmissionOutcome> {
    if request.session_epoch < session.epoch {
        return Err(AdmissionOutcome::Rejected(
            AdmissionRejection::StaleSession {
                current: session.epoch,
            },
        ));
    }
    if request.session_epoch > session.epoch {
        return Err(AdmissionOutcome::Rejected(
            AdmissionRejection::FutureSession {
                current: session.epoch,
            },
        ));
    }

    let recomputed = RequestFingerprint::of(&command);
    if request.fingerprint != recomputed {
        return Err(AdmissionOutcome::Rejected(
            AdmissionRejection::FingerprintMismatch {
                expected: recomputed,
            },
        ));
    }

    let mut expected = Sequence::first();
    if let Some((completed, cached_command, result)) = session.completed {
        if request.sequence < completed {
            return Err(AdmissionOutcome::Rejected(
                AdmissionRejection::StaleSequence { highest: completed },
            ));
        }
        if request.sequence == completed {
            return Err(if command == cached_command {
                AdmissionOutcome::Replayed { result }
            } else {
                AdmissionOutcome::Rejected(AdmissionRejection::ConflictingRetry)
            });
        }
        let Some(successor) = completed.successor() else {
            return Err(AdmissionOutcome::Rejected(
                AdmissionRejection::SequenceExhausted,
            ));
        };
        expected = successor;
    }

    if let Some((outstanding, queued_command, queued_id)) = session.outstanding {
        if request.sequence == outstanding {
            return Err(if command == queued_command {
                AdmissionOutcome::AlreadyQueued { work: queued_id }
            } else {
                AdmissionOutcome::Rejected(AdmissionRejection::ConflictingRetry)
            });
        }
    }
    if request.sequence != expected {
        return Err(AdmissionOutcome::Rejected(
            AdmissionRejection::SequenceGap { expected },
        ));
    }
    Ok(())
}

fn work_id(value: u64) -> WorkId {
    WorkId::new(value).expect("work identifiers start at one and only rise")
}

fn following_pass(pass: PassIndex) -> PassIndex {
    pass.successor()
        .expect("a scheduler cannot arm enough plans to exhaust a u64")
}

fn group_view(group: GroupId, state: &Group) -> GroupView {
    GroupView {
        group,
        incarnation: state.incarnation,
        state: state.state,
        poisoned: state.poisoned,
        stalled: state.stalled,
        counter: state.counter,
        queued: state.queued,
        quota: state.quota,
        servicing: state.servicing,
    }
}

fn slot_index(group: GroupId) -> usize {
    usize::try_from(group.get()).expect("group identifiers fit in usize")
}

fn group_id(index: usize) -> GroupId {
    GroupId::new(u32::try_from(index).expect("slot indices stay within the configured u32 bound"))
}

fn count(value: usize) -> u32 {
    u32::try_from(value).expect("bounded counts stay within the configured u32 bound")
}
