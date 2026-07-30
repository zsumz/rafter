use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::Debug,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use super::{
    AdmissionReceipt, AdmissionRejected, AdmissionRejection, ArmPass, BeginDispatch,
    CompletionError, Dispatch, DispatchCompletion, DispatchCompletionPermit, DispatchId,
    DispatchItem, FailedQueuedItem, GroupStateError, IdentityError, ManagedConfig, ManagedMetrics,
    PassCompletion, PassId, PassPlan, RegisterError, RemoveError, SkipReason, SkippedOpportunity,
    WorkClass, WorkDisposition, WorkId,
};

#[derive(Debug)]
struct QueuedWork<T> {
    work_id: WorkId,
    payload: T,
}

#[derive(Debug)]
struct GroupQueue<T> {
    available: bool,
    quota: NonZeroUsize,
    classes: [VecDeque<QueuedWork<T>>; WorkClass::COUNT],
    in_flight: bool,
}

impl<T> GroupQueue<T> {
    fn new(quota: NonZeroUsize) -> Self {
        Self {
            available: false,
            quota,
            classes: std::array::from_fn(|_| VecDeque::new()),
            in_flight: false,
        }
    }

    fn len(&self) -> usize {
        self.classes.iter().map(VecDeque::len).sum()
    }

    fn pop(&mut self) -> Option<DispatchItem<T>> {
        self.classes
            .iter_mut()
            .enumerate()
            .find_map(|(index, queue)| {
                queue.pop_front().map(|work| DispatchItem {
                    work_id: work.work_id,
                    class: WorkClass::from_index(index),
                    payload: work.payload,
                })
            })
    }
}

#[derive(Debug)]
struct OpenPass<G> {
    id: PassId,
    groups: VecDeque<G>,
    planned: usize,
    dispatched: usize,
    skipped: usize,
}

#[derive(Debug)]
struct InFlight<G> {
    group_id: G,
    work_ids: Vec<WorkId>,
}

/// Deterministic, bounded, sans-I/O many-group scheduler.
#[derive(Debug)]
pub struct ManagedScheduler<G, T> {
    authority: Arc<()>,
    config: ManagedConfig,
    groups: BTreeMap<G, GroupQueue<T>>,
    ready: BTreeSet<G>,
    in_flight: BTreeMap<DispatchId, InFlight<G>>,
    queued: usize,
    in_flight_work: usize,
    next_work_id: u64,
    next_pass_id: u64,
    next_dispatch_id: u64,
    open_pass: Option<OpenPass<G>>,
    passes_armed: u64,
    passes_completed: u64,
    admitted: u64,
    serviced: u64,
    failed: u64,
}

impl<G, T> ManagedScheduler<G, T>
where
    G: Clone + Ord + Debug,
{
    /// Creates an empty scheduler with fixed bounds.
    #[must_use]
    pub fn new(config: ManagedConfig) -> Self {
        Self {
            authority: Arc::new(()),
            config,
            groups: BTreeMap::new(),
            ready: BTreeSet::new(),
            in_flight: BTreeMap::new(),
            queued: 0,
            in_flight_work: 0,
            next_work_id: 1,
            next_pass_id: 1,
            next_dispatch_id: 1,
            open_pass: None,
            passes_armed: 0,
            passes_completed: 0,
            admitted: 0,
            serviced: 0,
            failed: 0,
        }
    }

    /// Returns whether a group is registered.
    #[must_use]
    pub fn contains_group(&self, group_id: &G) -> bool {
        self.groups.contains_key(group_id)
    }

    /// Registers an unavailable group with an optional quota override.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::AlreadyRegistered`] for an existing key.
    pub fn register_group(
        &mut self,
        group_id: G,
        quota: Option<NonZeroUsize>,
    ) -> Result<(), RegisterError<G>> {
        if self.groups.contains_key(&group_id) {
            return Err(RegisterError::AlreadyRegistered(group_id));
        }
        self.groups.insert(
            group_id,
            GroupQueue::new(quota.unwrap_or(self.config.default_quota())),
        );
        Ok(())
    }

    /// Removes an empty, idle group.
    ///
    /// Returns `Ok(false)` when the group is not registered.
    ///
    /// # Errors
    ///
    /// Refuses a group with queued or in-flight accepted work.
    pub fn remove_group(&mut self, group_id: &G) -> Result<bool, RemoveError<G>> {
        if !self.can_remove_group(group_id)? {
            return Ok(false);
        }
        self.ready.remove(group_id);
        self.groups.remove(group_id);
        Ok(true)
    }

    /// Checks the exact preconditions for removing a group without changing it.
    ///
    /// This is useful when a caller must durably publish a removal transaction
    /// before detaching the already-proven-idle driver.
    ///
    /// # Errors
    ///
    /// Refuses a group with queued or in-flight accepted work.
    pub fn can_remove_group(&self, group_id: &G) -> Result<bool, RemoveError<G>> {
        let Some(group) = self.groups.get(group_id) else {
            return Ok(false);
        };
        let queued = group.len();
        if queued != 0 {
            return Err(RemoveError::Queued {
                group_id: group_id.clone(),
                items: queued,
            });
        }
        if group.in_flight {
            return Err(RemoveError::InFlight(group_id.clone()));
        }
        Ok(true)
    }

    /// Explicitly fails every queued item for one group in class/FIFO order.
    ///
    /// An in-flight dispatch is deliberately untouched: its owner must finish
    /// through the exact dispatch-completion protocol. The returned payloads
    /// are the only copy held by the scheduler.
    ///
    /// # Errors
    ///
    /// Returns [`GroupStateError::UnknownGroup`] for an unregistered key.
    pub fn fail_queued(
        &mut self,
        group_id: &G,
    ) -> Result<Vec<FailedQueuedItem<T>>, GroupStateError<G>> {
        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| GroupStateError::UnknownGroup(group_id.clone()))?;
        let mut failed = Vec::with_capacity(group.len());
        while let Some(item) = group.pop() {
            failed.push(FailedQueuedItem {
                work_id: item.work_id,
                class: item.class,
                payload: item.payload,
            });
        }
        self.queued -= failed.len();
        self.failed += failed.len() as u64;
        self.refresh_ready(group_id);
        Ok(failed)
    }

    /// Sets whether a group may appear in newly armed passes.
    ///
    /// # Errors
    ///
    /// Returns [`GroupStateError::UnknownGroup`] for an unregistered key.
    pub fn set_available(
        &mut self,
        group_id: &G,
        available: bool,
    ) -> Result<(), GroupStateError<G>> {
        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| GroupStateError::UnknownGroup(group_id.clone()))?;
        group.available = available;
        self.refresh_ready(group_id);
        Ok(())
    }

    /// Admits one payload or returns it unchanged with a typed refusal.
    ///
    /// # Errors
    ///
    /// Returns the payload unchanged when the group is unknown, either queue
    /// bound is full, or stable work identities are exhausted.
    pub fn admit(
        &mut self,
        group_id: &G,
        class: WorkClass,
        payload: T,
    ) -> Result<AdmissionReceipt, AdmissionRejected<G, T>> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Err(AdmissionRejected {
                reason: AdmissionRejection::UnknownGroup(group_id.clone()),
                payload,
            });
        };
        if group.len() >= self.config.max_group_queue().get() {
            return Err(AdmissionRejected {
                reason: AdmissionRejection::GroupQueueFull {
                    group_id: group_id.clone(),
                    bound: self.config.max_group_queue().get(),
                },
                payload,
            });
        }
        if self.queued >= self.config.max_global_queue().get() {
            return Err(AdmissionRejected {
                reason: AdmissionRejection::GlobalQueueFull {
                    bound: self.config.max_global_queue().get(),
                },
                payload,
            });
        }
        let Some(raw_id) = NonZeroU64::new(self.next_work_id) else {
            return Err(AdmissionRejected {
                reason: AdmissionRejection::WorkIdentityExhausted,
                payload,
            });
        };
        self.next_work_id = self.next_work_id.checked_add(1).unwrap_or(0);
        let work_id = WorkId::new(raw_id);
        group.classes[class.index()].push_back(QueuedWork { work_id, payload });
        self.queued += 1;
        self.admitted += 1;
        let group_queue_depth = group.len();
        self.refresh_ready(group_id);
        Ok(AdmissionReceipt {
            work_id,
            group_queue_depth,
            global_queue_depth: self.queued,
        })
    }

    /// Arms an immutable pass from the current ready set.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::PassExhausted`] when no stable pass identity
    /// remains.
    pub fn arm_pass(&mut self) -> Result<ArmPass<G>, IdentityError> {
        if let Some(pass) = &self.open_pass {
            return Ok(ArmPass::AlreadyArmed(pass.id));
        }
        if self.ready.is_empty() {
            return Ok(ArmPass::Idle);
        }
        let Some(raw_id) = NonZeroU64::new(self.next_pass_id) else {
            return Err(IdentityError::PassExhausted);
        };
        self.next_pass_id = self.next_pass_id.checked_add(1).unwrap_or(0);
        let pass_id = PassId::new(raw_id);
        let groups = self.ready.iter().cloned().collect::<Vec<_>>();
        self.open_pass = Some(OpenPass {
            id: pass_id,
            groups: groups.iter().cloned().collect(),
            planned: groups.len(),
            dispatched: 0,
            skipped: 0,
        });
        self.passes_armed += 1;
        Ok(ArmPass::Armed(PassPlan { pass_id, groups }))
    }

    /// Opens the next planned group turn when a worker is free.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::DispatchExhausted`] without consuming a pass
    /// position when no stable dispatch identity remains.
    pub fn begin_dispatch(&mut self) -> Result<BeginDispatch<G, T>, IdentityError> {
        let Some(pass) = &self.open_pass else {
            return Ok(BeginDispatch::NoPass);
        };
        if pass.groups.is_empty() {
            let Some(pass) = self.open_pass.take() else {
                return Ok(BeginDispatch::NoPass);
            };
            self.passes_completed += 1;
            return Ok(BeginDispatch::PassComplete(PassCompletion {
                pass_id: pass.id,
                planned: pass.planned,
                dispatched: pass.dispatched,
                skipped: pass.skipped,
            }));
        }
        if self.in_flight.len() >= self.config.workers().get() {
            return Ok(BeginDispatch::WorkersOccupied);
        }

        let Some(pass) = self.open_pass.as_mut() else {
            return Ok(BeginDispatch::NoPass);
        };
        let Some(group_id) = pass.groups.pop_front() else {
            return Ok(BeginDispatch::NoPass);
        };
        let pass_id = pass.id;
        let Some(group) = self.groups.get_mut(&group_id) else {
            pass.skipped += 1;
            return Ok(BeginDispatch::Skipped(SkippedOpportunity {
                pass_id,
                group_id,
                reason: SkipReason::Unavailable,
            }));
        };
        let skip = if !group.available {
            Some(SkipReason::Unavailable)
        } else if group.in_flight {
            Some(SkipReason::InFlight)
        } else if group.len() == 0 {
            Some(SkipReason::Empty)
        } else {
            None
        };
        if let Some(reason) = skip {
            pass.skipped += 1;
            self.refresh_ready(&group_id);
            return Ok(BeginDispatch::Skipped(SkippedOpportunity {
                pass_id,
                group_id,
                reason,
            }));
        }
        let Some(raw_id) = NonZeroU64::new(self.next_dispatch_id) else {
            pass.groups.push_front(group_id);
            return Err(IdentityError::DispatchExhausted);
        };
        let dispatch_id = DispatchId::new(raw_id);
        self.next_dispatch_id = self.next_dispatch_id.checked_add(1).unwrap_or(0);
        let quota = group.quota.get();
        let mut items = Vec::with_capacity(quota.min(group.len()));
        for _ in 0..quota {
            let Some(item) = group.pop() else {
                break;
            };
            items.push(item);
        }
        group.in_flight = true;
        self.queued -= items.len();
        self.in_flight_work += items.len();
        self.ready.remove(&group_id);
        pass.dispatched += 1;
        self.in_flight.insert(
            dispatch_id,
            InFlight {
                group_id: group_id.clone(),
                work_ids: items.iter().map(|item| item.work_id).collect(),
            },
        );
        Ok(BeginDispatch::Dispatched(Dispatch {
            authority: Arc::clone(&self.authority),
            pass_id,
            dispatch_id,
            group_id,
            items,
        }))
    }

    /// Releases one dispatch after exact per-item disposition.
    ///
    /// # Errors
    ///
    /// Refuses unknown, partial, extra, or mismatched completion without
    /// releasing the worker or changing conservation counters.
    pub fn complete_dispatch(
        &mut self,
        permit: &DispatchCompletionPermit<G>,
        dispositions: &[WorkDisposition],
    ) -> Result<DispatchCompletion<G>, CompletionError> {
        let dispatch_id = permit.dispatch_id;
        if !Arc::ptr_eq(&self.authority, &permit.authority) {
            return Err(CompletionError::ForeignDispatch(dispatch_id));
        }
        let in_flight = self
            .in_flight
            .get(&dispatch_id)
            .ok_or(CompletionError::UnknownDispatch(dispatch_id))?;
        if in_flight.group_id != permit.group_id {
            return Err(CompletionError::ForeignDispatch(dispatch_id));
        }
        if dispositions.len() != in_flight.work_ids.len() {
            return Err(CompletionError::WrongItemCount {
                dispatch_id,
                expected: in_flight.work_ids.len(),
                actual: dispositions.len(),
            });
        }
        for (expected, disposition) in in_flight.work_ids.iter().zip(dispositions) {
            if *expected != disposition.work_id() {
                return Err(CompletionError::WrongWork {
                    dispatch_id,
                    expected: *expected,
                    actual: disposition.work_id(),
                });
            }
        }
        let group_id = in_flight.group_id.clone();
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(CompletionError::UnknownDispatch(dispatch_id))?;
        group.in_flight = false;

        let Some(in_flight) = self.in_flight.remove(&dispatch_id) else {
            return Err(CompletionError::UnknownDispatch(dispatch_id));
        };
        let serviced = dispositions
            .iter()
            .filter(|item| matches!(item, WorkDisposition::Serviced(_)))
            .count();
        let failed = dispositions.len() - serviced;
        self.in_flight_work -= dispositions.len();
        self.serviced += serviced as u64;
        self.failed += failed as u64;
        self.refresh_ready(&group_id);
        Ok(DispatchCompletion {
            dispatch_id,
            group_id: in_flight.group_id,
            serviced,
            failed,
        })
    }

    pub(super) fn validate_dispatch(
        &self,
        dispatch: &Dispatch<G, T>,
    ) -> Result<(), CompletionError> {
        if !Arc::ptr_eq(&self.authority, &dispatch.authority) {
            return Err(CompletionError::ForeignDispatch(dispatch.dispatch_id));
        }
        let in_flight = self
            .in_flight
            .get(&dispatch.dispatch_id)
            .ok_or(CompletionError::UnknownDispatch(dispatch.dispatch_id))?;
        if in_flight.group_id != dispatch.group_id {
            return Err(CompletionError::ForeignDispatch(dispatch.dispatch_id));
        }
        if in_flight.work_ids.len() != dispatch.items.len() {
            return Err(CompletionError::WrongItemCount {
                dispatch_id: dispatch.dispatch_id,
                expected: in_flight.work_ids.len(),
                actual: dispatch.items.len(),
            });
        }
        for (expected, item) in in_flight.work_ids.iter().zip(&dispatch.items) {
            if *expected != item.work_id {
                return Err(CompletionError::WrongWork {
                    dispatch_id: dispatch.dispatch_id,
                    expected: *expected,
                    actual: item.work_id,
                });
            }
        }
        Ok(())
    }

    /// Returns scheduler-wide bounded metrics.
    #[must_use]
    pub fn metrics(&self) -> ManagedMetrics {
        ManagedMetrics {
            groups: self.groups.len(),
            ready_groups: self.ready.len(),
            queued: self.queued,
            in_flight_work: self.in_flight_work,
            occupied_workers: self.in_flight.len(),
            workers: self.config.workers().get(),
            passes_armed: self.passes_armed,
            passes_completed: self.passes_completed,
            admitted: self.admitted,
            serviced: self.serviced,
            failed: self.failed,
            open_pass: self.open_pass.as_ref().map(|pass| pass.id),
        }
    }

    fn refresh_ready(&mut self, group_id: &G) {
        let ready = self
            .groups
            .get(group_id)
            .is_some_and(|group| group.available && !group.in_flight && group.len() != 0);
        if ready {
            self.ready.insert(group_id.clone());
        } else {
            self.ready.remove(group_id);
        }
    }
}
