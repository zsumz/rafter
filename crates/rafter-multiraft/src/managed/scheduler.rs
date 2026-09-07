//! The bounded, deterministic many-group scheduler itself.
//!
//! Admission, ready-set membership, pass and dispatch identity, and worker
//! occupancy move only through this type, and a dispatch releases its worker
//! only on a completion this scheduler's own authority validates item by item.
//! It has no threads, clocks, or I/O: the caller performs every turn.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::Debug,
    num::NonZeroUsize,
    sync::Arc,
};

use super::{
    CompletionError, Dispatch, DispatchId, DispatchItem, ManagedConfig, ManagedMetrics, PassId,
    WorkClass, WorkId,
};

mod dispatch;
mod group;

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
