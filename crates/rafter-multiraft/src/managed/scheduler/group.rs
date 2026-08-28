//! Group registration, availability, and admission into bounded queues.
//!
//! A group arrives unavailable and stays that way until the caller says
//! otherwise; removal refuses while any accepted work is still queued or in
//! flight. Admission hands the caller's payload back unchanged whenever a
//! bound refuses it, so nothing is lost to a full queue.

use std::{
    fmt::Debug,
    num::{NonZeroU64, NonZeroUsize},
};

use crate::managed::{
    AdmissionReceipt, AdmissionRejected, AdmissionRejection, FailedQueuedItem, GroupStateError,
    RegisterError, RemoveError, WorkClass, WorkId,
};

use super::{GroupQueue, ManagedScheduler, QueuedWork};

impl<G, T> ManagedScheduler<G, T>
where
    G: Clone + Ord + Debug,
{
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
}
