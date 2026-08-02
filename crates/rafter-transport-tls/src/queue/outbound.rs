//! Nonblocking per-peer queue with reserved control capacity.

mod state;

use std::{
    sync::{Condvar, Mutex},
    time::Duration,
};

use crate::{RuntimeLimits, TrafficClass};

use self::state::{discard_class, queued_usage, release_retained, select_next, OutboundState};
use super::{OutboundItem, QueueUsage};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueueFull {
    pub(crate) usage: QueueUsage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboundQueueError {
    Full(QueueFull),
    Closed,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequeueOutcome {
    Queued,
    SenderStopped,
}

#[derive(Debug)]
pub(crate) struct OutboundQueue<G> {
    limits: RuntimeLimits,
    state: Mutex<OutboundState<G>>,
    available: Condvar,
}

impl<G> OutboundQueue<G> {
    pub(crate) fn new(limits: RuntimeLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(OutboundState::default()),
            available: Condvar::new(),
        }
    }

    pub(crate) fn try_push(&self, item: OutboundItem<G>) -> Result<(), OutboundQueueError> {
        let class = item.class();
        let prepared = item.prepared().is_some();
        let bytes = item.bytes();
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        if state.closed {
            return Err(OutboundQueueError::Closed);
        }
        let accepted = match class {
            TrafficClass::Control => state.retained.can_add(
                bytes,
                self.limits.outbound_frames_per_peer(),
                self.limits.outbound_bytes_per_peer(),
            ),
            TrafficClass::Replication | TrafficClass::Snapshot => {
                let bulk_frames = self
                    .limits
                    .outbound_frames_per_peer()
                    .saturating_sub(self.limits.reserved_control_frames());
                let bulk_bytes = self
                    .limits
                    .outbound_bytes_per_peer()
                    .saturating_sub(self.limits.reserved_control_bytes());
                state.retained.can_add(
                    bytes,
                    self.limits.outbound_frames_per_peer(),
                    self.limits.outbound_bytes_per_peer(),
                ) && state.retained_bulk.can_add(bytes, bulk_frames, bulk_bytes)
            }
        };
        if !accepted {
            return Err(OutboundQueueError::Full(QueueFull {
                usage: state.retained,
            }));
        }

        state.retained = state
            .retained
            .added(bytes)
            .ok_or(OutboundQueueError::Poisoned)?;
        if class != TrafficClass::Control {
            state.retained_bulk = state
                .retained_bulk
                .added(bytes)
                .ok_or(OutboundQueueError::Poisoned)?;
        }
        match class {
            TrafficClass::Control => state.control.push_back(item),
            TrafficClass::Replication => state.replication.push_back(item),
            TrafficClass::Snapshot if prepared => state.snapshot_ready.push_back(item),
            TrafficClass::Snapshot => state.snapshot_pending.push_back(item),
        }
        self.available.notify_one();
        Ok(())
    }

    /// Removes one unresolved snapshot directive for the dedicated resolver lane.
    pub(crate) fn pop_snapshot_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<OutboundItem<G>>, OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        if state.snapshot_pending.is_empty() && !state.closed {
            let waited = self
                .available
                .wait_timeout(state, timeout)
                .map_err(|_| OutboundQueueError::Poisoned)?;
            state = waited.0;
        }
        Ok(state.snapshot_pending.pop_front())
    }

    /// Returns already-accounted work to the sender lane when it still exists.
    ///
    /// [`RequeueOutcome::SenderStopped`] means retirement atomically released
    /// and discarded the item, so the caller records but does not release it.
    pub(crate) fn requeue_ready(
        &self,
        item: OutboundItem<G>,
    ) -> Result<RequeueOutcome, OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        if state.sender_stopped {
            release_retained(&mut state, item.class(), item.bytes())?;
            return Ok(RequeueOutcome::SenderStopped);
        }
        match item.class() {
            TrafficClass::Control => state.control.push_front(item),
            TrafficClass::Replication => state.replication.push_back(item),
            TrafficClass::Snapshot => state.snapshot_ready.push_back(item),
        }
        self.available.notify_all();
        Ok(RequeueOutcome::Queued)
    }

    pub(crate) fn snapshots_closed_and_empty(&self) -> Result<bool, OutboundQueueError> {
        self.state
            .lock()
            .map(|state| state.closed && state.snapshot_pending.is_empty())
            .map_err(|_| OutboundQueueError::Poisoned)
    }

    /// Removes one queued item while retaining its capacity until `release`.
    pub(crate) fn pop_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<OutboundItem<G>>, OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        if state.sender_is_empty() && (!state.closed || state.retained.frames > 0) {
            let waited = self
                .available
                .wait_timeout(state, timeout)
                .map_err(|_| OutboundQueueError::Poisoned)?;
            state = waited.0;
        }
        Ok(select_next(&mut state, self.limits.control_burst()))
    }

    /// Releases count-and-byte capacity held by one popped item.
    pub(crate) fn release(&self, item: &OutboundItem<G>) -> Result<(), OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        release_retained(&mut state, item.class(), item.bytes())
    }

    pub(crate) fn depth(&self) -> Result<QueueUsage, OutboundQueueError> {
        self.state
            .lock()
            .map(|state| state.retained)
            .map_err(|_| OutboundQueueError::Poisoned)
    }

    pub(crate) fn is_closed_and_empty(&self) -> Result<bool, OutboundQueueError> {
        self.state
            .lock()
            .map(|state| state.closed && state.retained.frames == 0)
            .map_err(|_| OutboundQueueError::Poisoned)
    }

    pub(crate) fn close(&self) -> Result<(), OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        state.closed = true;
        self.available.notify_all();
        Ok(())
    }

    /// Atomically retires the sender and discards items still owned by the queue.
    pub(crate) fn stop_sender_and_discard_queued(&self) -> Result<QueueUsage, OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        state.sender_stopped = true;
        state.closed = true;
        let discarded = queued_usage(&state)?;
        discard_class(&mut state, TrafficClass::Control)?;
        discard_class(&mut state, TrafficClass::Replication)?;
        discard_class(&mut state, TrafficClass::Snapshot)?;
        Ok(discarded)
    }
}

#[cfg(test)]
#[path = "outbound_test.rs"]
mod tests;
