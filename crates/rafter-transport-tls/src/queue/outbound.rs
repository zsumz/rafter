//! Nonblocking per-peer queue with reserved control capacity.

use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex},
    time::Duration,
};

use crate::{RuntimeLimits, TrafficClass};

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

#[derive(Debug)]
pub(crate) struct OutboundQueue<G> {
    limits: RuntimeLimits,
    state: Mutex<OutboundState<G>>,
    available: Condvar,
}

#[derive(Debug)]
struct OutboundState<G> {
    control: VecDeque<OutboundItem<G>>,
    replication: VecDeque<OutboundItem<G>>,
    snapshot: VecDeque<OutboundItem<G>>,
    retained: QueueUsage,
    retained_bulk: QueueUsage,
    control_streak: usize,
    prefer_snapshot: bool,
    closed: bool,
}

impl<G> Default for OutboundState<G> {
    fn default() -> Self {
        Self {
            control: VecDeque::new(),
            replication: VecDeque::new(),
            snapshot: VecDeque::new(),
            retained: QueueUsage::default(),
            retained_bulk: QueueUsage::default(),
            control_streak: 0,
            prefer_snapshot: false,
            closed: false,
        }
    }
}

impl<G> OutboundState<G> {
    fn queued_is_empty(&self) -> bool {
        self.control.is_empty() && self.replication.is_empty() && self.snapshot.is_empty()
    }
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
            TrafficClass::Snapshot => state.snapshot.push_back(item),
        }
        self.available.notify_one();
        Ok(())
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
        if state.queued_is_empty() && !state.closed {
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

    /// Discards only items still owned by the queue.
    pub(crate) fn discard_queued(&self) -> Result<QueueUsage, OutboundQueueError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OutboundQueueError::Poisoned)?;
        let discarded = queued_usage(&state)?;
        discard_class(&mut state, TrafficClass::Control)?;
        discard_class(&mut state, TrafficClass::Replication)?;
        discard_class(&mut state, TrafficClass::Snapshot)?;
        Ok(discarded)
    }
}

fn discard_class<G>(
    state: &mut OutboundState<G>,
    class: TrafficClass,
) -> Result<(), OutboundQueueError> {
    loop {
        let item = match class {
            TrafficClass::Control => state.control.front(),
            TrafficClass::Replication => state.replication.front(),
            TrafficClass::Snapshot => state.snapshot.front(),
        };
        let Some(item) = item else {
            return Ok(());
        };
        let bytes = item.bytes();
        release_retained(state, class, bytes)?;
        let _ = match class {
            TrafficClass::Control => state.control.pop_front(),
            TrafficClass::Replication => state.replication.pop_front(),
            TrafficClass::Snapshot => state.snapshot.pop_front(),
        };
    }
}

fn release_retained<G>(
    state: &mut OutboundState<G>,
    class: TrafficClass,
    bytes: usize,
) -> Result<(), OutboundQueueError> {
    state.retained = state
        .retained
        .removed(bytes)
        .ok_or(OutboundQueueError::Poisoned)?;
    if class != TrafficClass::Control {
        state.retained_bulk = state
            .retained_bulk
            .removed(bytes)
            .ok_or(OutboundQueueError::Poisoned)?;
    }
    Ok(())
}

fn queued_usage<G>(state: &OutboundState<G>) -> Result<QueueUsage, OutboundQueueError> {
    let mut usage = QueueUsage::default();
    for item in state
        .control
        .iter()
        .chain(state.replication.iter())
        .chain(state.snapshot.iter())
    {
        usage = usage
            .added(item.bytes())
            .ok_or(OutboundQueueError::Poisoned)?;
    }
    Ok(usage)
}

fn select_next<G>(state: &mut OutboundState<G>, control_burst: usize) -> Option<OutboundItem<G>> {
    let bulk_waiting = !state.replication.is_empty() || !state.snapshot.is_empty();
    if !state.control.is_empty() && (!bulk_waiting || state.control_streak < control_burst) {
        state.control_streak = state.control_streak.saturating_add(1);
        return state.control.pop_front();
    }

    state.control_streak = 0;
    let selected = if state.prefer_snapshot {
        state
            .snapshot
            .pop_front()
            .or_else(|| state.replication.pop_front())
    } else {
        state
            .replication
            .pop_front()
            .or_else(|| state.snapshot.pop_front())
    };
    if selected.is_some() {
        state.prefer_snapshot = !state.prefer_snapshot;
        return selected;
    }
    state.control.pop_front()
}

#[cfg(test)]
#[path = "outbound_test.rs"]
mod tests;
