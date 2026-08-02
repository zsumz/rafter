//! Queue lanes, accounting, and weighted sender selection.

use std::collections::VecDeque;

use crate::TrafficClass;

use super::super::{OutboundItem, QueueUsage};
use super::OutboundQueueError;

#[derive(Debug)]
pub(super) struct OutboundState<G> {
    pub(super) control: VecDeque<OutboundItem<G>>,
    pub(super) replication: VecDeque<OutboundItem<G>>,
    pub(super) snapshot_ready: VecDeque<OutboundItem<G>>,
    pub(super) snapshot_pending: VecDeque<OutboundItem<G>>,
    pub(super) retained: QueueUsage,
    pub(super) retained_bulk: QueueUsage,
    pub(super) control_streak: usize,
    pub(super) prefer_snapshot: bool,
    pub(super) closed: bool,
    pub(super) sender_stopped: bool,
}

impl<G> Default for OutboundState<G> {
    fn default() -> Self {
        Self {
            control: VecDeque::new(),
            replication: VecDeque::new(),
            snapshot_ready: VecDeque::new(),
            snapshot_pending: VecDeque::new(),
            retained: QueueUsage::default(),
            retained_bulk: QueueUsage::default(),
            control_streak: 0,
            prefer_snapshot: false,
            closed: false,
            sender_stopped: false,
        }
    }
}

impl<G> OutboundState<G> {
    pub(super) fn sender_is_empty(&self) -> bool {
        self.control.is_empty() && self.replication.is_empty() && self.snapshot_ready.is_empty()
    }
}

pub(super) fn discard_class<G>(
    state: &mut OutboundState<G>,
    class: TrafficClass,
) -> Result<(), OutboundQueueError> {
    loop {
        let item = match class {
            TrafficClass::Control => state.control.front(),
            TrafficClass::Replication => state.replication.front(),
            TrafficClass::Snapshot => state
                .snapshot_ready
                .front()
                .or_else(|| state.snapshot_pending.front()),
        };
        let Some(item) = item else {
            return Ok(());
        };
        let bytes = item.bytes();
        release_retained(state, class, bytes)?;
        let _ = match class {
            TrafficClass::Control => state.control.pop_front(),
            TrafficClass::Replication => state.replication.pop_front(),
            TrafficClass::Snapshot => state
                .snapshot_ready
                .pop_front()
                .or_else(|| state.snapshot_pending.pop_front()),
        };
    }
}

pub(super) fn release_retained<G>(
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

pub(super) fn queued_usage<G>(state: &OutboundState<G>) -> Result<QueueUsage, OutboundQueueError> {
    let mut usage = QueueUsage::default();
    for item in state
        .control
        .iter()
        .chain(state.replication.iter())
        .chain(state.snapshot_ready.iter())
        .chain(state.snapshot_pending.iter())
    {
        usage = usage
            .added(item.bytes())
            .ok_or(OutboundQueueError::Poisoned)?;
    }
    Ok(usage)
}

pub(super) fn select_next<G>(
    state: &mut OutboundState<G>,
    control_burst: usize,
) -> Option<OutboundItem<G>> {
    let bulk_waiting = !state.replication.is_empty() || !state.snapshot_ready.is_empty();
    if !state.control.is_empty() && (!bulk_waiting || state.control_streak < control_burst) {
        state.control_streak = state.control_streak.saturating_add(1);
        return state.control.pop_front();
    }

    state.control_streak = 0;
    let selected = if state.prefer_snapshot {
        state
            .snapshot_ready
            .pop_front()
            .or_else(|| state.replication.pop_front())
    } else {
        state
            .replication
            .pop_front()
            .or_else(|| state.snapshot_ready.pop_front())
    };
    if selected.is_some() {
        state.prefer_snapshot = !state.prefer_snapshot;
        return selected;
    }
    state.control.pop_front()
}
