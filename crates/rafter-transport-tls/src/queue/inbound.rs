//! Global and per-peer count-and-byte-bounded inbound queue.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

use rafter_service::AuthenticatedPeerEnvelope;

use crate::{PeerId, RuntimeLimits};

use super::QueueUsage;
use super::ReceiveMemoryPermit;

#[derive(Debug)]
struct InboundItem<G> {
    peer: PeerId,
    bytes: usize,
    envelope: AuthenticatedPeerEnvelope<G, PeerId>,
    _memory: ReceiveMemoryPermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundQueueFull {
    Peer,
    Global,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundQueueError {
    Full(InboundQueueFull),
    Closed,
    Poisoned,
}

#[derive(Debug)]
pub(crate) struct InboundQueue<G> {
    limits: RuntimeLimits,
    state: Mutex<InboundState<G>>,
}

#[derive(Debug)]
struct InboundState<G> {
    items: VecDeque<InboundItem<G>>,
    total: QueueUsage,
    by_peer: BTreeMap<PeerId, QueueUsage>,
    closed: bool,
}

impl<G> InboundQueue<G> {
    pub(crate) fn new(limits: RuntimeLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(InboundState {
                items: VecDeque::new(),
                total: QueueUsage::default(),
                by_peer: BTreeMap::new(),
                closed: false,
            }),
        }
    }

    pub(crate) fn try_push(
        &self,
        peer: PeerId,
        bytes: usize,
        envelope: AuthenticatedPeerEnvelope<G, PeerId>,
        memory: ReceiveMemoryPermit,
    ) -> Result<(), InboundQueueError> {
        let mut state = self.state.lock().map_err(|_| InboundQueueError::Poisoned)?;
        if state.closed {
            return Err(InboundQueueError::Closed);
        }
        let peer_usage = state.by_peer.get(&peer).copied().unwrap_or_default();
        if !peer_usage.can_add(
            bytes,
            self.limits.inbound_frames_per_peer(),
            self.limits.inbound_bytes_per_peer(),
        ) {
            return Err(InboundQueueError::Full(InboundQueueFull::Peer));
        }
        if !state.total.can_add(
            bytes,
            self.limits.inbound_frames_global(),
            self.limits.inbound_bytes_global(),
        ) {
            return Err(InboundQueueError::Full(InboundQueueFull::Global));
        }
        let total = state
            .total
            .added(bytes)
            .ok_or(InboundQueueError::Poisoned)?;
        let peer_total = peer_usage.added(bytes).ok_or(InboundQueueError::Poisoned)?;
        state.total = total;
        state.by_peer.insert(peer.clone(), peer_total);
        state.items.push_back(InboundItem {
            peer,
            bytes,
            envelope,
            _memory: memory,
        });
        Ok(())
    }

    pub(crate) fn drain(
        &self,
        maximum: usize,
    ) -> Result<Vec<AuthenticatedPeerEnvelope<G, PeerId>>, InboundQueueError> {
        let mut state = self.state.lock().map_err(|_| InboundQueueError::Poisoned)?;
        let count = maximum.min(state.items.len());
        let mut envelopes = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(item) = state.items.front() else {
                break;
            };
            let total = state
                .total
                .removed(item.bytes)
                .ok_or(InboundQueueError::Poisoned)?;
            let peer_total = state
                .by_peer
                .get(&item.peer)
                .copied()
                .ok_or(InboundQueueError::Poisoned)?
                .removed(item.bytes)
                .ok_or(InboundQueueError::Poisoned)?;
            let item = state.items.pop_front().ok_or(InboundQueueError::Poisoned)?;
            state.total = total;
            if peer_total.frames == 0 {
                state.by_peer.remove(&item.peer);
            } else {
                state.by_peer.insert(item.peer.clone(), peer_total);
            }
            envelopes.push(item.envelope);
        }
        Ok(envelopes)
    }

    pub(crate) fn depth(&self) -> Result<QueueUsage, InboundQueueError> {
        self.state
            .lock()
            .map(|state| state.total)
            .map_err(|_| InboundQueueError::Poisoned)
    }

    pub(crate) fn peer_depth(&self, peer: &PeerId) -> Result<QueueUsage, InboundQueueError> {
        self.state
            .lock()
            .map(|state| state.by_peer.get(peer).copied().unwrap_or_default())
            .map_err(|_| InboundQueueError::Poisoned)
    }

    pub(crate) fn close(&self) -> Result<(), InboundQueueError> {
        let mut state = self.state.lock().map_err(|_| InboundQueueError::Poisoned)?;
        state.closed = true;
        Ok(())
    }
}

#[cfg(test)]
#[path = "inbound_test.rs"]
mod tests;
