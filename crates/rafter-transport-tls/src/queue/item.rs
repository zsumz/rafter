//! Connection-independent work retained by one physical peer queue.

use rafter::{NodeId, SnapshotChunkSend};

use crate::directory::RouteAuthorization;
use crate::wire::PreparedPeerFrame;
use crate::TrafficClass;

/// Maximum retries retained after ambiguous bulk writes.
const MAX_BULK_WRITE_RETRIES: u8 = 8;

#[derive(Debug)]
pub(crate) struct OutboundItem<G> {
    from: NodeId,
    to: NodeId,
    class: TrafficClass,
    reserved_bytes: usize,
    authorization: RouteAuthorization,
    failed_attempts: u8,
    payload: OutboundPayload<G>,
}

#[derive(Debug)]
enum OutboundPayload<G> {
    Prepared(PreparedPeerFrame),
    Snapshot {
        group_id: G,
        chunk: SnapshotChunkSend,
    },
}

impl<G> OutboundItem<G> {
    pub(crate) fn message(
        from: NodeId,
        to: NodeId,
        class: TrafficClass,
        frame: PreparedPeerFrame,
        authorization: RouteAuthorization,
    ) -> Self {
        let reserved_bytes = frame.wire_len();
        Self {
            from,
            to,
            class,
            reserved_bytes,
            authorization,
            failed_attempts: 0,
            payload: OutboundPayload::Prepared(frame),
        }
    }

    pub(crate) fn snapshot(
        group_id: G,
        from: NodeId,
        to: NodeId,
        reserved_bytes: usize,
        chunk: SnapshotChunkSend,
        authorization: RouteAuthorization,
    ) -> Self {
        Self {
            from,
            to,
            class: TrafficClass::Snapshot,
            reserved_bytes,
            authorization,
            failed_attempts: 0,
            payload: OutboundPayload::Snapshot { group_id, chunk },
        }
    }

    pub(crate) fn from(&self) -> NodeId {
        self.from
    }

    pub(crate) fn to(&self) -> NodeId {
        self.to
    }

    pub(crate) fn class(&self) -> TrafficClass {
        self.class
    }

    /// Queue-accounted complete-frame bytes.
    ///
    /// For a snapshot directive this is the exact eventual wire length computed
    /// from the bounded directive before admission, not the small in-memory
    /// directive representation. Queue limits therefore describe network work
    /// rather than spare `Vec` capacity or Rust object size.
    pub(crate) fn bytes(&self) -> usize {
        self.reserved_bytes
    }

    pub(crate) fn is_authorized(&self) -> bool {
        self.authorization.is_valid()
    }

    /// Records one ambiguous bulk write and returns whether another attempt is allowed.
    pub(crate) fn retry_bulk(&mut self) -> bool {
        debug_assert_ne!(self.class, TrafficClass::Control);
        self.failed_attempts = self.failed_attempts.saturating_add(1);
        self.failed_attempts <= MAX_BULK_WRITE_RETRIES
    }

    pub(crate) fn prepared(&self) -> Option<&PreparedPeerFrame> {
        match &self.payload {
            OutboundPayload::Prepared(frame) => Some(frame),
            OutboundPayload::Snapshot { .. } => None,
        }
    }

    pub(crate) fn snapshot_parts(&self) -> Option<(&G, &SnapshotChunkSend)> {
        match &self.payload {
            OutboundPayload::Prepared(_) => None,
            OutboundPayload::Snapshot { group_id, chunk } => Some((group_id, chunk)),
        }
    }

    /// Replaces a directive with the frame it resolved into.
    ///
    /// The wire length must equal the bytes reserved at admission. A mismatch
    /// means either queue accounting or snapshot framing drifted, so the worker
    /// fails closed instead of retaining an under-accounted frame.
    pub(crate) fn install_prepared(&mut self, frame: PreparedPeerFrame) -> bool {
        if frame.wire_len() != self.reserved_bytes {
            return false;
        }
        self.payload = OutboundPayload::Prepared(frame);
        true
    }
}
