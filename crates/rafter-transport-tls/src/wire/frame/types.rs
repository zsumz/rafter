//! Peer-frame value types and reusable storage.

use rafter::{Message, NodeId};
use rafter_service::transport::message_sender;

use super::PeerFrameError;
use crate::ConnectionSequence;

/// Bytes in the big-endian outer length prefix.
pub const PEER_FRAME_LENGTH_PREFIX_BYTES: usize = 4;
/// Version-1 frame kind carrying one Rafter peer message.
pub const PEER_FRAME_KIND_MESSAGE: u8 = 1;
/// Fixed version-1 body bytes outside the group ID and inner message.
///
/// The fields are: kind (1), sequence (8), group length (2), sender (8),
/// recipient (8), and message length (4).
pub const PEER_FRAME_FIXED_BODY_BYTES: usize = 31;

/// One validated group-aware peer message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerFrame<G> {
    sequence: ConnectionSequence,
    group_id: G,
    from: NodeId,
    to: NodeId,
    message: Message,
}

/// Cheap outer route decoded before constructing an inner Raft message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PeerFrameRoute<G> {
    pub(crate) sequence: ConnectionSequence,
    pub(crate) group_id: G,
    pub(crate) from: NodeId,
    pub(crate) to: NodeId,
}

impl<G> PeerFrame<G> {
    /// Creates a frame after checking the outer and embedded senders agree.
    ///
    /// # Errors
    ///
    /// Returns [`PeerFrameError::SenderMismatch`] when `message` names a
    /// sender other than `from`.
    pub fn new(
        sequence: ConnectionSequence,
        group_id: G,
        from: NodeId,
        to: NodeId,
        message: Message,
    ) -> Result<Self, PeerFrameError> {
        let embedded = message_sender(&message);
        if embedded != from {
            return Err(PeerFrameError::SenderMismatch {
                envelope_from: from,
                message_from: embedded,
            });
        }
        Ok(Self {
            sequence,
            group_id,
            from,
            to,
            message,
        })
    }

    /// Exact in-connection sequence number.
    #[must_use]
    pub const fn sequence(&self) -> ConnectionSequence {
        self.sequence
    }

    /// Caller-owned group route.
    #[must_use]
    pub const fn group_id(&self) -> &G {
        &self.group_id
    }

    /// Raft sender identity.
    #[must_use]
    pub const fn from(&self) -> NodeId {
        self.from
    }

    /// Raft recipient identity.
    #[must_use]
    pub const fn to(&self) -> NodeId {
        self.to
    }

    /// Inner versioned Rafter peer message.
    #[must_use]
    pub const fn message(&self) -> &Message {
        &self.message
    }

    /// Consumes the frame into its caller-owned group and message.
    #[must_use]
    pub fn into_parts(self) -> (ConnectionSequence, G, NodeId, NodeId, Message) {
        (
            self.sequence,
            self.group_id,
            self.from,
            self.to,
            self.message,
        )
    }
}

/// Reusable caller-owned buffers for peer-frame encoding and canonical decoding.
#[derive(Debug, Default)]
pub struct PeerFrameScratch {
    pub(super) group_id: Vec<u8>,
    pub(super) message: Vec<u8>,
}

impl PeerFrameScratch {
    /// Creates empty reusable buffers.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            group_id: Vec::new(),
            message: Vec::new(),
        }
    }

    /// Retained capacity of the canonical group-ID buffer.
    #[must_use]
    pub const fn group_id_capacity(&self) -> usize {
        self.group_id.capacity()
    }

    /// Retained capacity of the inner-message buffer.
    #[must_use]
    pub const fn message_capacity(&self) -> usize {
        self.message.capacity()
    }
}
