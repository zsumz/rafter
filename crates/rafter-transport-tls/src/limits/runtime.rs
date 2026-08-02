//! Finite queue and connection limits for the blocking transport runtime.

mod inbound;
mod outbound;
mod receive;

use std::{error::Error, fmt};

pub use inbound::InboundQueueLimits;
pub use outbound::OutboundQueueLimits;
pub use receive::{ReceiveMemoryLimits, MIN_SAFE_DECODE_AMPLIFICATION};

/// Runtime resource whose zero value was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RuntimeLimitKind {
    /// Complete outbound frames retained for one peer.
    OutboundFrames,
    /// Complete outbound frame bytes retained for one peer.
    OutboundBytes,
    /// Outbound frame slots reserved for control traffic.
    ReservedControlFrames,
    /// Outbound bytes reserved for control traffic.
    ReservedControlBytes,
    /// Consecutive control selections before bulk traffic gets an opportunity.
    ControlBurst,
    /// Authenticated inbound frames retained for one peer.
    InboundPeerFrames,
    /// Authenticated inbound frame bytes retained for one peer.
    InboundPeerBytes,
    /// Authenticated inbound frames retained across all peers.
    InboundGlobalFrames,
    /// Authenticated inbound frame bytes retained across all peers.
    InboundGlobalBytes,
    /// Concurrent accepted or handshaking inbound connections.
    InboundConnections,
    /// Weighted memory retained by inbound reads, decoders, and queued envelopes.
    ReceiveMemoryBytes,
    /// Conservative decoded-memory charge per wire byte.
    DecodeAmplification,
}

impl fmt::Display for RuntimeLimitKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OutboundFrames => "outbound frames per peer",
            Self::OutboundBytes => "outbound bytes per peer",
            Self::ReservedControlFrames => "reserved control frames",
            Self::ReservedControlBytes => "reserved control bytes",
            Self::ControlBurst => "control burst",
            Self::InboundPeerFrames => "inbound frames per peer",
            Self::InboundPeerBytes => "inbound bytes per peer",
            Self::InboundGlobalFrames => "global inbound frames",
            Self::InboundGlobalBytes => "global inbound bytes",
            Self::InboundConnections => "inbound connections",
            Self::ReceiveMemoryBytes => "global receive memory bytes",
            Self::DecodeAmplification => "decode amplification",
        })
    }
}

/// Invalid blocking-runtime resource limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RuntimeLimitError {
    /// A required finite bound was zero.
    Zero {
        /// Invalid bound.
        kind: RuntimeLimitKind,
    },
    /// Reserved control capacity exceeded total outbound capacity.
    ControlReserveExceedsTotal {
        /// Reserved frame slots.
        reserved_frames: usize,
        /// Total frame slots.
        total_frames: usize,
        /// Reserved bytes.
        reserved_bytes: usize,
        /// Total bytes.
        total_bytes: usize,
    },
    /// Reserved control capacity left no slot or byte capacity for bulk work.
    ControlReserveConsumesTotal {
        /// Reserved frame slots.
        reserved_frames: usize,
        /// Total frame slots.
        total_frames: usize,
        /// Reserved bytes.
        reserved_bytes: usize,
        /// Total bytes.
        total_bytes: usize,
    },
    /// One peer could consume more than the complete inbound queue.
    PeerInboundExceedsGlobal {
        /// Per-peer frame slots.
        peer_frames: usize,
        /// Global frame slots.
        global_frames: usize,
        /// Per-peer bytes.
        peer_bytes: usize,
        /// Global bytes.
        global_bytes: usize,
    },
    /// The receive-memory charge is below the allocation-counted safe floor.
    DecodeAmplificationTooSmall {
        /// Requested charge per declared wire byte.
        actual: usize,
        /// Smallest evidence-backed charge.
        minimum: usize,
    },
}

impl fmt::Display for RuntimeLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero { kind } => write!(formatter, "runtime limit {kind} must be nonzero"),
            Self::ControlReserveExceedsTotal {
                reserved_frames,
                total_frames,
                reserved_bytes,
                total_bytes,
            } => write!(
                formatter,
                "control reserve {reserved_frames} frames/{reserved_bytes} bytes exceeds total \
                 outbound capacity {total_frames} frames/{total_bytes} bytes"
            ),
            Self::ControlReserveConsumesTotal {
                reserved_frames,
                total_frames,
                reserved_bytes,
                total_bytes,
            } => write!(
                formatter,
                "control reserve {reserved_frames} frames/{reserved_bytes} bytes leaves no bulk \
                 capacity within {total_frames} frames/{total_bytes} bytes"
            ),
            Self::PeerInboundExceedsGlobal {
                peer_frames,
                global_frames,
                peer_bytes,
                global_bytes,
            } => write!(
                formatter,
                "per-peer inbound capacity {peer_frames} frames/{peer_bytes} bytes exceeds global \
                 capacity {global_frames} frames/{global_bytes} bytes"
            ),
            Self::DecodeAmplificationTooSmall { actual, minimum } => write!(
                formatter,
                "decode amplification {actual} is below evidence-backed minimum {minimum}"
            ),
        }
    }
}

impl Error for RuntimeLimitError {}

/// Finite resource limits owned by one blocking transport runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimits {
    outbound: OutboundQueueLimits,
    inbound: InboundQueueLimits,
    max_inbound_connections: usize,
    receive_memory: ReceiveMemoryLimits,
}

impl RuntimeLimits {
    /// Default finite runtime bounds.
    pub const DEFAULT: Self = Self {
        outbound: OutboundQueueLimits::DEFAULT,
        inbound: InboundQueueLimits::DEFAULT,
        max_inbound_connections: 128,
        receive_memory: ReceiveMemoryLimits::DEFAULT,
    };

    /// Creates complete runtime bounds from independently validated queues.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeLimitError`] when `max_inbound_connections` is zero.
    pub fn new(
        outbound: OutboundQueueLimits,
        inbound: InboundQueueLimits,
        max_inbound_connections: usize,
    ) -> Result<Self, RuntimeLimitError> {
        if max_inbound_connections == 0 {
            return Err(RuntimeLimitError::Zero {
                kind: RuntimeLimitKind::InboundConnections,
            });
        }
        Ok(Self {
            outbound,
            inbound,
            max_inbound_connections,
            receive_memory: ReceiveMemoryLimits::DEFAULT,
        })
    }

    /// Complete per-peer outbound queue limits.
    #[must_use]
    pub const fn outbound(self) -> OutboundQueueLimits {
        self.outbound
    }

    /// Complete authenticated inbound queue limits.
    #[must_use]
    pub const fn inbound(self) -> InboundQueueLimits {
        self.inbound
    }

    /// Replaces this transport runtime's shared receive/decode memory budget.
    #[must_use]
    pub const fn with_receive_memory(mut self, receive_memory: ReceiveMemoryLimits) -> Self {
        self.receive_memory = receive_memory;
        self
    }

    /// Process-wide receive/decode memory budget.
    #[must_use]
    pub const fn receive_memory(self) -> ReceiveMemoryLimits {
        self.receive_memory
    }

    /// Maximum outbound frames retained for one physical peer.
    #[must_use]
    pub const fn outbound_frames_per_peer(self) -> usize {
        self.outbound.frames_per_peer()
    }

    /// Maximum outbound complete-frame bytes retained for one peer.
    #[must_use]
    pub const fn outbound_bytes_per_peer(self) -> usize {
        self.outbound.bytes_per_peer()
    }

    /// Frame slots that non-control traffic may not consume.
    #[must_use]
    pub const fn reserved_control_frames(self) -> usize {
        self.outbound.reserved_control_frames()
    }

    /// Bytes that non-control traffic may not consume.
    #[must_use]
    pub const fn reserved_control_bytes(self) -> usize {
        self.outbound.reserved_control_bytes()
    }

    /// Maximum consecutive control selections while bulk work is queued.
    #[must_use]
    pub const fn control_burst(self) -> usize {
        self.outbound.control_burst()
    }

    /// Maximum inbound frames retained for one authenticated peer.
    #[must_use]
    pub const fn inbound_frames_per_peer(self) -> usize {
        self.inbound.frames_per_peer()
    }

    /// Maximum inbound bytes retained for one authenticated peer.
    #[must_use]
    pub const fn inbound_bytes_per_peer(self) -> usize {
        self.inbound.bytes_per_peer()
    }

    /// Maximum inbound frames retained across all peers.
    #[must_use]
    pub const fn inbound_frames_global(self) -> usize {
        self.inbound.frames_global()
    }

    /// Maximum inbound bytes retained across all peers.
    #[must_use]
    pub const fn inbound_bytes_global(self) -> usize {
        self.inbound.bytes_global()
    }

    /// Maximum concurrently accepted TLS connections.
    #[must_use]
    pub const fn max_inbound_connections(self) -> usize {
        self.max_inbound_connections
    }
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
