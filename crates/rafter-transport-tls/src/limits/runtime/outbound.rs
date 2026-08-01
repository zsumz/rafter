//! Per-peer outbound queue bounds and reserved control capacity.

use super::{RuntimeLimitError, RuntimeLimitKind};

/// Count-and-byte bounds for one physical peer's outbound queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboundQueueLimits {
    frames_per_peer: usize,
    bytes_per_peer: usize,
    reserved_control_frames: usize,
    reserved_control_bytes: usize,
    control_burst: usize,
}

impl OutboundQueueLimits {
    /// Default finite outbound queue bounds.
    pub const DEFAULT: Self = Self {
        frames_per_peer: 256,
        bytes_per_peer: 16 * 1024 * 1024,
        reserved_control_frames: 32,
        reserved_control_bytes: 1024 * 1024,
        control_burst: 16,
    };

    /// Validates one peer's outbound memory and scheduling bounds.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeLimitError`] for zero values or a control reservation
    /// that leaves no finite capacity for replication and snapshot work.
    pub fn new(
        frames_per_peer: usize,
        bytes_per_peer: usize,
        reserved_control_frames: usize,
        reserved_control_bytes: usize,
        control_burst: usize,
    ) -> Result<Self, RuntimeLimitError> {
        for (kind, value) in [
            (RuntimeLimitKind::OutboundFrames, frames_per_peer),
            (RuntimeLimitKind::OutboundBytes, bytes_per_peer),
            (
                RuntimeLimitKind::ReservedControlFrames,
                reserved_control_frames,
            ),
            (
                RuntimeLimitKind::ReservedControlBytes,
                reserved_control_bytes,
            ),
            (RuntimeLimitKind::ControlBurst, control_burst),
        ] {
            if value == 0 {
                return Err(RuntimeLimitError::Zero { kind });
            }
        }
        if reserved_control_frames > frames_per_peer || reserved_control_bytes > bytes_per_peer {
            return Err(RuntimeLimitError::ControlReserveExceedsTotal {
                reserved_frames: reserved_control_frames,
                total_frames: frames_per_peer,
                reserved_bytes: reserved_control_bytes,
                total_bytes: bytes_per_peer,
            });
        }
        if reserved_control_frames == frames_per_peer || reserved_control_bytes == bytes_per_peer {
            return Err(RuntimeLimitError::ControlReserveConsumesTotal {
                reserved_frames: reserved_control_frames,
                total_frames: frames_per_peer,
                reserved_bytes: reserved_control_bytes,
                total_bytes: bytes_per_peer,
            });
        }
        Ok(Self {
            frames_per_peer,
            bytes_per_peer,
            reserved_control_frames,
            reserved_control_bytes,
            control_burst,
        })
    }

    /// Maximum complete frames retained for one physical peer.
    #[must_use]
    pub const fn frames_per_peer(self) -> usize {
        self.frames_per_peer
    }

    /// Maximum complete-frame bytes retained for one physical peer.
    #[must_use]
    pub const fn bytes_per_peer(self) -> usize {
        self.bytes_per_peer
    }

    /// Frame slots non-control traffic may not consume.
    #[must_use]
    pub const fn reserved_control_frames(self) -> usize {
        self.reserved_control_frames
    }

    /// Bytes non-control traffic may not consume.
    #[must_use]
    pub const fn reserved_control_bytes(self) -> usize {
        self.reserved_control_bytes
    }

    /// Maximum consecutive control selections while bulk work waits.
    #[must_use]
    pub const fn control_burst(self) -> usize {
        self.control_burst
    }
}

impl Default for OutboundQueueLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
