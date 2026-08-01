//! Per-peer and global authenticated inbound queue bounds.

use super::{RuntimeLimitError, RuntimeLimitKind};

/// Count-and-byte bounds for authenticated inbound envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InboundQueueLimits {
    frames_per_peer: usize,
    bytes_per_peer: usize,
    frames_global: usize,
    bytes_global: usize,
}

impl InboundQueueLimits {
    /// Default finite inbound queue bounds.
    pub const DEFAULT: Self = Self {
        frames_per_peer: 128,
        bytes_per_peer: 8 * 1024 * 1024,
        frames_global: 512,
        bytes_global: 32 * 1024 * 1024,
    };

    /// Validates per-peer and global inbound memory bounds.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeLimitError`] for zero values or a per-peer bound larger
    /// than the complete queue.
    pub fn new(
        frames_per_peer: usize,
        bytes_per_peer: usize,
        frames_global: usize,
        bytes_global: usize,
    ) -> Result<Self, RuntimeLimitError> {
        for (kind, value) in [
            (RuntimeLimitKind::InboundPeerFrames, frames_per_peer),
            (RuntimeLimitKind::InboundPeerBytes, bytes_per_peer),
            (RuntimeLimitKind::InboundGlobalFrames, frames_global),
            (RuntimeLimitKind::InboundGlobalBytes, bytes_global),
        ] {
            if value == 0 {
                return Err(RuntimeLimitError::Zero { kind });
            }
        }
        if frames_per_peer > frames_global || bytes_per_peer > bytes_global {
            return Err(RuntimeLimitError::PeerInboundExceedsGlobal {
                peer_frames: frames_per_peer,
                global_frames: frames_global,
                peer_bytes: bytes_per_peer,
                global_bytes: bytes_global,
            });
        }
        Ok(Self {
            frames_per_peer,
            bytes_per_peer,
            frames_global,
            bytes_global,
        })
    }

    /// Maximum frames retained for one authenticated physical peer.
    #[must_use]
    pub const fn frames_per_peer(self) -> usize {
        self.frames_per_peer
    }

    /// Maximum complete-frame bytes retained for one physical peer.
    #[must_use]
    pub const fn bytes_per_peer(self) -> usize {
        self.bytes_per_peer
    }

    /// Maximum frames retained across all authenticated peers.
    #[must_use]
    pub const fn frames_global(self) -> usize {
        self.frames_global
    }

    /// Maximum complete-frame bytes retained across all peers.
    #[must_use]
    pub const fn bytes_global(self) -> usize {
        self.bytes_global
    }
}

impl Default for InboundQueueLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
