//! Bounds enforced by handshake and peer-frame decoding.

use crate::wire::{PEER_FRAME_FIXED_BODY_BYTES, PEER_FRAME_LENGTH_PREFIX_BYTES};

use super::{require_at_most, require_nonzero, LimitError, LimitKind};

/// Append-entries budget used by [`WireLimits::default`].
pub const DEFAULT_MAX_APPEND_ENTRIES_BYTES: usize = 512 * 1024;
/// Default maximum canonical group identity length.
pub const DEFAULT_MAX_GROUP_ID_BYTES: usize = 128;
/// Default maximum peer-frame body length, excluding its four-byte prefix.
pub const DEFAULT_MAX_FRAME_BODY_BYTES: usize = PEER_FRAME_FIXED_BODY_BYTES
    + DEFAULT_MAX_GROUP_ID_BYTES
    + rafter_codec::max_receive_frame_bytes(DEFAULT_MAX_APPEND_ENTRIES_BYTES);
/// Default maximum complete peer-frame length.
pub const DEFAULT_MAX_FRAME_BYTES: usize =
    DEFAULT_MAX_FRAME_BODY_BYTES + PEER_FRAME_LENGTH_PREFIX_BYTES;

/// Bounds enforced by the transport handshake and peer-frame codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireLimits {
    max_frame_body_bytes: usize,
    max_group_id_bytes: usize,
}

impl WireLimits {
    /// Validates wire bounds.
    ///
    /// `max_frame_body_bytes` excludes the four-byte length prefix. The complete
    /// frame must fit the handshake's `u32` bound and local `usize`, and must
    /// leave at least one byte for an inner `rafter-codec` message after the
    /// fixed header and maximum group ID.
    ///
    /// # Errors
    ///
    /// Returns [`LimitError`] when a bound is zero, unrepresentable, or
    /// internally inconsistent.
    pub fn new(max_frame_body_bytes: usize, max_group_id_bytes: usize) -> Result<Self, LimitError> {
        require_nonzero(LimitKind::FrameBodyBytes, max_frame_body_bytes)?;
        require_nonzero(LimitKind::GroupIdBytes, max_group_id_bytes)?;

        let u32_max = match usize::try_from(u32::MAX) {
            Ok(value) => value,
            Err(_) => usize::MAX,
        };
        let max_wire_body = u32_max.saturating_sub(PEER_FRAME_LENGTH_PREFIX_BYTES);
        let max_local_body = usize::MAX - PEER_FRAME_LENGTH_PREFIX_BYTES;
        require_at_most(
            LimitKind::FrameBodyBytes,
            max_frame_body_bytes,
            max_wire_body.min(max_local_body),
        )?;
        require_at_most(
            LimitKind::GroupIdBytes,
            max_group_id_bytes,
            usize::from(u16::MAX),
        )?;

        let minimum = PEER_FRAME_FIXED_BODY_BYTES + max_group_id_bytes + 1;
        if max_frame_body_bytes < minimum {
            return Err(LimitError::FrameBodyTooSmall {
                frame_body_bytes: max_frame_body_bytes,
                minimum,
            });
        }
        Ok(Self {
            max_frame_body_bytes,
            max_group_id_bytes,
        })
    }

    /// Maximum frame body bytes, excluding the four-byte length prefix.
    #[must_use]
    pub const fn max_frame_body_bytes(self) -> usize {
        self.max_frame_body_bytes
    }

    /// Maximum complete frame bytes, including the length prefix.
    #[must_use]
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_body_bytes + PEER_FRAME_LENGTH_PREFIX_BYTES
    }

    /// Maximum canonical group ID bytes.
    #[must_use]
    pub const fn max_group_id_bytes(self) -> usize {
        self.max_group_id_bytes
    }
}

impl Default for WireLimits {
    fn default() -> Self {
        Self {
            max_frame_body_bytes: DEFAULT_MAX_FRAME_BODY_BYTES,
            max_group_id_bytes: DEFAULT_MAX_GROUP_ID_BYTES,
        }
    }
}
