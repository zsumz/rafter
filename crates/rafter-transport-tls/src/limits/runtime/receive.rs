//! Transport-runtime-wide receive and decode memory budget.

use super::{RuntimeLimitError, RuntimeLimitKind};

/// Conservative weighted budget for encoded and decoded inbound frames.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveMemoryLimits {
    bytes_global: usize,
    decode_amplification: usize,
}

impl ReceiveMemoryLimits {
    /// Default permits roughly three maximum-size v1 frames concurrently.
    ///
    /// The 32x charge clears the allocation-counted 24.88x hostile
    /// minimum-entry peak, including the temporary `Vec` to shared-slice
    /// conversion, while leaving headroom for outer routing and allocator
    /// rounding.
    pub const DEFAULT: Self = Self {
        bytes_global: 256 * 1024 * 1024,
        decode_amplification: 32,
    };

    /// Validates the runtime-wide budget and conservative decode weight.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeLimitError`] when either finite bound is zero.
    pub fn new(
        bytes_global: usize,
        decode_amplification: usize,
    ) -> Result<Self, RuntimeLimitError> {
        if bytes_global == 0 {
            return Err(RuntimeLimitError::Zero {
                kind: RuntimeLimitKind::ReceiveMemoryBytes,
            });
        }
        if decode_amplification == 0 {
            return Err(RuntimeLimitError::Zero {
                kind: RuntimeLimitKind::DecodeAmplification,
            });
        }
        Ok(Self {
            bytes_global,
            decode_amplification,
        })
    }

    /// Maximum weighted receive memory across readers, decoders, and the queue.
    #[must_use]
    pub const fn bytes_global(self) -> usize {
        self.bytes_global
    }

    /// Conservative memory charge per declared wire byte.
    #[must_use]
    pub const fn decode_amplification(self) -> usize {
        self.decode_amplification
    }

    pub(crate) const fn charge(self, frame_bytes: usize) -> usize {
        frame_bytes.saturating_mul(self.decode_amplification)
    }
}

impl Default for ReceiveMemoryLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
