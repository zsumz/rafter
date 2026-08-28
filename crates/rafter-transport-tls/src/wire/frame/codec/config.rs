//! Validated codec construction and its declared memory bounds.
//!
//! This module owns the only path that admits a caller-supplied group codec
//! into [`PeerFrameCodec`], rejecting a bound of zero or one wider than the
//! configured wire limits. It reports the bounds the runtime reserves against;
//! it does not read or write any frame byte.

use std::marker::PhantomData;

use crate::wire::frame::PeerFrameCodecConfigError;
use crate::{GroupIdCodec, WireLimits};

use super::PeerFrameCodec;

impl<G, C> PeerFrameCodec<G, C>
where
    C: GroupIdCodec<G>,
{
    /// Validates and creates a peer-frame codec.
    ///
    /// # Errors
    ///
    /// Returns [`PeerFrameCodecConfigError`] when the group codec's declared
    /// bound is zero or exceeds `limits`.
    pub fn new(group_codec: C, limits: WireLimits) -> Result<Self, PeerFrameCodecConfigError> {
        let group_id_bound = group_codec.max_encoded_len();
        if group_id_bound == 0 {
            return Err(PeerFrameCodecConfigError::ZeroGroupIdBound);
        }
        if group_id_bound > limits.max_group_id_bytes() {
            return Err(PeerFrameCodecConfigError::GroupIdBoundTooLarge {
                codec_maximum: group_id_bound,
                wire_maximum: limits.max_group_id_bytes(),
            });
        }
        Ok(Self {
            decoded_group_bound: std::mem::size_of::<G>()
                .saturating_add(group_codec.max_decoded_heap_bytes()),
            group_codec,
            limits,
            group_id_bound,
            marker: PhantomData,
        })
    }

    /// Returns the configured wire limits.
    #[must_use]
    pub const fn limits(&self) -> WireLimits {
        self.limits
    }

    /// Returns the caller-supplied group codec.
    #[must_use]
    pub const fn group_codec(&self) -> &C {
        &self.group_codec
    }

    /// Maximum in-place and codec-controlled heap memory for group decoding.
    ///
    /// This includes `size_of::<G>()` and the codec's declared peak across
    /// decoding, error construction, and canonical re-encoding.
    #[must_use]
    pub const fn max_decoded_group_bytes(&self) -> usize {
        self.decoded_group_bound
    }

    /// Maximum canonical group bytes emitted into reusable scratch storage.
    ///
    /// The blocking runtime reserves at least this many receive-memory bytes
    /// for the lifetime of every authenticated inbound receiver.
    #[must_use]
    pub const fn max_encoded_group_bytes(&self) -> usize {
        self.group_id_bound
    }
}
