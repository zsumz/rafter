//! Successful negotiated Rafter-over-TLS connection parameters.

use std::num::{NonZeroU16, NonZeroU32};

use crate::PeerId;

/// Authenticated remote principal and selected post-TLS wire parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegotiatedTlsHandshake {
    remote_peer_id: PeerId,
    transport_version: NonZeroU16,
    peer_codec_version: NonZeroU16,
    frame_bytes: NonZeroU32,
}

impl NegotiatedTlsHandshake {
    pub(super) fn new(
        remote_peer_id: PeerId,
        transport_version: NonZeroU16,
        peer_codec_version: NonZeroU16,
        frame_bytes: NonZeroU32,
    ) -> Self {
        Self {
            remote_peer_id,
            transport_version,
            peer_codec_version,
            frame_bytes,
        }
    }

    /// Stable authenticated physical peer.
    #[must_use]
    pub const fn remote_peer_id(&self) -> &PeerId {
        &self.remote_peer_id
    }

    /// Selected outer transport-envelope version.
    #[must_use]
    pub const fn transport_version(&self) -> u16 {
        self.transport_version.get()
    }

    /// Selected `rafter-codec` peer-wire version.
    #[must_use]
    pub const fn peer_codec_version(&self) -> u16 {
        self.peer_codec_version.get()
    }

    /// Maximum complete peer-frame bytes accepted on the connection.
    #[must_use]
    pub const fn frame_bytes(&self) -> u32 {
        self.frame_bytes.get()
    }
}
