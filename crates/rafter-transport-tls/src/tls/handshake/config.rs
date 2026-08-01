//! Immutable local Rafter-over-TLS handshake policy.

use std::num::NonZeroU32;

use crate::{ClusterId, PeerId, ServerHello, ServerRefusal, VersionRange, WireLimits};

use super::TlsHandshakeConfigError;

/// Smallest complete version-1 peer frame: fixed fields plus one route byte and
/// one inner-message byte.
pub const MIN_PEER_FRAME_BYTES: u32 = 4 + 31 + 2;

/// Immutable deployment, version, identity, and frame policy for one endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlsHandshakeConfig {
    cluster_id: ClusterId,
    local_peer_id: PeerId,
    transport_versions: VersionRange,
    peer_codec_versions: VersionRange,
    max_frame_bytes: NonZeroU32,
}

impl TlsHandshakeConfig {
    /// Creates a validated local handshake policy.
    ///
    /// # Errors
    ///
    /// Returns [`TlsHandshakeConfigError`] when `max_frame_bytes` is below the
    /// smallest structurally valid peer frame.
    pub fn new(
        cluster_id: ClusterId,
        local_peer_id: PeerId,
        transport_versions: VersionRange,
        peer_codec_versions: VersionRange,
        max_frame_bytes: NonZeroU32,
    ) -> Result<Self, TlsHandshakeConfigError> {
        if max_frame_bytes.get() < MIN_PEER_FRAME_BYTES {
            return Err(TlsHandshakeConfigError::FrameBoundTooSmall {
                actual: max_frame_bytes.get(),
                minimum: MIN_PEER_FRAME_BYTES,
            });
        }
        Ok(Self {
            cluster_id,
            local_peer_id,
            transport_versions,
            peer_codec_versions,
            max_frame_bytes,
        })
    }

    /// Creates the current version-1 policy from exact peer-frame limits.
    ///
    /// # Errors
    ///
    /// Returns [`TlsHandshakeConfigError`] if the complete frame bound cannot
    /// fit the handshake's `u32` field or the current peer-codec version is
    /// invalid.
    pub fn current(
        cluster_id: ClusterId,
        local_peer_id: PeerId,
        wire_limits: WireLimits,
    ) -> Result<Self, TlsHandshakeConfigError> {
        let actual = wire_limits.max_frame_bytes();
        let encoded =
            u32::try_from(actual).map_err(|_| TlsHandshakeConfigError::FrameBoundTooLarge {
                actual,
                maximum: u32::MAX,
            })?;
        let max_frame_bytes =
            NonZeroU32::new(encoded).ok_or(TlsHandshakeConfigError::FrameBoundTooSmall {
                actual: encoded,
                minimum: MIN_PEER_FRAME_BYTES,
            })?;
        let codec = u16::from(rafter_codec::VERSION);
        let peer_codec_versions =
            VersionRange::new(codec, codec).map_err(TlsHandshakeConfigError::PeerCodecVersion)?;
        Self::new(
            cluster_id,
            local_peer_id,
            VersionRange::current_transport(),
            peer_codec_versions,
            max_frame_bytes,
        )
    }

    /// Exact deployment boundary.
    #[must_use]
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Stable local transport principal claimed in hellos.
    #[must_use]
    pub const fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Supported outer transport-envelope versions.
    #[must_use]
    pub const fn transport_versions(&self) -> VersionRange {
        self.transport_versions
    }

    /// Supported `rafter-codec` peer-wire versions.
    #[must_use]
    pub const fn peer_codec_versions(&self) -> VersionRange {
        self.peer_codec_versions
    }

    /// Maximum complete peer-frame bytes this endpoint offers.
    #[must_use]
    pub const fn max_frame_bytes(&self) -> NonZeroU32 {
        self.max_frame_bytes
    }

    /// Constructs the canonical typed refusal for this endpoint.
    #[must_use]
    pub fn refusal(&self, reason: ServerRefusal) -> ServerHello {
        ServerHello::refused(self.cluster_id.clone(), self.local_peer_id.clone(), reason)
    }
}
