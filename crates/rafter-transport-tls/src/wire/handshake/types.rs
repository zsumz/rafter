//! Handshake value types.

use std::num::{NonZeroU16, NonZeroU32};

use crate::{ClusterId, ConnectionSession, PeerId, MAX_ID_BYTES};

use super::VersionRangeError;

/// Rafter TLS handshake magic.
pub const HANDSHAKE_MAGIC: [u8; 10] = *b"RAFTER-TLS";
/// Current outer transport-envelope version.
pub const CURRENT_TRANSPORT_VERSION: u16 = 1;
/// Maximum encoded client hello bytes.
pub const MAX_CLIENT_HELLO_BYTES: usize =
    HANDSHAKE_MAGIC.len() + 8 + 1 + MAX_ID_BYTES + 1 + MAX_ID_BYTES + 8 + 4;
/// Maximum encoded server hello bytes.
pub const MAX_SERVER_HELLO_BYTES: usize =
    HANDSHAKE_MAGIC.len() + 2 + 2 + 1 + MAX_ID_BYTES + 1 + MAX_ID_BYTES + 4 + 1;

/// Inclusive nonzero wire-version range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionRange {
    minimum: NonZeroU16,
    maximum: NonZeroU16,
}

impl VersionRange {
    /// Validates an inclusive version range.
    ///
    /// # Errors
    ///
    /// Returns [`VersionRangeError`] when either endpoint is zero or the
    /// minimum exceeds the maximum.
    pub fn new(minimum: u16, maximum: u16) -> Result<Self, VersionRangeError> {
        let minimum = NonZeroU16::new(minimum).ok_or(VersionRangeError::ZeroMinimum)?;
        let maximum = NonZeroU16::new(maximum).ok_or(VersionRangeError::ZeroMaximum)?;
        if minimum > maximum {
            return Err(VersionRangeError::Reversed {
                minimum: minimum.get(),
                maximum: maximum.get(),
            });
        }
        Ok(Self { minimum, maximum })
    }

    /// A range containing only [`CURRENT_TRANSPORT_VERSION`].
    #[must_use]
    pub const fn current_transport() -> Self {
        Self {
            minimum: NonZeroU16::MIN,
            maximum: NonZeroU16::MIN,
        }
    }

    /// Lowest supported version.
    #[must_use]
    pub const fn minimum(self) -> u16 {
        self.minimum.get()
    }

    /// Highest supported version.
    #[must_use]
    pub const fn maximum(self) -> u16 {
        self.maximum.get()
    }

    /// Returns whether `version` falls within this inclusive range.
    #[must_use]
    pub const fn contains(self, version: u16) -> bool {
        version >= self.minimum.get() && version <= self.maximum.get()
    }
}

/// Returns the highest version supported by both inclusive ranges.
#[must_use]
pub fn highest_common_version(left: VersionRange, right: VersionRange) -> Option<u16> {
    let minimum = left.minimum().max(right.minimum());
    let maximum = left.maximum().min(right.maximum());
    (minimum <= maximum).then_some(maximum)
}

/// Client's authenticated Rafter handshake proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientHello {
    transport_versions: VersionRange,
    peer_codec_versions: VersionRange,
    cluster_id: ClusterId,
    claimed_peer_id: PeerId,
    connection_session: ConnectionSession,
    max_send_frame_bytes: NonZeroU32,
}

impl ClientHello {
    /// Creates one structurally valid client hello.
    #[must_use]
    pub fn new(
        transport_versions: VersionRange,
        peer_codec_versions: VersionRange,
        cluster_id: ClusterId,
        claimed_peer_id: PeerId,
        connection_session: ConnectionSession,
        max_send_frame_bytes: NonZeroU32,
    ) -> Self {
        Self {
            transport_versions,
            peer_codec_versions,
            cluster_id,
            claimed_peer_id,
            connection_session,
            max_send_frame_bytes,
        }
    }

    /// Supported outer transport versions.
    #[must_use]
    pub const fn transport_versions(&self) -> VersionRange {
        self.transport_versions
    }

    /// Supported `rafter-codec` peer versions.
    #[must_use]
    pub const fn peer_codec_versions(&self) -> VersionRange {
        self.peer_codec_versions
    }

    /// Exact deployment boundary claimed by the client.
    #[must_use]
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Principal claimed after TLS proves the client certificate.
    #[must_use]
    pub const fn claimed_peer_id(&self) -> &PeerId {
        &self.claimed_peer_id
    }

    /// Durable outbound connection epoch.
    #[must_use]
    pub const fn connection_session(&self) -> ConnectionSession {
        self.connection_session
    }

    /// Complete peer-frame bound the client requires for this send direction.
    ///
    /// A server must accept this exact bound or refuse the connection. A lower
    /// negotiated value would let locally valid queued work be discarded after
    /// admission, violating the transport's accepted-work contract.
    #[must_use]
    pub const fn max_send_frame_bytes(&self) -> NonZeroU32 {
        self.max_send_frame_bytes
    }
}

/// Typed server refusal written after TLS succeeds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerRefusal {
    /// The presented leaf certificate is not in the explicit directory.
    UnknownCertificate,
    /// The claimed peer does not match the authenticated certificate mapping.
    IdentityMismatch,
    /// Client and server cluster identities differ.
    ClusterMismatch,
    /// No outer transport-envelope version overlaps.
    TransportVersionMismatch,
    /// No `rafter-codec` peer version overlaps.
    PeerCodecVersionMismatch,
    /// The proposed frame limit cannot be accepted.
    FrameLimitRejected,
    /// The durable session is not newer than the accepted high water.
    StaleSession,
    /// The server has reached a configured connection or admission bound.
    ServerBusy,
}

impl ServerRefusal {
    pub(super) const fn wire_tag(self) -> u8 {
        match self {
            Self::UnknownCertificate => 1,
            Self::IdentityMismatch => 2,
            Self::ClusterMismatch => 3,
            Self::TransportVersionMismatch => 4,
            Self::PeerCodecVersionMismatch => 5,
            Self::FrameLimitRejected => 6,
            Self::StaleSession => 7,
            Self::ServerBusy => 8,
        }
    }

    pub(super) const fn from_wire_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::UnknownCertificate),
            2 => Some(Self::IdentityMismatch),
            3 => Some(Self::ClusterMismatch),
            4 => Some(Self::TransportVersionMismatch),
            5 => Some(Self::PeerCodecVersionMismatch),
            6 => Some(Self::FrameLimitRejected),
            7 => Some(Self::StaleSession),
            8 => Some(Self::ServerBusy),
            _ => None,
        }
    }
}

/// Accepted or typed-refused server handshake status.
///
/// This enum is exhaustive for transport handshake version 1: a server either
/// accepts the negotiated channel or sends one typed refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerHelloStatus {
    /// Negotiation succeeded and peer frames may follow.
    Accepted,
    /// Negotiation failed before any peer frame was admitted.
    Refused(ServerRefusal),
}

/// Server's Rafter handshake result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerHello {
    pub(super) selected_transport_version: Option<NonZeroU16>,
    pub(super) selected_peer_codec_version: Option<NonZeroU16>,
    pub(super) cluster_id: ClusterId,
    pub(super) server_peer_id: PeerId,
    pub(super) accepted_frame_bytes: Option<NonZeroU32>,
    pub(super) status: ServerHelloStatus,
}

impl ServerHello {
    /// Creates an accepted server hello.
    #[must_use]
    pub fn accepted(
        selected_transport_version: NonZeroU16,
        selected_peer_codec_version: NonZeroU16,
        cluster_id: ClusterId,
        server_peer_id: PeerId,
        accepted_frame_bytes: NonZeroU32,
    ) -> Self {
        Self {
            selected_transport_version: Some(selected_transport_version),
            selected_peer_codec_version: Some(selected_peer_codec_version),
            cluster_id,
            server_peer_id,
            accepted_frame_bytes: Some(accepted_frame_bytes),
            status: ServerHelloStatus::Accepted,
        }
    }

    /// Creates a canonical refused server hello.
    ///
    /// Refused hellos encode zero for both selected versions and the accepted
    /// frame bound. The typed status is the only negotiated result.
    #[must_use]
    pub fn refused(cluster_id: ClusterId, server_peer_id: PeerId, refusal: ServerRefusal) -> Self {
        Self {
            selected_transport_version: None,
            selected_peer_codec_version: None,
            cluster_id,
            server_peer_id,
            accepted_frame_bytes: None,
            status: ServerHelloStatus::Refused(refusal),
        }
    }

    /// Selected outer transport version, present only when accepted.
    #[must_use]
    pub const fn selected_transport_version(&self) -> Option<NonZeroU16> {
        self.selected_transport_version
    }

    /// Selected `rafter-codec` version, present only when accepted.
    #[must_use]
    pub const fn selected_peer_codec_version(&self) -> Option<NonZeroU16> {
        self.selected_peer_codec_version
    }

    /// Exact server deployment boundary.
    #[must_use]
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Authenticated server transport principal.
    #[must_use]
    pub const fn server_peer_id(&self) -> &PeerId {
        &self.server_peer_id
    }

    /// Complete peer-frame bound accepted by the server.
    #[must_use]
    pub const fn accepted_frame_bytes(&self) -> Option<NonZeroU32> {
        self.accepted_frame_bytes
    }

    /// Accepted or typed-refused status.
    #[must_use]
    pub const fn status(&self) -> ServerHelloStatus {
        self.status
    }
}
