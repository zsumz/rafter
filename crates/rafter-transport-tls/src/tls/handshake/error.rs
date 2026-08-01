//! Handshake configuration, durable-store, and client validation failures.

use std::{error::Error, fmt};

use crate::{ClusterId, PeerId, ServerRefusal, VersionRangeError};

/// Invalid local handshake policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsHandshakeConfigError {
    /// Local complete-frame bound cannot be represented in the handshake.
    FrameBoundTooLarge {
        /// Actual local complete-frame bound.
        actual: usize,
        /// Largest representable handshake value.
        maximum: u32,
    },
    /// Local complete-frame bound cannot hold the smallest outer frame.
    FrameBoundTooSmall {
        /// Actual local complete-frame bound.
        actual: u32,
        /// Smallest structurally valid complete frame.
        minimum: u32,
    },
    /// The public `rafter-codec` version could not form a nonzero range.
    PeerCodecVersion(VersionRangeError),
}

impl fmt::Display for TlsHandshakeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameBoundTooLarge { actual, maximum } => write!(
                formatter,
                "TLS handshake frame bound {actual} exceeds u32 maximum {maximum}"
            ),
            Self::FrameBoundTooSmall { actual, minimum } => write!(
                formatter,
                "TLS handshake frame bound {actual} is below minimum {minimum}"
            ),
            Self::PeerCodecVersion(source) => {
                write!(formatter, "invalid current peer-codec version: {source}")
            }
        }
    }
}

impl Error for TlsHandshakeConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PeerCodecVersion(source) => Some(source),
            Self::FrameBoundTooLarge { .. } | Self::FrameBoundTooSmall { .. } => None,
        }
    }
}

/// Durable session-store failure while beginning or accepting a handshake.
#[derive(Debug)]
pub struct TlsHandshakeStoreError<E> {
    source: E,
}

impl<E> TlsHandshakeStoreError<E> {
    pub(super) const fn new(source: E) -> Self {
        Self { source }
    }

    /// Borrows the exact session-store failure.
    #[must_use]
    pub const fn source_error(&self) -> &E {
        &self.source
    }

    /// Returns the exact session-store failure.
    #[must_use]
    pub fn into_source(self) -> E {
        self.source
    }
}

impl<E> fmt::Display for TlsHandshakeStoreError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "TLS handshake session store failed: {}",
            self.source
        )
    }
}

impl<E> Error for TlsHandshakeStoreError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Failure while validating an authenticated server hello on the client.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsClientHandshakeError {
    /// TLS authenticated a principal other than the dial target.
    AuthenticatedPeerMismatch {
        /// Principal selected for the endpoint.
        expected: PeerId,
        /// Principal proved by the leaf certificate.
        actual: PeerId,
    },
    /// The server hello claims a different principal than its certificate.
    ServerIdentityMismatch {
        /// Principal proved by TLS.
        authenticated: PeerId,
        /// Principal claimed by the server hello.
        claimed: PeerId,
    },
    /// The server's deployment boundary differs from the client configuration.
    ClusterMismatch {
        /// Client deployment boundary.
        expected: ClusterId,
        /// Server deployment boundary.
        actual: ClusterId,
    },
    /// An accepted hello did not carry all canonical negotiated fields.
    NonCanonicalAccepted,
    /// The authenticated server returned a typed refusal.
    Refused {
        /// Server refusal category.
        reason: ServerRefusal,
    },
    /// The server selected an outer version the client did not offer.
    TransportVersionNotOffered {
        /// Invalid selected version.
        selected: u16,
    },
    /// The server selected a peer-codec version the client did not offer.
    PeerCodecVersionNotOffered {
        /// Invalid selected version.
        selected: u16,
    },
    /// The accepted complete-frame bound is outside the client's valid range.
    FrameLimitInvalid {
        /// Invalid accepted bound.
        accepted: u32,
        /// Minimum structurally valid bound.
        minimum: u32,
        /// Maximum the client offered.
        maximum: u32,
    },
}

impl fmt::Display for TlsClientHandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthenticatedPeerMismatch { expected, actual } => write!(
                formatter,
                "TLS authenticated {actual}, not dial target {expected}"
            ),
            Self::ServerIdentityMismatch {
                authenticated,
                claimed,
            } => write!(
                formatter,
                "TLS authenticated {authenticated}, but server hello claims {claimed}"
            ),
            Self::ClusterMismatch { expected, actual } => write!(
                formatter,
                "server cluster {actual} does not match configured cluster {expected}"
            ),
            Self::NonCanonicalAccepted => {
                formatter.write_str("accepted server hello is missing negotiated fields")
            }
            Self::Refused { reason } => {
                write!(
                    formatter,
                    "server refused the Rafter TLS handshake: {reason:?}"
                )
            }
            Self::TransportVersionNotOffered { selected } => write!(
                formatter,
                "server selected unoffered transport version {selected}"
            ),
            Self::PeerCodecVersionNotOffered { selected } => write!(
                formatter,
                "server selected unoffered peer-codec version {selected}"
            ),
            Self::FrameLimitInvalid {
                accepted,
                minimum,
                maximum,
            } => write!(
                formatter,
                "server accepted frame bound {accepted}, valid range is {minimum}..={maximum}"
            ),
        }
    }
}

impl Error for TlsClientHandshakeError {}
