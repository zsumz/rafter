//! Handshake decoding and version-range errors.

use std::{error::Error, fmt};

use crate::IdentityError;

/// Invalid inclusive wire-version range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum VersionRangeError {
    /// Minimum version was zero.
    ZeroMinimum,
    /// Maximum version was zero.
    ZeroMaximum,
    /// Minimum version exceeded maximum version.
    Reversed {
        /// Invalid minimum.
        minimum: u16,
        /// Invalid maximum.
        maximum: u16,
    },
}

impl fmt::Display for VersionRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMinimum => formatter.write_str("minimum wire version must be nonzero"),
            Self::ZeroMaximum => formatter.write_str("maximum wire version must be nonzero"),
            Self::Reversed { minimum, maximum } => write!(
                formatter,
                "minimum wire version {minimum} exceeds maximum {maximum}"
            ),
        }
    }
}

impl Error for VersionRangeError {}

/// Named handshake field used in typed decoding errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HandshakeField {
    /// Client transport-version range.
    TransportVersions,
    /// Client peer-codec-version range.
    PeerCodecVersions,
    /// Cluster identity.
    ClusterId,
    /// Client-claimed peer identity.
    ClaimedPeerId,
    /// Server peer identity.
    ServerPeerId,
}

impl fmt::Display for HandshakeField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::TransportVersions => "transport versions",
            Self::PeerCodecVersions => "peer codec versions",
            Self::ClusterId => "cluster ID",
            Self::ClaimedPeerId => "claimed peer ID",
            Self::ServerPeerId => "server peer ID",
        };
        formatter.write_str(name)
    }
}

/// Malformed version-1 transport handshake.
#[derive(Debug)]
#[non_exhaustive]
pub enum DecodeHandshakeError {
    /// Input exceeded the fixed maximum hello size.
    TooLong {
        /// Actual input bytes.
        actual: usize,
        /// Maximum input bytes.
        maximum: usize,
    },
    /// Input ended before one field was complete.
    Truncated,
    /// Magic did not identify a Rafter TLS handshake.
    InvalidMagic,
    /// One advertised version range was invalid.
    InvalidVersionRange {
        /// Invalid field.
        field: HandshakeField,
        /// Range validation error.
        source: VersionRangeError,
    },
    /// One length-prefixed identity was not UTF-8.
    InvalidUtf8 {
        /// Invalid field.
        field: HandshakeField,
    },
    /// A cluster or peer identity failed semantic validation.
    InvalidIdentity {
        /// Invalid field.
        field: HandshakeField,
        /// Identity validation error.
        source: IdentityError,
    },
    /// The durable connection session was zero.
    ZeroSession,
    /// The proposed or accepted frame limit was zero.
    ZeroFrameLimit,
    /// The server status byte was not allocated by transport version 1.
    UnknownServerStatus(u8),
    /// An accepted hello encoded a zero version or frame bound.
    NonCanonicalAccepted,
    /// A refused hello encoded nonzero negotiated values.
    NonCanonicalRefusal,
    /// Bytes remained after the complete hello.
    TrailingBytes {
        /// Number of unconsumed bytes.
        remaining: usize,
    },
}

impl fmt::Display for DecodeHandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { actual, maximum } => write!(
                formatter,
                "handshake is {actual} bytes, exceeding maximum {maximum}"
            ),
            Self::Truncated => formatter.write_str("handshake ended before a field was complete"),
            Self::InvalidMagic => formatter.write_str("handshake magic is not RAFTER-TLS"),
            Self::InvalidVersionRange { field, source } => {
                write!(formatter, "invalid {field}: {source}")
            }
            Self::InvalidUtf8 { field } => {
                write!(formatter, "handshake {field} is not UTF-8")
            }
            Self::InvalidIdentity { field, source } => {
                write!(formatter, "invalid handshake {field}: {source}")
            }
            Self::ZeroSession => formatter.write_str("connection session must be nonzero"),
            Self::ZeroFrameLimit => formatter.write_str("frame limit must be nonzero"),
            Self::UnknownServerStatus(status) => {
                write!(formatter, "server hello status {status} is not allocated")
            }
            Self::NonCanonicalAccepted => formatter.write_str(
                "accepted server hello must carry nonzero selected versions and \
                 frame bound",
            ),
            Self::NonCanonicalRefusal => formatter.write_str(
                "refused server hello must carry zero selected versions and \
                 frame bound",
            ),
            Self::TrailingBytes { remaining } => {
                write!(formatter, "handshake has {remaining} trailing bytes")
            }
        }
    }
}

impl Error for DecodeHandshakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidVersionRange { source, .. } => Some(source),
            Self::InvalidIdentity { source, .. } => Some(source),
            _ => None,
        }
    }
}
