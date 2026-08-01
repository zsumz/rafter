//! Shared finite-limit validation failures.

use std::{error::Error, fmt};

/// Which finite resource limit was invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LimitKind {
    /// Number of groups retained by a peer directory.
    Groups,
    /// Number of principal/node bindings retained per group.
    BindingsPerGroup,
    /// Number of peers retained by an endpoint book.
    EndpointPeers,
    /// Number of endpoints retained for one peer.
    EndpointsPerPeer,
    /// Number of certificate fingerprints retained by a certificate directory.
    CertificateFingerprints,
    /// Number of distinct principals retained by a certificate directory.
    CertificatePeers,
    /// Number of physical-peer records retained by a session store.
    SessionPeers,
    /// Maximum canonical group identity bytes in a peer frame.
    GroupIdBytes,
    /// Maximum peer-frame body bytes, excluding the length prefix.
    FrameBodyBytes,
}

impl fmt::Display for LimitKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Groups => "groups",
            Self::BindingsPerGroup => "bindings per group",
            Self::EndpointPeers => "endpoint peers",
            Self::EndpointsPerPeer => "endpoints per peer",
            Self::CertificateFingerprints => "certificate fingerprints",
            Self::CertificatePeers => "certificate peers",
            Self::SessionPeers => "session peers",
            Self::GroupIdBytes => "group ID bytes",
            Self::FrameBodyBytes => "frame body bytes",
        };
        formatter.write_str(name)
    }
}

/// Invalid finite transport limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LimitError {
    /// A limit that must be positive was zero.
    Zero {
        /// Invalid limit.
        kind: LimitKind,
    },
    /// A limit exceeded what its wire field or local address space can hold.
    TooLarge {
        /// Invalid limit.
        kind: LimitKind,
        /// Requested value.
        value: usize,
        /// Maximum accepted value.
        maximum: usize,
    },
    /// The total frame bound cannot hold its fixed fields and maximum group ID.
    FrameBodyTooSmall {
        /// Requested frame body limit.
        frame_body_bytes: usize,
        /// Smallest body leaving one byte for an inner message.
        minimum: usize,
    },
}

impl fmt::Display for LimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero { kind } => {
                write!(formatter, "transport limit {kind} must be nonzero")
            }
            Self::TooLarge {
                kind,
                value,
                maximum,
            } => write!(
                formatter,
                "transport limit {kind} is {value}, exceeding maximum {maximum}"
            ),
            Self::FrameBodyTooSmall {
                frame_body_bytes,
                minimum,
            } => write!(
                formatter,
                "frame body limit {frame_body_bytes} is smaller than minimum {minimum}"
            ),
        }
    }
}

impl Error for LimitError {}

pub(super) fn require_nonzero(kind: LimitKind, value: usize) -> Result<(), LimitError> {
    if value == 0 {
        Err(LimitError::Zero { kind })
    } else {
        Ok(())
    }
}

pub(super) fn require_at_most(
    kind: LimitKind,
    value: usize,
    maximum: usize,
) -> Result<(), LimitError> {
    if value > maximum {
        Err(LimitError::TooLarge {
            kind,
            value,
            maximum,
        })
    } else {
        Ok(())
    }
}
