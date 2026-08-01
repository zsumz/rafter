//! Authenticated-certificate and local-principal failures.

use std::{error::Error, fmt};

use crate::{CertificateFingerprint, PeerId};

/// Failure while mapping a completed TLS connection to a configured principal.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsPeerAuthenticationError {
    /// Certificate or ALPN state was inspected before the handshake completed.
    HandshakeIncomplete,
    /// No ALPN protocol was negotiated.
    MissingAlpn,
    /// The peer negotiated an ALPN protocol other than `rafter/1`.
    UnexpectedAlpn {
        /// Negotiated bytes, retained exactly for diagnostics.
        selected: Vec<u8>,
    },
    /// The completed connection exposed no authenticated peer certificate.
    MissingPeerCertificate,
    /// The validated leaf certificate was not explicitly configured.
    UnknownCertificate {
        /// SHA-256 fingerprint of the unconfigured leaf.
        fingerprint: CertificateFingerprint,
    },
}

impl fmt::Display for TlsPeerAuthenticationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HandshakeIncomplete => {
                formatter.write_str("TLS peer identity is unavailable before handshake completion")
            }
            Self::MissingAlpn => {
                formatter.write_str("TLS peer did not negotiate the required rafter/1 ALPN")
            }
            Self::UnexpectedAlpn { selected } => write!(
                formatter,
                "TLS peer negotiated unexpected ALPN bytes {selected:?}"
            ),
            Self::MissingPeerCertificate => {
                formatter.write_str("TLS connection has no authenticated peer certificate")
            }
            Self::UnknownCertificate { fingerprint } => write!(
                formatter,
                "TLS leaf certificate {fingerprint} is not in the explicit directory"
            ),
        }
    }
}

impl Error for TlsPeerAuthenticationError {}

/// Failure while binding the loaded local certificate to its configured peer ID.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LocalTlsIdentityError {
    /// The loaded leaf certificate is absent from the explicit directory.
    UnknownCertificate {
        /// Loaded leaf fingerprint.
        fingerprint: CertificateFingerprint,
    },
    /// The directory maps the loaded certificate to another principal.
    PeerMismatch {
        /// Principal the transport is being configured as.
        expected: PeerId,
        /// Principal assigned to the certificate.
        actual: PeerId,
    },
}

impl fmt::Display for LocalTlsIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCertificate { fingerprint } => write!(
                formatter,
                "local TLS leaf certificate {fingerprint} is not in the explicit directory"
            ),
            Self::PeerMismatch { expected, actual } => write!(
                formatter,
                "local TLS certificate maps to {actual}, not configured principal {expected}"
            ),
        }
    }
}

impl Error for LocalTlsIdentityError {}
