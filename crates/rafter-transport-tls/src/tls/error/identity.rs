//! TLS identity input and configuration failures.

use std::{error::Error, fmt, io, path::PathBuf};

/// File role read while loading one TLS identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsIdentityFile {
    /// Local leaf certificate followed by any intermediates.
    CertificateChain,
    /// Local unencrypted private key.
    PrivateKey,
    /// Trust roots used for both server and client certificate validation.
    TrustRoots,
}

impl fmt::Display for TlsIdentityFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CertificateChain => formatter.write_str("certificate chain"),
            Self::PrivateKey => formatter.write_str("private key"),
            Self::TrustRoots => formatter.write_str("trust roots"),
        }
    }
}

/// TLS configuration side whose protocol-version selection failed.
///
/// This enum is exhaustive: a mutual-TLS configuration has exactly one client
/// side and one server side.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsConfigSide {
    /// Outbound client configuration.
    Client,
    /// Inbound server configuration.
    Server,
}

impl fmt::Display for TlsConfigSide {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client => formatter.write_str("client"),
            Self::Server => formatter.write_str("server"),
        }
    }
}

/// Failure while loading and validating one local mutual-TLS identity.
#[derive(Debug)]
#[non_exhaustive]
pub enum TlsIdentityError {
    /// Reading one configured PEM file failed.
    ReadFile {
        /// File role.
        field: TlsIdentityFile,
        /// Configured path.
        path: PathBuf,
        /// Filesystem failure.
        source: io::Error,
    },
    /// One configured PEM file exceeded its finite input bound.
    FileTooLarge {
        /// File role.
        field: TlsIdentityFile,
        /// Configured path.
        path: PathBuf,
        /// Maximum accepted bytes.
        maximum: usize,
    },
    /// One PEM stream was malformed.
    ParsePem {
        /// PEM role.
        field: TlsIdentityFile,
        /// Parser failure.
        source: rustls::pki_types::pem::Error,
    },
    /// The local certificate chain contained no certificate.
    EmptyCertificateChain,
    /// The trust-root PEM contained no certificate.
    EmptyTrustRoots,
    /// The private-key PEM contained no supported key.
    MissingPrivateKey,
    /// The private-key PEM contained more than one key.
    MultiplePrivateKeys,
    /// One configured trust root was not a valid trust anchor.
    InvalidTrustRoot {
        /// Zero-based root index.
        index: usize,
        /// Certificate parser failure.
        source: rustls::Error,
    },
    /// The selected crypto provider could not support TLS 1.3.
    Tls13Configuration {
        /// Configuration side.
        side: TlsConfigSide,
        /// Rustls configuration failure.
        source: rustls::Error,
    },
    /// The local certificate and key could not form a client identity.
    ClientIdentity {
        /// Rustls key/certificate failure.
        source: rustls::Error,
    },
    /// The mandatory client-certificate verifier could not be constructed.
    ClientVerifier {
        /// Verifier construction failure.
        source: rustls::server::VerifierBuilderError,
    },
    /// The local certificate and key could not form a server identity.
    ServerIdentity {
        /// Rustls key/certificate failure.
        source: rustls::Error,
    },
}

impl fmt::Display for TlsIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadFile {
                field,
                path,
                source,
            } => write!(
                formatter,
                "could not read TLS {field} {}: {source}",
                path.display()
            ),
            Self::FileTooLarge {
                field,
                path,
                maximum,
            } => write!(
                formatter,
                "TLS {field} {} exceeds the {maximum}-byte limit",
                path.display()
            ),
            Self::ParsePem { field, source } => {
                write!(formatter, "could not parse TLS {field} PEM: {source}")
            }
            Self::EmptyCertificateChain => {
                formatter.write_str("TLS certificate chain contains no certificate")
            }
            Self::EmptyTrustRoots => {
                formatter.write_str("TLS trust-root PEM contains no certificate")
            }
            Self::MissingPrivateKey => {
                formatter.write_str("TLS private-key PEM contains no supported key")
            }
            Self::MultiplePrivateKeys => {
                formatter.write_str("TLS private-key PEM contains more than one key")
            }
            Self::InvalidTrustRoot { index, source } => {
                write!(formatter, "TLS trust root {index} is invalid: {source}")
            }
            Self::Tls13Configuration { side, source } => write!(
                formatter,
                "could not configure TLS 1.3 for the {side}: {source}"
            ),
            Self::ClientIdentity { source } => {
                write!(
                    formatter,
                    "could not configure TLS client identity: {source}"
                )
            }
            Self::ClientVerifier { source } => write!(
                formatter,
                "could not configure mandatory TLS client authentication: {source}"
            ),
            Self::ServerIdentity { source } => {
                write!(
                    formatter,
                    "could not configure TLS server identity: {source}"
                )
            }
        }
    }
}

impl Error for TlsIdentityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadFile { source, .. } => Some(source),
            Self::ParsePem { source, .. } => Some(source),
            Self::InvalidTrustRoot { source, .. }
            | Self::Tls13Configuration { source, .. }
            | Self::ClientIdentity { source }
            | Self::ServerIdentity { source } => Some(source),
            Self::ClientVerifier { source } => Some(source),
            Self::FileTooLarge { .. }
            | Self::EmptyCertificateChain
            | Self::EmptyTrustRoots
            | Self::MissingPrivateKey
            | Self::MultiplePrivateKeys => None,
        }
    }
}
