//! Blocking runtime construction failures.

use std::{error::Error, fmt, io};

use crate::{
    BoxError, EndpointBookError, LocalTlsIdentityError, PeerFrameCodecConfigError, PeerId,
    TlsHandshakeConfigError, TlsTransportStartError,
};

/// Failure while constructing or activating a blocking TLS transport runtime.
#[derive(Debug)]
#[non_exhaustive]
pub enum TlsTransportBuildError {
    /// The builder was not supplied a local TLS identity.
    MissingIdentity,
    /// The builder was not supplied a certificate directory.
    MissingCertificates,
    /// The builder was not supplied a peer directory.
    MissingDirectory,
    /// The builder was not supplied an endpoint book.
    MissingEndpoints,
    /// The builder was not supplied a durable session store.
    MissingSessionStore,
    /// A supplied bounded component disagrees with aggregate configuration.
    DependencyLimitsMismatch {
        /// Stable component name.
        component: &'static str,
    },
    /// The shared receive-memory budget cannot admit one receiver and maximum frame.
    ReceiveMemoryTooSmall {
        /// Weighted bytes required by one receiver scratch and maximum frame.
        required: usize,
        /// Configured runtime-wide weighted-byte budget.
        maximum: usize,
    },
    /// The local leaf certificate did not prove the configured local peer.
    LocalIdentity {
        /// Original identity mismatch.
        source: LocalTlsIdentityError,
    },
    /// Current Rafter handshake policy could not be constructed.
    HandshakeConfig {
        /// Original handshake configuration error.
        source: TlsHandshakeConfigError,
    },
    /// The caller-owned group codec is incompatible with wire limits.
    FrameCodec {
        /// Original frame-codec configuration error.
        source: PeerFrameCodecConfigError,
    },
    /// The endpoint book could not be read safely.
    EndpointBook {
        /// Original endpoint-book error.
        source: EndpointBookError,
    },
    /// The endpoint book incorrectly contains the local physical principal.
    LocalPeerEndpoint {
        /// Configured local principal.
        peer: PeerId,
    },
    /// A configured sender peer has no explicitly authorized certificate.
    UnconfiguredCertificate {
        /// Physical peer missing a certificate mapping.
        peer: PeerId,
    },
    /// The durable session store failed aggregate startup preflight.
    SessionStore {
        /// Original store failure.
        source: BoxError,
    },
    /// Binding the configured listener failed.
    Bind {
        /// Requested listener address.
        address: std::net::SocketAddr,
        /// Original socket error.
        source: io::Error,
    },
    /// Configuring the listener's nonblocking mode failed.
    ConfigureListener {
        /// Original socket error.
        source: io::Error,
    },
    /// Reading the effective listener address failed.
    LocalAddress {
        /// Original socket error.
        source: io::Error,
    },
    /// One owned runtime worker could not be spawned.
    SpawnWorker {
        /// Stable worker role.
        role: String,
        /// Original thread-spawn error.
        source: io::Error,
    },
    /// A fully assembled paused runtime failed before ordinary activation.
    Start {
        /// Original lifecycle refusal.
        source: TlsTransportStartError,
    },
}

impl fmt::Display for TlsTransportBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingIdentity => formatter.write_str("TLS identity was not configured"),
            Self::MissingCertificates => {
                formatter.write_str("certificate directory was not configured")
            }
            Self::MissingDirectory => formatter.write_str("peer directory was not configured"),
            Self::MissingEndpoints => formatter.write_str("endpoint book was not configured"),
            Self::MissingSessionStore => {
                formatter.write_str("durable session store was not configured")
            }
            Self::DependencyLimitsMismatch { component } => write!(
                formatter,
                "{component} limits do not match the aggregate transport configuration"
            ),
            Self::ReceiveMemoryTooSmall { required, maximum } => write!(
                formatter,
                "receive-memory budget is {maximum} bytes, below the {required} bytes required \
                 for one receiver scratch and maximum frame"
            ),
            Self::LocalIdentity { source } => {
                write!(formatter, "local TLS identity is invalid: {source}")
            }
            Self::HandshakeConfig { source } => {
                write!(formatter, "handshake configuration is invalid: {source}")
            }
            Self::FrameCodec { source } => {
                write!(formatter, "peer-frame codec is invalid: {source}")
            }
            Self::EndpointBook { source } => {
                write!(formatter, "endpoint book could not be read: {source}")
            }
            Self::LocalPeerEndpoint { peer } => write!(
                formatter,
                "endpoint book contains local peer {peer}; local loopback is not a remote sender"
            ),
            Self::UnconfiguredCertificate { peer } => write!(
                formatter,
                "endpoint peer {peer} has no configured certificate fingerprint"
            ),
            Self::SessionStore { source } => {
                write!(
                    formatter,
                    "durable session-store preflight failed: {source}"
                )
            }
            Self::Bind { address, source } => {
                write!(
                    formatter,
                    "failed to bind TLS peer listener at {address}: {source}"
                )
            }
            Self::ConfigureListener { source } => {
                write!(formatter, "failed to configure TLS listener: {source}")
            }
            Self::LocalAddress { source } => {
                write!(formatter, "failed to read TLS listener address: {source}")
            }
            Self::SpawnWorker { role, source } => {
                write!(formatter, "failed to spawn {role} worker: {source}")
            }
            Self::Start { source } => {
                write!(formatter, "failed to activate TLS transport: {source}")
            }
        }
    }
}

impl Error for TlsTransportBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalIdentity { source } => Some(source),
            Self::HandshakeConfig { source } => Some(source),
            Self::FrameCodec { source } => Some(source),
            Self::EndpointBook { source } => Some(source),
            Self::SessionStore { source } => Some(source.as_ref()),
            Self::Bind { source, .. }
            | Self::ConfigureListener { source }
            | Self::LocalAddress { source }
            | Self::SpawnWorker { source, .. } => Some(source),
            Self::Start { source } => Some(source),
            _ => None,
        }
    }
}
