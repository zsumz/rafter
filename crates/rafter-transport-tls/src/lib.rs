//! Bounded mutually authenticated TLS-over-TCP transport for Rafter peers.
//!
//! The crate implements [`rafter_service::RaftTransport`] with a synchronous,
//! nonblocking admission handle backed by persistent per-peer worker threads.
//! It owns mutual TLS, explicit certificate-to-principal authentication,
//! cluster/version negotiation, canonical multiplexed group routing, durable
//! connection epochs, finite queues, reconnect policy, inbound authentication,
//! diagnostics, and graceful shutdown.
//!
//! It deliberately does not own Raft group lifecycle, node-ID allocation,
//! service discovery, DNS, certificate issuance, application storage, or an
//! application task runtime. Callers provide resolved endpoints, PKI material,
//! canonical group encoding, group-specific identity bindings, and durable
//! session state.
//!
//! A [`PeerId`] is the stable principal proved by mutual TLS. A
//! [`rafter::NodeId`] is a single-use identity inside one Raft group. One
//! physical peer may host many groups, so connections are keyed by `PeerId` and
//! group-specific admission is checked by [`TlsPeerDirectory`].

#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod builder;
mod certificate;
mod config;
mod connection;
mod diagnostics;
mod directory;
mod endpoint;
mod error;
mod group_codec;
mod inbound;
mod limits;
mod principal;
mod queue;
mod runtime;
mod sender;
mod session;
mod snapshot;
mod tls;
mod transport;
mod wire;

pub use builder::TlsPeerTransportBuilder;
pub use certificate::{
    CertificateDirectory, CertificateDirectoryBuilder, CertificateDirectoryError,
    CertificateFingerprint, CertificateFingerprintParseError, CertificatePemError,
    MAX_CERTIFICATE_PEM_BYTES,
};
pub use config::{
    TimeoutKind, TransportConfig, TransportIoTimeouts, TransportRuntimeTimeouts,
    TransportTimeoutError, TransportTimeouts,
};
pub use diagnostics::{PeerDiagnostics, QueueDepths, TransportDiagnostics, TransportHealth};
pub use directory::{DirectoryError, InstalledPeerPolicy, PeerAuthorization, TlsPeerDirectory};
pub use endpoint::{
    EndpointBook, EndpointBookError, EndpointGeneration, EndpointSnapshot, PeerEndpoint,
    TlsServerName, TlsServerNameError, MAX_TLS_SERVER_NAME_BYTES,
};
pub use error::{
    BoxError, TlsInboundError, TlsTransportBuildError, TlsTransportError, TlsTransportJoinError,
    TlsTransportStartError,
};
pub use group_codec::GroupIdCodec;
pub use inbound::TlsInbound;
pub use limits::{
    CertificateDirectoryLimits, DirectoryLimits, EndpointBookLimits, InboundQueueLimits,
    LimitError, LimitKind, OutboundQueueLimits, RuntimeLimitError, RuntimeLimitKind, RuntimeLimits,
    SessionStoreLimits, TransportLimits, WireLimits, DEFAULT_MAX_APPEND_ENTRIES_BYTES,
    DEFAULT_MAX_FRAME_BODY_BYTES, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_GROUP_ID_BYTES,
    DEFAULT_MAX_SESSION_PEER_RECORDS, MAX_SESSION_PEER_RECORDS,
};
pub use principal::{ClusterId, IdentityError, IdentityKind, PeerId, MAX_ID_BYTES};
pub use queue::TrafficClass;
pub use sender::TlsSender;
pub use session::{
    decode_transport_session_state, encode_transport_session_state,
    encode_transport_session_state_into, max_transport_session_state_bytes, ConnectionSequence,
    ConnectionSession, CreateTransportSessionStoreError, DecodeTransportSessionStateError,
    EncodeTransportSessionStateError, FileTransportSessionStore, FileTransportSessionStoreError,
    InboundSequence, InboundSessionDecision, OpenTransportSessionStoreError, OutboundSequence,
    PeerSessionState, PersistedTransportSessionState, SequenceError, SequenceExhausted,
    SessionIdentityField, SessionStateError, TransportSessionState, TransportSessionStore,
    ZeroConnectionNumber, SESSION_STATE_MAGIC, SESSION_STATE_VERSION,
};
pub use snapshot::{
    SnapshotChunkResolveRequest, SnapshotChunkResolver, SnapshotChunkSourceResolver,
};
pub use tls::{
    authenticate_client_connection, authenticate_server_connection, AuthenticatedTlsPeer,
    LocalTlsIdentityError, NegotiatedTlsHandshake, TlsClientHandshakeError, TlsConfigSide,
    TlsConnectionError, TlsHandshakeConfig, TlsHandshakeConfigError, TlsHandshakeStoreError,
    TlsIdentity, TlsIdentityError, TlsIdentityFile, TlsPeerAuthenticationError,
    MAX_CERTIFICATE_CHAIN_PEM_BYTES, MAX_PRIVATE_KEY_PEM_BYTES, MAX_TRUST_ROOTS_PEM_BYTES,
    MIN_PEER_FRAME_BYTES, TLS_ALPN_PROTOCOL,
};
pub use transport::TlsPeerTransport;
pub use wire::{
    decode_client_hello, decode_server_hello, encode_client_hello_into, encode_server_hello_into,
    highest_common_version, ClientHello, DecodeHandshakeError, DecodePeerFrameError,
    EncodePeerFrameError, HandshakeField, PeerFrame, PeerFrameCodec, PeerFrameCodecConfigError,
    PeerFrameError, PeerFrameScratch, ServerHello, ServerHelloStatus, ServerRefusal, VersionRange,
    VersionRangeError, CURRENT_TRANSPORT_VERSION, HANDSHAKE_MAGIC, MAX_CLIENT_HELLO_BYTES,
    MAX_SERVER_HELLO_BYTES, PEER_FRAME_FIXED_BODY_BYTES, PEER_FRAME_KIND_MESSAGE,
    PEER_FRAME_LENGTH_PREFIX_BYTES,
};
