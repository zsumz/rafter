//! TLS 1.3 identity, authenticated-principal, and Rafter handshake adapters.

mod authentication;
mod error;
mod handshake;
mod identity;
mod pem;

pub use authentication::{
    authenticate_client_connection, authenticate_server_connection, AuthenticatedTlsPeer,
};
pub use error::{
    LocalTlsIdentityError, TlsConfigSide, TlsConnectionError, TlsIdentityError, TlsIdentityFile,
    TlsPeerAuthenticationError,
};
pub use handshake::{
    NegotiatedTlsHandshake, TlsClientHandshakeError, TlsHandshakeConfig, TlsHandshakeConfigError,
    TlsHandshakeStoreError, MIN_PEER_FRAME_BYTES,
};
pub use identity::TlsIdentity;
pub use pem::{
    MAX_CERTIFICATE_CHAIN_PEM_BYTES, MAX_PRIVATE_KEY_PEM_BYTES, MAX_TRUST_ROOTS_PEM_BYTES,
};

/// Required ALPN protocol for every Rafter peer connection.
pub const TLS_ALPN_PROTOCOL: &[u8] = b"rafter/1";
