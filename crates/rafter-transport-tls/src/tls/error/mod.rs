//! Typed TLS identity, connection, and authenticated-principal failures.

mod authentication;
mod connection;
mod identity;

pub use authentication::{LocalTlsIdentityError, TlsPeerAuthenticationError};
pub use connection::TlsConnectionError;
pub use identity::{TlsConfigSide, TlsIdentityError, TlsIdentityFile};
