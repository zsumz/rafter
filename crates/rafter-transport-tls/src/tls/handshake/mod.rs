//! Durable client-session allocation and authenticated hello negotiation.

mod client;
mod config;
mod error;
mod server;
mod types;

pub use config::{TlsHandshakeConfig, MIN_PEER_FRAME_BYTES};
pub use error::{TlsClientHandshakeError, TlsHandshakeConfigError, TlsHandshakeStoreError};
pub use types::NegotiatedTlsHandshake;
