//! Transport/codec negotiation and deployment identity handshake.

mod codec;
mod error;
mod types;

pub use codec::{
    decode_client_hello, decode_server_hello, encode_client_hello_into, encode_server_hello_into,
};
pub use error::{DecodeHandshakeError, HandshakeField, VersionRangeError};
pub use types::{
    highest_common_version, ClientHello, ServerHello, ServerHelloStatus, ServerRefusal,
    VersionRange, CURRENT_TRANSPORT_VERSION, HANDSHAKE_MAGIC, MAX_CLIENT_HELLO_BYTES,
    MAX_SERVER_HELLO_BYTES,
};
