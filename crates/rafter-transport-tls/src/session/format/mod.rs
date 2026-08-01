//! Versioned, checksummed durable session-state format.

mod decode;
mod encode;
mod error;
mod read;
mod types;

pub use decode::decode_transport_session_state;
pub use encode::{
    encode_transport_session_state, encode_transport_session_state_into,
    max_transport_session_state_bytes,
};
pub use error::{DecodeTransportSessionStateError, EncodeTransportSessionStateError};
pub use types::{
    PersistedTransportSessionState, SessionIdentityField, SESSION_STATE_MAGIC,
    SESSION_STATE_VERSION,
};

use read::Reader;
