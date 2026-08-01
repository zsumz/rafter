//! Durable connection epochs and exact live-connection sequencing.

mod file;
mod format;
mod number;
mod sequence;
mod state;
mod store;

pub use file::{
    CreateTransportSessionStoreError, FileTransportSessionStore, FileTransportSessionStoreError,
    OpenTransportSessionStoreError,
};
pub use format::{
    decode_transport_session_state, encode_transport_session_state,
    encode_transport_session_state_into, max_transport_session_state_bytes,
    DecodeTransportSessionStateError, EncodeTransportSessionStateError,
    PersistedTransportSessionState, SessionIdentityField, SESSION_STATE_MAGIC,
    SESSION_STATE_VERSION,
};
pub use number::{ConnectionSequence, ConnectionSession, ZeroConnectionNumber};
pub use sequence::{InboundSequence, OutboundSequence, SequenceError, SequenceExhausted};
pub use state::{
    InboundSessionDecision, PeerSessionState, SessionStateError, TransportSessionState,
};
pub use store::TransportSessionStore;
