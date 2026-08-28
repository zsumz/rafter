//! Blocking insecure demo-only TCP transport for Rafter peer messages.
//!
//! This crate is intentionally small: it adds a four-byte big-endian length
//! prefix around `rafter-codec` peer-message frames and a std-only TCP helper
//! that opens a connection per outbound message with bounded reconnect
//! backoff. It is for examples, local testing, and demo wiring only.
//! It owns only demo frame I/O and a minimal peer-address map.
//! It does not authenticate peers and is not production-ready.
//!
//! This is not Rafter's production transport reference: it has no persistent
//! per-peer streams, no bounded outbound queues, no write deadlines, and no
//! read timeouts. The connection-per-message shape can reorder under retries
//! and can block on a slow or dead peer. Production transports should follow
//! the delivery, ordering, and backpressure contract exposed by
//! `rafter-service`.
//!
//! This transport does not authenticate connections, prove that the remote
//! endpoint owns the node ID embedded in the peer message, or fence traffic
//! from removed members. Production transports must provide those properties
//! before passing messages into the Raft core; otherwise an unauthorized
//! higher-term message can still cause the normal Raft term update before the
//! kernel rejects the sender at the membership layer. See the production
//! boundary in the repository README for the full embedding contract.

mod backoff;
mod error;
mod frame;
mod transport;

pub use backoff::ReconnectBackoff;
pub use error::{ReadFrameError, WriteFrameError};
pub use frame::{
    message_sender, read_message_frame, write_message_frame, write_message_frame_into,
    DEFAULT_MAX_FRAME_LEN,
};
pub use transport::{InsecureTcpTransport, ReceivedPeerMessage, TcpTransportError};

#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use rafter::NodeId;

#[cfg(test)]
use backoff::connect_with_backoff_using;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
