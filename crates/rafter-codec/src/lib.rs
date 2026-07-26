//! Versioned wire codec for Rafter peer messages.
//!
//! This crate owns peer-message serialization, version dispatch, and
//! accidental-corruption detection. It does not own stream framing, transport
//! authentication, delivery policy, backpressure, storage, or scheduling.
//!
//! See `WIRE_FORMAT_V1.md` in the crate root for the byte-level version 1
//! contract.

#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod error;
mod frame;
mod limits;
mod v1;
mod wire;

pub use error::{DecodePeerMessageError, EncodePeerMessageError};
pub use frame::{decode_message, encode_message, encode_message_into, MAGIC, VERSION};
pub use limits::{
    max_receive_frame_bytes, MAX_CONFIGURATION_APPEND_FRAME_BYTES,
    MAX_INSTALL_SNAPSHOT_CHUNK_FRAME_BYTES,
};

#[cfg(test)]
mod tests;
