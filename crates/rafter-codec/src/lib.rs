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
mod v1;
mod wire;

pub use error::{DecodePeerMessageError, EncodePeerMessageError};
pub use frame::{decode_message, encode_message, encode_message_into, MAGIC, VERSION};

#[cfg(test)]
mod tests;
