//! Shared byte-level mechanics for Rafter's durable formats.
//!
//! This module owns bounds-checked big-endian cursors and trailing CRC32
//! handling. Individual artifact codecs continue to own their magic values,
//! versions, tags, field order, canonicality rules, and public typed errors.

mod cursor;
mod envelope;
pub(crate) mod v1;

pub(crate) use cursor::{CursorError, Reader, Writer};
pub(crate) use envelope::{finish_checksummed, verify_checksum, ChecksumError};

#[cfg(test)]
mod tests;
