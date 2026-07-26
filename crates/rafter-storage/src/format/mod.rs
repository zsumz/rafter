//! Shared byte-level mechanics for Rafter's durable formats.
//!
//! This module owns bounds-checked big-endian cursors, trailing CRC32 handling,
//! and the successor bound every log-position field shares. Individual artifact
//! codecs continue to own their magic values, versions, tags, field order,
//! canonicality rules, and public typed errors.

mod cursor;
mod envelope;
mod log_position;
pub(crate) mod v1;

pub(crate) use cursor::{CursorError, Reader, Writer};
pub(crate) use envelope::{finish_checksummed, verify_checksum, ChecksumError};
pub(crate) use log_position::advanceable_log_index;

#[cfg(test)]
mod tests;
