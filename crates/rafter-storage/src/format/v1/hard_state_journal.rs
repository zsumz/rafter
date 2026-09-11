//! Version-1 journal header and fixed-width hard-state record grammar.
//!
//! RFHJ identifies an append-only stream of complete RFHS v1 envelopes.
//! An incomplete final envelope is a recoverable append; a complete envelope
//! must pass the existing checksum and canonical-field validation.

use crate::{crc32, decode_raft_hard_state, DecodeRaftHardStateError, RaftHardState};

pub(crate) const HEADER_LEN: usize = 9;
pub(crate) const RECORD_LEN: usize = 51;
const PREFIX: &[u8; 5] = b"RFHJ\x01";

pub(crate) fn header() -> [u8; HEADER_LEN] {
    let mut bytes = [0; HEADER_LEN];
    bytes[..5].copy_from_slice(PREFIX);
    bytes[5..].copy_from_slice(&crc32(PREFIX).to_be_bytes());
    bytes
}

pub(crate) fn decode_record(
    bytes: &[u8; RECORD_LEN],
) -> Result<RaftHardState, DecodeRaftHardStateError> {
    decode_raft_hard_state(bytes)
}
