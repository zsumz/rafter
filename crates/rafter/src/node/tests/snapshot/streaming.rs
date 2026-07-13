//! Bounded-memory snapshot streaming, restart resume, and dynamic authorization.

pub(super) use super::super::helpers::{bootstrap_entry, node};
pub(super) use super::support::{
    install_snapshot_response_from_outputs, snapshot_chunk_send_from_output, test_snapshot,
};
pub(super) use super::*;

const SNAPSHOT_CHUNK_BYTES: u64 = 64 * 1024;
const PEAK_RESIDENT_PAYLOAD_LIMIT_BYTES: u64 = 3 * SNAPSHOT_CHUNK_BYTES;
const SYNTHETIC_BLOCK_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
const SYNTHETIC_BLOCK_INCREMENT: u64 = 0xD1B5_4A32_D192_ED03;

mod large;
mod membership;
mod support;
mod transfer;
