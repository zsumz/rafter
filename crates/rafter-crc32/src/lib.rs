//! Table-driven CRC-32 for Rafter's corruption-detection envelopes.
//!
//! This crate is intentionally tiny and dependency-free. It centralizes the
//! IEEE CRC-32 implementation used by storage envelopes and peer-message frames
//! so both boundaries keep byte-compatible checksums without duplicating the hot
//! byte-scanning code.

mod checksum;

pub use checksum::{crc32, RunningCrc32};

#[cfg(test)]
#[path = "lib_test.rs"]
mod tests;
