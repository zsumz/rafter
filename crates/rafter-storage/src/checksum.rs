//! Storage checksum vocabulary shared by envelope and streaming paths.
//!
//! This module reexports the dependency-free CRC32 implementation; artifact
//! grammars and durable stores decide what each checksum covers.

pub use rafter_crc32::crc32;
pub(crate) use rafter_crc32::RunningCrc32;
