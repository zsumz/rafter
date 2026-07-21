//! Compatibility facade for the version-1 log-compaction marker format.
//!
//! The RFLC byte grammar lives in `format::v1::log_compaction`; this module
//! preserves the storage crate's established internal module path.

pub use crate::format::v1::log_compaction::{
    decode_raft_log_compaction_marker, encode_raft_log_compaction_marker,
    DecodeRaftLogCompactionMarkerError,
};
