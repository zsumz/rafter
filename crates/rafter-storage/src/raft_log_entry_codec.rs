//! Compatibility facade for the version-1 persisted-log-entry format.
//!
//! The RFLE byte grammar lives in `format::v1::log_entry`; this module keeps
//! the established crate module path and flat public API stable.

pub use crate::format::v1::log_entry::{
    decode_raft_log_entry, encode_borrowed_raft_log_entry, encode_raft_log_entry,
    BorrowedPersistedRaftLogEntry, DecodeRaftLogEntryError, EncodeRaftLogEntryError,
    PersistedRaftLogEntry, RAFT_LOG_ENTRY_MAGIC, RAFT_LOG_ENTRY_VERSION,
};
