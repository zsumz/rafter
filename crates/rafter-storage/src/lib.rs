//! Durable storage for the Rafter consensus runtime.
//!
//! This crate owns the persistence contract of a durable Raft node: the
//! hard-state store, the append-only log segment, and the snapshot store,
//! each defined as a trait with file-backed and in-memory implementations.
//! It does not run Raft, apply committed entries, own application state, choose
//! transport behavior, or decide when a datastore may serve traffic after
//! recovery; those obligations sit in the runtime and application layers.
//! Every on-disk format is a versioned, checksummed envelope. This pre-release
//! crate supports the current first-public storage formats only; unsupported
//! versions fail loudly, and new bytes require new envelope versions plus an
//! explicit migration or compatibility plan.
//! `FileRaftNodeStores` opens the standard file-backed node layout with
//! batched creation syncs for the empty log file and snapshot directory.
//!
//! # Format Compatibility
//!
//! Current writers emit version 1 hard-state, log-entry, snapshot, and pending
//! snapshot-transfer manifest envelopes. Earlier internal draft layouts are
//! intentionally unsupported before the first public compatibility promise.
//! Unknown versions return typed errors rather than being interpreted as older
//! meanings.
//!
//! # Integrity Model
//!
//! Storage checksums are CRC32 corruption checks. They are useful for torn
//! writes, partial files, stale manifests, and accidental media corruption in
//! non-Byzantine deployments, but they are not tamper evidence against an
//! adversary who can rewrite bytes and checksums. Deployments with that threat
//! model should authenticate storage below this crate or have the application
//! snapshot format carry and verify a stronger digest.

mod checksum;
mod durable_fs;
mod file_node_stores;
mod raft_hard_state_codec;
mod raft_hard_state_store;
mod raft_log_compaction;
mod raft_log_entry_codec;
mod raft_log_segment;
mod raft_snapshot_codec;
mod raft_snapshot_store;

#[cfg(test)]
mod raft_hard_state_codec_test;

pub use checksum::crc32;
pub use file_node_stores::{FileRaftNodeStores, OpenFileRaftNodeStoresError};
pub use raft_hard_state_codec::{
    decode_raft_hard_state, encode_raft_hard_state, DecodeRaftHardStateError, RaftHardState,
    RAFT_HARD_STATE_MAGIC, RAFT_HARD_STATE_VERSION,
};
pub use raft_hard_state_store::{
    FileRaftHardStateStore, InMemoryRaftHardStateStore, OpenRaftHardStateStoreError,
    RaftHardStateStore, RaftHardStateStoreWriteError,
};
pub use raft_log_entry_codec::{
    decode_raft_log_entry, encode_raft_log_entry, DecodeRaftLogEntryError, EncodeRaftLogEntryError,
    PersistedRaftLogEntry, RAFT_LOG_ENTRY_MAGIC, RAFT_LOG_ENTRY_VERSION,
};
pub use raft_log_segment::{
    FileRaftLogSegment, InMemoryRaftLogSegment, OpenRaftLogSegmentError, RaftLogReplayError,
    RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError,
};
pub use raft_snapshot_codec::{
    decode_raft_snapshot, encode_raft_snapshot, DecodeRaftSnapshotError, EncodeRaftSnapshotError,
    PersistedRaftSnapshot, RAFT_SNAPSHOT_MAGIC, RAFT_SNAPSHOT_VERSION,
};
pub use raft_snapshot_store::{
    DecodePendingSnapshotTransferError, FileRaftSnapshotStore, InMemoryRaftSnapshotStore,
    OpenRaftSnapshotStoreError, PendingSnapshotTransferStagingStatus,
    RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError, RaftSnapshotStore,
    RaftSnapshotStoreWriteError,
};
