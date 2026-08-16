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
//! The exact version-1 bytes are specified in `STORAGE_FORMAT_V1.md`; durable
//! publication and recovery ordering are specified in `DURABILITY_PROTOCOL.md`.
//!
//! # The two stores a replica cannot restart without
//!
//! The in-memory implementations satisfy the same trait contracts as the
//! file-backed ones, which is what makes them usable as the durable half of a
//! deterministic test. They are not durable, and nothing here makes them so.
//!
//! ```
//! use rafter::{LogIndex, NodeId, Term};
//! use rafter_storage::{
//!     InMemoryRaftHardStateStore, InMemoryRaftLogSegment, PersistedRaftLogEntry, RaftHardState,
//!     RaftHardStateStore, RaftLogSegment,
//! };
//!
//! // Hard state is the set of promises this replica has already made. A store
//! // that returned Ok has made them durable, which is what stops a restarted
//! // node from voting a second time in a term it has already voted in.
//! let mut hard_state_store = InMemoryRaftHardStateStore::new();
//! hard_state_store
//!     .write_hard_state(RaftHardState {
//!         current_term: Term(7),
//!         voted_for: Some(NodeId(2)),
//!         commit_index: LogIndex::ZERO,
//!         committed_configuration: None,
//!     })
//!     .expect("in-memory hard-state write succeeds");
//! assert_eq!(hard_state_store.current().voted_for, Some(NodeId(2)));
//!
//! // The log segment is append-only above its compacted prefix, and replay
//! // hands back exactly what a restart would rebuild the kernel from.
//! let mut log_segment = InMemoryRaftLogSegment::new();
//! log_segment
//!     .append_entries(&[PersistedRaftLogEntry::application(
//!         LogIndex(1),
//!         Term(7),
//!         b"set alpha=one".to_vec(),
//!     )])
//!     .expect("the first entry starts at the segment's next index");
//! assert_eq!(log_segment.next_index(), LogIndex(2));
//! assert_eq!(log_segment.replay_entries().len(), 1);
//! ```
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
//!
//! # Operational Errors
//!
//! Filesystem failures retain their original [`std::io::Error`] and expose it
//! through [`std::error::Error::source`]. [`StorageIoError`] keeps that source
//! cloneable for runtime poison state while preserving its kind and OS code.
//!
//! # File-backed Ownership
//!
//! [`FileRaftNodeStores`] acquires exclusive cooperating-process ownership of a
//! replica directory before opening or repairing its stores. Direct file-store
//! constructors support custom layouts but require caller-enforced exclusivity.

mod checksum;
mod durable_fs;
mod file_node_stores;
mod file_store_health;
mod file_store_ownership;
mod format;
mod io_error;
mod raft_hard_state_codec;
mod raft_hard_state_store;
mod raft_log_compaction;
mod raft_log_entry_codec;
mod raft_log_segment;
mod raft_snapshot_codec;
mod raft_snapshot_store;

#[cfg(test)]
mod raft_hard_state_codec_test;
#[cfg(test)]
mod storage_failpoint_test;

pub use checksum::crc32;
pub use file_node_stores::{FileRaftNodeStores, OpenFileRaftNodeStoresError};
pub use io_error::StorageIoError;
pub use raft_hard_state_codec::{
    decode_raft_hard_state, encode_raft_hard_state, DecodeRaftHardStateError, RaftHardState,
    RAFT_HARD_STATE_MAGIC, RAFT_HARD_STATE_VERSION,
};
pub use raft_hard_state_store::{
    FileRaftHardStateStore, InMemoryRaftHardStateStore, OpenRaftHardStateStoreError,
    RaftHardStateStore, RaftHardStateStoreWriteError,
};
pub use raft_log_entry_codec::{
    decode_raft_log_entry, encode_borrowed_raft_log_entry, encode_raft_log_entry,
    BorrowedPersistedRaftLogEntry, DecodeRaftLogEntryError, EncodeRaftLogEntryError,
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
    DecodePendingSnapshotTransferError, FileRaftSnapshotStore, FileRaftSnapshotStoreOpenReport,
    InMemoryRaftSnapshotStore, OpenRaftSnapshotStoreError, OpenedFileRaftSnapshotStore,
    PendingSnapshotTransferRecovery, PendingSnapshotTransferStagingStatus,
    RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError, RaftSnapshotStore,
    RaftSnapshotStoreWriteError, SnapshotFileIdentity, SnapshotFileInfo, SnapshotInventory,
    SnapshotInventoryError, SnapshotPruneError, SnapshotPruneReport, SnapshotRetention,
    SnapshotTemporaryFileInfo, SnapshotTemporaryFileKind,
};
