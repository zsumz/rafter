//! Public snapshot vocabulary facade.
//!
//! Snapshot identity, metadata, payload sourcing, transfer state, status, and
//! validation errors remain separate internally while callers use the flat
//! `rafter::{...}` API.

mod error;
mod identity;
mod metadata;
mod source;
mod status;
#[cfg(test)]
mod tests;
mod transfer;
mod transfer_id;

pub use error::SnapshotMetadataError;
pub use identity::{ApplicationSnapshotKind, SnapshotGroupId, SnapshotIdError};
pub use metadata::{
    ApplicationSnapshotMetadata, ApplicationSnapshotVersion, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotCommittedConfiguration,
};
pub use source::{
    InMemorySnapshotChunkSource, InMemorySnapshotSourceError, SnapshotChunkRequest,
    SnapshotChunkSource,
};
pub use status::{
    FollowerSnapshotTransferStatus, LeaderSnapshotTransferStatus, SnapshotChunkRejectionCounters,
    SnapshotTransferStatus,
};
pub use transfer::{
    PendingSnapshotTransfer, SnapshotChunkSend, SnapshotTransferId, StagedSnapshotChunk,
};

pub(crate) use metadata::application_payload_crc32;
pub(crate) use transfer_id::snapshot_transfer_id_from_parts;
