//! Core Raft domain values and public type facade.
//!
//! Identity, membership, configuration, payload, replication, and snapshot
//! vocabulary is re-exported here so callers keep the flat `rafter::{...}` API.

mod configuration;
mod id;
mod membership;
mod payload;
mod replication;
mod snapshot;

#[cfg(test)]
mod configuration_test;
#[cfg(test)]
mod id_test;
#[cfg(test)]
mod membership_test;
#[cfg(test)]
mod payload_test;

pub use configuration::{
    CommittedConfiguration, ConfigurationEntry, ConfigurationId, ConfigurationPhase, LogEntryKind,
    PromotionBarrier,
};
pub use id::{LocalProposalId, LogIndex, NodeId, ReadId, Term};
pub use membership::{JointMembership, MembershipConfig, MembershipSet, MembershipValidationError};
pub use payload::SharedPayload;
pub use replication::{ReplicationProgress, ReplicationState};
pub(crate) use snapshot::snapshot_transfer_id_from_parts;
pub use snapshot::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    FollowerSnapshotTransferStatus, InMemorySnapshotChunkSource, InMemorySnapshotSourceError,
    LeaderSnapshotTransferStatus, PendingSnapshotTransfer, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotChunkRejectionCounters, SnapshotChunkRequest, SnapshotChunkSend, SnapshotChunkSource,
    SnapshotCommittedConfiguration, SnapshotGroupId, SnapshotIdError, SnapshotMetadataError,
    SnapshotTransferId, SnapshotTransferStatus, StagedSnapshotChunk,
};
