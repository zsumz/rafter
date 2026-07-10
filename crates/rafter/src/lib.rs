//! Deterministic, sans-IO Raft protocol core.
//!
//! This crate owns Raft protocol state transitions, membership validation,
//! replication/read/snapshot state, and the typed input/output vocabulary used
//! by higher layers.
//! This crate deliberately contains no sockets, files, async runtime, wall
//! clock, or stream-policy logic. Production code and simulations both drive it
//! through explicit inputs and inspect explicit outputs.
//!
//! Production datastore embeddings must also satisfy the repository-level
//! production boundary described in the README, including durable output
//! ordering, applied-index ownership, authenticated transport identity, and
//! removed-peer fencing.
//!
//! # Public Message Buffers
//!
//! [`AppendEntries::entries`] is [`SharedEntries`], an immutable shared slice of
//! [`LogEntry`] values. Consumers should inspect it with [`SharedEntries::iter`],
//! [`SharedEntries::as_slice`], or normal slice coercions. Call
//! [`SharedEntries::to_vec`] only at boundaries that genuinely need owned,
//! mutable entries.
//!
//! This is an in-process allocation-sharing boundary, not a Raft wire-format
//! distinction. A leader may share one bounded log slice across several
//! `AppendEntries` outputs in the same deterministic broadcast round, while each
//! recipient still observes normal ordered Raft log entries.
//!
//! # Integrity Model
//!
//! Snapshot and message checksums in this project are accidental-corruption
//! checks for non-Byzantine deployments. [`SnapshotTransferId`] is a stable
//! routing identity, not a collision-resistant digest, and CRC32 values are
//! not authentication tags. Deployments that need adversarial integrity must
//! use authenticated transport/storage and have the application snapshot
//! format carry and verify its own cryptographic digest before applying
//! recovered state.
//!
//! # Peer Authorization
//!
//! The Raft core validates protocol membership, but it does not authenticate
//! transport peers or maintain tombstones for removed node IDs. As in classic
//! Raft, a higher-term message can advance local term before later request
//! validation rejects the sender. Production embeddings must authenticate the
//! sender identity, ensure it matches the message's embedded node ID, and
//! fence removed or otherwise unauthorized peers at the transport or routing
//! layer.
//!
//! # Snapshot Sender Authorization
//!
//! Snapshot receivers authorize a snapshot sender against the snapshot's Raft
//! boundary membership when the metadata carries committed membership. A
//! leader must therefore serve a snapshot whose boundary membership includes
//! that leader as a voter. Under the built-in policy, a newly added leader is
//! rejected if it tries to send an older compacted snapshot whose boundary
//! predates its addition, even when that leader is a valid member of the
//! current log suffix.
//!
//! Snapshot stores that can be reused across membership changes must respect
//! this protocol constraint: do not hand a newly added leader an older snapshot
//! descriptor unless that descriptor's boundary membership authorizes the
//! leader, or unless the embedding layer replaces the built-in policy with a
//! stronger authenticated authorization rule outside the Raft core.

mod message;
mod node;
mod types;

pub use message::{
    AppendEntries, AppendEntriesResponse, InstallSnapshot, InstallSnapshotChunk,
    InstallSnapshotResponse, LogEntry, Message, PreVote, PreVoteResponse, RequestVote,
    RequestVoteResponse, SharedEntries, TimeoutNow,
};
pub use node::{
    BootstrapLogEntry, BootstrapState, BootstrapValidationError, ClientProposalInput,
    ConfigurationProposalRejection, Input, LeadershipTransferRejection, LocalProposalDropReason,
    Node, NodeConfig, NodeConfigError, Output, PendingSnapshotTransferResumeError,
    ProposalRejection, ReadIndexCancelReason, ReadIndexRejection, Role,
};
pub use types::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationEntry, ConfigurationId, ConfigurationPhase,
    FollowerSnapshotTransferStatus, InMemorySnapshotChunkSource, InMemorySnapshotSourceError,
    JointMembership, LeaderSnapshotTransferStatus, LocalProposalId, LogEntryKind, LogIndex,
    MembershipConfig, MembershipSet, MembershipValidationError, NodeId, PendingSnapshotTransfer,
    PromotionBarrier, RaftSnapshot, RaftSnapshotMetadata, ReadId, ReplicationProgress,
    ReplicationState, SharedPayload, SnapshotChunkRejectionCounters, SnapshotChunkRequest,
    SnapshotChunkSend, SnapshotChunkSource, SnapshotCommittedConfiguration, SnapshotGroupId,
    SnapshotIdError, SnapshotMetadataError, SnapshotTransferId, SnapshotTransferStatus,
    StagedSnapshotChunk, Term,
};
