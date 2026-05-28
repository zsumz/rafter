use std::fmt;

mod configuration;
#[cfg(test)]
mod configuration_test;
mod membership;
#[cfg(test)]
mod membership_test;
mod payload;
mod snapshot;
#[cfg(test)]
mod snapshot_test;

pub use configuration::{
    CommittedConfiguration, ConfigurationEntry, ConfigurationId, ConfigurationPhase, LogEntryKind,
    PromotionBarrier,
};
pub use membership::{JointMembership, MembershipConfig, MembershipSet, MembershipValidationError};
pub use payload::SharedPayload;
pub(crate) use snapshot::snapshot_transfer_id_from_parts;
pub use snapshot::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    FollowerSnapshotTransferStatus, InMemorySnapshotChunkSource, InMemorySnapshotSourceError,
    LeaderSnapshotTransferStatus, PendingSnapshotTransfer, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotChunkRejectionCounters, SnapshotChunkRequest, SnapshotChunkSend, SnapshotChunkSource,
    SnapshotCommittedConfiguration, SnapshotGroupId, SnapshotIdError, SnapshotMetadataError,
    SnapshotTransferId, SnapshotTransferStatus, StagedSnapshotChunk,
};

/// Stable Raft node identity used in messages, configuration, and logs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// Local-only proposal correlation handle.
///
/// This ID is volatile runtime metadata for the caller that submitted a
/// proposal. It is not a Raft protocol identity: it is not replicated, not
/// durable, not included in log entries, not included in wire messages, not
/// included in snapshots, not restored after restart, and not meaningful to
/// any other node.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalProposalId(pub u64);

impl fmt::Display for LocalProposalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "local-proposal-{}", self.0)
    }
}

/// Local-only read-index correlation handle.
///
/// This ID is volatile runtime metadata used to correlate a local read-index
/// request with the corresponding local read-index output. It is not
/// replicated, durable, or meaningful to other nodes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReadId(pub u64);

impl fmt::Display for ReadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "read-{}", self.0)
    }
}

/// Raft term number.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Term(pub u64);

impl Term {
    /// Returns the next term.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Returns whether this is the zero bootstrap term.
    #[must_use]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One-based Raft log index; zero is the sentinel before the first entry.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogIndex(pub u64);

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LogIndex {
    /// Sentinel index before the first log entry.
    pub const ZERO: Self = Self(0);

    /// Returns the next log index.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Leader replication progress for one follower.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationProgress {
    pub follower_id: NodeId,
    pub match_index: LogIndex,
    pub next_index: LogIndex,
    pub state: ReplicationState,
}

/// The send discipline a leader currently applies to one follower.
///
/// This enum is exhaustive because replication progress has a closed set of
/// protocol states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationState {
    /// One bounded append at a time until the follower confirms its log
    /// position (leadership start, or after a rejection).
    Probing,
    /// Confirmed position: appends fill the in-flight window and the send
    /// index advances optimistically.
    Replicating,
    /// The follower is behind the snapshot boundary; the current snapshot is
    /// streaming to it and log replication is paused.
    Snapshotting {
        /// The next payload byte offset to send.
        next_offset: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_domain_values_have_stable_display_and_debug_output() {
        assert_eq!(NodeId(7).to_string(), "node-7");
        assert_eq!(LocalProposalId(8).to_string(), "local-proposal-8");
        assert_eq!(ReadId(9).to_string(), "read-9");
        assert_eq!(Term(9).to_string(), "9");
        assert_eq!(LogIndex(11).to_string(), "11");
        assert_eq!(format!("{:?}", NodeId(7)), "NodeId(7)");
        assert_eq!(format!("{:?}", LocalProposalId(8)), "LocalProposalId(8)");
        assert_eq!(format!("{:?}", ReadId(9)), "ReadId(9)");
        assert_eq!(format!("{:?}", Term(9)), "Term(9)");
        assert_eq!(format!("{:?}", LogIndex(11)), "LogIndex(11)");
    }

    #[test]
    fn raft_domain_values_order_by_their_protocol_value() {
        assert_eq!(Term::default(), Term(0));
        assert!(Term::default().is_zero());
        assert!(Term(4).next() > Term(4));
        assert_eq!(LogIndex::ZERO, LogIndex(0));
        assert!(LogIndex::ZERO.next() > LogIndex::ZERO);
        assert!(NodeId(2) > NodeId(1));
        assert!(LocalProposalId(2) > LocalProposalId(1));
        assert!(ReadId(2) > ReadId(1));
    }
}
