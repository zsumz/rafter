//! Persisted Raft state accepted by restart hydration.

use crate::{
    CommittedConfiguration, ConfigurationEntry, LogEntry, LogEntryKind, LogIndex, NodeId,
    RaftSnapshot, Term,
};

/// Durable state used to hydrate a node after restart.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct BootstrapState {
    /// Latest term durably observed by this node.
    pub current_term: Term,
    /// Candidate this node durably voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
    /// Durable commit index recorded with the hard state. Recovery treats the
    /// snapshot boundary as a floor, so a crash after snapshot promotion but
    /// before the final hard-state write still boots at the compacted prefix.
    pub commit_index: LogIndex,
    /// Identity of the latest committed configuration entry when it is known
    /// to the durable runtime. Bootstrap verifies it against any retained log
    /// entry it still covers; compacted entries are represented by snapshot
    /// committed-configuration metadata.
    pub committed_configuration: Option<CommittedConfiguration>,
    /// The persisted snapshot descriptor: metadata plus payload length. The
    /// payload itself stays in the application's snapshot store; the kernel
    /// only needs its length to derive the transfer identity and serve chunk
    /// directives.
    pub snapshot: Option<RaftSnapshot>,
    /// Retained log entries above the snapshot boundary. A matching boundary
    /// entry may be included as a validation sentinel and is not retained.
    pub log: Vec<BootstrapLogEntry>,
}

/// One durable log entry with its persisted index.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BootstrapLogEntry {
    /// One-based durable log index.
    pub index: LogIndex,
    /// Term in which this entry was created.
    pub term: Term,
    /// Logical command carried by the entry.
    pub kind: LogEntryKind,
}

impl BootstrapLogEntry {
    /// Builds a persisted application entry.
    #[must_use]
    pub fn application(index: LogIndex, term: Term, payload: Vec<u8>) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::application(payload),
        }
    }

    /// Builds a persisted configuration entry.
    #[must_use]
    pub fn configuration(index: LogIndex, term: Term, configuration: ConfigurationEntry) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::configuration(configuration),
        }
    }

    /// Builds a persisted no-op entry.
    #[must_use]
    pub const fn noop(index: LogIndex, term: Term) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::noop(),
        }
    }
}

pub(in crate::node) struct BootstrapParts {
    pub(in crate::node) current_term: Term,
    pub(in crate::node) voted_for: Option<NodeId>,
    pub(in crate::node) commit_index: LogIndex,
    pub(in crate::node) committed_configuration: Option<CommittedConfiguration>,
    pub(in crate::node) snapshot: Option<RaftSnapshot>,
    pub(in crate::node) log: Vec<LogEntry>,
}
