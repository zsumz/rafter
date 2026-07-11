//! Strongly typed protocol and local correlation identities.

use std::fmt;

/// Stable Raft node identity used in messages, configuration, and logs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "node-{}", self.0)
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "local-proposal-{}", self.0)
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "read-{}", self.0)
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// One-based Raft log index; zero is the sentinel before the first entry.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogIndex(pub u64);

impl fmt::Display for LogIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
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
