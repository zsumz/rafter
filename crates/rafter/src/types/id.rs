//! Strongly typed protocol and local correlation identities.

use std::fmt;

/// Stable Raft node identity used in messages, configuration, and logs.
///
/// # An ID is single-use within its group
///
/// A `NodeId` names one replica for as long as that replica is a member, and a
/// **committed removal retires it permanently**. A retired ID is never validly
/// added back to the same group: a replacement replica — even one serving the
/// same data, on the same host, from the same directory — joins under a fresh
/// ID. Allocate IDs monotonically per group and never hand one out twice.
///
/// The reason is that a removal is not only a membership edit. Layers above the
/// kernel bind durable authorization to the ID: a managed driver fences the
/// removed replica's transport principal, and a fence is permanent for that
/// principal by design — the transport boundary offers no inverse of it. An ID
/// added back after its fence has landed names a replica that can never speak
/// again, so the change appears to commit and the replica silently never
/// participates.
///
/// **Restarting a replica is not removing it.** A replica that crashes, is
/// killed, or is restarted keeps its ID and its identity: no removal committed,
/// so nothing was retired. Reopening durable state under the same ID is the
/// ordinary restart path and stays that way.
///
/// This is a stated precondition rather than a checked one, and the kernel says
/// so rather than implying it. A node cannot recognize an ID it has removed
/// after log compaction has erased the configuration history that named it, and
/// keeping a permanent tombstone set for every ID a group ever removed would put
/// unbounded state under a retention policy the kernel cannot see. Enforcement
/// across process lifetimes is the deployment's own allocation discipline;
/// within one, the managed service driver refuses a re-added ID and reports it.
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
    /// The highest representable term, which has no successor.
    pub const MAX: Self = Self(u64::MAX);

    /// Returns the next term, saturating at [`Term::MAX`].
    ///
    /// Saturating rather than wrapping is a safety decision, not a taste one.
    /// `Term(u64::MAX) + 1` wraps to zero, and zero is the bootstrap sentinel
    /// every term comparison in the protocol is ordered against: a node that
    /// wrapped would accept its own history again as newer. Overflow also
    /// panics under `debug_assertions` and wraps without one, so the unguarded
    /// form made a safety property depend on the build profile.
    ///
    /// Saturating keeps the value legal but makes the successor equal to the
    /// term it came from, which no caller advancing a term wants silently. Use
    /// [`Term::checked_next`] wherever the difference decides something; the
    /// kernel's election paths do.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the next term, or `None` at [`Term::MAX`].
    #[must_use]
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
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
