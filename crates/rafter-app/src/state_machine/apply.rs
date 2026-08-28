//! The apply-path payload types a committed entry travels in.
//!
//! A batch, its entries, the results the state machine returns for them,
//! and the barrier a query is served under. Values only: the trait that
//! consumes them lives beside this module.

use rafter::{LocalProposalId, LogIndex, Term};

/// A batch of committed commands ready for ordered application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyBatch<C> {
    /// Committed application entries in strictly increasing log order.
    pub entries: Vec<ApplyEntry<C>>,
}

/// A single committed command in an application batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyEntry<C> {
    /// Committed Raft log index.
    pub index: LogIndex,
    /// Raft term that appended the entry.
    pub term: Term,
    /// Decoded application command.
    pub command: C,
    /// Volatile local correlation ID when this node originated the command.
    pub local_proposal_id: Option<LocalProposalId>,
}

/// The application result for one applied command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyResult<R> {
    /// Applied Raft log index.
    pub index: LogIndex,
    /// Raft term of the applied entry.
    pub term: Term,
    /// Application result produced by the command.
    pub result: R,
    /// Volatile local correlation ID copied from the applied entry.
    pub local_proposal_id: Option<LocalProposalId>,
}

/// A barrier proving the local state machine is fresh enough for a read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadBarrier {
    /// Minimum application index required by the read proof.
    pub required_applied_index: LogIndex,
    /// Application index observed when the query is served.
    pub local_applied_index: LogIndex,
}
