//! The per-read waiter state a group holds between steps.
//!
//! A barrier outlives the call that started it, so its freshness floor, the
//! read index a quorum round certified, and any query waiting on it are
//! held here until a later step observes the cause that ends the barrier.

use super::{LogIndex, ReadProof};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GrantedReadIndex {
    /// What the quorum round certified.
    pub(super) read_index: LogIndex,
    /// The highest committed application entry at or below `read_index`, and
    /// therefore the applied index a state machine can actually reach.
    pub(super) application_floor: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PendingRead {
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) granted: Option<GrantedReadIndex>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingQueryRead {
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) context: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CompletedQueryRead<G> {
    pub(super) proof: ReadProof<G>,
    pub(super) min_applied_index: Option<LogIndex>,
    pub(super) context: Vec<u8>,
}
