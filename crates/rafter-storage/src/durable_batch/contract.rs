//! Domain identity, borrowed batch inputs, and acknowledged recovery positions.
use crate::{BorrowedPersistedRaftLogEntry, RaftHardState};
use rafter::LogIndex;
use std::sync::Arc;

/// Identity of one live storage coordinator, distinct across opens.
///
/// Clone this identity into the hard-state and log handles of one backend.
/// Equality uses coordinator identity, never a pathname or bare log index.
#[derive(Clone, Debug)]
pub struct PersistenceDomain(Arc<()>);
impl PersistenceDomain {
    /// Creates a domain for one newly opened coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(()))
    }
}
impl Default for PersistenceDomain {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for PersistenceDomain {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for PersistenceDomain {}

/// Compatible mutations published atomically by a shared WAL coordinator.
///
/// Truncation, when present, precedes the contiguous append. Hard state is the
/// final state of this batch, never a promise to flush later. Compaction and
/// snapshot publication remain separate explicit operations.
#[derive(Debug)]
pub struct RaftPersistenceBatch<'a> {
    /// Optional first log index to remove before appending replacement entries.
    pub truncate_from: Option<LogIndex>,
    /// New contiguous suffix entries, borrowed until publication returns.
    pub entries: &'a [BorrowedPersistedRaftLogEntry<'a>],
    /// Optional final durable hard state; omission retains the previous state.
    pub hard_state: Option<RaftHardState>,
}

/// Exact acknowledged state after one successful atomic persistence operation.
///
/// A backend returns this only after the recoverable record is synchronized.
/// The operation identifies a publication in its domain; the log position
/// alone cannot identify a write after suffix replacement or reopening.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableReceipt {
    /// Coordinator that published this operation.
    pub domain: PersistenceDomain,
    /// Monotonic publication number within this live coordinator.
    pub operation: u64,
    /// Latest acknowledged hard state.
    pub hard_state: RaftHardState,
    /// Next index after the acknowledged retained suffix.
    pub next_index: LogIndex,
    /// Acknowledged compacted prefix, covered by separately published snapshot data.
    pub compacted_through: LogIndex,
}
