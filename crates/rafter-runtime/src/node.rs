//! The durable runtime's shape, and the two ways one incarnation ends.
//!
//! This module owns what a durable node is made of — kernel, three stores,
//! suffix-repair bookkeeping, poison — and how those parts are handed back: a
//! recovery split from its outputs, and durable storage returned to a
//! successor. Persist-before-output sequencing belongs to the stepping
//! modules, not here.

use rafter::{Node as RaftNode, Output as RaftOutput};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

use crate::{log_repair, RaftRuntimeFatalError};

/// Durable single-node runtime that persists Raft state before releasing
/// kernel outputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRaftNode<
    H = InMemoryRaftHardStateStore,
    L = InMemoryRaftLogSegment,
    S = InMemoryRaftSnapshotStore,
> {
    pub(crate) node: RaftNode,
    pub(crate) hard_state_store: H,
    pub(crate) log_segment: L,
    pub(crate) snapshot_store: S,
    /// Tail of the persisted log as of the last successful persist; lets
    /// suffix repair skip its divergence scan on the common no-conflict
    /// step. `None` forces one full scan, after which it is exact again.
    pub(crate) persisted_tail: Option<log_repair::PersistedTail>,
    pub(crate) fatal_error: Option<RaftRuntimeFatalError>,
}

/// Recovered durable runtime plus committed application outputs discovered
/// during recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "recovery outputs must be applied or intentionally discarded before using the node"]
pub struct RecoveredDurableRaftNode<
    H = InMemoryRaftHardStateStore,
    L = InMemoryRaftLogSegment,
    S = InMemoryRaftSnapshotStore,
> {
    pub(crate) node: DurableRaftNode<H, L, S>,
    pub(crate) recovery_outputs: Vec<RaftOutput>,
}

/// The durable stores of a decomposed [`DurableRaftNode`].
///
/// These three are the node's whole durable truth: a new runtime recovered over
/// them reconstructs the kernel exactly, which is why decomposing even a
/// poisoned runtime is a sanctioned recovery.
#[derive(Debug)]
pub struct DurableRaftNodeStorage<H, L, S> {
    /// Persisted term and vote — the promises this node has already made, and
    /// the reason a restarted node cannot vote twice in one term.
    pub hard_state_store: H,
    /// The persisted log suffix above the snapshot boundary.
    pub log_segment: L,
    /// Promoted snapshots and any staging area for a transfer in flight.
    pub snapshot_store: S,
}

impl<H, L, S> RecoveredDurableRaftNode<H, L, S> {
    /// Splits the recovered runtime from the committed application outputs
    /// discovered during construction.
    ///
    /// Apply `recovery_outputs` before serving reads or accepting new writes
    /// from the recovered node unless the application has independently
    /// persisted those outputs.
    #[must_use]
    pub fn into_parts(self) -> (DurableRaftNode<H, L, S>, Vec<RaftOutput>) {
        (self.node, self.recovery_outputs)
    }
}

impl<H, L, S> DurableRaftNode<H, L, S> {
    /// Drains application outputs for committed log entries above the
    /// applied floor supplied at construction.
    ///
    /// Prefer
    /// [`DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through`]
    /// for restart paths; it returns these outputs alongside the recovered
    /// node so callers cannot miss the recovery step accidentally. This
    /// method remains available for constructors that intentionally use the
    /// older two-step flow.
    #[must_use]
    pub fn drain_committed_outputs(&mut self) -> Vec<RaftOutput> {
        self.node.drain_committed_outputs()
    }

    /// Consumes the runtime and returns its durable stores.
    ///
    /// This is how an in-process restart reclaims storage from the incarnation
    /// it replaces. The kernel's volatile state is discarded; the returned
    /// stores are the durable truth a new [`DurableRaftNode`] recovers from.
    /// Decomposing a poisoned runtime is the sanctioned recovery — poison means
    /// in-memory state ran ahead of the medium, and the medium is exactly what
    /// comes back.
    ///
    /// The stores are returned as the runtime held them. A store that fenced
    /// itself after a failed mutation is still fenced; reopening the medium is
    /// that store's own operation, not a side effect of this call. Nothing is
    /// flushed, because every durable obligation the runtime accepted was
    /// already satisfied before it released the outputs that depended on it.
    #[must_use]
    pub fn into_storage(self) -> DurableRaftNodeStorage<H, L, S> {
        DurableRaftNodeStorage {
            hard_state_store: self.hard_state_store,
            log_segment: self.log_segment,
            snapshot_store: self.snapshot_store,
        }
    }
}
