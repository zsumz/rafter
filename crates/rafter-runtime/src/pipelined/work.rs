//! Owned I/O work and unforgeable completions; neither exposes an unstable node.
use crate::{DurableRaftNode, RaftRuntimeError};
use rafter::{LogIndex, Output, SnapshotChunkSource};
use rafter_storage::{
    durable_batch::PersistenceDomain, RaftHardState, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore,
};

/// Identity of one submission, independent of reusable log indexes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistenceOperation {
    pub(super) generation: PersistenceDomain,
    pub(super) sequence: u64,
}
impl PersistenceOperation {
    /// Monotonic submission sequence within this driver's unique generation.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns whether two operations belong to the same live driver generation.
    #[must_use]
    pub fn same_generation(&self, other: &Self) -> bool {
        self.generation == other.generation
    }
}

/// Either fully fenced outputs or eligible sends accompanied by owned I/O work.
#[derive(Debug)]
#[non_exhaustive]
pub enum PreparedProposals<H, L, S> {
    /// The ordinary synchronous path completed all required persistence.
    Durable(Vec<Output>),
    /// Only these replication sends are independent of the outstanding local write.
    Pending {
        /// May be sent immediately; never includes follower success or application output.
        replication: Vec<Output>,
        /// Submit to one ordered I/O executor, then return its completion to the owner.
        work: Box<PersistenceWork<H, L, S>>,
    },
}

/// A node temporarily owned by one outstanding durability operation.
///
/// Moving this work to an I/O worker enables overlap. It cannot process inputs
/// or expose its unstable kernel. Dropping it abandons the proposal without any
/// success output; recovery remains the only way to reopen its storage.
#[derive(Debug)]
pub struct PersistenceWork<H, L, S> {
    pub(super) operation: PersistenceOperation,
    pub(super) node: DurableRaftNode<H, L, S>,
    pub(super) before: RaftHardState,
    pub(super) commit_floor: LogIndex,
    pub(super) deferred: Vec<Output>,
}
impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    PersistenceWork<H, L, S>
{
    /// Identifies the exact generation and operation being submitted.
    #[must_use]
    pub fn operation(&self) -> &PersistenceOperation {
        &self.operation
    }
    /// Performs the same storage fence used by the synchronous runtime.
    ///
    /// The completion retains failures and the poisoned node. Pass it back to
    /// the originating driver; it exposes no dependent outputs on its own.
    #[must_use]
    pub fn persist(mut self) -> PersistenceCompletion<H, L, S> {
        let result = self
            .node
            .persist_stepped(self.before, self.commit_floor, self.deferred);
        PersistenceCompletion {
            operation: self.operation,
            node: self.node,
            result,
        }
    }
}

/// A completed write that only its originating driver can accept.
#[derive(Debug)]
pub struct PersistenceCompletion<H, L, S> {
    pub(super) operation: PersistenceOperation,
    pub(super) node: DurableRaftNode<H, L, S>,
    pub(super) result: Result<Vec<Output>, RaftRuntimeError>,
}
impl<H, L, S> PersistenceCompletion<H, L, S> {
    /// Identifies the exact submission; log indexes alone never authorize completion.
    #[must_use]
    pub fn operation(&self) -> &PersistenceOperation {
        &self.operation
    }
}
