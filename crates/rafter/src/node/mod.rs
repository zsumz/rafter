//! The deterministic Raft state machine.
//!
//! [`Node`] owns protocol state. Transition logic is organized by Raft concept:
//! construction, election, replication, commitment, membership, reads,
//! leadership transfer, and snapshots. External behavior enters through
//! `Node::step` or `Node::step_batch` and leaves as ordered [`Output`] values.

mod bootstrap;
mod commit;
mod config;
mod construction;
mod dispatch;
mod election;
mod event;
mod lifecycle;
mod log;
mod membership;
mod observe;
mod read_index;
mod replication;
mod state;
mod transfer;

#[cfg(test)]
mod tests;

pub use bootstrap::{BootstrapLogEntry, BootstrapState, BootstrapValidationError};
pub use config::{NodeConfig, NodeConfigError};
pub use event::{
    ClientProposalInput, ConfigurationProposalRejection, Input, LeadershipTransferRejection,
    LocalProposalDropReason, Output, ProposalRejection, ReadIndexCancelReason, ReadIndexRejection,
    Role,
};
pub use log::LocalSnapshotInstallError;
pub use replication::PendingSnapshotTransferResumeError;
use state::{DerivedState, ElectionState, LeaderState, PersistentState, VolatileState};

/// Pure deterministic Raft state machine.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Node {
    config: NodeConfig,

    persistent: PersistentState,
    volatile: VolatileState,

    election: ElectionState,
    leader: LeaderState,

    derived: DerivedState,
}
