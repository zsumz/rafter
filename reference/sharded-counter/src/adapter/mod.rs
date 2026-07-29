//! Real Rafter-backed adoption of the counter contract.
//!
//! This module owns counter lifecycle, sessions, codecs, snapshots, and the
//! deterministic test network. The public `rafter-multiraft` layer sees only
//! neutral group inputs and scheduling classes.

mod cluster;
mod codec;
mod state_machine;

pub use cluster::{
    AdapterError, CounterAdmissionRejected, CounterAdmissionRejection, DriveReport,
    ManagedCounterCluster, NetworkConfig, ProposalReceipt,
};
pub use codec::{MAX_COMMAND_BYTES, MAX_SNAPSHOT_BYTES};
pub use state_machine::{
    CounterApplyRejection, CounterApplyResult, CounterStateMachine, ReplicatedCounterCommand,
    SessionApplyResult,
};
