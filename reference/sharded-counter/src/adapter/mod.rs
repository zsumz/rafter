//! Real Rafter-backed adoption of the counter contract.
//!
//! This module owns counter lifecycle, sessions, codecs, snapshots, and the
//! deterministic test network. The public `rafter-multiraft` layer sees only
//! neutral group inputs and scheduling classes.

mod audit;
mod cluster;
mod codec;
mod state_machine;

pub use audit::{
    audit_acceptance, AcceptanceAuditReport, AcceptanceExpectation, AcceptanceViolation,
    ExpectedWork,
};
pub use cluster::{
    AdapterError, CheckpointError, CheckpointOutstanding, CheckpointSession,
    CounterAdmissionRejected, CounterAdmissionRejection, CounterGroupCheckpoint,
    CounterSubmitOutcome, DriveReport, DriveTurn, DrivenDisposition, DrivenItem,
    ManagedCounterCluster, NetworkConfig, PeerTrafficRefusal, ProposalFailure, ProposalReceipt,
    RestoredCounterGroup, RoutedPeerEnvelope, SessionSubmitOutcome,
};
pub use codec::{MAX_COMMAND_BYTES, MAX_SNAPSHOT_BYTES};
pub use state_machine::{
    CounterApplyRejection, CounterApplyResult, CounterCompletedView, CounterSessionView,
    CounterStateMachine, CounterStateMachineError, CounterStateView, ReplicatedCounterCommand,
    SessionApplyResult,
};
