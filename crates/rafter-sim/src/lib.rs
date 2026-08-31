//! Deterministic simulation harness for the Rafter consensus stack.
//!
//! This crate owns in-memory cluster simulation, scheduled message delivery,
//! restart/fault modeling, and model-check/soak support for the deterministic
//! kernel and integration layers. It does not provide production storage,
//! networking, runtime APIs, or application embedding contracts; those remain
//! in `rafter-storage`, `rafter-runtime-api`, `rafter-runtime`, `rafter-app`,
//! and `rafter-service`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rafter::{BootstrapState, InMemorySnapshotChunkSource, LogIndex, Node, NodeConfig, NodeId};
#[cfg(test)]
use rafter::{ConfigurationEntry, MembershipSet};

/// Disk fault models used by the deterministic protocol simulator.
pub mod disk_fault;
mod inputs;
mod inspection;
mod lifecycle;
/// Bounded model-checking, replay, soak, and TLA projection helpers.
pub mod model_check;
mod network;
mod projection;
mod records;
mod restart;
mod snapshot;
mod time;

pub use network::Envelope;
use network::QueuedEnvelope;
pub use records::{
    Applied, DurableSnapshotDigest, DurableStateDigest, ExecutedLogEntry, ExecutionWitness,
    ReadGranted, ReadRegistered, ReadTerminalOutput, ReferenceState, SnapshotInstalled,
};
use records::{
    ExecutionCursor, ExecutionLedger, ProposalRejected, StagedSnapshotTransfer, TransferRejected,
};
use time::SimRng;
pub use time::{SimClock, SimSeed, SimTick};

/// Deterministic in-memory Raft cluster used by tests, soaks, and explorers.
#[derive(Clone, Debug, Hash)]
pub struct Cluster {
    clock: SimClock,
    configs: BTreeMap<NodeId, NodeConfig>,
    nodes: BTreeMap<NodeId, Node>,
    network: VecDeque<QueuedEnvelope>,
    rng: SimRng,
    applied: Vec<Applied>,
    /// Append-only exact application/configuration execution history used by
    /// the AP-02 reference-contract oracle.
    execution_history: ExecutionLedger,
    /// Per-node live cursor used only to derive the next immutable execution
    /// witness. Restart and snapshot transitions replace the cursor without
    /// deleting prior history.
    execution_cursors: BTreeMap<NodeId, ExecutionCursor>,
    /// Canonical empty application/configuration state for each static node
    /// configuration. Application-state loss without a snapshot resets here.
    initial_reference_states: BTreeMap<NodeId, ReferenceState>,
    /// Per-node application incarnation. Explicit application-state-loss
    /// restarts advance this while ordinary process restarts preserve it.
    application_epochs: BTreeMap<NodeId, u64>,
    /// Immutable applied floor at the beginning of every application epoch.
    /// Application-state loss retains a snapshot boundary when one survives.
    application_epoch_start_floors: BTreeMap<(NodeId, u64), LogIndex>,
    /// Per-node durable application floor. Plain restarts preserve this
    /// state-machine durability and replay only committed entries above it.
    durable_applied: BTreeMap<NodeId, LogIndex>,
    snapshot_installs: Vec<SnapshotInstalled>,
    /// Per-node durable snapshot payload stores. The kernel holds only
    /// descriptors; leader chunk directives resolve against the sending
    /// node's store, and installed snapshots are registered in the
    /// receiving node's store so it can serve them if it later leads.
    snapshot_sources: BTreeMap<NodeId, InMemorySnapshotChunkSource>,
    /// Per-node staging areas for in-flight inbound snapshot transfers.
    /// Volatile like the kernel's transfer bookkeeping: dropped on plain
    /// restarts unless the harness explicitly models a durable resume.
    snapshot_staging: BTreeMap<NodeId, StagedSnapshotTransfer>,
    read_grants: Vec<ReadGranted>,
    read_registrations: Vec<ReadRegistered>,
    /// Append-only explicit rejection and cancellation outputs for read-index
    /// requests. These are verifier evidence, not live protocol state.
    read_terminal_outputs: Vec<ReadTerminalOutput>,
    /// Registration generations abandoned when a restart discards volatile
    /// read state. Later reuse of the caller-visible ID must skip them.
    retired_read_operations: BTreeSet<u64>,
    read_output_correlation_errors: BTreeSet<String>,
    proposal_rejections: Vec<ProposalRejected>,
    transfer_rejections: Vec<TransferRejected>,
    /// Directional blocked pairs: a sustained partition drops traffic at
    /// enqueue until healed, so it holds across elections.
    blocked_pairs: BTreeSet<(NodeId, NodeId)>,
    /// The highest match index each follower has CONFIRMED to a leader — the
    /// acknowledgement envelope was actually delivered, not merely sent. A
    /// legal lossy restart also preserves the node's local committed prefix;
    /// everything above those floors is loss the protocol must tolerate.
    delivered_ack_floor: BTreeMap<NodeId, LogIndex>,
    /// Bootstrap states captured by [`Cluster::mark_synced`] for the
    /// design-note lossy restart shape.
    synced_marks: BTreeMap<NodeId, BootstrapState>,
}

#[cfg(test)]
mod tests;
