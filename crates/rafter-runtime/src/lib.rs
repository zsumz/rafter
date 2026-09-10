//! Durable runtime wrapper for the deterministic Rafter core.
//!
//! This crate persists hard state, log entries, snapshot staging, promoted
//! snapshots, and local compaction before returning Raft outputs to the
//! embedding. It owns persist-before-output sequencing for one durable Raft
//! node over the storage traits. It does not own application state,
//! state-machine apply idempotence, transport delivery, authenticated peer
//! identity, peer fencing, or application snapshot payload validation.
//! Datastore users should read the production boundary in the repository
//! README before treating the runtime as production glue.
//!
//! Prefer the `recover_with_storage_and_snapshot_store*` constructors on
//! restart paths so committed-but-unapplied recovery outputs are explicit and
//! can be applied before serving reads or accepting new writes.
//!
//! # Persist before output, made concrete
//!
//! A lone voter reaches a decision without a peer, so the whole write path fits
//! in one sequence of steps. The storage type parameters default to the
//! in-memory stores, which is what keeps this example short; a deployment hands
//! in `rafter_storage::FileRaftNodeStores`.
//!
//! ```
//! use rafter::{Input, LogIndex, NodeConfig, NodeId, Output, Role};
//! use rafter_runtime::DurableRaftNode;
//! use rafter_storage::{InMemoryRaftHardStateStore, RaftHardStateStore, RaftLogSegment};
//!
//! let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("valid raft config");
//! let mut node = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
//!     .expect("a fresh in-memory node opens");
//!
//! for _ in 0..3 {
//!     node.step(Input::Tick).expect("each tick persists what it changed");
//! }
//! assert_eq!(node.role(), Role::Leader);
//!
//! let outputs = node
//!     .step(Input::ClientProposal {
//!         payload: b"set alpha=one".to_vec(),
//!     })
//!     .expect("the leader appends, commits, and persists its own proposal");
//! assert!(outputs.iter().any(|output| matches!(
//!     output,
//!     Output::Apply {
//!         index: LogIndex(2),
//!         ..
//!     }
//! )));
//!
//! // That `Apply` was released only after the entry it names was durable.
//! // Decomposing the runtime shows the medium already holding it: this is the
//! // whole difference between this crate and stepping the kernel yourself.
//! let storage = node.into_storage();
//! let persisted = storage.log_segment.replay_entries();
//! assert_eq!(persisted.len(), 2, "the leader's term no-op, then the command");
//! assert_eq!(
//!     persisted[1].kind.application_payload(),
//!     Some(&b"set alpha=one"[..]),
//! );
//! assert_eq!(
//!     storage.hard_state_store.current().commit_index,
//!     LogIndex(2),
//! );
//! ```

#[cfg(test)]
use rafter::{
    BootstrapValidationError, Input as RaftInput, LogEntry, LogIndex, NodeId as RaftNodeId,
    Output as RaftOutput, RaftSnapshot, RaftSnapshotMetadata, Role as RaftRole,
    SnapshotChunkRequest, SnapshotChunkSource, Term,
};
#[cfg(test)]
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState, RaftHardStateStore,
    RaftHardStateStoreWriteError, RaftLogSegment, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError, RaftSnapshotStore,
    RaftSnapshotStoreWriteError,
};

mod compaction;
mod construction;
mod error;
mod hard_state;
mod inspect;
mod log_repair;
mod node;
mod peer_batch;
mod runtime_api;
mod step;

pub use error::{RaftRuntimeError, RaftRuntimeFatalError};
pub use node::{DurableRaftNode, DurableRaftNodeStorage, RecoveredDurableRaftNode};
pub use peer_batch::PeerBatchGate;
pub use rafter_runtime_api::PersistedRaftRuntime;

#[cfg(test)]
mod tests;
