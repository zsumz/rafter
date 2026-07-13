use rafter::{
    BootstrapLogEntry, CommittedConfiguration, LogEntryKind, LogIndex, MembershipConfig, NodeId,
    RaftSnapshotMetadata, SharedPayload, SnapshotTransferId, Term,
};

/// One application payload applied by a simulated node.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Applied {
    pub node_id: NodeId,
    pub application_epoch: u64,
    pub commit_index_at_emit: LogIndex,
    pub index: LogIndex,
    pub payload: SharedPayload,
}

/// Exact identity of one logical entry incorporated into application state.
/// No-op entries advance the execution cursor but do not produce an
/// [`ExecutionWitness`] because they leave the reference application state
/// unchanged.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExecutedLogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub kind: LogEntryKind,
}

/// Canonical state of the simulator's deterministic reference application.
///
/// Application commands are modeled as assignments to one byte-string
/// register. Configuration entries update the committed membership and its
/// exact configuration identity. Snapshot payload bytes are that same
/// register value, while snapshot metadata carries the same membership state,
/// so equal application states compare equally whether reached through a full
/// retained prefix or snapshot installation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReferenceState {
    pub application_value: SharedPayload,
    pub committed_membership: MembershipConfig,
    pub committed_configuration: Option<CommittedConfiguration>,
}

/// Immutable witness for one state-machine-visible committed log application.
///
/// Application commands and configuration entries share this record so the
/// AP-02 oracle can detect cross-kind disagreement at the same logical index.
/// The prior and resulting reference states are captured when the actual node
/// applied cursor crosses this entry, then preserved across process restarts,
/// snapshots, and application epochs.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExecutionWitness {
    pub node_id: NodeId,
    pub application_epoch: u64,
    pub commit_index_at_emit: LogIndex,
    pub entry: ExecutedLogEntry,
    pub prior_state: ReferenceState,
    pub resulting_state: ReferenceState,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ExecutionCursor {
    pub(crate) application_epoch: u64,
    pub(crate) applied_through: LogIndex,
    pub(crate) state: ReferenceState,
}

/// A snapshot installation observed on a node, recorded alongside the
/// position it occupies in the applied stream so invariants can reason
/// about ordering between installs and entry applies.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotInstalled {
    pub node_id: NodeId,
    pub application_epoch: u64,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub committed_membership: Option<MembershipConfig>,
    pub payload: Vec<u8>,
    pub applied_records_before_install: usize,
}

/// Compact durable-state fingerprint for one simulated node.
///
/// This captures the pieces that an ordinary process restart must reconstruct
/// exactly: Raft hard state, committed local state, retained log suffix,
/// installed snapshot descriptor, and the simulator's durable application
/// floor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DurableStateDigest {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
    pub committed_configuration: Option<CommittedConfiguration>,
    pub snapshot: Option<DurableSnapshotDigest>,
    pub log: Vec<BootstrapLogEntry>,
    pub application_epoch: u64,
    pub applied_through: LogIndex,
}

/// Compact installed-snapshot identity used by [`DurableStateDigest`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DurableSnapshotDigest {
    pub transfer_id: SnapshotTransferId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub hard_state_term: Term,
    pub application_payload_len: u64,
    pub application_payload_crc32: u32,
    pub committed_configuration: Option<CommittedConfiguration>,
}

/// A node's in-progress staging area for one inbound snapshot transfer: the
/// simulated snapshot store accumulating [`rafter::Output::StageSnapshotChunk`]
/// bytes until the matching [`rafter::Output::ApplySnapshot`] promotes them.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StagedSnapshotTransfer {
    pub(crate) leader_id: NodeId,
    pub(crate) transfer_id: SnapshotTransferId,
    pub(crate) metadata: RaftSnapshotMetadata,
    pub(crate) total_payload_len: u64,
    pub(crate) application_payload_crc32: u32,
    pub(crate) bytes: Vec<u8>,
}

/// A read barrier granted by a node, recorded for scenario assertions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReadGranted {
    pub node_id: NodeId,
    pub application_epoch: u64,
    pub request_id: u64,
    pub read_index: LogIndex,
    pub local_applied_index: LogIndex,
}

/// A read-barrier registration, recorded with the highest commit index any
/// node had reached at registration time: the committed-floor freshness bar the
/// eventual grant must not undercut.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReadRegistered {
    pub node_id: NodeId,
    pub request_id: u64,
    pub committed_floor: LogIndex,
}
