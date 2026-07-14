use std::hash::{Hash, Hasher};

use rafter::{
    BootstrapLogEntry, CommittedConfiguration, LocalProposalDropReason, LocalProposalId,
    LogEntryKind, LogIndex, MembershipConfig, NodeId, ProposalRejection, RaftSnapshotMetadata,
    ReadIndexCancelReason, ReadIndexRejection, SharedPayload, SnapshotTransferId, Term,
};

use crate::Envelope;

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

/// Immutable witness for one committed logical log execution.
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
    /// Payload carried by the actual `Output::Apply` for an application entry.
    /// Configuration and no-op entries have no application output.
    pub emitted_application_payload: Option<SharedPayload>,
    pub prior_state: ReferenceState,
    pub resulting_state: ReferenceState,
}

/// Structurally append-only owner of execution witnesses.
///
/// Production code can only append. Test-only corruption hooks advance a
/// revision so the incremental verifier can detect rewritten consumed history
/// without rescanning every payload-rich prefix after each transition.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionLedger {
    witnesses: Vec<ExecutionWitness>,
    rewrite_revision: u64,
}

impl Hash for ExecutionLedger {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.witnesses.hash(state);
        self.rewrite_revision.hash(state);
    }
}

impl ExecutionLedger {
    pub(crate) fn push(&mut self, witness: ExecutionWitness) {
        self.witnesses.push(witness);
    }

    pub(crate) fn as_slice(&self) -> &[ExecutionWitness] {
        &self.witnesses
    }

    pub(crate) const fn rewrite_revision(&self) -> u64 {
        self.rewrite_revision
    }

    #[cfg(test)]
    pub(crate) fn from_witnesses(witnesses: Vec<ExecutionWitness>) -> Self {
        let mut ledger = Self::default();
        for witness in witnesses {
            ledger.push(witness);
        }
        ledger
    }

    #[cfg(test)]
    pub(crate) fn rewrite(&mut self, index: usize, witness: ExecutionWitness) {
        self.witnesses[index] = witness;
        self.rewrite_revision = self.rewrite_revision.saturating_add(1);
    }

    #[cfg(test)]
    pub(crate) fn swap(&mut self, first: usize, second: usize) {
        self.witnesses.swap(first, second);
        self.rewrite_revision = self.rewrite_revision.saturating_add(1);
    }
}

/// Local-only proposal correlation emitted by one simulated node transition.
///
/// These events preserve the kernel's explicit outcome boundary without
/// inferring acceptance, completion, or loss from payload equality or later
/// protocol state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocalProposalEvent {
    Appended {
        node_id: NodeId,
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
    },
    Applied {
        node_id: NodeId,
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        payload: SharedPayload,
    },
    Dropped {
        node_id: NodeId,
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        reason: LocalProposalDropReason,
    },
    Rejected {
        node_id: NodeId,
        proposal_id: LocalProposalId,
        reason: ProposalRejection,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RecordedOutputs {
    pub(crate) emitted: Vec<Envelope>,
    pub(crate) local_proposals: Vec<LocalProposalEvent>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProposalRejected {
    pub(crate) node_id: NodeId,
    pub(crate) proposal_id: Option<LocalProposalId>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TransferRejected {
    pub(crate) node_id: NodeId,
    pub(crate) target: NodeId,
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
    pub commit_index_at_emit: LogIndex,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub committed_membership: Option<MembershipConfig>,
    pub payload: Vec<u8>,
    pub applied_records_before_install: usize,
}

/// Exact durable-state image for one simulated node.
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

/// Exact installed-snapshot image used by [`DurableStateDigest`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DurableSnapshotDigest {
    pub transfer_id: SnapshotTransferId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub hard_state_term: Term,
    pub application_payload_len: u64,
    pub application_payload_crc32: u32,
    pub application_payload: Vec<u8>,
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
    /// Simulator-local immutable registration generation, when correlation succeeded.
    pub operation_id: Option<u64>,
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
    /// Simulator-local immutable generation that distinguishes reused `ReadId` values.
    pub operation_id: u64,
    pub request_id: u64,
    pub committed_floor: LogIndex,
}

/// An explicit terminal read-index output preserved in simulator history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadTerminalOutput {
    /// The node refused the request without registering a barrier.
    Rejected {
        node_id: NodeId,
        /// Simulator-local immutable registration generation, when correlation succeeded.
        operation_id: Option<u64>,
        request_id: u64,
        reason: ReadIndexRejection,
    },
    /// The node cleared a previously registered barrier before granting it.
    Canceled {
        node_id: NodeId,
        /// Simulator-local immutable registration generation, when correlation succeeded.
        operation_id: Option<u64>,
        request_id: u64,
        reason: ReadIndexCancelReason,
    },
}

impl ReadTerminalOutput {
    pub(crate) fn matches_operation(self, operation_id: u64) -> bool {
        match self {
            Self::Rejected {
                operation_id: recorded,
                ..
            }
            | Self::Canceled {
                operation_id: recorded,
                ..
            } => recorded == Some(operation_id),
        }
    }

    pub(crate) const fn operation_id(self) -> Option<u64> {
        match self {
            Self::Rejected { operation_id, .. } | Self::Canceled { operation_id, .. } => {
                operation_id
            }
        }
    }
}

impl Hash for ReadTerminalOutput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Rejected {
                node_id,
                operation_id,
                request_id,
                reason,
            } => {
                0_u8.hash(state);
                node_id.hash(state);
                operation_id.hash(state);
                request_id.hash(state);
                hash_read_rejection(*reason, state);
            }
            Self::Canceled {
                node_id,
                operation_id,
                request_id,
                reason,
            } => {
                1_u8.hash(state);
                node_id.hash(state);
                operation_id.hash(state);
                request_id.hash(state);
                hash_read_cancellation(*reason, state);
            }
        }
    }
}

fn hash_read_rejection<H: Hasher>(reason: ReadIndexRejection, state: &mut H) {
    match reason {
        ReadIndexRejection::NotLeader { role, term } => {
            0_u8.hash(state);
            role.hash(state);
            term.hash(state);
        }
        ReadIndexRejection::NoCommitInCurrentTerm => 1_u8.hash(state),
        ReadIndexRejection::LeadershipTransferInProgress { target } => {
            2_u8.hash(state);
            target.hash(state);
        }
        ReadIndexRejection::TooManyPendingReads => 3_u8.hash(state),
    }
}

fn hash_read_cancellation<H: Hasher>(reason: ReadIndexCancelReason, state: &mut H) {
    match reason {
        ReadIndexCancelReason::LeadershipLost => 0_u8.hash(state),
        ReadIndexCancelReason::LeaderStateReset => 1_u8.hash(state),
        ReadIndexCancelReason::LeadershipTransfer { target } => {
            2_u8.hash(state);
            target.hash(state);
        }
    }
}
