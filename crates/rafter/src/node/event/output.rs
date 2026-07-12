//! Ordered side effects emitted by deterministic node transitions.
//!
//! Output order is load-bearing. Embeddings persist the resulting durable state
//! and staged snapshot data before releasing dependent sends, applies, or read
//! grants.

use crate::{
    LocalProposalId, LogIndex, Message, NodeId, RaftSnapshot, ReadId, SharedPayload,
    SnapshotChunkSend, StagedSnapshotChunk, Term,
};

use super::rejection::{
    LeadershipTransferRejection, LocalProposalDropReason, ProposalRejection, ReadIndexCancelReason,
    ReadIndexRejection,
};

/// Ordered side effects emitted by one [`Node`](crate::Node) step.
///
/// This is the raw kernel API. The order of a returned `Vec<Output>` is
/// load-bearing and must be preserved by direct embedders. Before releasing
/// externally visible effects such as [`Output::Send`],
/// [`Output::ReadIndexGranted`], [`Output::Apply`], or
/// [`Output::ApplySnapshot`], crash-safe embedders must durably persist the
/// corresponding node state and any staged snapshot data required by earlier
/// outputs in the same step. In particular, [`Output::StageSnapshotChunk`] can
/// be paired with an acknowledgement message from the same step; stage the
/// chunk durably before sending that acknowledgement.
///
/// Most applications should use `rafter-runtime` or `rafter-app`, which encode
/// the persist-before-output and app-apply ordering for common embeddings.
///
/// This enum is exhaustive because node steps emit this closed set of side
/// effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Output {
    /// A tracked local proposal was appended by this node while it was leader.
    ///
    /// This is local-only correlation metadata, not client-facing write
    /// success. The entry may still fail to commit or apply. A managed write
    /// API must wait for the later committed application output before
    /// reporting success.
    LocalProposalAppended {
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
    },
    /// Volatile local tracking for a proposal was cleared before the proposal
    /// applied on this node.
    ///
    /// This is local-only correlation metadata. It is not replicated,
    /// persisted, sent on the wire, stored in snapshots, or part of Raft's
    /// protocol state. The proposal may still commit elsewhere; upper layers
    /// should treat this as an unknown-outcome boundary for local waiters.
    LocalProposalDropped {
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        reason: LocalProposalDropReason,
    },
    /// The committed entry at `index` is ready for the state machine.
    ///
    /// `local_proposal_id` is present only when this process still has
    /// volatile local tracking for a tracked proposal at the same index and
    /// term. The payload shares the log's allocation; holding it is cheap.
    Apply {
        index: LogIndex,
        term: Term,
        payload: SharedPayload,
        local_proposal_id: Option<LocalProposalId>,
    },
    /// A snapshot at `snapshot.metadata.last_included_index` replaces the
    /// state machine. The kernel holds no payload bytes: the content is the
    /// staged transfer identified by `snapshot.transfer_id()`, completed by
    /// the [`Output::StageSnapshotChunk`] emitted in the same step (or, for
    /// an application-installed snapshot, already in the application's
    /// store). Promote the staged content before acting on this output.
    ApplySnapshot { snapshot: RaftSnapshot },
    /// Streams one snapshot chunk toward `to`. The transport resolves the
    /// directive against its [`SnapshotChunkSource`](crate::SnapshotChunkSource)
    /// via [`SnapshotChunkSend::resolve`] and sends the resulting
    /// [`InstallSnapshotChunk`](crate::InstallSnapshotChunk) message. An
    /// unresolvable directive is dropped like a lost message.
    SendSnapshotChunk {
        to: NodeId,
        chunk: SnapshotChunkSend,
    },
    /// A validated inbound snapshot chunk for the receiver's snapshot store.
    /// Stage it durably before releasing the acknowledgement emitted in the
    /// same step — the persist-before-output contract; a crash between the
    /// two must never leave the leader ahead of the staged prefix.
    StageSnapshotChunk { chunk: StagedSnapshotChunk },
    /// A client proposal was rejected without being appended.
    RejectProposal {
        proposal_id: Option<LocalProposalId>,
        reason: ProposalRejection,
    },
    /// A leadership-transfer request was rejected.
    LeadershipTransferRejected {
        target: NodeId,
        reason: LeadershipTransferRejection,
    },
    /// The read barrier `read_id` is confirmed at `read_index`: a quorum
    /// acknowledged this node's leadership after the barrier was registered.
    ReadIndexGranted {
        read_id: ReadId,
        read_index: LogIndex,
    },
    /// A read-index request was rejected without being registered.
    ReadIndexRejected {
        read_id: ReadId,
        reason: ReadIndexRejection,
    },
    /// A previously pending local read-index request was cleared before it
    /// could be granted.
    ///
    /// This is local-only correlation metadata for upper-layer waiters. It is
    /// not replicated, persisted, sent on the wire, or part of Raft protocol
    /// state. Callers may retry the read by issuing a new barrier to the
    /// current leader.
    ReadIndexCanceled {
        read_id: ReadId,
        reason: ReadIndexCancelReason,
    },
    /// Sends one Raft protocol message to `to`.
    Send { to: NodeId, message: Message },
}
