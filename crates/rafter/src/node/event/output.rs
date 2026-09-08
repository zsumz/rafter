//! Ordered side effects emitted by deterministic node transitions.
//!
//! Output order is load-bearing. Embeddings persist the resulting durable state
//! and staged snapshot data before releasing dependent sends, applies, or read
//! grants.

use crate::{
    ConfigurationEntry, LocalProposalId, LogIndex, MembershipConfig, Message, NodeId, RaftSnapshot,
    ReadId, SharedPayload, SnapshotChunkSend, StagedSnapshotChunk, Term,
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
        /// Local-only proposal identity.
        proposal_id: LocalProposalId,
        /// Assigned log index.
        index: LogIndex,
        /// Term in which the entry was appended.
        term: Term,
    },
    /// Volatile local tracking for a proposal was cleared before the proposal
    /// was dispatched by this core.
    ///
    /// This is local-only correlation metadata. It is not replicated,
    /// persisted, sent on the wire, stored in snapshots, or part of Raft's
    /// protocol state. The proposal may still commit elsewhere; upper layers
    /// should treat this as an unknown-outcome boundary for local waiters.
    LocalProposalDropped {
        /// Local-only proposal identity.
        proposal_id: LocalProposalId,
        /// Log index formerly correlated with the proposal.
        index: LogIndex,
        /// Term formerly correlated with the proposal.
        term: Term,
        /// Boundary at which local tracking was lost.
        reason: LocalProposalDropReason,
    },
    /// The committed entry at `index` is ready for the state machine.
    /// Returning this value does not execute the command or persist it.
    ///
    /// `local_proposal_id` is present only when this process still has
    /// volatile local tracking for a tracked proposal at the same index and
    /// term. The payload shares the log's allocation; holding it is cheap.
    Apply {
        /// Committed log index.
        index: LogIndex,
        /// Term stored with the committed entry.
        term: Term,
        /// Opaque application command.
        payload: SharedPayload,
        /// Local-only proposal correlation, when still tracked.
        local_proposal_id: Option<LocalProposalId>,
    },
    /// One committed configuration transition, emitted in log order.
    ///
    /// `previous` is the membership immediately before this entry, not the
    /// consumer's current membership. Replay must not compare historical
    /// entries against a later configuration. Every crossed configuration is
    /// emitted, including intermediate transitions within a single step.
    ///
    /// Snapshot installation emits no historical transitions: it retains only
    /// the boundary membership. See [`Output::ApplySnapshot`] and the
    /// [replay argument and executable cases](https://github.com/zsumz/rafter/blob/main/crates/rafter/CONFIGURATION_REPLAY.md).
    ConfigurationCommitted {
        /// Log index of the committed configuration entry.
        index: LogIndex,
        /// Term of the committed configuration entry.
        term: Term,
        /// The membership in effect immediately before `configuration`.
        ///
        /// This also covers bootstrap and snapshot predecessors, which need
        /// not correspond to a retained configuration entry.
        previous: MembershipConfig,
        /// Configuration entry that crossed the commit index.
        configuration: ConfigurationEntry,
    },
    /// A snapshot at `snapshot.metadata.last_included_index` replaces the
    /// state machine. The kernel holds no payload bytes: the content is the
    /// staged transfer identified by `snapshot.transfer_id()`, completed by
    /// the [`Output::StageSnapshotChunk`] emitted in the same step (or, for
    /// an application-installed snapshot, already in the application's
    /// store). Promote the staged content before acting on this output.
    ApplySnapshot {
        /// Snapshot descriptor whose staged application payload must be installed.
        snapshot: RaftSnapshot,
    },
    /// Streams one snapshot chunk toward `to`. The transport resolves the
    /// directive against its [`SnapshotChunkSource`](crate::SnapshotChunkSource)
    /// via [`SnapshotChunkSend::resolve`] and sends the resulting
    /// [`InstallSnapshotChunk`](crate::InstallSnapshotChunk) message. An
    /// unresolvable directive is dropped like a lost message.
    SendSnapshotChunk {
        /// Follower receiving the chunk.
        to: NodeId,
        /// Payload-free chunk directive for the transport to resolve.
        chunk: SnapshotChunkSend,
    },
    /// A validated inbound snapshot chunk for the receiver's snapshot store.
    /// Stage it durably before releasing the acknowledgement emitted in the
    /// same step — the persist-before-output contract; a crash between the
    /// two must never leave the leader ahead of the staged prefix.
    StageSnapshotChunk {
        /// Validated chunk to persist in receiver staging.
        chunk: StagedSnapshotChunk,
    },
    /// A client proposal was rejected without being appended.
    RejectProposal {
        /// Local-only proposal identity, when the input supplied one.
        proposal_id: Option<LocalProposalId>,
        /// Protocol reason the proposal was not appended.
        reason: ProposalRejection,
    },
    /// A leadership-transfer request was rejected.
    LeadershipTransferRejected {
        /// Requested transfer target.
        target: NodeId,
        /// Protocol reason the transfer could not start.
        reason: LeadershipTransferRejection,
    },
    /// The read barrier `read_id` is confirmed at `read_index`: a quorum
    /// acknowledged this node's leadership after the barrier was registered.
    ReadIndexGranted {
        /// Local-only read barrier identity.
        read_id: ReadId,
        /// Committed log barrier. The embedding must execute application
        /// entries through this position before reading; no-ops and
        /// configurations do not produce application-command callbacks.
        read_index: LogIndex,
    },
    /// A read-index request was rejected without being registered.
    ReadIndexRejected {
        /// Local-only read barrier identity.
        read_id: ReadId,
        /// Protocol reason the barrier was not registered.
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
        /// Local-only identity of the canceled barrier.
        read_id: ReadId,
        /// Boundary at which the pending barrier was cleared.
        reason: ReadIndexCancelReason,
    },
    /// Sends one Raft protocol message to `to`.
    Send {
        /// Destination node.
        to: NodeId,
        /// Protocol frame to transmit.
        message: Message,
    },
}
