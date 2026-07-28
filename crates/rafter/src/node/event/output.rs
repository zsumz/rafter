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
    /// The configuration entry at `index` crossed the commit index.
    ///
    /// **One output per configuration entry the commit index crossed, in index
    /// order, and that is the whole reason it exists.** One step can advance the
    /// commit index across several configuration entries at once — a lagging
    /// replica catching up receives them in a single `AppendEntries` whose
    /// leader commit covers all of them — and a consumer that instead sampled
    /// [`Node::committed_membership`](crate::Node::committed_membership) once
    /// after the step would see only the last. Every configuration between is a
    /// membership the cluster genuinely committed: it authorized replicas, and
    /// the identities it named are spent whether or not any later configuration
    /// still names them. Sampling loses exactly the ones that lived and died
    /// inside one step, and loses them silently, because the sampled value can
    /// be identical before and after.
    ///
    /// `index` and `term` name the configuration entry itself rather than the
    /// commit index the step reached, so the outputs of one step are totally
    /// ordered by `index` and a consumer can correlate each membership with the
    /// entry that carried it.
    ///
    /// This is a *committed* fact and therefore permanent: unlike the effective
    /// configuration, which a new leader can truncate back off the log, a
    /// configuration reported here can never be taken back. Consumers that may
    /// only narrow a peer set or retire an identity on a committed fact may act
    /// on each of these.
    ///
    /// **A snapshot install emits none of these**, and cannot: a snapshot
    /// carries only the committed configuration at its boundary, so
    /// configurations that committed and were superseded below that boundary
    /// are not reconstructible from it. See
    /// [`Output::ApplySnapshot`] and
    /// [`Node::install_local_snapshot`](crate::Node::install_local_snapshot).
    ///
    /// # A transition, not a state
    ///
    /// `previous` and `configuration` are the two ends of one committed move,
    /// and carrying both is what makes this output mean the same thing wherever
    /// it is folded. A consumer that retires identities wants the *difference* —
    /// which replicas this configuration admitted, which it removed — and a
    /// membership state alone cannot answer that. It has to be subtracted from
    /// something, and the only correct something is the membership that stood
    /// immediately before, which the consumer can supply only if its own state
    /// happens to sit exactly there.
    ///
    /// That "happens to" is the whole problem. These outputs are replayed: a
    /// process recovering from durable storage receives every configuration
    /// entry above its applied floor, and a process that already holds a *later*
    /// membership — from a checkpoint, or from a snapshot boundary — computes
    /// `later − historical` and reads every replica the newer configurations
    /// added as a removal. An addition-only history retires replicas that way,
    /// permanently, which is the opposite of the fact it was handed.
    ///
    /// Computed here the difference is chronological by construction, because
    /// this walk knows the configuration in effect before each entry and the
    /// consumer does not. So a consumer may fold these in any order, from any
    /// starting state, any number of times.
    ConfigurationCommitted {
        index: LogIndex,
        term: Term,
        /// The membership in effect immediately before `configuration`.
        ///
        /// A membership rather than a [`ConfigurationEntry`], and that is not a
        /// loss of provenance — it is the only total answer. The state before
        /// the first configuration entry of a log is the bootstrap membership,
        /// and the state before the first one above a snapshot boundary is the
        /// boundary configuration; neither is an entry in the retained log, so
        /// an entry-typed field would have to be optional. An `Option` here
        /// would hand the consumer back exactly the question this field exists
        /// to answer — "what do I subtract?" — and it would be absent precisely
        /// at a boundary, which is where a wrong answer costs a live replica.
        previous: MembershipConfig,
        configuration: ConfigurationEntry,
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
