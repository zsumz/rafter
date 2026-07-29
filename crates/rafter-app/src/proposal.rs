//! Proposal lifecycle and application-level request identity types.
//!
//! Local proposal IDs are volatile runtime correlation handles. Durable
//! idempotency or duplicate-detection identities belong in application
//! commands or state-machine data.

use rafter::{LocalProposalDropReason, LocalProposalId, LogIndex, NodeId, ProposalRejection, Term};

use crate::transport::PeerEnvelope;

/// Optional application-level request identity.
///
/// This is not a Raft protocol identity and Rafter never generates it
/// automatically. Applications that need idempotency, duplicate detection, or
/// conflict semantics must persist the relevant identity/fingerprint data in
/// their command or state-machine layer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientRequestId {
    /// Application-defined client identity.
    pub client_id: u128,
    /// Monotonic request sequence within that client identity.
    pub sequence: u64,
}

/// A command proposed through the app layer.
///
/// `local_proposal_id` is volatile local correlation only. It is not
/// replicated, durable, or meaningful to another node. Within a
/// `RaftGroup`, local proposal IDs must be strictly increasing for the
/// lifetime of the group so stale runtime outputs cannot complete a newly
/// reused waiter. A local proposal ID is consumed once command encoding
/// succeeds and the proposal is submitted to the runtime, even if the runtime
/// returns an error or no lifecycle event. Command encode failure does not
/// consume the ID.
/// `client_request_id` is optional application metadata and must not be
/// confused with Raft-level proposal tracking.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Proposal<C> {
    /// Volatile group-local correlation ID, consumed after submission.
    pub local_proposal_id: LocalProposalId,
    /// Optional durable application identity carried beside the command.
    pub client_request_id: Option<ClientRequestId>,
    /// Application command to encode and replicate.
    pub command: C,
}

/// Immediate result of beginning a local proposal.
///
/// `Appended` means the local node appended the proposal while acting as
/// leader; it is not client-facing write success. A managed write must wait
/// for a later committed apply result. `UnknownOutcome` means the runtime can
/// no longer prove whether the proposal will commit and apply. It is distinct
/// from a provable rejection; `LifecycleUnreported` means the runtime emitted
/// no lifecycle evidence, not that the proposal definitely failed to start.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProposalBegin<G, R> {
    /// The leader appended the command; commit and application are still pending.
    Appended {
        /// Group that accepted the command.
        group_id: G,
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Log index assigned by the leader.
        index: LogIndex,
        /// Leader term that appended the entry.
        term: Term,
        /// Peer messages the caller must route.
        peer_messages: Vec<PeerEnvelope<G>>,
    },
    /// A single-node group appended, committed, and applied the command.
    Completed {
        /// Group that completed the command.
        group_id: G,
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Applied log index.
        index: LogIndex,
        /// Term of the applied entry.
        term: Term,
        /// Application result returned by the state machine.
        result: R,
        /// Peer messages the caller must route.
        peer_messages: Vec<PeerEnvelope<G>>,
    },
    /// The local node proved the command was not appended.
    Rejected {
        /// Group that refused the command.
        group_id: G,
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Protocol reason the command did not enter the log.
        reason: ProposalRejection,
        /// Best-effort leader identity observed with the refusal.
        leader_hint: Option<NodeId>,
    },
    /// Submission occurred, but the app layer cannot prove the final fate.
    UnknownOutcome {
        /// Group whose command fate is unresolved.
        group_id: G,
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Optional durable identity callers may use for safe retry.
        client_request_id: Option<ClientRequestId>,
        /// Diagnostic explanation for the lost outcome.
        reason: ProposalUnknownOutcomeReason,
        /// Peer messages still emitted by the submission step.
        peer_messages: Vec<PeerEnvelope<G>>,
    },
}

/// Diagnostic cause for an app-layer proposal with unknown outcome.
///
/// This reason explains why the app layer lost the final proposal outcome. It
/// does not prove whether the command committed or applied.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProposalUnknownOutcomeReason {
    /// The Raft/runtime layer reported that local tracking for this proposal
    /// was dropped before the app layer observed commit/apply.
    LocalProposalDropped {
        /// Log index formerly correlated with the local proposal.
        index: LogIndex,
        /// Term that appended the entry.
        term: Term,
        /// Kernel lifecycle reason local tracking ended.
        reason: LocalProposalDropReason,
    },
    /// Reserved diagnostic for proposals abandoned because the group entered a
    /// poison state before the app layer could observe a terminal result.
    GroupPoisoned,
    /// The runtime accepted a proposal input but returned no lifecycle output.
    ///
    /// Silence is not proof that the proposal stayed out of durable state, so
    /// callers must treat its fate as unresolved.
    LifecycleUnreported,
}

/// Proposal lifecycle events emitted by the app/group layer.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProposalEvent<R> {
    /// The local leader appended the proposal.
    Appended {
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Assigned log index.
        index: LogIndex,
        /// Leader term that appended the entry.
        term: Term,
    },
    /// The committed proposal was applied to the state machine.
    Applied {
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Applied log index.
        index: LogIndex,
        /// Term of the applied entry.
        term: Term,
        /// Application result returned by the state machine.
        result: R,
    },
    /// The local node refused this proposal before replication.
    ///
    /// `leader_hint` is the leader this node believed in when the rejection was
    /// recorded. It is a redirect hint, never authority: it may be `None`, it
    /// may already be stale, and it may name this node when the rejection was
    /// not about leadership. It is recorded at the same point as the hints on
    /// [`crate::read::ReadEvent::Rejected`] and
    /// [`crate::group::LeadershipTransferEvent::Rejected`], so a caller that
    /// observes this rejection asynchronously sees the same value the immediate
    /// [`ProposalBegin::Rejected`] carries for the same rejection.
    Rejected {
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Protocol reason the proposal did not enter the log.
        reason: ProposalRejection,
        /// Best-effort leader identity observed with the refusal.
        leader_hint: Option<NodeId>,
    },
    /// The runtime can no longer determine whether this local proposal
    /// eventually committed and applied.
    ///
    /// This does not mean the write failed. Retry only with application-level
    /// idempotency if duplicate effects matter.
    UnknownOutcome {
        /// Volatile local proposal correlation ID.
        local_proposal_id: LocalProposalId,
        /// Optional durable identity callers may use for safe retry.
        client_request_id: Option<ClientRequestId>,
        /// Diagnostic explanation for the lost outcome.
        reason: ProposalUnknownOutcomeReason,
    },
}
