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
    pub client_id: u128,
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
    pub local_proposal_id: LocalProposalId,
    pub client_request_id: Option<ClientRequestId>,
    pub command: C,
}

/// Immediate result of beginning a local proposal.
///
/// `Appended` means the local node appended the proposal while acting as
/// leader; it is not client-facing write success. A managed write must wait
/// for a later committed apply result. `UnknownOutcome` means the runtime can
/// no longer prove whether the proposal will commit and apply; it is distinct
/// from rejection or failing to start.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProposalBegin<G, R> {
    Appended {
        group_id: G,
        local_proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        peer_messages: Vec<PeerEnvelope<G>>,
    },
    Completed {
        group_id: G,
        local_proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        result: R,
        peer_messages: Vec<PeerEnvelope<G>>,
    },
    Rejected {
        group_id: G,
        local_proposal_id: LocalProposalId,
        reason: ProposalRejection,
        leader_hint: Option<NodeId>,
    },
    UnknownOutcome {
        group_id: G,
        local_proposal_id: LocalProposalId,
        client_request_id: Option<ClientRequestId>,
        reason: ProposalUnknownOutcomeReason,
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
        index: LogIndex,
        term: Term,
        reason: LocalProposalDropReason,
    },
    /// Reserved diagnostic for proposals abandoned because the group entered a
    /// poison state before the app layer could observe a terminal result.
    GroupPoisoned,
    /// Reserved diagnostic for malformed/custom runtimes that accept a
    /// proposal input but return no proposal lifecycle output.
    ProposalDidNotStart,
}

/// Proposal lifecycle events emitted by the app/group layer.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProposalEvent<R> {
    Appended {
        local_proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
    },
    Applied {
        local_proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        result: R,
    },
    Rejected {
        local_proposal_id: LocalProposalId,
        reason: ProposalRejection,
    },
    /// The runtime can no longer determine whether this local proposal
    /// eventually committed and applied.
    ///
    /// This does not mean the write failed. Retry only with application-level
    /// idempotency if duplicate effects matter.
    UnknownOutcome {
        local_proposal_id: LocalProposalId,
        client_request_id: Option<ClientRequestId>,
        reason: ProposalUnknownOutcomeReason,
    },
}
