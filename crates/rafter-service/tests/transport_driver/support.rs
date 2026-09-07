//! The one inbound frame this suite hands the driver by hand.
//!
//! An authenticated vote from a chosen replica, carrying a term above the local
//! one so a delivery meant to depose the leader does. Every refusal case here —
//! unauthorized, retired, wrong group — refuses this same frame, so what the
//! scenario measures is the check rather than the frame it was handed.

use rafter_service::AuthenticatedPeerEnvelope;

use super::support::transport::{Principal, GROUP};
use super::support::{LogIndex, Message, NodeId, RequestVote, Term};

pub(super) fn vote_envelope(from: NodeId, to: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: to,
        message: Message::RequestVote(RequestVote {
            term: Term(9),
            candidate_id: from,
            last_log_index: LogIndex::ZERO,
            last_log_term: Term(0),
        }),
    }
}
