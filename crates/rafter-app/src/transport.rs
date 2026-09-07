//! Peer envelope types for caller-owned transport and routing.
//!
//! This module defines explicit group-aware envelopes. Authentication,
//! authorization, and the refusal of retired peers remain the responsibility of
//! the embedding runtime before messages enter the group driver.

use rafter::{Message, NodeId};

mod envelope;
mod policy;
mod validation;

pub use envelope::{AuthenticatedPeerEnvelope, PeerEnvelope};
pub use policy::{AuthenticatedPeerEnvelopeError, AuthenticatedPeerValidator};

/// Returns the Raft node ID carried as sender by a protocol message.
#[must_use]
pub fn message_sender(message: &Message) -> NodeId {
    match message {
        Message::AppendEntries(message) => message.leader_id,
        Message::AppendEntriesResponse(message) => message.follower_id,
        Message::InstallSnapshot(message) => message.leader_id,
        Message::InstallSnapshotChunk(message) => message.leader_id,
        Message::InstallSnapshotResponse(message) => message.follower_id,
        Message::PreVote(message) => message.candidate_id,
        Message::PreVoteResponse(message) => message.voter_id,
        Message::TimeoutNow(message) => message.leader_id,
        Message::RequestVote(message) => message.candidate_id,
        Message::RequestVoteResponse(message) => message.voter_id,
    }
}

#[cfg(test)]
#[path = "transport_test.rs"]
mod tests;
