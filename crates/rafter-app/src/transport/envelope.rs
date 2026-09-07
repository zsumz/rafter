//! The two peer envelopes: one routed, one authenticated but unchecked.
//!
//! Both carry caller-defined group identity beside the Raft message.
//! Neither validates anything: the admission policy and the check that
//! applies it live beside this module.

use rafter::{Message, NodeId};

/// A Raft peer message annotated with caller-defined group identity.
///
/// The app layer returns envelopes to the caller; it does not send them. A
/// multi-group or route-aware runtime can inspect `group_id`, authenticate the
/// sender at its own transport boundary, and dispatch the embedded Raft
/// message under its own routing and admission policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerEnvelope<G> {
    /// Caller-defined Raft group identity.
    pub group_id: G,
    /// Raft sender identity.
    pub from: NodeId,
    /// Raft recipient identity.
    pub to: NodeId,
    /// Protocol message to route without reinterpretation.
    pub message: Message,
}

/// A transport-authenticated inbound peer message before app-layer validation.
///
/// Production runtimes should validate this envelope before converting it to
/// [`PeerEnvelope`]:
///
/// - `group_id` is known locally;
/// - `authenticated_peer` maps to `raft_from`;
/// - `raft_to` is the local node ID;
/// - the peer is not retired;
/// - the peer is authorized for the group;
/// - the sender embedded in the Raft message matches `raft_from`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedPeerEnvelope<G, P> {
    /// Caller-defined Raft group identity.
    pub group_id: G,
    /// Principal established by the transport security boundary.
    pub authenticated_peer: P,
    /// Raft sender claimed by the envelope.
    pub raft_from: NodeId,
    /// Raft recipient claimed by the envelope.
    pub raft_to: NodeId,
    /// Protocol message whose embedded sender must agree with the envelope.
    pub message: Message,
}
