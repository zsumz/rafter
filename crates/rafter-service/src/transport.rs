//! The managed service transport trait and its validation glue.
//!
//! Production transports must authenticate peers before constructing an
//! authenticated envelope, keep each group's authorization policy current —
//! which is who may speak *and* which identities the cluster has retired, in one
//! value — and use the current `rafter-codec` peer wire format. This crate
//! intentionally provides a trait only, so that no transport ships as the
//! default by being the one that is here.
//!
//! One trait, and it is synchronous. [`RaftTransport::send`] means "accepted or
//! enqueued", which is the seam an async link layer already needs: the embedder
//! owns the queue and the task that drains it, and this crate hands frames to
//! the queue. An async twin of this trait would describe how that task is
//! spawned and awaited, which is a design this crate has never run and cannot
//! validate — see the fourth revision of the Transport-Attached Group Driver
//! entry in `docs/api-promotions.md`.
//!
//! An unauthenticated transport must say so in its own name, not only in its
//! documentation: `rafter-transport-tcp-insecure` is the shipped example of the
//! rule, and it is a demo rather than a deployment target.
//!
//! The boundary itself lives in `raft` and the value it carries in `policy`,
//! both private and re-exported here. What stays in this file is the inbound
//! direction: the one function an embedder calls before a frame reaches a group.

use rafter::NodeId;

pub use rafter_app::transport::{
    message_sender, AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError,
    AuthenticatedPeerValidator, PeerEnvelope,
};

mod policy;
mod raft;

pub use policy::PeerPolicy;
pub use raft::{RaftTransport, SnapshotChunkEnvelope};

/// Validates and converts one authenticated inbound envelope before it enters
/// a managed [`rafter_app::group::RaftGroup`].
///
/// # Errors
///
/// Returns [`AuthenticatedPeerEnvelopeError`] if the authenticated principal
/// is not mapped to the Raft sender, the target is not the local node, the
/// group is unknown, the peer is retired or unauthorized, or the embedded Raft
/// message sender disagrees with the envelope sender.
pub fn validate_inbound_peer_envelope<G, P, V>(
    envelope: AuthenticatedPeerEnvelope<G, P>,
    local_node_id: NodeId,
    validator: &V,
) -> Result<PeerEnvelope<G>, AuthenticatedPeerEnvelopeError>
where
    V: AuthenticatedPeerValidator<G, P>,
{
    envelope.try_into_peer_envelope(local_node_id, validator)
}

#[cfg(test)]
#[path = "transport_test.rs"]
mod tests;
