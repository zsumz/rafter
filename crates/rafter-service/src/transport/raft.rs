#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The synchronous transport boundary an embedder implements.
//!
//! Three obligations and no more: hand a peer frame to the link layer, resolve
//! and send one leader snapshot-chunk directive, and install the current
//! authorization policy. Rafter opens no sockets and spawns no tasks, so the
//! async half of every one of these belongs to the embedder.

use std::error::Error;

use rafter::{NodeId, SnapshotChunkSend};

use super::*;

/// One leader snapshot chunk directive addressed to a peer.
///
/// A directive rather than a message, because the kernel never holds an
/// application snapshot payload: `chunk` names the bytes by transfer, offset,
/// and length, and the transport reads them from the snapshot store with
/// [`SnapshotChunkSend::resolve`] before framing them. This is the shape the
/// kernel already documents for the boundary — payload bytes flow from the
/// application's snapshot store to the network without entering kernel state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunkEnvelope<G> {
    /// The group whose snapshot is being transferred.
    pub group_id: G,
    /// The leader sending the chunk.
    pub from: NodeId,
    /// The follower being caught up.
    pub to: NodeId,
    /// Which bytes to send, named by transfer, offset, and length.
    ///
    /// Resolve it against the local snapshot store with
    /// [`SnapshotChunkSend::resolve`]; the bytes themselves never pass through
    /// kernel state.
    pub chunk: SnapshotChunkSend,
}

/// Synchronous service transport boundary.
///
/// # Delivery semantics
///
/// Raft safety tolerates dropped, duplicated, reordered, reconnected, and
/// non-FIFO peer messages. A production transport may redial, retry, or deliver
/// messages after later messages without violating safety; the protocol
/// validates terms, indices, and snapshot metadata before accepting effects.
///
/// Liveness and performance still depend on transport discipline. Use bounded
/// queues or another explicit backpressure policy rather than unbounded memory
/// growth, and provide eventual delivery between healthy authorized peers.
/// Per-peer FIFO is not required for safety, but it reduces wasted work and is
/// usually beneficial. A successful [`RaftTransport::send`] means the message
/// was accepted or enqueued by the transport, not that it was delivered,
/// processed, committed, or applied.
///
/// Message sizing follows `rafter-codec`: append-entries frames fit within the
/// configured append budget plus headers, while snapshot transfers use
/// `InstallSnapshotChunk` and `InstallSnapshotResponse` frames. The current
/// peer wire format does not encode whole `InstallSnapshot` payloads. This
/// pre-release crate supports only the current peer wire format; future public
/// wire-format bumps need an explicit compatibility or migration plan.
pub trait RaftTransport<G>: Send + Sync + 'static {
    /// The transport's own authenticated identity for a peer.
    ///
    /// This is what the link layer proved, not what Raft believes: a
    /// certificate subject, a mutual-TLS identity, a signed token. Rafter never
    /// derives one from a [`NodeId`], because a node ID travels inside frames
    /// and proves nothing. An [`AuthenticatedPeerValidator`] maps between the
    /// two in both directions, and the mapping is the trust boundary — a
    /// principal that maps to the wrong node authorizes that node's votes.
    type PeerPrincipal;
    /// Error returned when the transport cannot send or update peer metadata.
    ///
    /// Transport errors are part of the public app/service error stack, so
    /// implementations expose typed errors rather than debug-only strings —
    /// the same contract [`rafter_runtime_api::PersistedRaftRuntime::Error`]
    /// and [`rafter_app::state_machine::ReplicatedStateMachine::Error`] state
    /// for the other halves of it. Without the bound a driver cannot preserve
    /// a send failure as a [`crate::error::ErrorCause`], and would have to
    /// render it.
    type Error: Error + Send + Sync + 'static;

    /// Sends one validated outbound Raft peer envelope.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the frame cannot be
    /// sent.
    fn send(&self, envelope: PeerEnvelope<G>) -> Result<(), Self::Error>;

    /// Resolves one leader snapshot chunk directive and sends it.
    ///
    /// This is the only path by which a follower below the leader's snapshot
    /// boundary is caught up. Implementations resolve `envelope.chunk` against
    /// their own [`rafter::SnapshotChunkSource`] with
    /// [`SnapshotChunkSend::resolve`] and send the resulting
    /// [`rafter::Message::InstallSnapshotChunk`] frame. A directive the source
    /// cannot serve is dropped like a lost message, exactly as `resolve`
    /// documents: the transfer resumes from the follower's acknowledged offset
    /// once the source and the kernel agree on the current snapshot again.
    ///
    /// There is no provided body on purpose. The only one expressible returns
    /// `Ok(())` and drops the chunk, which would let a transport disable
    /// snapshot transfer by omission and report success for it.
    ///
    /// A runtime that resolves directives itself — `DurableRaftNode` does,
    /// because it owns its snapshot store — never produces one of these, so an
    /// embedder over the shipped runtime may implement this as a refusal.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the chunk cannot be
    /// sent. Like [`RaftTransport::send`], a refusal is counted by the driver
    /// rather than propagated to a client.
    fn send_snapshot_chunk(&self, envelope: SnapshotChunkEnvelope<G>) -> Result<(), Self::Error>;

    /// Replaces the authorization policy for `group_id`: who may speak, and
    /// which identities are retired.
    ///
    /// **The only admission statement this boundary carries**, and it used to be
    /// two. A driver published a peer set here and retired replicas through a
    /// separate per-principal `fence_peer` call that was permanent, fallible, and
    /// therefore owed until accepted. Both statements are now one value, which is
    /// what removes the driver's obligation ledger and everything that could go
    /// wrong inside it — see [`PeerPolicy`] for what the floor buys and what it
    /// gives up.
    ///
    /// Idempotent, and may be called with a policy the transport already holds.
    /// A driver retries a refused publication at its next entry point rather than
    /// waiting for the cluster's next configuration change, so an implementation
    /// must treat a repeat of the current policy as a no-op rather than as a
    /// reconfiguration.
    ///
    /// # What a refusal costs
    ///
    /// A refused publication leaves the link layer holding the *previous* policy,
    /// which authorizes a set the cluster has moved on from and retires fewer
    /// identities than it should. That is stale rather than wrong in the
    /// dangerous direction — the floor is monotone, so an older policy never
    /// retires an identity the current one does not — and the driver's own
    /// inbound membership check refuses the retired replica in the meantime. Two
    /// layers, and this is the outer one.
    ///
    /// **Restarting a replica is not removing it.** A replica killed and
    /// restarted under the same node ID was never removed, stays in the peer set,
    /// and keeps its principal across the restart. Nothing on this boundary
    /// changes for it.
    ///
    /// # Errors
    ///
    /// Returns the transport implementation's error when the policy cannot be
    /// installed. A refusal is not final: the driver holds the policy it could
    /// not publish and tries again at every later entry point.
    fn update_peers(
        &self,
        group_id: &G,
        policy: PeerPolicy<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error>;
}
