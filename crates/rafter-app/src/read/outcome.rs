//! What a read hands back: immediate outcomes and lifecycle events.
//!
//! A barrier ends in whichever step observes its cause, so the outcome a
//! read call returns and the events later steps report are two views of
//! one barrier. Both live here; the request vocabulary is beside them.

use rafter::{LogIndex, NodeId, ReadId, ReadIndexCancelReason, ReadIndexRejection};

use crate::transport::PeerEnvelope;

use super::ReadProof;

/// Immediate outcome from the simple state-machine read helper.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadOutcome<G, R> {
    /// The read completed against the local state machine.
    Ready {
        /// Application query result.
        result: R,
        /// Linearizable proof, or `None` for a local read.
        proof: Option<ReadProof<G>>,
    },
    /// The read-index round is still in flight. Keep driving the group and
    /// retry with the same `read_id`, freshness, and context; call
    /// `RaftGroup::cancel_read` if abandoning the helper read.
    ///
    /// `peer_messages` duplicates the step report's list so that
    /// `RaftGroup::read_outcome` callers, who never see a report, can route
    /// the round. A `RaftGroup::read` caller must route the report's list or
    /// this one, never both — routing both sends every read-index frame
    /// twice.
    Pending {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Peer messages the caller must route exactly once.
        peer_messages: Vec<PeerEnvelope<G>>,
    },
    /// The read was rejected and no local helper state remains reserved.
    Rejected {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Protocol reason the read-index request did not start.
        reason: ReadIndexRejection,
        /// Best-effort leader identity observed with the refusal.
        leader_hint: Option<NodeId>,
    },
    /// The read was canceled by local runtime lifecycle, usually leadership
    /// loss, and no local helper state remains reserved.
    Canceled {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Lifecycle event that invalidated the pending proof.
        reason: ReadIndexCancelReason,
        /// Best-effort leader identity observed with the cancellation.
        leader_hint: Option<NodeId>,
    },
    /// A linearizable read-index proof exists or is in progress, but the local
    /// state machine has not applied through the required index yet. Keep
    /// driving the group and retry with the same `read_id`, freshness, and
    /// context, or call `RaftGroup::cancel_read` before abandoning the helper
    /// read. Canceling removes local waiter state; it does not make the
    /// submitted `ReadId` reusable.
    LinearizableFreshnessUnavailable {
        /// Consumed read correlation ID whose proof remains pending locally.
        read_id: ReadId,
        /// Application index the state machine must reach.
        required_applied_index: LogIndex,
        /// Application index currently reported by the state machine.
        local_applied_index: LogIndex,
    },
    /// A local read requested a minimum applied index that the local state
    /// machine has not reached. No read-index operation was submitted and no
    /// local read state is reserved.
    LocalFreshnessUnavailable {
        /// Application index the caller required.
        required_applied_index: LogIndex,
        /// Application index currently reported by the state machine.
        local_applied_index: LogIndex,
    },
}

/// Immediate outcome from beginning a read barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadProofOutcome<G> {
    /// The barrier is satisfied and includes a proof for the required index.
    Granted {
        /// Completed linearizable-read proof.
        proof: ReadProof<G>,
    },
    /// The read-index round is still in flight. Route `peer_messages`, keep
    /// driving the group, and retry or observe later [`ReadEvent`] values.
    /// The `ReadId` remains consumed even if the caller later cancels the
    /// local waiter.
    Pending {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Peer messages the caller must route exactly once.
        peer_messages: Vec<PeerEnvelope<G>>,
    },
    /// The barrier was rejected and no local barrier state remains reserved.
    Rejected {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Protocol reason the read-index request did not start.
        reason: ReadIndexRejection,
        /// Best-effort leader identity observed with the refusal.
        leader_hint: Option<NodeId>,
    },
    /// The barrier was canceled by local runtime lifecycle and no local
    /// barrier state remains reserved.
    Canceled {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Lifecycle event that invalidated the pending proof.
        reason: ReadIndexCancelReason,
        /// Best-effort leader identity observed with the cancellation.
        leader_hint: Option<NodeId>,
    },
    /// The barrier has a read index, but the local state machine has not
    /// applied through the required index yet. The low-level barrier remains
    /// active until it is granted, rejected, canceled by the runtime, or
    /// canceled locally with `RaftGroup::cancel_read`; local cancellation does
    /// not make the submitted `ReadId` reusable.
    FreshnessUnavailable {
        /// Consumed read correlation ID whose proof remains active.
        read_id: ReadId,
        /// Application index the state machine must reach.
        required_applied_index: LogIndex,
        /// Application index currently reported by the state machine.
        local_applied_index: LogIndex,
    },
}

/// Read events emitted from group steps after a barrier has started.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadEvent<G> {
    /// A previously pending barrier is now satisfied.
    Granted {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Completed linearizable-read proof.
        proof: ReadProof<G>,
    },
    /// A previously pending barrier was rejected and local waiter state was
    /// cleared.
    Rejected {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Protocol reason the read-index request did not start.
        reason: ReadIndexRejection,
        /// Best-effort leader identity observed with the refusal.
        leader_hint: Option<NodeId>,
    },
    /// A previously pending barrier was canceled by local runtime lifecycle and
    /// local waiter state was cleared.
    Canceled {
        /// Consumed read correlation ID.
        read_id: ReadId,
        /// Lifecycle event that invalidated the pending proof.
        reason: ReadIndexCancelReason,
        /// Best-effort leader identity observed with the cancellation.
        leader_hint: Option<NodeId>,
    },
    /// A read-index is known, but the local state machine has not applied far
    /// enough yet. The read remains pending unless the caller cancels it. The
    /// `ReadId` remains consumed after cancellation.
    FreshnessUnavailable {
        /// Consumed read correlation ID whose proof remains active.
        read_id: ReadId,
        /// Application index the state machine must reach.
        required_applied_index: LogIndex,
        /// Application index currently reported by the state machine.
        local_applied_index: LogIndex,
    },
}
