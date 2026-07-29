//! Linearizable and proof-producing read barrier types.
//!
//! The app layer turns the kernel `ReadIndex` primitive into user-facing read
//! modes while preserving a lower-level proof path for embedded runtimes.

use rafter::{LogIndex, NodeId, ReadId, ReadIndexCancelReason, ReadIndexRejection, Term};

use crate::transport::PeerEnvelope;

/// Application-facing read consistency modes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadConsistency {
    /// Safe default. Uses the Raft `ReadIndex` primitive and requires the local
    /// state machine to have applied every committed application command at or
    /// below the returned read index.
    ///
    /// That floor is not the read index itself. Elections and membership
    /// changes commit entries the state machine is never told about — every
    /// leader's first entry in its term is a `Noop` — so a barrier that
    /// required the state machine to reach the read index would wait for an
    /// index it can never report. The floor is resolved once, when the quorum
    /// round grants the read index, and reported as
    /// [`ReadProof::required_applied_index`].
    Linearizable,
    /// Fast leader-local reads. Reserved for future app-layer lease support.
    ///
    /// This variant is hidden from generated docs because the app layer
    /// currently returns [`crate::error::GroupError::UnsupportedReadConsistency`]
    /// instead of serving lease reads.
    #[doc(hidden)]
    LeaseRead,
    /// Reads local state only. This may be stale.
    Local,
}

/// Simple state-machine read request for callers that do not need to assemble
/// a proof-producing read barrier manually.
///
/// Local reads do not submit the Raft read-index primitive and do not carry or
/// consume a [`ReadId`]. Linearizable helper reads require a strictly
/// increasing `ReadId`; if they return [`ReadOutcome::Pending`] or
/// [`ReadOutcome::LinearizableFreshnessUnavailable`], the ID may remain
/// reserved until the read is rejected, canceled, consumed by retrying with
/// matching freshness/context, or explicitly removed through group cleanup
/// APIs. If later group progress completes the proof, the proof is cached for
/// that retry rather than applied to a different read.
///
/// A `min_applied_index` is honored verbatim: it is not capped at the read
/// index, not lowered, and not snapped to an application entry. A caller may be
/// expressing "at least as fresh as the write I already observed", and Rafter
/// must not silently weaken that. The natural source of the value —
/// [`crate::proposal::ProposalEvent::Applied`] — always names an application
/// entry, so the natural usage is always reachable. A caller that instead
/// sources it from a commit index, a read index, or a snapshot boundary may
/// name an entry the state machine will never be told about and will stall on
/// [`ReadOutcome::LinearizableFreshnessUnavailable`] or
/// [`ReadOutcome::LocalFreshnessUnavailable`] forever; convert such an index
/// with `RaftGroup::committed_application_index_through` first.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadRequest<G, Q> {
    /// Reads local state only. This may be stale and does not consume a
    /// `ReadId`.
    Local {
        /// Group whose local state machine is queried.
        group_id: G,
        /// Application query.
        query: Q,
        /// Optional caller-required application freshness floor.
        min_applied_index: Option<LogIndex>,
    },
    /// Uses the Raft read-index primitive and requires a strictly increasing
    /// local `ReadId`.
    Linearizable {
        /// Group whose state machine is queried.
        group_id: G,
        /// Strictly increasing local read correlation ID.
        read_id: ReadId,
        /// Application query.
        query: Q,
        /// Optional caller-required application freshness floor.
        min_applied_index: Option<LogIndex>,
        /// Opaque bytes echoed through the read-index quorum round.
        context: Vec<u8>,
    },
    /// Fast leader-local reads. Reserved for future app-layer lease support.
    ///
    /// This variant is hidden from generated docs because the app layer
    /// currently returns [`crate::error::GroupError::UnsupportedReadConsistency`]
    /// instead of serving lease reads.
    #[doc(hidden)]
    Lease {
        /// Group whose state machine would be queried.
        group_id: G,
        /// Application query.
        query: Q,
        /// Optional caller-required application freshness floor.
        min_applied_index: Option<LogIndex>,
    },
}

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

/// Request for a proof-producing linearizable read barrier.
///
/// Read-index `ReadId`s are consumed when submitted. A caller that starts a
/// later read-index operation must use a strictly larger `ReadId`, even if an
/// earlier read was rejected, canceled, dropped, or abandoned locally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBarrierRequest<G> {
    /// Group whose read authority is requested.
    pub group_id: G,
    /// Strictly increasing local read correlation ID.
    pub read_id: ReadId,
    /// Optional caller-required application freshness floor.
    pub min_applied_index: Option<LogIndex>,
    /// Opaque bytes echoed through the read-index quorum round.
    pub context: Vec<u8>,
}

/// Proof that the local state machine is fresh enough for a read.
///
/// The three indexes are distinct and each means what it says. `read_index` is
/// what the quorum round certified. `required_applied_index` is what the state
/// machine had to reach: the highest committed application entry at or below
/// `read_index`, raised by any caller-supplied `min_applied_index`. It is at or
/// below `read_index` unless a caller raised it, because the entry at the read
/// index is frequently one the state machine is never told about.
/// `local_applied_index` is where the state machine actually was, at or above
/// the requirement — the barrier certifies a lower bound on freshness, never a
/// point to rewind to, and serving state fresher than the cut is still
/// linearizable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadProof<G> {
    /// Group for which the proof was granted.
    pub group_id: G,
    /// Node that completed the quorum round.
    pub issued_by: NodeId,
    /// Leader term that granted the proof.
    pub term: Term,
    /// Commit index certified by the quorum round.
    pub read_index: LogIndex,
    /// Application index required to serve the read.
    pub required_applied_index: LogIndex,
    /// Application index observed when the proof was completed.
    pub local_applied_index: LogIndex,
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
