#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Where one step's report goes.
//!
//! Peer frames to the transport, snapshot directives to the transport,
//! membership facts to the one transaction that may state them, and lifecycle
//! events to the waiters they belong to. A refused send is counted rather than
//! propagated: Raft re-sends, so a client write must not fail because one
//! heartbeat could not be delivered.

use crate::transport::{AuthenticatedPeerValidator, RaftTransport, SnapshotChunkEnvelope};

use super::state::TransportDriverState;
use super::*;

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    /// Resolves every waiter the group handed over when it poisoned.
    ///
    /// A poison is not an event stream. `RaftGroup::enter_poisoned` moves every
    /// pending proposal and every reserved read out of the group's live tables
    /// into `poisoned_waiters` and emits nothing further for them, so a driver
    /// that routes reports and nothing else leaves those clients waiting
    /// forever while every later call raises the same refusal.
    ///
    /// Writes resolve as unknown rather than refused, for the reason a released
    /// driver's do: the entry may be in the durable log, and an incarnation
    /// reopened over that log can still commit it. Reads resolve as poisoned,
    /// which is the whole truth about them — a barrier the group dropped
    /// produces no answer, ever.
    pub(super) fn drain_poisoned_waiters(&mut self) {
        let Some(group) = self.group.as_mut() else {
            return;
        };
        if group.poisoned_waiters().is_empty() {
            return;
        }
        let GroupFatalState::Poisoned { reason } = group.fatal_state().clone() else {
            // Adoption refuses a healthy group holding poisoned waiters, so
            // there is no poison to report and nothing honest to say.
            return;
        };
        let cause = group.poison_cause().cloned();
        let waiters = group.drain_poisoned_waiters();
        for (local_proposal_id, client_request_id) in waiters.proposals {
            let client_request_id = client_request_id.or_else(|| {
                self.write_waiters
                    .get(&local_proposal_id)
                    .and_then(|waiter| waiter.options.client_request_id)
            });
            self.resolve_write(
                local_proposal_id,
                Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id,
                    reason: UnknownOutcomeReason::GroupPoisoned,
                }),
            );
        }
        for read_id in waiters.reads {
            self.resolve_read(
                read_id,
                Err(ReadError::Poisoned {
                    reason: reason.clone(),
                    cause: cause.clone(),
                }),
            );
        }
    }

    /// Hands the report's peer messages to the transport and its lifecycle
    /// events to the waiters they belong to.
    ///
    /// A refused send is counted rather than propagated: Raft tolerates drops
    /// and re-sends, so a write must not fail because one heartbeat could not
    /// be delivered.
    ///
    /// Read events are routed here for the same reason proposal events are: the
    /// app layer ends a barrier in whichever step observes the cause, and for a
    /// leadership change that step is a tick or a delivery rather than a read
    /// call. A driver that read only its own read calls' outcomes would leave
    /// that client waiting forever and then ask the group to re-reserve a spent
    /// `ReadId`.
    pub(super) fn route_report(&mut self, report: DriverStepReport<G, A>) {
        for envelope in report.peer_messages {
            if self.transport.send(envelope).is_err() {
                self.refused_sends = self.refused_sends.saturating_add(1);
            }
        }
        for event in report.snapshot_events {
            self.route_snapshot_event(event);
        }
        // **One statement for the whole report**, not one per event. The
        // membership events of a single report are folded into a candidate
        // together and published once — see [`super::reconciliation`] — because a
        // report whose second event contradicts its first must not leave the link
        // layer holding a permanent statement licensed by the first alone. The
        // sends above and the resolutions below are deliberately not in that
        // transaction: both are loss-tolerant, and neither is retractable-only.
        self.route_membership_events(&report.membership_events);
        for event in &report.proposal_events {
            self.observe_proposal_event(event);
        }
        for event in &report.read_events {
            self.observe_read_event(event);
        }
    }

    /// Hands a leader chunk directive to the transport, and lets the other two
    /// snapshot events alone.
    ///
    /// `SendChunk` is the only snapshot effect a driver owns.
    /// [`crate::RaftTransport::send_snapshot_chunk`] resolves the directive
    /// against the embedder's snapshot store and frames it; a refusal is counted
    /// like any other, because the protocol re-sends.
    ///
    /// `StageChunk` is already durable. The runtime contract forbids releasing
    /// an output whose snapshot obligation has not completed, and staging the
    /// chunk *is* that obligation — `DurableRaftNode` discharges it before it
    /// returns anything. A second staging area in the driver could only diverge
    /// from the one recovery actually reads. `Apply` is likewise done: the group
    /// installed the snapshot into the state machine and moved its own applied
    /// floor before emitting the event.
    fn route_snapshot_event(&mut self, event: SnapshotEvent<G>) {
        let SnapshotEvent::SendChunk {
            group_id,
            to,
            chunk,
        } = event
        else {
            return;
        };
        let envelope = SnapshotChunkEnvelope {
            group_id,
            from: self.node_id,
            to,
            chunk,
        };
        if self.transport.send_snapshot_chunk(envelope).is_err() {
            self.refused_sends = self.refused_sends.saturating_add(1);
        }
    }

    /// Publishes the group's metrics, and discards the one thing publishing can
    /// report.
    ///
    /// This driver owns the publisher and is the only thing that closes it, in
    /// `shutdown`. A refusal here therefore means the driver is already down,
    /// and a metrics snapshot from a driver that is already down is exactly the
    /// one nobody is waiting for.
    pub(super) fn publish_metrics(&self) {
        if let Some(group) = self.group.as_ref() {
            let _ = self.metrics.publish(group.metrics());
        }
    }
}
