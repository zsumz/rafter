#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! How a waiter stops waiting.
//!
//! Three ways one ends, and they are not interchangeable: the group announced a
//! terminal outcome, the caller decided to stop, or the incarnation let go of
//! everything at once. Each resolves a client rather than removing an entry, so
//! a caller still holding its future gets an answer on the next poll.

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

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
    /// Resolves a barrier the group itself ended, and records one it granted.
    ///
    /// The terminal mapping is [`terminal_read_error`], which
    /// [`InMemoryRaftState::route_report`] also uses, so a barrier the group
    /// ended reads the same on either driver. The mapping agrees with
    /// [`TransportDriverState::handle_read_outcome`] too, so a barrier resolves
    /// identically whichever step observed its end. The terminal arm never
    /// touches the group again: the event carries the whole answer, and the
    /// group has already dropped that barrier's state.
    ///
    /// `Granted` is the one event this driver reads further. It is not terminal
    /// — the proof is cached in the group and a later read call consumes it —
    /// and recording it is what lets `drive_pending_reads` attempt exactly the
    /// barriers whose answer can have changed.
    pub(super) fn observe_read_event(&mut self, event: &ReadEvent<G>) {
        if let ReadEvent::Granted { read_id, .. } = event {
            if let Some(waiter) = self.read_waiters.get_mut(read_id) {
                waiter.proof_ready = true;
            }
            return;
        }
        if let Some((read_id, error)) = terminal_read_error(event) {
            self.resolve_read(read_id, Err(error));
        }
    }

    pub(super) fn observe_proposal_event(&mut self, event: &ProposalEvent<A::CommandResult>) {
        let (local_proposal_id, outcome) = match event {
            // A local append is not a terminal outcome and is not recorded.
            // The driver used to keep it as a fate discriminator; it could not
            // be one, because "no append was observed" is not "no append
            // happened", and the fate this driver reports is only ever the one
            // it observed.
            ProposalEvent::Applied {
                local_proposal_id,
                index,
                term,
                result,
            } => (
                *local_proposal_id,
                Ok(WriteReceipt {
                    index: *index,
                    term: *term,
                    result: result.clone(),
                }),
            ),
            ProposalEvent::Rejected {
                local_proposal_id,
                reason,
                leader_hint,
            } => (
                *local_proposal_id,
                Err(write_error_from_rejection(reason.clone(), *leader_hint)),
            ),
            ProposalEvent::UnknownOutcome {
                local_proposal_id,
                client_request_id,
                reason,
            } => {
                let options = self
                    .write_waiters
                    .get(local_proposal_id)
                    .map_or_else(WriteOptions::default, |waiter| waiter.options);
                (
                    *local_proposal_id,
                    Err(WriteError::UnknownOutcome {
                        local_proposal_id: *local_proposal_id,
                        client_request_id: client_request_id.or(options.client_request_id),
                        reason: managed_unknown_reason_from_app(reason),
                    }),
                )
            }
            _ => return,
        };
        self.resolve_write(local_proposal_id, outcome);
    }

    /// Stops waiting for one write and resolves its client.
    ///
    /// Resolving rather than removing: a caller that abandons may still hold the
    /// future, and a future that answered `ManagedInvariantViolation` because its
    /// own caller abandoned it would be a worse answer than the one it asked
    /// for. The slot is freed either way, because `max_pending_waiters` counts
    /// unresolved waiters.
    pub(super) fn abandon_write(&mut self, local_proposal_id: LocalProposalId) -> bool {
        let Some(waiter) = self.write_waiters.get(&local_proposal_id) else {
            return false;
        };
        if waiter.outcome.is_some() {
            return false;
        }
        let client_request_id = waiter.options.client_request_id;
        self.resolve_write(
            local_proposal_id,
            Err(WriteError::UnknownOutcome {
                local_proposal_id,
                client_request_id,
                reason: UnknownOutcomeReason::DriveBoundReached,
            }),
        );
        true
    }

    /// Stops waiting for one read, cancelling its barrier through the group
    /// first so `reserved_reads` returns to its previous value.
    pub(super) fn abandon_read(&mut self, read_id: ReadId) -> bool {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return false;
        };
        if waiter.outcome.is_some() {
            return false;
        }
        if let Some(group) = self.group.as_mut() {
            group.cancel_read(read_id);
        }
        self.resolve_read(
            read_id,
            Err(ReadError::Abandoned {
                read_id,
                reason: ReadAbandonReason::DriveBoundReached,
            }),
        );
        true
    }

    /// Resolves every outstanding waiter as the incarnation lets go of them.
    ///
    /// Writes are unknown rather than refused: a proposal already appended is
    /// still in the durable log and may commit under the next incarnation.
    /// Reads are terminal, and their barriers are cancelled through the group
    /// first so the retired group is quiescent *in reads*. The group's proposal
    /// table is left alone, so the released group is not quiescent in the sense
    /// [`super::TransportRaftDriver::new`] requires.
    pub(super) fn release_waiters(&mut self) {
        let local_proposal_ids = self.write_waiters.keys().copied().collect::<Vec<_>>();
        for local_proposal_id in local_proposal_ids {
            let options = self
                .write_waiters
                .get(&local_proposal_id)
                .map_or_else(WriteOptions::default, |waiter| waiter.options);
            self.resolve_write(
                local_proposal_id,
                Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id: options.client_request_id,
                    reason: UnknownOutcomeReason::DriverReleased,
                }),
            );
        }
        let read_ids = self.read_waiters.keys().copied().collect::<Vec<_>>();
        for read_id in read_ids {
            if let Some(group) = self.group.as_mut() {
                group.cancel_read(read_id);
            }
            self.resolve_read(
                read_id,
                Err(ReadError::Abandoned {
                    read_id,
                    reason: ReadAbandonReason::DriverReleased,
                }),
            );
        }
    }
}
