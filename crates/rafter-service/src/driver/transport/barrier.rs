#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Finishing a linearizable read.
//!
//! A grant is announced by a routed event, and the proof it announces is
//! consumed by a read call that runs the state machine — which this driver will
//! not do inside a tick the embedder asked for on its own timer. So collecting
//! granted barriers is its own pass, and one barrier's fault never denies
//! service to the rest.

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
    /// Collects every barrier whose proof a routed grant said is ready.
    ///
    /// Barriers still waiting on a quorum round, and granted barriers this
    /// replica has not applied through, are left alone. They cannot answer
    /// differently until the group emits a [`ReadEvent::Granted`] for them, and
    /// attempting them anyway would spin: a read against a barrier the group
    /// already tracks returns an unstepped report.
    ///
    /// One barrier's fault never denies service to the rest. Every group error a
    /// read call can raise is about the one `ReadId` that call named, so it
    /// resolves that barrier's client and the pass continues; nothing but a
    /// released group leaves this method as an error.
    pub(super) fn drive_pending_reads(&mut self) -> Result<(), ManagedDriverError> {
        if self.group.is_none() {
            return Err(ManagedDriverError::NoGroup);
        }
        let ready = self
            .read_waiters
            .iter()
            .filter(|(_, waiter)| waiter.outcome.is_none() && waiter.proof_ready)
            .map(|(read_id, _)| *read_id)
            .collect::<Vec<_>>();
        for read_id in ready {
            self.attempt_read(read_id)?;
        }
        self.publish_metrics();
        Ok(())
    }

    /// Runs one read call for one barrier and resolves whatever it produced.
    ///
    /// This both starts a barrier, when [`TransportDriverState::begin_read`]
    /// calls it, and consumes a granted one afterwards. Only the first of those
    /// steps the group.
    ///
    /// A failure is the barrier's own. `RaftGroup::read` is called with one
    /// request naming one `ReadId`, so there is no second barrier the error
    /// could be about, and it reaches the client through the same category
    /// mapping `InMemoryRaftDriver` uses: a state machine that refuses a query
    /// is reported as a state-machine failure with its own error preserved, not
    /// as a driver invariant violation naming a routing defect that did not
    /// occur. The two ID variants still map to that violation, because for them
    /// it is what happened.
    pub(super) fn attempt_read(&mut self, read_id: ReadId) -> Result<(), ManagedDriverError> {
        let Some(waiter) = self.read_waiters.get(&read_id) else {
            return Ok(());
        };
        if waiter.outcome.is_some() {
            return Ok(());
        }
        let request = waiter.request.clone();
        let read = self.group_mut()?.read(request);
        match read {
            Ok(read) => {
                self.route_report(read.report);
                self.reconcile_membership();
                self.drain_poisoned_waiters();
                self.handle_read_outcome(read_id, read.outcome);
            }
            Err(error) => {
                // A read that starts a barrier steps the runtime, so it reaches
                // this driver's membership reconciliation like any other step.
                self.reconcile_membership();
                // The drain runs before the resolution so a barrier the group
                // handed over keeps the poison's own answer; `resolve_read`
                // keeps the first outcome either way, and both arms say
                // `Poisoned`.
                self.drain_poisoned_waiters();
                self.resolve_read(read_id, Err(read_error_from_group(error)));
            }
        }
        Ok(())
    }

    /// Resolves one barrier's outcome, or leaves it waiting for a grant.
    pub(super) fn handle_read_outcome(
        &mut self,
        read_id: ReadId,
        outcome: ReadOutcome<G, A::QueryResult>,
    ) {
        match outcome {
            ReadOutcome::Ready { result, proof } => {
                self.resolve_read(read_id, Ok(QueryReceipt { result, proof }));
            }
            // Both are waits rather than retries. The quorum round needs an
            // inbound frame only `deliver` can bring, and a granted barrier
            // ahead of this replica's applied index needs an apply; either way
            // the group announces the change with a `ReadEvent::Granted` that
            // `route_report` records.
            ReadOutcome::Pending { .. } | ReadOutcome::LinearizableFreshnessUnavailable { .. } => {}
            ReadOutcome::Rejected {
                read_id: rejected,
                reason,
                leader_hint,
            } => self.resolve_read(
                read_id,
                Err(ReadError::Rejected {
                    read_id: Some(rejected),
                    reason,
                    leader_hint,
                }),
            ),
            ReadOutcome::Canceled {
                read_id: canceled,
                reason,
                leader_hint,
            } => self.resolve_read(
                read_id,
                Err(ReadError::Canceled {
                    read_id: canceled,
                    reason,
                    leader_hint,
                }),
            ),
            ReadOutcome::LocalFreshnessUnavailable {
                required_applied_index,
                local_applied_index,
            } => self.resolve_read(
                read_id,
                Err(ReadError::FreshnessUnavailable {
                    read_id: None,
                    required_applied_index,
                    local_applied_index,
                }),
            ),
            _ => self.resolve_read(
                read_id,
                Err(ReadError::ManagedInvariantViolation {
                    message: "managed driver received unsupported app-layer read outcome variant"
                        .to_owned(),
                }),
            ),
        }
    }
}
