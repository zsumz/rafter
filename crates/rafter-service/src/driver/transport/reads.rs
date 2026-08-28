#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Starting one read, at either level this driver serves.
//!
//! The two levels part company at the first branch and never rejoin: a
//! linearizable read reserves a barrier some later step resolves, and a local
//! read is answered inside the call that starts it. What they share is the set
//! of refusals that precede both, including the one a local read makes tempting
//! to skip — a replica the cluster is not replicating to has no bounded view to
//! answer from.

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::state::{StartedRead, TransportDriverState};
use super::waiters::ReadWaiter;
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
    pub(super) fn begin_read(
        &mut self,
        group_id: &G,
        query: A::Query,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> Result<StartedRead<G, A::QueryResult>, ReadError> {
        self.reject_read_before_start(group_id)?;
        match consistency {
            // Answered here rather than through a waiter, and before the waiter
            // limit is consulted: a local read registers nothing, so it cannot
            // contribute to the condition that bound would be refusing for.
            ReadConsistency::Local => Ok(StartedRead::Answered(self.read_local(query, options))),
            ReadConsistency::Linearizable => {
                self.begin_barrier(query, options).map(StartedRead::Barrier)
            }
            // Every remaining level, including `LeaseRead`, which the app layer
            // itself refuses. Forwarding it would spend a group step to reach
            // the same answer with a `GroupError` in the middle.
            _ => Err(ReadError::UnsupportedConsistency { consistency }),
        }
    }

    /// Starts a barrier for a caller that will hold its [`ReadId`].
    ///
    /// The same body [`TransportDriverState::begin_read`] runs for
    /// [`ReadConsistency::Linearizable`], reached without a consistency argument
    /// because a caller that wants the ID wants the level that has one.
    pub(super) fn begin_linearizable_read(
        &mut self,
        group_id: &G,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<ReadId, ReadError> {
        self.reject_read_before_start(group_id)?;
        self.begin_barrier(query, options)
    }

    /// The refusals that precede every read, whatever level it asked for.
    ///
    /// A released group is refused here rather than inside the linearizable
    /// branch for the reason the write side gives: no barrier was reserved, so
    /// there is no `ReadId` to abandon and `ReadId(0)` named one that never
    /// existed. It covers a local read too, which has no state machine to read
    /// either.
    ///
    /// The service-state refusal covers both consistency levels for a reason a
    /// local read makes tempting to skip. A local read answers from this
    /// replica's own applied state and proves nothing about any other, so it
    /// looks harmless — but a decommissioned replica, and a rolled-back one that
    /// no configuration names, are both replicas the cluster has stopped
    /// replicating to. Answering from either is answering from a snapshot of the
    /// past with no bound on how old, and serving it would be the one way a
    /// client could not tell such a replica from a live one.
    fn reject_read_before_start(&self, group_id: &G) -> Result<(), ReadError> {
        if self.shutting_down {
            return Err(ReadError::ShuttingDown);
        }
        if group_id != &self.group_id {
            return Err(ReadError::WrongGroup);
        }
        if let Err(reason) = self.reject_if_not_serving() {
            return Err(ReadError::Unavailable { reason });
        }
        Ok(())
    }

    fn begin_barrier(
        &mut self,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<ReadId, ReadError> {
        let unresolved = self
            .read_waiters
            .values()
            .filter(|waiter| waiter.outcome.is_none())
            .count();
        if unresolved >= self.options.max_pending_waiters {
            return Err(ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::PendingWaiterLimit {
                    max_pending_waiters: self.options.max_pending_waiters,
                }),
            });
        }
        let next = self.next_read_id.ok_or(ReadError::ReadIdExhausted)?;
        self.next_read_id = next.checked_add(1);
        let read_id = ReadId(next);
        // Registered before the barrier starts, for the reason a write waiter
        // is: a terminal event emitted inside the very step that starts the
        // barrier must find a waiter listening. The request is stored whole
        // because `drive_pending_reads` retries with it: the app layer refuses
        // a retry whose freshness or context moved, so the caller's floor has
        // to survive on the waiter rather than be rebuilt per attempt.
        self.read_waiters.insert(
            read_id,
            ReadWaiter {
                request: ReadRequest::Linearizable {
                    group_id: self.group_id.clone(),
                    read_id,
                    query,
                    min_applied_index: options.min_applied_index,
                    context: Vec::new(),
                },
                proof_ready: false,
                outcome: None,
                waker: None,
            },
        );
        if let Err(error) = self.attempt_read(read_id) {
            self.resolve_read(
                read_id,
                Err(ReadError::Transport {
                    cause: ErrorCause::new(error),
                }),
            );
        }
        self.publish_metrics();
        Ok(read_id)
    }

    /// Answers one read from this replica's own applied state.
    ///
    /// [`RaftGroup::read`] is the whole implementation, and going through it
    /// rather than around it is the point: it refuses a poisoned group and a
    /// state machine below the runtime's snapshot boundary before it reads
    /// anything, and it honors the caller's `min_applied_index` verbatim. A
    /// projection taken through [`TransportRaftDriver::with_group`] gets none of
    /// those.
    ///
    /// The report is routed even though a local read never steps the runtime and
    /// the report is therefore empty for this group. Routing it unconditionally
    /// keeps this path from being the one exception to the driver's discipline,
    /// and an empty report costs a walk over five empty lists.
    fn read_local(
        &mut self,
        query: A::Query,
        options: ReadOptions,
    ) -> Result<QueryReceipt<G, A::QueryResult>, ReadError> {
        let request = ReadRequest::Local {
            group_id: self.group_id.clone(),
            query,
            min_applied_index: options.min_applied_index,
        };
        // `reject_read_before_start` already refused a released driver, so this
        // holds a group. Mapped rather than unwrapped because the caller gets a
        // typed refusal either way and a panic here would be the driver's own
        // invariant, not the caller's fault.
        let read = self.group_mut().map_err(|_| ReadError::Unavailable {
            reason: DriverUnavailableReason::Released,
        })?;
        let answered = match read.read(request) {
            Ok(read) => {
                self.route_report(read.report);
                local_read_outcome(read.outcome)
            }
            Err(error) => Err(read_error_from_group(error)),
        };
        // The drain runs whichever way the read went, for the reason
        // `attempt_read` gives: a poison captured during the call hands this
        // driver waiters it must resolve, and this read owns none of them.
        self.drain_poisoned_waiters();
        self.publish_metrics();
        answered
    }
}

/// Maps the two outcomes a local read can produce.
///
/// It produces exactly two. `RaftGroup::read_local` returns `Ready` or
/// `LocalFreshnessUnavailable` and reaches no other arm, because it reserves no
/// barrier: there is nothing to leave pending, reject, or cancel. The catch-all
/// is therefore an invariant violation rather than a case, and it says so in the
/// same vocabulary the barrier path uses.
fn local_read_outcome<G, QR>(
    outcome: ReadOutcome<G, QR>,
) -> Result<QueryReceipt<G, QR>, ReadError> {
    match outcome {
        ReadOutcome::Ready { result, proof } => Ok(QueryReceipt { result, proof }),
        ReadOutcome::LocalFreshnessUnavailable {
            required_applied_index,
            local_applied_index,
        } => Err(ReadError::FreshnessUnavailable {
            read_id: None,
            required_applied_index,
            local_applied_index,
        }),
        _ => Err(ReadError::ManagedInvariantViolation {
            message: "managed driver received unsupported app-layer read outcome variant"
                .to_owned(),
        }),
    }
}
