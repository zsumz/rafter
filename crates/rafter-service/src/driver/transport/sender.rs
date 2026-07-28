#![allow(clippy::wildcard_imports)]

//! This driver's [`DriverCommandSender`] surface.
//!
//! Split from [`super`] along the line between the driver's *own* API and the
//! trait every driver in this crate implements. That file is the lifecycle and
//! the operations only a transport driver has — construction over a recovered
//! checkpoint, adoption, release, `tick`, `deliver`, and the two `begin_*` calls
//! that return the ID they allocated. This one is the surface a
//! [`crate::RaftHandle`] reaches through, which `InMemoryRaftDriver` implements
//! too and which says nothing about transports at all.
//!
//! Nothing here holds a rule of its own: every method either forwards to the
//! same state the file above drives, or resolves a client future the same two
//! builders make.

use std::future::ready;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::state::{StartedRead, StepFailure};
use super::TransportRaftDriver;

impl<G, A, R, T, V> DriverCommandSender<G, A::Command, A::Query, A::CommandResult, A::QueryResult>
    for TransportRaftDriver<G, A, R, T, V>
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
    fn write(
        &self,
        group_id: G,
        command: A::Command,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        // Registered synchronously, polled later: the waiter exists before the
        // group is stepped, so a terminal event emitted inside that very step
        // resolves it rather than arriving before anything is listening.
        let started = self.inner.lock().begin_write(&group_id, command, options);
        match started {
            Ok(local_proposal_id) => self.write_future(local_proposal_id),
            Err(error) => Box::pin(ready(Err(error))),
        }
    }

    fn read(
        &self,
        group_id: G,
        query: A::Query,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> DriverFuture<Result<QueryReceipt<G, A::QueryResult>, ReadError>> {
        let started = self
            .inner
            .lock()
            .begin_read(&group_id, query, consistency, options);
        match started {
            Ok(StartedRead::Barrier(read_id)) => self.barrier_future(read_id),
            // A local read is already finished. No waiter was registered, so
            // there is no guard to hold and nothing to poll.
            Ok(StartedRead::Answered(answered)) => Box::pin(ready(answered)),
            Err(error) => Box::pin(ready(Err(error))),
        }
    }

    fn transfer_leadership(
        &self,
        group_id: G,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock();
            if state.shutting_down {
                return Err(TransferLeadershipError::ShuttingDown);
            }
            if group_id != state.group_id {
                return Err(TransferLeadershipError::WrongGroup);
            }
            // Through the state's own stepping path, not around it: a transfer
            // step can commit, apply, and poison, and the drain that resolves
            // what a poison captured runs there on both paths.
            let rejection = state
                .step_transfer(target)
                .map_err(|failure| match failure {
                    StepFailure::NoGroup => TransferLeadershipError::Transport {
                        cause: ErrorCause::new(ManagedDriverError::NoGroup),
                    },
                    StepFailure::Group(error) => transfer_error_from_group(error),
                })?;
            rejection.map_or(Ok(()), Err)
        })
    }

    fn metrics(&self, group_id: G) -> Result<MetricsWatch<G>, MetricsError> {
        let state = self.inner.lock();
        if group_id != state.group_id {
            return Err(MetricsError::WrongGroup);
        }
        Ok(state.metrics.watch())
    }

    fn shutdown(&self, group_id: G) -> DriverFuture<Result<(), ShutdownError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock();
            if group_id != state.group_id {
                return Err(ShutdownError::WrongGroup);
            }
            if state.shutting_down {
                return Err(ShutdownError::AlreadyShutDown);
            }
            state.shutting_down = true;
            state.release_waiters();
            state.metrics.close();
            Ok(())
        })
    }
}
