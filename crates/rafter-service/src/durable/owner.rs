//! The backend remains uniquely owned until all terminal client decisions.

use super::{shared::Shared, DurableWriteLimits, DurableWriteSender};
use rafter::{Input, LogIndex, NodeConfig, SnapshotChunkSource};
use rafter_runtime::{
    application::{ApplicationEntry, DurableApplication},
    service::{
        CommittedCommand, DurableService, ServiceAdapterError, ServiceError, ServiceInput,
        ServiceOptions, ServicePoll, ServiceState, ServiceStepError,
    },
    DurableRaftNode, DurableRaftNodeStorage,
};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::{fmt, sync::Arc};

mod construction;
mod dispatch;
mod lifecycle;
mod reads;

use super::{read::ReadShared, DurableReadSender};

type Query<A, R> = Box<dyn Fn(&A, &[u8]) -> R + Send>;
struct Reads<A, R> {
    shared: Arc<ReadShared<R>>,
    query: Query<A, R>,
}

/// Owns the durable backend and its bounded write and optional read clients.
///
/// Construct a fresh incarnation through `recover`; there is no adoption of a
/// running backend whose local proposal IDs could collide. Cloned senders do
/// not keep the backend alive. On drop, workers and protocol state are released
/// before the remaining waiters receive terminal outcomes.
pub struct DurableClientOwner<
    H,
    L,
    S,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    R: Send + 'static = (),
> {
    service: Option<DurableService<H, L, S, T, A>>,
    shared: Arc<Shared<A::Outcome>>,
    options: ServiceOptions,
    failed: bool,
    reads: Option<Reads<A, R>>,
    prefer_reads: bool,
}

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Returns a cloneable encoded-write endpoint for this incarnation.
    #[must_use]
    pub fn sender(&self) -> DurableWriteSender<A::Outcome> {
        DurableWriteSender {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Returns the read endpoint when configured by `recover_with_reads`.
    #[must_use]
    pub fn read_sender(&self) -> Option<DurableReadSender<R>> {
        self.reads.as_ref().map(|reads| DurableReadSender {
            shared: Arc::clone(&reads.shared),
        })
    }

    /// Returns current backend readiness, or `Stopped` after shutdown.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        if self.failed {
            return ServiceState::Failed;
        }
        self.service
            .as_ref()
            .map_or(ServiceState::Stopped, DurableService::state)
    }

    /// Inspects the runtime or reads stored snapshot chunks when available.
    #[must_use]
    pub fn ready_node(&self) -> Option<&DurableRaftNode<H, L, S>> {
        self.service.as_ref().and_then(DurableService::ready_node)
    }

    /// Returns the verified application durable floor when its store is owned.
    #[must_use]
    pub fn durable_through(&self) -> Option<LogIndex> {
        self.service
            .as_ref()
            .and_then(DurableService::durable_through)
    }

    /// Admits clock, authenticated peer, and control inputs under the existing
    /// service durability contract. Peer authentication and group routing stay
    /// with the embedding. Use the sender for all local proposals and the read
    /// sender for all read barriers when managed reads are configured.
    /// # Errors
    /// Returns unmanaged proposal/read inputs unchanged, or the service's refusal
    /// or fatal error. On a fatal error, discard this owner and recover.
    pub fn try_step(&mut self, input: Input) -> Result<(), DurableClientStepError> {
        if self.failed {
            return Err(DurableClientStepError::Service(ServiceStepError::Fatal(
                ServiceError::Failed,
            )));
        }
        if matches!(
            input,
            Input::ClientProposal { .. } | Input::TrackedClientProposal { .. }
        ) {
            return Err(DurableClientStepError::UnmanagedProposal(Box::new(input)));
        }
        if self.reads.is_some() && matches!(input, Input::ReadIndex { .. }) {
            return Err(DurableClientStepError::UnmanagedRead(Box::new(input)));
        }
        let input = ServiceInput::from(input);
        let Some(service) = self.service.as_mut() else {
            return Err(DurableClientStepError::Service(
                ServiceStepError::Unavailable {
                    state: ServiceState::Stopped,
                    input,
                },
            ));
        };
        let result = service.try_step(input);
        if matches!(result, Err(ServiceStepError::Fatal(_))) {
            self.close_clients();
        }
        result.map_err(DurableClientStepError::Service)
    }

    /// Returns an installed application store after an `InstallSnapshot` event.
    /// # Errors
    /// Preserves the underlying service's exact snapshot-floor checks.
    pub fn resume_snapshot(&mut self, application: A) -> Result<(), ServiceError> {
        self.service
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?
            .resume_snapshot(application)
    }

    /// Closes new client and consensus admission. Poll existing work and finish
    /// any snapshot handoff before attempting `finish_shutdown`.
    pub fn begin_shutdown(&mut self) {
        self.close_clients();
        if let Some(service) = self.service.as_mut() {
            service.begin_shutdown();
        }
    }

    /// Joins drained workers, releases the backend, and resolves any remaining
    /// writes with proven refusal or unknown outcome before returning the store.
    /// # Errors
    /// Returns `NotReady` while owned work remains; continue polling. Fatal
    /// failures require discarding this owner and reopening the stores.
    pub fn finish_shutdown(&mut self) -> Result<A, ServiceError> {
        if self.failed {
            return Err(ServiceError::Failed);
        }
        let application = self
            .service
            .as_mut()
            .ok_or(ServiceError::NotReady(ServiceState::Stopped))?
            .finish_shutdown()?;
        self.close_clients();
        drop(self.service.take());
        self.terminate_clients();
        Ok(application)
    }
}

/// A caller bypassed managed proposal admission, or the backend refused input.
#[derive(Debug)]
#[non_exhaustive]
pub enum DurableClientStepError {
    /// Use the sender; raw proposals could collide with this owner's IDs.
    UnmanagedProposal(Box<Input>),
    /// Use the read sender; raw read IDs could collide with managed barriers.
    UnmanagedRead(Box<Input>),
    /// The underlying service's unchanged refusal or fatal failure.
    Service(ServiceStepError),
}
impl fmt::Display for DurableClientStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable client input: {self:?}")
    }
}
impl std::error::Error for DurableClientStepError {}
